//! Multi-Verse Scripture node shell over independent [`VerseRuntime`]s.
//!
//! Starts every configured Verse independently and reports per-line outcomes.
//! Does not invent discovery, polling, peer RPC, or a configuration store.

use std::collections::BTreeMap;

use holylog::virtual_log::VirtualLog;
use scripture::{Clock, JournalId, ReceiptFuture, Submission, Timer, VerseId};

use crate::canon_route::{CanonRoute, CanonRouteError};
use crate::canon_transition::CanonTransitionOutcome;
use crate::verse_runtime::{
    VerseAdmitError, VerseHandoffError, VerseHandoffRequest, VerseRuntime, VerseRuntimeConfig,
    VerseRuntimeStartError,
};

/// Stable key for one configured Verse inside a Scripture node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VerseKey {
    /// Logical Scripture journal.
    pub journal_id: JournalId,
    /// Physical Verse.
    pub verse_id: VerseId,
}

impl VerseKey {
    /// Builds a key from journal and Verse identities.
    #[must_use]
    pub const fn new(journal_id: JournalId, verse_id: VerseId) -> Self {
        Self {
            journal_id,
            verse_id,
        }
    }

    pub fn from_config(config: &VerseRuntimeConfig) -> Self {
        Self::new(config.journal_id, config.verse_id)
    }
}

/// Aggregate result of starting every configured Verse.
#[derive(Debug)]
pub struct ScriptureNodeStart {
    /// Successful runtimes keyed by journal/line.
    pub runtimes: BTreeMap<VerseKey, VerseRuntime>,
    /// Per-line failures that did not produce a runtime.
    pub failures: BTreeMap<VerseKey, VerseRuntimeStartError>,
}

impl ScriptureNodeStart {
    /// True when every configured Verse produced a runtime.
    #[must_use]
    pub fn all_started(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Failures that refuse to begin any Verse startup.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScriptureNodeConfigError {
    /// Duplicate journal/line keys in the configuration list.
    #[error("duplicate Verse configuration for journal/line key")]
    DuplicateVerse {
        /// Conflicting key.
        key: VerseKey,
    },
}

/// Lookups and admission against a started Scripture node.
#[derive(Debug, thiserror::Error)]
pub enum ScriptureNodeError {
    /// Requested Verse was not among the started runtimes.
    #[error("no started Verse runtime for the requested journal/line")]
    UnknownVerse {
        /// Missing key.
        key: VerseKey,
    },
    /// Route resolution failed.
    #[error(transparent)]
    Route(#[from] CanonRouteError),
    /// Admission failed.
    #[error(transparent)]
    Admit(#[from] VerseAdmitError),
}

/// Handoff failures at the node shell boundary.
#[derive(Debug, thiserror::Error)]
pub enum ScriptureNodeHandoffError {
    /// Requested Verse was not among the started runtimes.
    #[error("no started Verse runtime for the requested journal/line")]
    UnknownVerse {
        /// Missing key.
        key: VerseKey,
    },
    /// Handoff failure. The runtime remains in the node unchanged for a
    /// precondition error and terminal/non-serving after ownership was taken.
    #[error(transparent)]
    Handoff(#[from] VerseHandoffError),
}

/// Collection of independent Verse runtimes owned by one Scripture process.
pub struct ScriptureNode {
    verses: BTreeMap<VerseKey, VerseRuntime>,
}

impl ScriptureNode {
    /// Starts every Verse independently after rejecting duplicate keys.
    ///
    /// `virtual_log_for` must return the VirtualLog bound to that Verse's
    /// ConditionalRegister. One bad Verse does not invent a serving state for
    /// another; failures are reported alongside successes.
    pub async fn start<C, T, F>(
        configs: Vec<VerseRuntimeConfig>,
        mut virtual_log_for: F,
        clock: C,
        timer: T,
    ) -> Result<ScriptureNodeStart, ScriptureNodeConfigError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
        F: FnMut(&VerseRuntimeConfig) -> VirtualLog,
    {
        let mut seen = BTreeMap::<VerseKey, ()>::new();
        for config in &configs {
            let key = VerseKey::from_config(config);
            if seen.insert(key, ()).is_some() {
                return Err(ScriptureNodeConfigError::DuplicateVerse { key });
            }
        }

        let mut runtimes = BTreeMap::new();
        let mut failures = BTreeMap::new();
        for config in configs {
            let key = VerseKey::from_config(&config);
            let virtual_log = virtual_log_for(&config);
            match VerseRuntime::start(config, virtual_log, clock.clone(), timer.clone()).await {
                Ok(runtime) => {
                    runtimes.insert(key, runtime);
                }
                Err(error) => {
                    failures.insert(key, error);
                }
            }
        }
        Ok(ScriptureNodeStart { runtimes, failures })
    }

    /// Builds a node from an already-partitioned start report's successes.
    #[must_use]
    pub fn from_started(runtimes: BTreeMap<VerseKey, VerseRuntime>) -> Self {
        Self { verses: runtimes }
    }

    /// Number of started Verse runtimes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.verses.len()
    }

    /// True when no Verse runtimes are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.verses.is_empty()
    }

    /// Borrow one started Verse runtime.
    pub fn verse(&self, key: VerseKey) -> Result<&VerseRuntime, ScriptureNodeError> {
        self.verses
            .get(&key)
            .ok_or(ScriptureNodeError::UnknownVerse { key })
    }

    /// Fresh route resolution for one configured Verse.
    pub async fn resolve_route(&self, key: VerseKey) -> Result<CanonRoute, ScriptureNodeError> {
        Ok(self.verse(key)?.resolve_route().await?)
    }

    /// Admit work only to a locally serving Verse.
    pub async fn submit(
        &self,
        key: VerseKey,
        submission: Submission,
    ) -> Result<ReceiptFuture, ScriptureNodeError> {
        Ok(self.verse(key)?.submit(submission).await?)
    }

    /// Flush one locally serving Verse.
    pub async fn flush(&self, key: VerseKey) -> Result<(), ScriptureNodeError> {
        Ok(self.verse(key)?.flush().await?)
    }

    /// Consuming handoff for one serving Verse.
    ///
    /// On success or post-take failure the Verse remains in the node as a
    /// terminal runtime. Pre-drain rejects restore the prior runtime and
    /// return [`ScriptureNodeHandoffError::Handoff`].
    pub async fn drain_seal_publish(
        &mut self,
        key: VerseKey,
        request: VerseHandoffRequest,
    ) -> Result<CanonTransitionOutcome, ScriptureNodeHandoffError> {
        let runtime = self
            .verses
            .remove(&key)
            .ok_or(ScriptureNodeHandoffError::UnknownVerse { key })?;
        match runtime.drain_seal_publish(request).await {
            Ok((runtime, outcome)) => {
                self.verses.insert(key, runtime);
                Ok(outcome)
            }
            Err(reject) => {
                let restored =
                    VerseKey::new(reject.runtime.journal_id(), reject.runtime.verse_id());
                self.verses.insert(restored, reject.runtime);
                Err(ScriptureNodeHandoffError::Handoff(reject.error))
            }
        }
    }
}

impl std::fmt::Debug for ScriptureNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScriptureNode")
            .field("verses", &self.verses.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use holylog::virtual_log::InMemoryConditionalRegister;
    use scripture::{
        CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId,
        RecoveryBound, SystemClock, VerseId, WriterId,
    };

    use super::{ScriptureNode, ScriptureNodeConfigError, VerseKey};
    use crate::canon_route::CanonRoute;
    use crate::verse_runtime::VerseRuntimeConfig;
    use crate::virtuallog_test_support::VirtualLogHarness;

    fn journal_a() -> JournalId {
        JournalId::from_bytes(*b"node-journal-a!!")
    }

    fn journal_b() -> JournalId {
        JournalId::from_bytes(*b"node-journal-b!!")
    }

    fn verse_a() -> VerseId {
        VerseId::from_bytes(*b"node-line-a!!!!!")
    }

    fn verse_b() -> VerseId {
        VerseId::from_bytes(*b"node-line-b!!!!!")
    }

    fn owner() -> OwnerId {
        OwnerId::from_bytes(*b"node-shell-ownr!")
    }

    fn other() -> OwnerId {
        OwnerId::from_bytes(*b"node-shell-othr!")
    }

    fn config(journal: JournalId, verse: VerseId, owner: OwnerId) -> VerseRuntimeConfig {
        VerseRuntimeConfig {
            journal_id: journal,
            verse_id: verse,
            owner_id: owner,
            cohort_id: CohortId::from_bytes(*b"node-shell-cohr!"),
            writer_id: WriterId::from_bytes(*b"node-shell-wrtr!"),
            policy: ChunkPolicy {
                max_chunk_bytes: 64 * 1024,
                max_record_bytes: 16 * 1024,
                max_chunk_records: 8,
                max_chunk_age: Duration::from_secs(60),
                max_buffered_bytes: 64 * 1024,
                max_inflight_chunks: 1,
                max_uncommitted_age: Duration::from_secs(60),
                recovery_scan: RecoveryBound::new(8).expect("bound"),
            },
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 16,
            dataref_blobs: None,
        }
    }

    fn fence(journal: JournalId, line: VerseId, revision: u64, owner: OwnerId) -> CanonFence {
        let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
        CanonFence::new(
            revision,
            journal,
            line,
            CanonOwner::Owned {
                owner_id: owner,
                endpoint,
                sequencer: None,
                writer_term: None,
            },
        )
    }

    async fn line_harness(name: &str) -> VirtualLogHarness {
        VirtualLogHarness::with_ids(
            name,
            &format!("{name}-second"),
            &format!("{name}-third"),
            Arc::new(InMemoryConditionalRegister::new()),
        )
        .await
    }

    #[tokio::test]
    async fn serving_and_standby_verses_are_independent() {
        let serve = line_harness("shell-serve").await;
        let standby = line_harness("shell-standby").await;
        serve
            .bootstrap_first(fence(journal_a(), verse_a(), 0, owner()).encode())
            .await;
        standby
            .bootstrap_first(fence(journal_b(), verse_b(), 0, other()).encode())
            .await;

        let logs = BTreeMap::from([
            (VerseKey::new(journal_a(), verse_a()), serve.virtual_log()),
            (VerseKey::new(journal_b(), verse_b()), standby.virtual_log()),
        ]);
        let started = ScriptureNode::start(
            vec![
                config(journal_a(), verse_a(), owner()),
                config(journal_b(), verse_b(), owner()),
            ],
            |cfg| logs.get(&VerseKey::from_config(cfg)).expect("log").clone(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        assert!(started.all_started());
        let node = ScriptureNode::from_started(started.runtimes);
        let serve_key = VerseKey::new(journal_a(), verse_a());
        let standby_key = VerseKey::new(journal_b(), verse_b());
        assert!(node.verse(serve_key).expect("serve").is_serving());
        assert!(node.verse(standby_key).expect("standby").is_standby());
        assert!(matches!(
            node.resolve_route(standby_key).await.expect("route"),
            CanonRoute::NotOwner { .. }
        ));
    }

    #[tokio::test]
    async fn duplicate_config_rejects_before_any_start() {
        let err = ScriptureNode::start(
            vec![
                config(journal_a(), verse_a(), owner()),
                config(journal_a(), verse_a(), owner()),
            ],
            |_| panic!("virtual_log_for must not run on duplicate config"),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect_err("duplicate");
        assert!(matches!(
            err,
            ScriptureNodeConfigError::DuplicateVerse { key }
            if key == VerseKey::new(journal_a(), verse_a())
        ));
    }

    #[tokio::test]
    async fn bad_verse_reports_failure_while_valid_verse_starts() {
        let good = line_harness("shell-good").await;
        good.bootstrap_first(fence(journal_a(), verse_a(), 0, owner()).encode())
            .await;
        let bad = line_harness("shell-bad").await;
        // Uninitialized register ⇒ typed startup error.
        let logs = BTreeMap::from([
            (VerseKey::new(journal_a(), verse_a()), good.virtual_log()),
            (VerseKey::new(journal_b(), verse_b()), bad.virtual_log()),
        ]);
        let started = ScriptureNode::start(
            vec![
                config(journal_a(), verse_a(), owner()),
                config(journal_b(), verse_b(), owner()),
            ],
            |cfg| logs.get(&VerseKey::from_config(cfg)).expect("log").clone(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        assert_eq!(started.runtimes.len(), 1);
        assert_eq!(started.failures.len(), 1);
        assert!(
            started
                .runtimes
                .get(&VerseKey::new(journal_a(), verse_a()))
                .expect("good")
                .is_serving()
        );
        assert!(
            started
                .failures
                .contains_key(&VerseKey::new(journal_b(), verse_b()))
        );
    }
}
