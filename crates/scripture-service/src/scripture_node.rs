//! Multi-Line Scripture node shell over independent [`LineRuntime`]s.
//!
//! Starts every configured Line independently and reports per-line outcomes.
//! Does not invent discovery, polling, peer RPC, or a configuration store.

use std::collections::BTreeMap;

use holylog::virtual_log::VirtualLog;
use scripture::{Clock, JournalId, LineId, ReceiptFuture, Submission, Timer};

use crate::canon_route::{CanonRoute, CanonRouteError};
use crate::canon_transition::CanonTransitionOutcome;
use crate::line_runtime::{
    LineAdmitError, LineHandoffError, LineHandoffRequest, LineRuntime, LineRuntimeConfig,
    LineRuntimeStartError,
};

/// Stable key for one configured Line inside a Scripture node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LineKey {
    /// Logical Scripture journal.
    pub journal_id: JournalId,
    /// Physical Line.
    pub line_id: LineId,
}

impl LineKey {
    /// Builds a key from journal and Line identities.
    #[must_use]
    pub const fn new(journal_id: JournalId, line_id: LineId) -> Self {
        Self {
            journal_id,
            line_id,
        }
    }

    pub fn from_config(config: &LineRuntimeConfig) -> Self {
        Self::new(config.journal_id, config.line_id)
    }
}

/// Aggregate result of starting every configured Line.
#[derive(Debug)]
pub struct ScriptureNodeStart {
    /// Successful runtimes keyed by journal/line.
    pub runtimes: BTreeMap<LineKey, LineRuntime>,
    /// Per-line failures that did not produce a runtime.
    pub failures: BTreeMap<LineKey, LineRuntimeStartError>,
}

impl ScriptureNodeStart {
    /// True when every configured Line produced a runtime.
    #[must_use]
    pub fn all_started(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Failures that refuse to begin any Line startup.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScriptureNodeConfigError {
    /// Duplicate journal/line keys in the configuration list.
    #[error("duplicate Line configuration for journal/line key")]
    DuplicateLine {
        /// Conflicting key.
        key: LineKey,
    },
}

/// Lookups and admission against a started Scripture node.
#[derive(Debug, thiserror::Error)]
pub enum ScriptureNodeError {
    /// Requested Line was not among the started runtimes.
    #[error("no started Line runtime for the requested journal/line")]
    UnknownLine {
        /// Missing key.
        key: LineKey,
    },
    /// Route resolution failed.
    #[error(transparent)]
    Route(#[from] CanonRouteError),
    /// Admission failed.
    #[error(transparent)]
    Admit(#[from] LineAdmitError),
}

/// Handoff failures at the node shell boundary.
#[derive(Debug, thiserror::Error)]
pub enum ScriptureNodeHandoffError {
    /// Requested Line was not among the started runtimes.
    #[error("no started Line runtime for the requested journal/line")]
    UnknownLine {
        /// Missing key.
        key: LineKey,
    },
    /// Pre-drain or mid-handoff rejection; runtime remains in the node on
    /// pre-drain identity/phase errors, and is terminal on post-take failures.
    #[error(transparent)]
    Handoff(#[from] LineHandoffError),
}

/// Collection of independent Line runtimes owned by one Scripture process.
pub struct ScriptureNode {
    lines: BTreeMap<LineKey, LineRuntime>,
}

impl ScriptureNode {
    /// Starts every Line independently after rejecting duplicate keys.
    ///
    /// `virtual_log_for` must return the VirtualLog bound to that Line's
    /// ConditionalRegister. One bad Line does not invent a serving state for
    /// another; failures are reported alongside successes.
    pub async fn start<C, T, F>(
        configs: Vec<LineRuntimeConfig>,
        mut virtual_log_for: F,
        clock: C,
        timer: T,
    ) -> Result<ScriptureNodeStart, ScriptureNodeConfigError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
        F: FnMut(&LineRuntimeConfig) -> VirtualLog,
    {
        let mut seen = BTreeMap::<LineKey, ()>::new();
        for config in &configs {
            let key = LineKey::from_config(config);
            if seen.insert(key, ()).is_some() {
                return Err(ScriptureNodeConfigError::DuplicateLine { key });
            }
        }

        let mut runtimes = BTreeMap::new();
        let mut failures = BTreeMap::new();
        for config in configs {
            let key = LineKey::from_config(&config);
            let virtual_log = virtual_log_for(&config);
            match LineRuntime::start(config, virtual_log, clock.clone(), timer.clone()).await {
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
    pub fn from_started(runtimes: BTreeMap<LineKey, LineRuntime>) -> Self {
        Self { lines: runtimes }
    }

    /// Number of started Line runtimes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// True when no Line runtimes are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// Borrow one started Line runtime.
    pub fn line(&self, key: LineKey) -> Result<&LineRuntime, ScriptureNodeError> {
        self.lines
            .get(&key)
            .ok_or(ScriptureNodeError::UnknownLine { key })
    }

    /// Fresh route resolution for one configured Line.
    pub async fn resolve_route(&self, key: LineKey) -> Result<CanonRoute, ScriptureNodeError> {
        Ok(self.line(key)?.resolve_route().await?)
    }

    /// Admit work only to a locally serving Line.
    pub async fn submit(
        &self,
        key: LineKey,
        submission: Submission,
    ) -> Result<ReceiptFuture, ScriptureNodeError> {
        Ok(self.line(key)?.submit(submission).await?)
    }

    /// Flush one locally serving Line.
    pub async fn flush(&self, key: LineKey) -> Result<(), ScriptureNodeError> {
        Ok(self.line(key)?.flush().await?)
    }

    /// Consuming handoff for one serving Line.
    ///
    /// On success or post-take failure the Line remains in the node as a
    /// terminal runtime. Pre-drain rejects restore the prior runtime and
    /// return [`ScriptureNodeHandoffError::Handoff`].
    pub async fn drain_seal_publish(
        &mut self,
        key: LineKey,
        request: LineHandoffRequest,
    ) -> Result<CanonTransitionOutcome, ScriptureNodeHandoffError> {
        let runtime = self
            .lines
            .remove(&key)
            .ok_or(ScriptureNodeHandoffError::UnknownLine { key })?;
        match runtime.drain_seal_publish(request).await {
            Ok((runtime, outcome)) => {
                self.lines.insert(key, runtime);
                Ok(outcome)
            }
            Err(reject) => {
                let restored = LineKey::new(reject.runtime.journal_id(), reject.runtime.line_id());
                self.lines.insert(restored, reject.runtime);
                Err(ScriptureNodeHandoffError::Handoff(reject.error))
            }
        }
    }
}

impl std::fmt::Debug for ScriptureNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScriptureNode")
            .field("lines", &self.lines.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use holylog::atomic::AtomicLog;
    use holylog::memory::InMemoryLogDrive;
    use holylog::virtual_log::{
        ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver, ResolveFuture,
        VirtualLog,
    };
    use scripture::{
        CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, LineId, OwnerEndpoint, OwnerId,
        RecoveryBound, SystemClock, WriterId,
    };

    use super::{LineKey, ScriptureNode, ScriptureNodeConfigError};
    use crate::canon_route::CanonRoute;
    use crate::line_runtime::LineRuntimeConfig;

    fn journal_a() -> JournalId {
        JournalId::from_bytes(*b"node-journal-a!!")
    }

    fn journal_b() -> JournalId {
        JournalId::from_bytes(*b"node-journal-b!!")
    }

    fn line_a() -> LineId {
        LineId::from_bytes(*b"node-line-a!!!!!")
    }

    fn line_b() -> LineId {
        LineId::from_bytes(*b"node-line-b!!!!!")
    }

    fn owner() -> OwnerId {
        OwnerId::from_bytes(*b"node-shell-ownr!")
    }

    fn other() -> OwnerId {
        OwnerId::from_bytes(*b"node-shell-othr!")
    }

    fn config(journal: JournalId, line: LineId, owner: OwnerId) -> LineRuntimeConfig {
        LineRuntimeConfig {
            journal_id: journal,
            line_id: line,
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
        }
    }

    fn fence(journal: JournalId, line: LineId, revision: u64, owner: OwnerId) -> CanonFence {
        CanonFence::new(
            revision,
            journal,
            line,
            CanonOwner::Owned {
                owner_id: owner,
                endpoint: OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint"),
            },
        )
    }

    #[derive(Default)]
    struct Resolver {
        loglets: Mutex<BTreeMap<LogletId, Arc<AtomicLog>>>,
    }

    impl Resolver {
        fn insert(&self, id: LogletId, log: Arc<AtomicLog>) {
            self.loglets.lock().expect("lock").insert(id, log);
        }
    }

    impl LogletResolver for Resolver {
        fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<Arc<AtomicLog>>> {
            let id = id.clone();
            Box::pin(async move { Ok(self.loglets.lock().expect("lock").get(&id).cloned()) })
        }
    }

    struct LineHarness {
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<Resolver>,
        first: LogletId,
    }

    impl LineHarness {
        fn memory(name: &str) -> Self {
            let resolver = Arc::new(Resolver::default());
            let first = LogletId::new(name).expect("id");
            resolver.insert(
                first.clone(),
                Arc::new(
                    AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                        .build()
                        .expect("log"),
                ),
            );
            Self {
                register: Arc::new(InMemoryConditionalRegister::new()),
                resolver,
                first,
            }
        }

        fn virtual_log(&self) -> VirtualLog {
            VirtualLog::new(
                Arc::clone(&self.register),
                Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
            )
        }
    }

    #[tokio::test]
    async fn serving_and_standby_lines_are_independent() {
        let serve = LineHarness::memory("shell-serve");
        let standby = LineHarness::memory("shell-standby");
        serve
            .virtual_log()
            .bootstrap_with_application_fence(
                serve.first.clone(),
                fence(journal_a(), line_a(), 0, owner()).encode(),
            )
            .await
            .expect("bootstrap serve");
        standby
            .virtual_log()
            .bootstrap_with_application_fence(
                standby.first.clone(),
                fence(journal_b(), line_b(), 0, other()).encode(),
            )
            .await
            .expect("bootstrap standby");

        let logs = BTreeMap::from([
            (LineKey::new(journal_a(), line_a()), serve.virtual_log()),
            (LineKey::new(journal_b(), line_b()), standby.virtual_log()),
        ]);
        let started = ScriptureNode::start(
            vec![
                config(journal_a(), line_a(), owner()),
                config(journal_b(), line_b(), owner()),
            ],
            |cfg| logs.get(&LineKey::from_config(cfg)).expect("log").clone(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        assert!(started.all_started());
        let node = ScriptureNode::from_started(started.runtimes);
        let serve_key = LineKey::new(journal_a(), line_a());
        let standby_key = LineKey::new(journal_b(), line_b());
        assert!(node.line(serve_key).expect("serve").is_serving());
        assert!(node.line(standby_key).expect("standby").is_standby());
        assert!(matches!(
            node.resolve_route(standby_key).await.expect("route"),
            CanonRoute::NotOwner { .. }
        ));
    }

    #[tokio::test]
    async fn duplicate_config_rejects_before_any_start() {
        let err = ScriptureNode::start(
            vec![
                config(journal_a(), line_a(), owner()),
                config(journal_a(), line_a(), owner()),
            ],
            |_| panic!("virtual_log_for must not run on duplicate config"),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect_err("duplicate");
        assert!(matches!(
            err,
            ScriptureNodeConfigError::DuplicateLine { key }
            if key == LineKey::new(journal_a(), line_a())
        ));
    }

    #[tokio::test]
    async fn bad_line_reports_failure_while_valid_line_starts() {
        let good = LineHarness::memory("shell-good");
        good.virtual_log()
            .bootstrap_with_application_fence(
                good.first.clone(),
                fence(journal_a(), line_a(), 0, owner()).encode(),
            )
            .await
            .expect("bootstrap");
        let bad = LineHarness::memory("shell-bad");
        // Uninitialized register ⇒ typed startup error.
        let logs = BTreeMap::from([
            (LineKey::new(journal_a(), line_a()), good.virtual_log()),
            (LineKey::new(journal_b(), line_b()), bad.virtual_log()),
        ]);
        let started = ScriptureNode::start(
            vec![
                config(journal_a(), line_a(), owner()),
                config(journal_b(), line_b(), owner()),
            ],
            |cfg| logs.get(&LineKey::from_config(cfg)).expect("log").clone(),
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
                .get(&LineKey::new(journal_a(), line_a()))
                .expect("good")
                .is_serving()
        );
        assert!(
            started
                .failures
                .contains_key(&LineKey::new(journal_b(), line_b()))
        );
    }
}
