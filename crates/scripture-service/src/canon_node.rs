//! Fail-closed Canon-aware Scripture node startup.
//!
//! A node serves only when a fresh Canon observation names this configured
//! owner and `recover_canon_owner` + `register_canon_owner` succeed. Non-owner
//! and Unowned outcomes return advisory standby route state without constructing
//! an actor. This module does not elect, discover, or resume owners.

use holylog::virtual_log::{VirtualLog, VirtualLogError};
use scripture::{
    CanonAuthorityError, CanonFence, CanonFenceError, CanonOwner, ChunkPolicy, Clock, CohortId,
    DriverError, JournalId, OwnerId, ReceiptFuture, RecoveryBound, Submission, Timer, VerseId,
    WriterId,
};

use crate::canon_owner::{CanonOwnerError, CanonOwnerRequest, recover_canon_owner};
use crate::canon_route::{CanonRoute, CanonRouteError, resolve_canon_route};
use crate::chunk_service::{ChunkJournalService, ChunkServiceError};

/// Stable local configuration for one Verse on one Scripture node.
///
/// `OwnerId` must be supplied by the deployment across restarts. This type does
/// not generate owner identities or treat endpoints as fencing grants.
#[derive(Debug, Clone)]
pub struct CanonNodeConfig {
    /// Logical Scripture journal.
    pub journal_id: JournalId,
    /// Physical Verse.
    pub verse_id: VerseId,
    /// This node's durable owner identity.
    pub owner_id: OwnerId,
    /// Cohort encoded into new chunk headers.
    pub cohort_id: CohortId,
    /// Writer identity encoded into new chunk headers.
    pub writer_id: WriterId,
    /// Driver admission / seal policy.
    pub policy: ChunkPolicy,
    /// Bound on the durable suffix inspected for dedup rebuild.
    pub recovery_bound: RecoveryBound,
    /// Bounded command-queue capacity for the actor.
    pub queue_capacity: usize,
}

impl CanonNodeConfig {
    /// Builds a recovery request whose journal/line/owner match this config.
    #[must_use]
    pub fn owner_request(&self) -> CanonOwnerRequest {
        CanonOwnerRequest {
            journal_id: self.journal_id,
            verse_id: self.verse_id,
            owner_id: self.owner_id,
            cohort_id: self.cohort_id,
            writer_id: self.writer_id,
            policy: self.policy,
            recovery_bound: self.recovery_bound,
            queue_capacity: self.queue_capacity,
        }
    }
}

/// Non-serving route snapshot returned by [`CanonNode::start`].
///
/// Narrower than [`CanonRoute`]: a standby result cannot represent `Serve`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonStandbyRoute {
    /// Fresh Canon names another owner. Endpoint is advisory only.
    NotOwner {
        /// Observed Canon / VirtualLog revision.
        canon_revision: u64,
        /// Owner identity named by the fence.
        owner_id: OwnerId,
        /// Advisory endpoint from the fence.
        endpoint: scripture::OwnerEndpoint,
    },
    /// Fresh Canon is explicitly Unowned.
    Recovering {
        /// Observed Canon / VirtualLog revision.
        canon_revision: u64,
    },
}

/// Outcome of [`CanonNode::start`].
pub enum CanonNodeStart {
    /// This node is the named Canon owner and has a running Canon-bound actor.
    Serving(CanonNode),
    /// Fresh Canon is Unowned or names another owner. No local actor was built.
    Standby {
        /// Advisory route derived from the standby observation.
        route: CanonStandbyRoute,
    },
}

impl std::fmt::Debug for CanonNodeStart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serving(node) => formatter
                .debug_struct("Serving")
                .field("journal_id", &node.journal_id)
                .field("verse_id", &node.verse_id)
                .field("owner_id", &node.owner_id)
                .finish_non_exhaustive(),
            Self::Standby { route } => formatter
                .debug_struct("Standby")
                .field("route", route)
                .finish(),
        }
    }
}

/// A serving Scripture node for one configured Verse.
///
/// Exposes only narrow admission and fresh route resolution. It does not expose
/// mutable service registration, compare tokens, or election APIs.
pub struct CanonNode {
    journal_id: JournalId,
    verse_id: VerseId,
    owner_id: OwnerId,
    virtual_log: VirtualLog,
    service: ChunkJournalService,
}

impl CanonNode {
    /// Starts one node attempt from durable Canon evidence.
    ///
    /// Owned-self relies on [`recover_canon_owner`]'s own fresh observation and
    /// recovery fence. A Canon advance during recovery fails closed and does not
    /// return [`CanonNodeStart::Serving`].
    pub async fn start<C, T>(
        config: CanonNodeConfig,
        virtual_log: VirtualLog,
        clock: C,
        timer: T,
    ) -> Result<CanonNodeStart, CanonNodeStartError>
    where
        C: Clock + Send + 'static,
        T: Timer + Send + 'static,
    {
        let observed = virtual_log.observe_membership().await?;
        let fence = CanonFence::from_virtual_log_state(&observed.state)?;
        if fence.journal_id != config.journal_id {
            return Err(CanonNodeStartError::Authority(
                CanonAuthorityError::JournalMismatch {
                    expected: config.journal_id,
                    actual: fence.journal_id,
                },
            ));
        }
        if fence.verse_id != config.verse_id {
            return Err(CanonNodeStartError::Authority(
                CanonAuthorityError::VerseMismatch {
                    expected: config.verse_id,
                    actual: fence.verse_id,
                },
            ));
        }

        match &fence.owner {
            CanonOwner::Unowned => Ok(CanonNodeStart::Standby {
                route: CanonStandbyRoute::Recovering {
                    canon_revision: fence.revision,
                },
            }),
            CanonOwner::Owned { owner_id, endpoint } if *owner_id != config.owner_id => {
                Ok(CanonNodeStart::Standby {
                    route: CanonStandbyRoute::NotOwner {
                        canon_revision: fence.revision,
                        owner_id: *owner_id,
                        endpoint: endpoint.clone(),
                    },
                })
            }
            CanonOwner::Owned { .. } => {
                let recovered =
                    recover_canon_owner(config.owner_request(), virtual_log.clone(), clock, timer)
                        .await?;
                let mut service = ChunkJournalService::new();
                service.register_canon_owner(recovered)?;
                Ok(CanonNodeStart::Serving(CanonNode {
                    journal_id: config.journal_id,
                    verse_id: config.verse_id,
                    owner_id: config.owner_id,
                    virtual_log,
                    service,
                }))
            }
        }
    }

    /// Configured journal identity.
    #[must_use]
    pub const fn journal_id(&self) -> JournalId {
        self.journal_id
    }

    /// Configured Verse identity.
    #[must_use]
    pub const fn verse_id(&self) -> VerseId {
        self.verse_id
    }

    /// Configured owner identity for this node.
    #[must_use]
    pub const fn owner_id(&self) -> OwnerId {
        self.owner_id
    }

    /// Fresh Canon route resolution for this node's configured Verse.
    pub async fn resolve_route(&self) -> Result<CanonRoute, CanonRouteError> {
        resolve_canon_route(
            &self.virtual_log,
            &self.service,
            self.journal_id,
            self.verse_id,
            self.owner_id,
        )
        .await
    }

    /// Submits through the Canon-bound local owner.
    pub async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, ChunkServiceError> {
        self.service.submit(self.journal_id, submission).await
    }

    /// Flushes the Canon-bound local owner's open chunk.
    pub async fn flush(&self) -> Result<(), ChunkServiceError> {
        self.service.flush(self.journal_id).await
    }

    /// Consumes a serving node so a Verse runtime can run a fenced handoff.
    pub(crate) fn into_parts(
        self,
    ) -> (JournalId, VerseId, OwnerId, VirtualLog, ChunkJournalService) {
        (
            self.journal_id,
            self.verse_id,
            self.owner_id,
            self.virtual_log,
            self.service,
        )
    }
}

/// Failures that refuse to invent a serving or silent standby node.
#[derive(Debug, thiserror::Error)]
pub enum CanonNodeStartError {
    /// Holylog register / VirtualLog failed.
    #[error(transparent)]
    VirtualLog(#[from] VirtualLogError),
    /// Opaque fence bytes failed to decode or bind.
    #[error(transparent)]
    Fence(#[from] CanonFenceError),
    /// Journal / Verse / owner observation refused this node.
    #[error(transparent)]
    Authority(#[from] CanonAuthorityError),
    /// Durable recovery failed (including mid-recovery Canon advance).
    #[error(transparent)]
    Recovery(#[from] CanonOwnerError),
    /// Local Canon registration failed after a successful recovery.
    #[error(transparent)]
    Service(#[from] ChunkServiceError),
    /// Driver construction failed after recovery (surfaced via recovery path).
    #[error(transparent)]
    Driver(#[from] DriverError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use holylog::atomic::AtomicLog;
    use holylog::memory::InMemoryLogDrive;
    use holylog::virtual_log::{
        ApplicationFence, CompareToken, ConditionalRegister, InMemoryConditionalRegister, LogletId,
        LogletResolver, RegisterFuture, ResolveFuture, VersionedState, VirtualLog, VirtualLogState,
    };
    use scripture::{
        CanonFence, CanonOwner, ChunkLogError, ChunkPolicy, CohortId, JournalId, OwnerEndpoint,
        OwnerId, ProducerId, Record, RecoveryBound, Submission, SystemClock, VerseId, WriterId,
    };

    use super::{
        CanonNode, CanonNodeConfig, CanonNodeStart, CanonNodeStartError, CanonStandbyRoute,
    };
    use crate::canon_owner::CanonOwnerError;
    use crate::canon_route::CanonRoute;

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"node-journal-id!")
    }

    fn verse() -> VerseId {
        VerseId::from_bytes(*b"node-line-id!!!!")
    }

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"node-owner-a!!!!")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"node-owner-b!!!!")
    }

    fn config(owner: OwnerId) -> CanonNodeConfig {
        CanonNodeConfig {
            journal_id: journal(),
            verse_id: verse(),
            owner_id: owner,
            cohort_id: CohortId::from_bytes(*b"node-cohort!!!!!"),
            writer_id: WriterId::from_bytes(*b"node-writer!!!!!"),
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

    fn fence(revision: u64, owner: OwnerId) -> CanonFence {
        CanonFence::new(
            revision,
            journal(),
            verse(),
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

    struct Harness {
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<Resolver>,
        first: LogletId,
    }

    impl Harness {
        fn memory() -> Self {
            Self::with_register(Arc::new(InMemoryConditionalRegister::new()))
        }

        fn with_register(register: Arc<dyn ConditionalRegister>) -> Self {
            let resolver = Arc::new(Resolver::default());
            let first = LogletId::new("node-first").expect("id");
            resolver.insert(
                first.clone(),
                Arc::new(
                    AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                        .build()
                        .expect("log"),
                ),
            );
            Self {
                register,
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

    struct FlipRegister {
        inner: InMemoryConditionalRegister,
        reads: AtomicUsize,
        flip_at: usize,
        flipped: Mutex<Option<VirtualLogState>>,
    }

    impl FlipRegister {
        fn new(flip_at: usize) -> Self {
            Self {
                inner: InMemoryConditionalRegister::new(),
                reads: AtomicUsize::new(0),
                flip_at,
                flipped: Mutex::new(None),
            }
        }

        fn arm(&self, state: VirtualLogState) {
            *self.flipped.lock().expect("lock") = Some(state);
        }
    }

    impl ConditionalRegister for FlipRegister {
        fn read(&self) -> RegisterFuture<'_, Option<VersionedState>> {
            Box::pin(async {
                let n = self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n >= self.flip_at
                    && let Some(state) = self.flipped.lock().expect("lock").clone()
                {
                    return Ok(Some(VersionedState {
                        token: CompareToken::from_revision(state.revision),
                        state,
                    }));
                }
                self.inner.read().await
            })
        }

        fn compare_and_swap(
            &self,
            expected: Option<&VersionedState>,
            new_state: VirtualLogState,
        ) -> RegisterFuture<'_, bool> {
            self.inner.compare_and_swap(expected, new_state)
        }
    }

    #[tokio::test]
    async fn named_self_starts_serving_and_resolves_serve() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let started = CanonNode::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        let CanonNodeStart::Serving(node) = started else {
            panic!("expected Serving");
        };
        assert!(matches!(
            node.resolve_route().await.expect("route"),
            CanonRoute::Serve {
                canon_revision: 0,
                owner_id,
                ..
            } if owner_id == owner_a()
        ));
        let pending = node
            .submit(Submission {
                producer_id: ProducerId::from_bytes(*b"node-producer!!!"),
                producer_epoch: 0,
                sequence: 0,
                records: vec![Record::new([], bytes::Bytes::from_static(b"ok"))],
            })
            .await
            .expect("admit");
        node.flush().await.expect("flush");
        let receipt = pending.await.expect("commit");
        assert_eq!(receipt.canon_revision, 0);
    }

    #[tokio::test]
    async fn other_owner_and_unowned_are_standby_without_actors() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_b()).encode())
            .await
            .expect("bootstrap");
        assert!(matches!(
            CanonNode::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .expect("standby other"),
            CanonNodeStart::Standby {
                route: CanonStandbyRoute::NotOwner {
                    canon_revision: 0,
                    owner_id,
                    ..
                }
            } if owner_id == owner_b()
        ));

        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                CanonFence::new(0, journal(), verse(), CanonOwner::Unowned).encode(),
            )
            .await
            .expect("bootstrap");
        assert!(matches!(
            CanonNode::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .expect("standby unowned"),
            CanonNodeStart::Standby {
                route: CanonStandbyRoute::Recovering { canon_revision: 0 }
            }
        ));
    }

    #[tokio::test]
    async fn malformed_and_mismatch_are_typed_startup_errors() {
        let harness = Harness::memory();
        assert!(matches!(
            CanonNode::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonNodeStartError::VirtualLog(_))
        ));

        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                ApplicationFence::new(b"not-a-canon-fence".to_vec()),
            )
            .await
            .expect("bootstrap");
        assert!(matches!(
            CanonNode::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonNodeStartError::Fence(_))
        ));

        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                CanonFence::new(
                    0,
                    JournalId::from_bytes(*b"other-journal!!!"),
                    verse(),
                    CanonOwner::Owned {
                        owner_id: owner_a(),
                        endpoint: OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint"),
                    },
                )
                .encode(),
            )
            .await
            .expect("bootstrap");
        assert!(matches!(
            CanonNode::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonNodeStartError::Authority(_))
        ));
    }

    #[tokio::test]
    async fn mid_recovery_cutover_does_not_serve() {
        let flip = Arc::new(FlipRegister::new(2));
        let harness = Harness::with_register(Arc::clone(&flip) as Arc<dyn ConditionalRegister>);
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        // Preliminary start observe + recovery authority observe see revision 0;
        // the closing re-inspect flips to revision 1 and must fail closed.
        flip.arm(VirtualLogState {
            revision: 1,
            generations: vec![holylog::virtual_log::GenerationDescriptor {
                loglet_id: harness.first.clone(),
                start: 0,
            }],
            application_fence: fence(1, owner_a()).encode(),
        });
        assert!(matches!(
            CanonNode::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonNodeStartError::Recovery(CanonOwnerError::Recovery(
                ChunkLogError::StaleCanonRecovery { .. }
            )))
        ));
    }
}
