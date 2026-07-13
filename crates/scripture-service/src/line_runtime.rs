//! Long-lived, transport-neutral runtime for one configured Scripture Line.
//!
//! Exists whether serving or standby. Standby holds no actor and never
//! auto-promotes when Canon later names this local owner. Fenced handoff is a
//! consuming operation that leaves the runtime irreversibly non-serving.
//! ConditionalRegister / VirtualLog remains the sole fencing authority.

use holylog::virtual_log::{LogletId, VirtualLog};
use scripture::{
    CanonAuthorityError, CanonFence, CanonOwner, Clock, JournalId, LineId, OwnerId, ReceiptFuture,
    Submission, Timer, observe_canon_authority_witnessed,
};

use crate::canon_node::{CanonNode, CanonNodeConfig, CanonNodeStart, CanonNodeStartError};
use crate::canon_route::{CanonRoute, CanonRouteError};
use crate::canon_transition::{
    CanonTransitionError, CanonTransitionOutcome, CanonTransitionRequest, PublishedCanon,
    publish_canon_transition,
};
use crate::chunk_service::{ChunkJournalService, ChunkServiceError, DrainError};

/// Stable configuration for one Line runtime (same inputs as [`CanonNodeConfig`]).
pub type LineRuntimeConfig = CanonNodeConfig;

/// Failures that refuse to invent a serving or standby Line runtime.
pub type LineRuntimeStartError = CanonNodeStartError;

/// In-process runtime for one configured Journal/Line.
///
/// Does not expose [`ChunkJournalService`], actor handles, compare tokens, or a
/// mutable registration API.
pub struct LineRuntime {
    journal_id: JournalId,
    line_id: LineId,
    owner_id: OwnerId,
    virtual_log: VirtualLog,
    phase: LinePhase,
}

enum LinePhase {
    Serving(ChunkJournalService),
    Standby,
    Terminal(LineTerminal),
}

/// Irreversible non-serving outcomes after a consuming handoff attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineTerminal {
    /// Successor Canon was published; local A was stopped. Does not recover B.
    Published(PublishedCanon),
    /// Competing CAS or lost ownership during handoff; A does not resume.
    ConflictNeedsInspect,
    /// Seal/CAS or post-drain failure; A does not resume.
    FailedNeedsReconcile,
}

/// Caller-supplied handoff inputs validated against the runtime configuration.
#[derive(Debug, Clone)]
pub struct LineHandoffRequest {
    /// Fresh empty successor Loglet.
    pub successor: LogletId,
    /// Desired next Canon owner (B or explicit Unowned).
    pub next_owner: CanonOwner,
    /// Must match the runtime's configured journal.
    pub journal_id: JournalId,
    /// Must match the runtime's configured Line.
    pub line_id: LineId,
}

/// Typed refusal to admit work on a non-serving Line runtime.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LineUnavailable {
    /// Runtime has no Canon-bound actor (standby or never recovered).
    #[error("Line runtime is standby and cannot admit work")]
    Standby,
    /// Runtime already completed or failed a consuming handoff.
    #[error("Line runtime is terminal and cannot admit work")]
    Terminal,
}

/// Pre-drain rejection that restores the caller's runtime unchanged.
#[derive(Debug)]
pub struct LineHandoffReject {
    /// Runtime still in its prior phase (Serving or non-serving).
    pub runtime: LineRuntime,
    /// Why the handoff did not begin.
    pub error: LineHandoffError,
}

/// Failures that refuse to begin a fenced handoff.
#[derive(Debug, thiserror::Error)]
pub enum LineHandoffError {
    /// Journal/Line in the request disagree with this runtime.
    #[error("handoff journal/line disagree with Line runtime configuration")]
    IdentityMismatch {
        /// Runtime journal.
        runtime_journal: JournalId,
        /// Runtime line.
        runtime_line: LineId,
        /// Request journal.
        request_journal: JournalId,
        /// Request line.
        request_line: LineId,
    },
    /// Only a serving runtime may drain and publish.
    #[error("Line runtime is not serving")]
    NotServing,
    /// Fresh witness observation failed before drain.
    #[error(transparent)]
    Authority(#[from] CanonAuthorityError),
    /// Local drain failed before publish.
    #[error(transparent)]
    Drain(#[from] DrainError),
    /// Publish refused before Holylog reconfiguration.
    #[error(transparent)]
    Transition(#[from] CanonTransitionError),
}

/// Admission failures from [`LineRuntime::submit`] / [`LineRuntime::flush`].
#[derive(Debug, thiserror::Error)]
pub enum LineAdmitError {
    /// Runtime is not serving.
    #[error(transparent)]
    Unavailable(#[from] LineUnavailable),
    /// Serving owner rejected the command.
    #[error(transparent)]
    Service(#[from] ChunkServiceError),
}

impl LineRuntime {
    /// Starts one Line runtime from durable Canon evidence.
    ///
    /// Uses the same fail-closed rules as [`CanonNode::start`], but always
    /// retains a runtime (and its VirtualLog) for standby Lines.
    pub async fn start<C, T>(
        config: LineRuntimeConfig,
        virtual_log: VirtualLog,
        clock: C,
        timer: T,
    ) -> Result<Self, LineRuntimeStartError>
    where
        C: Clock + Send + 'static,
        T: Timer + Send + 'static,
    {
        let journal_id = config.journal_id;
        let line_id = config.line_id;
        let owner_id = config.owner_id;
        // Clone so standby can keep a handle; CanonNode::start drops the log on
        // the Standby path.
        match CanonNode::start(config, virtual_log.clone(), clock, timer).await? {
            CanonNodeStart::Serving(node) => {
                let (journal_id, line_id, owner_id, virtual_log, service) = node.into_parts();
                Ok(Self {
                    journal_id,
                    line_id,
                    owner_id,
                    virtual_log,
                    phase: LinePhase::Serving(service),
                })
            }
            CanonNodeStart::Standby { .. } => Ok(Self {
                journal_id,
                line_id,
                owner_id,
                virtual_log,
                phase: LinePhase::Standby,
            }),
        }
    }

    /// Configured journal identity.
    #[must_use]
    pub const fn journal_id(&self) -> JournalId {
        self.journal_id
    }

    /// Configured Line identity.
    #[must_use]
    pub const fn line_id(&self) -> LineId {
        self.line_id
    }

    /// Configured local owner identity.
    #[must_use]
    pub const fn owner_id(&self) -> OwnerId {
        self.owner_id
    }

    /// Whether this runtime currently holds a Canon-bound serving actor.
    #[must_use]
    pub const fn is_serving(&self) -> bool {
        matches!(self.phase, LinePhase::Serving(_))
    }

    /// Whether this runtime is standby (no actor, not terminal).
    #[must_use]
    pub const fn is_standby(&self) -> bool {
        matches!(self.phase, LinePhase::Standby)
    }

    /// Whether a consuming handoff already completed or failed.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self.phase, LinePhase::Terminal(_))
    }

    /// Terminal handoff details when [`Self::is_terminal`].
    #[must_use]
    pub fn terminal(&self) -> Option<&LineTerminal> {
        match &self.phase {
            LinePhase::Terminal(terminal) => Some(terminal),
            _ => None,
        }
    }

    /// Fresh Canon route resolution for this configured Line.
    ///
    /// Standby and terminal runtimes never answer [`CanonRoute::Serve`], even
    /// when Canon currently names this local owner.
    pub async fn resolve_route(&self) -> Result<CanonRoute, CanonRouteError> {
        match &self.phase {
            LinePhase::Serving(service) => {
                crate::resolve_canon_route(
                    &self.virtual_log,
                    service,
                    self.journal_id,
                    self.line_id,
                    self.owner_id,
                )
                .await
            }
            LinePhase::Standby | LinePhase::Terminal(_) => {
                resolve_standby_route(
                    &self.virtual_log,
                    self.journal_id,
                    self.line_id,
                    self.owner_id,
                )
                .await
            }
        }
    }

    /// Submits through the Canon-bound owner while Serving.
    pub async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, LineAdmitError> {
        match &self.phase {
            LinePhase::Serving(service) => Ok(service.submit(self.journal_id, submission).await?),
            LinePhase::Standby => Err(LineAdmitError::Unavailable(LineUnavailable::Standby)),
            LinePhase::Terminal(_) => Err(LineAdmitError::Unavailable(LineUnavailable::Terminal)),
        }
    }

    /// Flushes the open chunk while Serving.
    pub async fn flush(&self) -> Result<(), LineAdmitError> {
        match &self.phase {
            LinePhase::Serving(service) => Ok(service.flush(self.journal_id).await?),
            LinePhase::Standby => Err(LineAdmitError::Unavailable(LineUnavailable::Standby)),
            LinePhase::Terminal(_) => Err(LineAdmitError::Unavailable(LineUnavailable::Terminal)),
        }
    }

    /// Consuming fenced A→B (or Unowned) handoff.
    ///
    /// Pre-drain identity / phase checks return [`LineHandoffReject`] with the
    /// runtime unchanged. Once exclusive ownership of the serving actor is taken,
    /// the runtime becomes irreversibly non-serving and the result is always a
    /// terminal runtime plus [`CanonTransitionOutcome`] (including conflict when
    /// ownership was already lost before drain).
    pub async fn drain_seal_publish(
        mut self,
        request: LineHandoffRequest,
    ) -> Result<(Self, CanonTransitionOutcome), LineHandoffReject> {
        if request.journal_id != self.journal_id || request.line_id != self.line_id {
            return Err(LineHandoffReject {
                error: LineHandoffError::IdentityMismatch {
                    runtime_journal: self.journal_id,
                    runtime_line: self.line_id,
                    request_journal: request.journal_id,
                    request_line: request.line_id,
                },
                runtime: self,
            });
        }
        if !matches!(self.phase, LinePhase::Serving(_)) {
            return Err(LineHandoffReject {
                error: LineHandoffError::NotServing,
                runtime: self,
            });
        }

        let mut service = match std::mem::replace(&mut self.phase, LinePhase::Standby) {
            LinePhase::Serving(service) => service,
            other => {
                self.phase = other;
                return Err(LineHandoffReject {
                    error: LineHandoffError::NotServing,
                    runtime: self,
                });
            }
        };

        let outcome = match run_fenced_handoff(
            &mut service,
            &self.virtual_log,
            self.journal_id,
            self.line_id,
            self.owner_id,
            request,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                self.phase = LinePhase::Terminal(LineTerminal::FailedNeedsReconcile);
                drop(service);
                return Err(LineHandoffReject {
                    runtime: self,
                    error,
                });
            }
        };

        self.phase = LinePhase::Terminal(match &outcome {
            CanonTransitionOutcome::Published(published) => {
                LineTerminal::Published(published.clone())
            }
            CanonTransitionOutcome::ConflictNeedsInspect => LineTerminal::ConflictNeedsInspect,
            CanonTransitionOutcome::FailedNeedsReconcile { .. } => {
                LineTerminal::FailedNeedsReconcile
            }
        });
        drop(service);
        Ok((self, outcome))
    }
}

async fn run_fenced_handoff(
    service: &mut ChunkJournalService,
    virtual_log: &VirtualLog,
    journal_id: JournalId,
    line_id: LineId,
    owner_id: OwnerId,
    request: LineHandoffRequest,
) -> Result<CanonTransitionOutcome, LineHandoffError> {
    let authority =
        match observe_canon_authority_witnessed(virtual_log, journal_id, line_id, owner_id).await {
            Ok(authority) => authority,
            Err(CanonAuthorityError::NotOwner { .. } | CanonAuthorityError::Unowned { .. }) => {
                return Ok(CanonTransitionOutcome::ConflictNeedsInspect);
            }
            Err(error) => return Err(LineHandoffError::Authority(error)),
        };
    let drained = service.drain_owner(journal_id, &authority).await?;
    Ok(publish_canon_transition(
        service,
        virtual_log,
        CanonTransitionRequest {
            authority,
            drained,
            successor: request.successor,
            next_owner: request.next_owner,
            journal_id,
            line_id,
        },
    )
    .await?)
}

async fn resolve_standby_route(
    virtual_log: &VirtualLog,
    journal_id: JournalId,
    line_id: LineId,
    this_owner: OwnerId,
) -> Result<CanonRoute, CanonRouteError> {
    let observed = virtual_log.observe_membership().await?;
    let fence = CanonFence::from_virtual_log_state(&observed.state)?;
    if fence.journal_id != journal_id {
        return Err(CanonRouteError::Authority(
            CanonAuthorityError::JournalMismatch {
                expected: journal_id,
                actual: fence.journal_id,
            },
        ));
    }
    if fence.line_id != line_id {
        return Err(CanonRouteError::Authority(
            CanonAuthorityError::LineMismatch {
                expected: line_id,
                actual: fence.line_id,
            },
        ));
    }
    match fence.owner {
        CanonOwner::Unowned => Ok(CanonRoute::Recovering {
            canon_revision: fence.revision,
        }),
        CanonOwner::Owned { owner_id, endpoint } if owner_id != this_owner => {
            Ok(CanonRoute::NotOwner {
                canon_revision: fence.revision,
                owner_id,
                endpoint,
            })
        }
        CanonOwner::Owned { .. } => Ok(CanonRoute::Recovering {
            canon_revision: fence.revision,
        }),
    }
}

impl std::fmt::Debug for LineRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LineRuntime")
            .field("journal_id", &self.journal_id)
            .field("line_id", &self.line_id)
            .field("owner_id", &self.owner_id)
            .field(
                "phase",
                &match &self.phase {
                    LinePhase::Serving(_) => "Serving",
                    LinePhase::Standby => "Standby",
                    LinePhase::Terminal(_) => "Terminal",
                },
            )
            .finish_non_exhaustive()
    }
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
        CanonFence, CanonOwner, ChunkLogError, ChunkPolicy, CohortId, JournalId, LineId,
        OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound, Submission, SystemClock,
        WriterId,
    };

    use super::{
        LineHandoffError, LineHandoffRequest, LineRuntime, LineRuntimeConfig,
        LineRuntimeStartError, LineUnavailable,
    };
    use crate::canon_node::CanonNodeStartError;
    use crate::canon_owner::CanonOwnerError;
    use crate::canon_route::CanonRoute;
    use crate::canon_transition::CanonTransitionOutcome;
    use crate::{LineAdmitError, recover_canon_owner};

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"line-runtime-id!")
    }

    fn line() -> LineId {
        LineId::from_bytes(*b"line-runtime-ln!")
    }

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"line-rt-owner-a!")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"line-rt-owner-b!")
    }

    fn config(owner: OwnerId) -> LineRuntimeConfig {
        LineRuntimeConfig {
            journal_id: journal(),
            line_id: line(),
            owner_id: owner,
            cohort_id: CohortId::from_bytes(*b"line-rt-cohort!!"),
            writer_id: WriterId::from_bytes(*b"line-rt-writer!!"),
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
            line(),
            CanonOwner::Owned {
                owner_id: owner,
                endpoint: OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint"),
            },
        )
    }

    fn owned(owner: OwnerId) -> CanonOwner {
        CanonOwner::Owned {
            owner_id: owner,
            endpoint: OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint"),
        }
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
            let first = LogletId::new("line-rt-first").expect("id");
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
    async fn named_self_starts_serving_and_commits() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let runtime = LineRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        assert!(runtime.is_serving());
        let pending = runtime
            .submit(Submission {
                producer_id: ProducerId::from_bytes(*b"line-rt-producr!"),
                producer_epoch: 0,
                sequence: 0,
                records: vec![Record::new([], bytes::Bytes::from_static(b"ok"))],
            })
            .await
            .expect("admit");
        runtime.flush().await.expect("flush");
        assert_eq!(pending.await.expect("commit").canon_revision, 0);
    }

    #[tokio::test]
    async fn other_owner_and_unowned_are_standby_without_actors() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_b()).encode())
            .await
            .expect("bootstrap");
        let runtime = LineRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("standby");
        assert!(runtime.is_standby());
        assert!(matches!(
            runtime.resolve_route().await.expect("route"),
            CanonRoute::NotOwner { owner_id, .. } if owner_id == owner_b()
        ));
        assert!(matches!(
            runtime
                .submit(Submission {
                    producer_id: ProducerId::from_bytes(*b"line-rt-producr!"),
                    producer_epoch: 0,
                    sequence: 0,
                    records: vec![Record::new([], bytes::Bytes::from_static(b"no"))],
                })
                .await,
            Err(LineAdmitError::Unavailable(LineUnavailable::Standby))
        ));
        assert_eq!(
            harness.virtual_log().check_tail().await.expect("tail").tail,
            0
        );

        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                CanonFence::new(0, journal(), line(), CanonOwner::Unowned).encode(),
            )
            .await
            .expect("bootstrap");
        let runtime = LineRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("unowned");
        assert!(runtime.is_standby());
        assert!(matches!(
            runtime.resolve_route().await.expect("route"),
            CanonRoute::Recovering { canon_revision: 0 }
        ));
    }

    #[tokio::test]
    async fn standby_named_self_later_resolves_recovering_not_serve() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_b()).encode())
            .await
            .expect("bootstrap");
        let runtime = LineRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("standby");
        let second = LogletId::new("line-rt-promote").expect("id");
        harness.resolver.insert(
            second.clone(),
            Arc::new(
                AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                    .build()
                    .expect("log"),
            ),
        );
        harness
            .virtual_log()
            .reconfigure_with_application_fence(second, fence(1, owner_a()).encode())
            .await
            .expect("name self");
        assert!(matches!(
            runtime.resolve_route().await.expect("route"),
            CanonRoute::Recovering { canon_revision: 1 }
        ));
        assert!(runtime.is_standby());
    }

    #[tokio::test]
    async fn malformed_mismatch_and_mid_recovery_are_typed_errors() {
        let harness = Harness::memory();
        assert!(matches!(
            LineRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(LineRuntimeStartError::VirtualLog(_))
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
            LineRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(LineRuntimeStartError::Fence(_))
        ));

        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                CanonFence::new(
                    0,
                    JournalId::from_bytes(*b"other-journal!!!"),
                    line(),
                    owned(owner_a()),
                )
                .encode(),
            )
            .await
            .expect("bootstrap");
        assert!(matches!(
            LineRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(LineRuntimeStartError::Authority(_))
        ));

        let flip = Arc::new(FlipRegister::new(2));
        let harness = Harness::with_register(Arc::clone(&flip) as Arc<dyn ConditionalRegister>);
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        flip.arm(VirtualLogState {
            revision: 1,
            generations: vec![holylog::virtual_log::GenerationDescriptor {
                loglet_id: harness.first.clone(),
                start: 0,
            }],
            application_fence: fence(1, owner_a()).encode(),
        });
        assert!(matches!(
            LineRuntime::start(
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

    #[tokio::test]
    async fn consuming_handoff_publishes_b_and_blocks_further_admit() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let runtime = LineRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        let pending = runtime
            .submit(Submission {
                producer_id: ProducerId::from_bytes(*b"line-rt-producr!"),
                producer_epoch: 0,
                sequence: 0,
                records: vec![Record::new([], bytes::Bytes::from_static(b"a"))],
            })
            .await
            .expect("admit");
        runtime.flush().await.expect("flush");
        let _ = pending.await.expect("commit");

        let second = LogletId::new("line-rt-second").expect("id");
        harness.resolver.insert(
            second.clone(),
            Arc::new(
                AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                    .build()
                    .expect("log"),
            ),
        );
        let (runtime, outcome) = runtime
            .drain_seal_publish(LineHandoffRequest {
                successor: second,
                next_owner: owned(owner_b()),
                journal_id: journal(),
                line_id: line(),
            })
            .await
            .expect("handoff");
        assert!(matches!(outcome, CanonTransitionOutcome::Published(_)));
        assert!(runtime.is_terminal());
        assert!(matches!(
            runtime
                .submit(Submission {
                    producer_id: ProducerId::from_bytes(*b"line-rt-producr!"),
                    producer_epoch: 0,
                    sequence: 1,
                    records: vec![Record::new([], bytes::Bytes::from_static(b"no"))],
                })
                .await,
            Err(LineAdmitError::Unavailable(LineUnavailable::Terminal))
        ));

        let recovered = recover_canon_owner(
            config(owner_b()).owner_request(),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover b");
        let mut service = crate::ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register b");
        let pending = service
            .submit(
                journal(),
                Submission {
                    producer_id: ProducerId::from_bytes(*b"line-rt-producr!"),
                    producer_epoch: 0,
                    sequence: 1,
                    records: vec![Record::new([], bytes::Bytes::from_static(b"b"))],
                },
            )
            .await
            .expect("admit b");
        service.flush(journal()).await.expect("flush b");
        let receipt = pending.await.expect("commit b");
        assert_eq!(receipt.first_offset.get(), 1);
        assert_eq!(receipt.canon_revision, 1);
    }

    #[tokio::test]
    async fn mismatched_handoff_identity_rejects_before_drain() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let runtime = LineRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        let second = LogletId::new("line-rt-mismatch").expect("id");
        harness.resolver.insert(
            second.clone(),
            Arc::new(
                AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                    .build()
                    .expect("log"),
            ),
        );
        let reject = runtime
            .drain_seal_publish(LineHandoffRequest {
                successor: second,
                next_owner: owned(owner_b()),
                journal_id: JournalId::from_bytes(*b"other-journal-id"),
                line_id: line(),
            })
            .await
            .expect_err("mismatch");
        assert!(matches!(
            reject.error,
            LineHandoffError::IdentityMismatch { .. }
        ));
        assert!(reject.runtime.is_serving());
        assert_eq!(
            harness
                .virtual_log()
                .observe_membership()
                .await
                .expect("observe")
                .state
                .revision,
            0
        );
    }

    #[tokio::test]
    async fn competing_handoff_is_terminal_conflict() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let runtime = LineRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        let second = LogletId::new("line-rt-race").expect("id");
        harness.resolver.insert(
            second.clone(),
            Arc::new(
                AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                    .build()
                    .expect("log"),
            ),
        );
        harness
            .virtual_log()
            .reconfigure_with_application_fence(second.clone(), fence(1, owner_b()).encode())
            .await
            .expect("competitor");
        let loser = LogletId::new("line-rt-loser").expect("id");
        harness.resolver.insert(
            loser.clone(),
            Arc::new(
                AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                    .build()
                    .expect("log"),
            ),
        );
        let (runtime, outcome) = runtime
            .drain_seal_publish(LineHandoffRequest {
                successor: loser,
                next_owner: owned(owner_b()),
                journal_id: journal(),
                line_id: line(),
            })
            .await
            .expect("handoff attempt");
        assert!(matches!(
            outcome,
            CanonTransitionOutcome::ConflictNeedsInspect
        ));
        assert!(runtime.is_terminal());
        assert!(matches!(
            runtime
                .submit(Submission {
                    producer_id: ProducerId::from_bytes(*b"line-rt-producr!"),
                    producer_epoch: 0,
                    sequence: 0,
                    records: vec![Record::new([], bytes::Bytes::from_static(b"no"))],
                })
                .await,
            Err(LineAdmitError::Unavailable(LineUnavailable::Terminal))
        ));
    }

    #[tokio::test]
    async fn unowned_publish_is_terminal_without_starting_owner() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let runtime = LineRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        let second = LogletId::new("line-rt-unowned").expect("id");
        harness.resolver.insert(
            second.clone(),
            Arc::new(
                AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                    .build()
                    .expect("log"),
            ),
        );
        let (runtime, outcome) = runtime
            .drain_seal_publish(LineHandoffRequest {
                successor: second,
                next_owner: CanonOwner::Unowned,
                journal_id: journal(),
                line_id: line(),
            })
            .await
            .expect("handoff");
        assert!(matches!(outcome, CanonTransitionOutcome::Published(_)));
        assert!(runtime.is_terminal());
        assert!(matches!(
            recover_canon_owner(
                config(owner_a()).owner_request(),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonOwnerError::Recovery(ChunkLogError::Authority(_)))
        ));
    }
}
