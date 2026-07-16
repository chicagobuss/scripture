//! Long-lived, transport-neutral runtime for one configured Scripture Verse.
//!
//! Exists whether serving or standby. Standby holds no actor and never
//! auto-promotes when Canon later names this local owner. Fenced handoff is a
//! consuming operation that leaves the runtime irreversibly non-serving.
//! ConditionalRegister / VirtualLog remains the sole fencing authority.

use holylog::virtual_log::VirtualLog;
use scripture::{
    CanonAuthorityError, CanonFence, CanonOwner, Clock, JournalId, OwnerId, ReceiptFuture,
    Submission, Timer, VerseId, observe_canon_authority_witnessed,
};

use crate::canon_node::{CanonNode, CanonNodeConfig, CanonNodeStart, CanonNodeStartError};
use crate::canon_route::{CanonRoute, CanonRouteError};
use crate::canon_transition::{
    CanonTransitionError, CanonTransitionOutcome, CanonTransitionRequest, ProvisionedSuccessor,
    PublishedCanon, publish_canon_transition,
};
use crate::chunk_service::{ChunkJournalService, ChunkServiceError, DrainError};

/// Stable configuration for one Verse runtime (same inputs as [`CanonNodeConfig`]).
pub type VerseRuntimeConfig = CanonNodeConfig;

/// Failures that refuse to invent a serving or standby Verse runtime.
pub type VerseRuntimeStartError = CanonNodeStartError;

/// In-process runtime for one configured Journal/Verse.
///
/// Does not expose [`ChunkJournalService`], actor handles, compare tokens, or a
/// mutable registration API.
pub struct VerseRuntime {
    journal_id: JournalId,
    verse_id: VerseId,
    owner_id: OwnerId,
    virtual_log: VirtualLog,
    phase: VersePhase,
}

enum VersePhase {
    Serving(ChunkJournalService),
    Standby,
    Terminal(VerseTerminal),
}

/// Irreversible non-serving outcomes after a consuming handoff attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerseTerminal {
    /// Successor Canon was published; local A was stopped. Does not recover B.
    Published(PublishedCanon),
    /// Competing CAS or lost ownership during handoff; A does not resume.
    ConflictNeedsInspect,
    /// Seal/CAS or post-drain failure; A does not resume.
    FailedNeedsReconcile,
}

/// Caller-supplied handoff inputs validated against the runtime configuration.
#[derive(Debug)]
pub struct VerseHandoffRequest {
    /// Provisioned empty successor (receipt + writable + bind).
    pub successor: ProvisionedSuccessor,
    /// Desired next Canon owner (B or explicit Unowned).
    pub next_owner: CanonOwner,
    /// Must match the runtime's configured journal.
    pub journal_id: JournalId,
    /// Must match the runtime's configured Verse.
    pub verse_id: VerseId,
}

/// Typed refusal to admit work on a non-serving Verse runtime.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VerseUnavailable {
    /// Runtime has no Canon-bound actor (standby or never recovered).
    #[error("Verse runtime is standby and cannot admit work")]
    Standby,
    /// Runtime already completed or failed a consuming handoff.
    #[error("Verse runtime is terminal and cannot admit work")]
    Terminal,
}

/// A failed handoff attempt that returns ownership of the runtime to the caller.
///
/// Precondition failures leave the returned runtime unchanged. Once the
/// operation has taken the serving owner, any failure returns a terminal,
/// non-serving runtime; it must not be resumed automatically.
#[derive(Debug)]
pub struct VerseHandoffFailure {
    /// Runtime still in its prior phase (Serving or non-serving).
    pub runtime: VerseRuntime,
    /// Why the handoff did not begin.
    pub error: VerseHandoffError,
}

/// Failures that refuse to begin a fenced handoff.
#[derive(Debug, thiserror::Error)]
pub enum VerseHandoffError {
    /// Journal/Verse in the request disagree with this runtime.
    #[error("handoff journal/line disagree with Verse runtime configuration")]
    IdentityMismatch {
        /// Runtime journal.
        runtime_journal: JournalId,
        /// Runtime line.
        runtime_verse: VerseId,
        /// Request journal.
        request_journal: JournalId,
        /// Request line.
        request_verse: VerseId,
    },
    /// Only a serving runtime may drain and publish.
    #[error("Verse runtime is not serving")]
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

/// Admission failures from [`VerseRuntime::submit`] / [`VerseRuntime::flush`].
#[derive(Debug, thiserror::Error)]
pub enum VerseAdmitError {
    /// Runtime is not serving.
    #[error(transparent)]
    Unavailable(#[from] VerseUnavailable),
    /// Serving owner rejected the command.
    #[error(transparent)]
    Service(#[from] ChunkServiceError),
}

impl VerseRuntime {
    /// Starts one Verse runtime from durable Canon evidence.
    ///
    /// Uses the same fail-closed rules as [`CanonNode::start`], but always
    /// retains a runtime (and its VirtualLog) for standby Verses.
    pub async fn start<C, T>(
        config: VerseRuntimeConfig,
        virtual_log: VirtualLog,
        clock: C,
        timer: T,
    ) -> Result<Self, VerseRuntimeStartError>
    where
        C: Clock + Send + 'static,
        T: Timer + Send + 'static,
    {
        let journal_id = config.journal_id;
        let verse_id = config.verse_id;
        let owner_id = config.owner_id;
        // Clone so standby can keep a handle; CanonNode::start drops the log on
        // the Standby path.
        match CanonNode::start(config, virtual_log.clone(), clock, timer).await? {
            CanonNodeStart::Serving(node) => {
                let (journal_id, verse_id, owner_id, virtual_log, service) = node.into_parts();
                Ok(Self {
                    journal_id,
                    verse_id,
                    owner_id,
                    virtual_log,
                    phase: VersePhase::Serving(service),
                })
            }
            CanonNodeStart::Standby { .. } => Ok(Self {
                journal_id,
                verse_id,
                owner_id,
                virtual_log,
                phase: VersePhase::Standby,
            }),
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

    /// Configured local owner identity.
    #[must_use]
    pub const fn owner_id(&self) -> OwnerId {
        self.owner_id
    }

    /// Whether this runtime currently holds a Canon-bound serving actor.
    #[must_use]
    pub const fn is_serving(&self) -> bool {
        matches!(self.phase, VersePhase::Serving(_))
    }

    /// Whether this runtime is standby (no actor, not terminal).
    #[must_use]
    pub const fn is_standby(&self) -> bool {
        matches!(self.phase, VersePhase::Standby)
    }

    /// Whether a consuming handoff already completed or failed.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self.phase, VersePhase::Terminal(_))
    }

    /// Terminal handoff details when [`Self::is_terminal`].
    #[must_use]
    pub fn terminal(&self) -> Option<&VerseTerminal> {
        match &self.phase {
            VersePhase::Terminal(terminal) => Some(terminal),
            _ => None,
        }
    }

    /// Fresh Canon route resolution for this configured Verse.
    ///
    /// Standby and terminal runtimes never answer [`CanonRoute::Serve`], even
    /// when Canon currently names this local owner.
    pub async fn resolve_route(&self) -> Result<CanonRoute, CanonRouteError> {
        match &self.phase {
            VersePhase::Serving(service) => {
                crate::resolve_canon_route(
                    &self.virtual_log,
                    service,
                    self.journal_id,
                    self.verse_id,
                    self.owner_id,
                )
                .await
            }
            VersePhase::Standby | VersePhase::Terminal(_) => {
                resolve_standby_route(
                    &self.virtual_log,
                    self.journal_id,
                    self.verse_id,
                    self.owner_id,
                )
                .await
            }
        }
    }

    /// Submits through the Canon-bound owner while Serving.
    pub async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, VerseAdmitError> {
        match &self.phase {
            VersePhase::Serving(service) => Ok(service.submit(self.journal_id, submission).await?),
            VersePhase::Standby => Err(VerseAdmitError::Unavailable(VerseUnavailable::Standby)),
            VersePhase::Terminal(_) => {
                Err(VerseAdmitError::Unavailable(VerseUnavailable::Terminal))
            }
        }
    }

    /// Flushes the open chunk while Serving.
    pub async fn flush(&self) -> Result<(), VerseAdmitError> {
        match &self.phase {
            VersePhase::Serving(service) => Ok(service.flush(self.journal_id).await?),
            VersePhase::Standby => Err(VerseAdmitError::Unavailable(VerseUnavailable::Standby)),
            VersePhase::Terminal(_) => {
                Err(VerseAdmitError::Unavailable(VerseUnavailable::Terminal))
            }
        }
    }

    /// Driver metrics while Serving and the owner handle is still present.
    #[must_use]
    pub fn driver_metrics(&self) -> Option<scripture::DriverMetrics> {
        match &self.phase {
            VersePhase::Serving(service) => service.driver_metrics(self.journal_id).ok().flatten(),
            VersePhase::Standby | VersePhase::Terminal(_) => None,
        }
    }

    /// Owner health while Serving.
    #[must_use]
    pub fn health(&self) -> Option<crate::chunk_service::OwnerHealth> {
        match &self.phase {
            VersePhase::Serving(service) => service.health(self.journal_id).ok(),
            VersePhase::Standby | VersePhase::Terminal(_) => None,
        }
    }

    /// Consuming fenced A→B (or Unowned) handoff.
    ///
    /// Precondition failures return [`VerseHandoffFailure`] with the runtime
    /// unchanged. Once exclusive ownership of the serving actor is taken, the
    /// runtime becomes irreversibly non-serving; a later failure returns that
    /// terminal runtime in [`VerseHandoffFailure`]. A lost ownership observation
    /// returns terminal [`CanonTransitionOutcome::ConflictNeedsInspect`].
    pub async fn drain_seal_publish(
        mut self,
        request: VerseHandoffRequest,
    ) -> Result<(Self, CanonTransitionOutcome), VerseHandoffFailure> {
        if request.journal_id != self.journal_id || request.verse_id != self.verse_id {
            return Err(VerseHandoffFailure {
                error: VerseHandoffError::IdentityMismatch {
                    runtime_journal: self.journal_id,
                    runtime_verse: self.verse_id,
                    request_journal: request.journal_id,
                    request_verse: request.verse_id,
                },
                runtime: self,
            });
        }
        if !matches!(self.phase, VersePhase::Serving(_)) {
            return Err(VerseHandoffFailure {
                error: VerseHandoffError::NotServing,
                runtime: self,
            });
        }

        let mut service = match std::mem::replace(&mut self.phase, VersePhase::Standby) {
            VersePhase::Serving(service) => service,
            other => {
                self.phase = other;
                return Err(VerseHandoffFailure {
                    error: VerseHandoffError::NotServing,
                    runtime: self,
                });
            }
        };

        let outcome = match run_fenced_handoff(
            &mut service,
            &self.virtual_log,
            self.journal_id,
            self.verse_id,
            self.owner_id,
            request,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                self.phase = VersePhase::Terminal(VerseTerminal::FailedNeedsReconcile);
                drop(service);
                return Err(VerseHandoffFailure {
                    runtime: self,
                    error,
                });
            }
        };

        self.phase = VersePhase::Terminal(match &outcome {
            CanonTransitionOutcome::Published(published) => {
                VerseTerminal::Published(published.clone())
            }
            CanonTransitionOutcome::ConflictNeedsInspect { .. } => {
                VerseTerminal::ConflictNeedsInspect
            }
            CanonTransitionOutcome::FailedNeedsReconcile { .. } => {
                VerseTerminal::FailedNeedsReconcile
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
    verse_id: VerseId,
    owner_id: OwnerId,
    request: VerseHandoffRequest,
) -> Result<CanonTransitionOutcome, VerseHandoffError> {
    let authority = match observe_canon_authority_witnessed(
        virtual_log,
        journal_id,
        verse_id,
        owner_id,
    )
    .await
    {
        Ok(authority) => authority,
        Err(CanonAuthorityError::NotOwner { .. } | CanonAuthorityError::Unowned { .. }) => {
            return Ok(CanonTransitionOutcome::ConflictNeedsInspect {
                candidate: request.successor.into_abandoned(),
            });
        }
        Err(error) => return Err(VerseHandoffError::Authority(error)),
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
            verse_id,
        },
    )
    .await?)
}

async fn resolve_standby_route(
    virtual_log: &VirtualLog,
    journal_id: JournalId,
    verse_id: VerseId,
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
    if fence.verse_id != verse_id {
        return Err(CanonRouteError::Authority(
            CanonAuthorityError::VerseMismatch {
                expected: verse_id,
                actual: fence.verse_id,
            },
        ));
    }
    match fence.owner {
        CanonOwner::Unowned => Ok(CanonRoute::Recovering {
            canon_revision: fence.revision,
        }),
        CanonOwner::Owned {
            owner_id,
            endpoint,
            sequencer,
            ..
        } if owner_id != this_owner => {
            let (sequencer_epoch, sequencer_endpoint) = sequencer
                .as_ref()
                .map(|binding| {
                    (
                        Some(binding.epoch),
                        Some(binding.sequencer_endpoint.clone()),
                    )
                })
                .unwrap_or((None, None));
            Ok(CanonRoute::NotOwner {
                canon_revision: fence.revision,
                owner_id,
                endpoint,
                sequencer_epoch,
                sequencer_endpoint,
            })
        }
        CanonOwner::Owned { .. } => Ok(CanonRoute::Recovering {
            canon_revision: fence.revision,
        }),
    }
}

impl std::fmt::Debug for VerseRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerseRuntime")
            .field("journal_id", &self.journal_id)
            .field("verse_id", &self.verse_id)
            .field("owner_id", &self.owner_id)
            .field(
                "phase",
                &match &self.phase {
                    VersePhase::Serving(_) => "Serving",
                    VersePhase::Standby => "Standby",
                    VersePhase::Terminal(_) => "Terminal",
                },
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
    use holylog::drive::LogDrive;
    use holylog::memory::InMemoryLogDrive;
    use holylog::provision::LogletComponents;
    use holylog::virtual_log::{
        ApplicationFence, CompareToken, ConditionalRegister, InMemoryConditionalRegister, LogletId,
        RegisterFuture, VersionedState, VirtualLogState,
    };
    use holylog_correctness::faults::{
        FaultableConditionalRegister, FaultableLogDrive, FaultableSeal,
    };
    use holylog_correctness::{
        ActorId, ActorTrace, ArmedFault, EventKind, FaultController, OperationId, RecordingSink,
        RunId, TraceSink, Verdict, check_trace, payload_digest,
    };
    use scripture::{
        CanonFence, CanonOwner, ChunkLogError, ChunkPolicy, CohortId, JournalId, OwnerEndpoint,
        OwnerId, ProducerId, Record, RecoveryBound, Submission, SystemClock, VerseId, WriterId,
    };

    use super::{
        VerseHandoffError, VerseHandoffRequest, VerseRuntime, VerseRuntimeConfig,
        VerseRuntimeStartError, VerseUnavailable,
    };
    use crate::canon_node::CanonNodeStartError;
    use crate::canon_owner::CanonOwnerError;
    use crate::canon_route::CanonRoute;
    use crate::canon_transition::CanonTransitionOutcome;
    use crate::virtuallog_test_support::VirtualLogHarness;
    use crate::{VerseAdmitError, recover_canon_owner};

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"line-runtime-id!")
    }

    fn verse() -> VerseId {
        VerseId::from_bytes(*b"line-runtime-ln!")
    }

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"line-rt-owner-a!")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"line-rt-owner-b!")
    }

    fn config(owner: OwnerId) -> VerseRuntimeConfig {
        VerseRuntimeConfig {
            journal_id: journal(),
            verse_id: verse(),
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
        let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
        CanonFence::new(
            revision,
            journal(),
            verse(),
            CanonOwner::Owned {
                owner_id: owner,
                endpoint,
                sequencer: None,
                writer_term: None,
            },
        )
    }

    fn owned(owner: OwnerId) -> CanonOwner {
        let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
        CanonOwner::Owned {
            owner_id: owner,
            endpoint,
            sequencer: None,
            writer_term: None,
        }
    }

    async fn line_harness() -> VirtualLogHarness {
        VirtualLogHarness::with_ids(
            "line-rt-first",
            "line-rt-second",
            "line-rt-third",
            Arc::new(InMemoryConditionalRegister::new()),
        )
        .await
    }

    async fn line_harness_with_register(
        register: Arc<dyn ConditionalRegister>,
    ) -> VirtualLogHarness {
        VirtualLogHarness::with_ids("line-rt-first", "line-rt-second", "line-rt-third", register)
            .await
    }

    fn traced_components(
        faults: Arc<FaultController>,
        trace: ActorTrace,
        loglet_id: &LogletId,
    ) -> LogletComponents {
        let drive = Arc::new(FaultableLogDrive::new(
            Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
            Arc::clone(&faults),
            trace.clone(),
            loglet_id.as_str(),
        ));
        let seal = Arc::new(FaultableSeal::new(
            Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
            faults,
            trace,
            loglet_id.as_str(),
        ));
        LogletComponents::new(
            drive as Arc<dyn LogDrive>,
            seal as Arc<dyn Seal>,
            Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
            0,
        )
    }

    fn trace_committed_receipt(
        trace: &ActorTrace,
        operation_id: OperationId,
        receipt: &scripture::Receipt,
        loglet_id: &LogletId,
    ) {
        let bytes = receipt.chunk_id.as_bytes();
        trace.emit(
            Some(operation_id),
            EventKind::ScriptureCommittedAck {
                logical_offset: receipt.first_offset.get(),
                digest: payload_digest(&bytes),
                size: bytes.len(),
                loglet_id: loglet_id.as_str().into(),
            },
        );
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
        let harness = line_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;
        let runtime = VerseRuntime::start(
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
        let harness = line_harness().await;
        harness.bootstrap_first(fence(0, owner_b()).encode()).await;
        let runtime = VerseRuntime::start(
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
            Err(VerseAdmitError::Unavailable(VerseUnavailable::Standby))
        ));
        assert_eq!(
            harness.virtual_log().check_tail().await.expect("tail").tail,
            0
        );

        let harness = line_harness().await;
        harness
            .bootstrap_first(CanonFence::new(0, journal(), verse(), CanonOwner::Unowned).encode())
            .await;
        let runtime = VerseRuntime::start(
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
        let harness = line_harness().await;
        harness.bootstrap_first(fence(0, owner_b()).encode()).await;
        let runtime = VerseRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("standby");
        let second = LogletId::new("line-rt-promote").expect("id");
        harness
            .reconfigure_id(&second, fence(1, owner_a()).encode())
            .await;
        assert!(matches!(
            runtime.resolve_route().await.expect("route"),
            CanonRoute::Recovering { canon_revision: 1 }
        ));
        assert!(runtime.is_standby());
    }

    #[tokio::test]
    async fn malformed_mismatch_and_mid_recovery_are_typed_errors() {
        let harness = line_harness().await;
        assert!(matches!(
            VerseRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(VerseRuntimeStartError::VirtualLog(_))
        ));

        harness
            .bootstrap_first(ApplicationFence::new(b"not-a-canon-fence".to_vec()))
            .await;
        assert!(matches!(
            VerseRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(VerseRuntimeStartError::Fence(_))
        ));

        let harness = line_harness().await;
        harness
            .bootstrap_first(
                CanonFence::new(
                    0,
                    JournalId::from_bytes(*b"other-journal!!!"),
                    verse(),
                    owned(owner_a()),
                )
                .encode(),
            )
            .await;
        assert!(matches!(
            VerseRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(VerseRuntimeStartError::Authority(_))
        ));

        let flip = Arc::new(FlipRegister::new(2));
        let harness =
            line_harness_with_register(Arc::clone(&flip) as Arc<dyn ConditionalRegister>).await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;
        flip.arm(VirtualLogState {
            revision: 1,
            generations: vec![holylog::virtual_log::GenerationDescriptor {
                loglet_id: harness.first.clone(),
                start: 0,
            }],
            application_fence: fence(1, owner_a()).encode(),
        });
        assert!(matches!(
            VerseRuntime::start(
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
        let harness = line_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;
        let runtime = VerseRuntime::start(
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
        let successor = harness.provision(&second, 0).await;
        let (runtime, outcome) = runtime
            .drain_seal_publish(VerseHandoffRequest {
                successor,
                next_owner: owned(owner_b()),
                journal_id: journal(),
                verse_id: verse(),
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
            Err(VerseAdmitError::Unavailable(VerseUnavailable::Terminal))
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
        let harness = line_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;
        let runtime = VerseRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        let second = LogletId::new("line-rt-mismatch").expect("id");
        let successor = harness.provision(&second, 0).await;
        let reject = runtime
            .drain_seal_publish(VerseHandoffRequest {
                successor,
                next_owner: owned(owner_b()),
                journal_id: JournalId::from_bytes(*b"other-journal-id"),
                verse_id: verse(),
            })
            .await
            .expect_err("mismatch");
        assert!(matches!(
            reject.error,
            VerseHandoffError::IdentityMismatch { .. }
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
        let harness = line_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;
        let runtime = VerseRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        let second = LogletId::new("line-rt-race").expect("id");
        harness
            .reconfigure_id(&second, fence(1, owner_b()).encode())
            .await;
        let loser = LogletId::new("line-rt-loser").expect("id");
        let loser_successor = harness.provision(&loser, 0).await;
        let (runtime, outcome) = runtime
            .drain_seal_publish(VerseHandoffRequest {
                successor: loser_successor,
                next_owner: owned(owner_b()),
                journal_id: journal(),
                verse_id: verse(),
            })
            .await
            .expect("handoff attempt");
        assert!(matches!(
            outcome,
            CanonTransitionOutcome::ConflictNeedsInspect { .. }
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
            Err(VerseAdmitError::Unavailable(VerseUnavailable::Terminal))
        ));
    }

    #[tokio::test]
    async fn unowned_publish_is_terminal_without_starting_owner() {
        let harness = line_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;
        let runtime = VerseRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start");
        let second = LogletId::new("line-rt-unowned").expect("id");
        let successor = harness.provision(&second, 0).await;
        let (runtime, outcome) = runtime
            .drain_seal_publish(VerseHandoffRequest {
                successor,
                next_owner: CanonOwner::Unowned,
                journal_id: journal(),
                verse_id: verse(),
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

    #[tokio::test]
    async fn real_verse_runtime_reconciles_applied_root_cas_reply_loss() {
        let run = RunId::new("scripture-runtime-root-cas-reply-loss-1");
        let sink = RecordingSink::new().shared();
        let foundation = ActorTrace::new(
            run.clone(),
            ActorId::new("foundation"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        );
        let root_faults = Arc::new(FaultController::new());
        let register = Arc::new(FaultableConditionalRegister::new(
            Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
            Arc::clone(&root_faults),
            foundation.clone(),
        ));
        let harness = VirtualLogHarness::with_ids(
            "correctness-runtime-first",
            "correctness-runtime-second",
            "correctness-runtime-third",
            register as Arc<dyn ConditionalRegister>,
        )
        .await;

        let first_faults = Arc::new(FaultController::new());
        let first = harness
            .fleet
            .provision_with_components(
                &harness.first,
                traced_components(
                    Arc::clone(&first_faults),
                    foundation.clone(),
                    &harness.first,
                ),
            )
            .await;
        harness
            .virtual_log()
            .bootstrap_with_receipt(
                first.receipt,
                first.writable.as_ref(),
                &first.bind,
                fence(0, owner_a()).encode(),
            )
            .await
            .expect("bootstrap A");

        let runtime_a = VerseRuntime::start(
            config(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start A");
        let producer = ProducerId::from_bytes(*b"correct-prod-000");
        let receipt_a = runtime_a
            .submit(Submission {
                producer_id: producer,
                producer_epoch: 0,
                sequence: 0,
                records: vec![Record::new(
                    [],
                    bytes::Bytes::from_static(b"before-cutover"),
                )],
            })
            .await
            .expect("admit A");
        runtime_a.flush().await.expect("flush A");
        let receipt_a = receipt_a.await.expect("commit A");
        trace_committed_receipt(
            &ActorTrace::new(
                run.clone(),
                ActorId::new("scripture-a"),
                Arc::clone(&sink) as Arc<dyn TraceSink>,
            ),
            OperationId::new("submission-a-0"),
            &receipt_a,
            &harness.first,
        );

        let second_faults = Arc::new(FaultController::new());
        let successor = harness
            .fleet
            .provision_with_components(
                &harness.second,
                traced_components(
                    Arc::clone(&second_faults),
                    foundation.clone(),
                    &harness.second,
                ),
            )
            .await;
        root_faults.arm(ArmedFault::RootCasReplyLost);
        let (runtime_a, outcome) = runtime_a
            .drain_seal_publish(VerseHandoffRequest {
                successor,
                next_owner: owned(owner_b()),
                journal_id: journal(),
                verse_id: verse(),
            })
            .await
            .expect("applied root CAS is reported as a terminal reconciliation outcome");
        assert!(matches!(
            outcome,
            CanonTransitionOutcome::FailedNeedsReconcile { .. }
        ));
        assert!(runtime_a.is_terminal());

        let observed = harness
            .virtual_log()
            .observe_membership()
            .await
            .expect("read back applied root CAS");
        assert_eq!(observed.state.revision, 1);
        assert_eq!(
            observed.state.active().expect("active").loglet_id,
            harness.second
        );

        let runtime_b = VerseRuntime::start(
            config(owner_b()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start B from readback evidence");
        assert!(runtime_b.is_serving());
        let receipt_b = runtime_b
            .submit(Submission {
                producer_id: producer,
                producer_epoch: 0,
                sequence: 1,
                records: vec![Record::new([], bytes::Bytes::from_static(b"after-cutover"))],
            })
            .await
            .expect("admit B");
        runtime_b.flush().await.expect("flush B");
        let receipt_b = receipt_b.await.expect("commit B");
        trace_committed_receipt(
            &ActorTrace::new(
                run,
                ActorId::new("scripture-b"),
                Arc::clone(&sink) as Arc<dyn TraceSink>,
            ),
            OperationId::new("submission-b-1"),
            &receipt_b,
            &harness.second,
        );
        assert_eq!(receipt_a.first_offset.get(), 0);
        assert_eq!(receipt_b.first_offset.get(), 1);
        assert_eq!(receipt_b.canon_revision, 1);

        let events = sink.events();
        assert!(
            matches!(check_trace(&events), Verdict::Pass),
            "real Scripture runtime trace must satisfy Holylog checker: {events:#?}"
        );
    }
}
