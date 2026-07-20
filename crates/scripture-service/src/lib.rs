//! Transport-neutral submission primitives for Scripture journals.
//!
//! The legacy [`JournalActor`] path remains for lab adapters still wired to the
//! v0 [`scripture::JournalWriter`]. New work targets [`ChunkJournalService`],
//! which routes Phase 1 [`scripture::ChunkDriverHandle`] owners without
//! duplicating admission or durability logic.
//!
//! Canon-authorized startup uses [`CanonNode::start`] / [`VerseRuntime::start`]
//! (or [`recover_canon_owner`] then [`ChunkJournalService::register_canon_owner`]).
//! Operator-directed A→B handoff uses [`VerseRuntime::drain_seal_publish`] (or
//! [`ChunkJournalService::drain_owner`] then [`publish_canon_transition`]).
//! Clients discover who may serve a Verse via [`resolve_canon_route`].
//! [`ChunkJournalService::register_owner`] remains a local lab registry only and
//! cannot drain for Canon publish.

mod authority_coordinator;
mod canon_node;
mod canon_owner;
mod canon_route;
mod canon_transition;
mod chunk_service;
mod legacy_journal;
pub mod reconcile;
pub mod runtime_observation;
mod scripture_node;
mod verse_runtime;

#[cfg(any(test, feature = "virtuallog-test-support"))]
pub mod virtuallog_test_support;

pub use authority_coordinator::{
    AuthorityCoordinator, CoordinatorError, CoordinatorFuture, DeterministicTransitionIdGenerator,
    FoundationTransitionError, JournalFoundationTransition, LocalServingEligibility,
    ObservedRootAuthority, SecureTransitionIdGenerator, TransitionClassification,
    TransitionIdGenerationError, TransitionIdGenerator,
};
pub use canon_node::{
    CanonNode, CanonNodeConfig, CanonNodeStart, CanonNodeStartError, CanonStandbyRoute,
};
pub use canon_owner::{
    CanonOwnerError, CanonOwnerRequest, RecoveredCanonOwner, recover_canon_owner,
};
pub use canon_route::{
    AdmissionDisposition, CanonRoute, CanonRouteError, admission_for, resolve_canon_route,
    resolve_canon_route_with_epoch,
};
pub use canon_transition::{
    AbandonedProvisionCandidate, CanonTransitionError, CanonTransitionOutcome,
    CanonTransitionRequest, ProvisionedSuccessor, PublishedCanon, publish_canon_transition,
};
pub use chunk_service::{
    ChunkJournalService, ChunkServiceError, DrainError, DrainedOwner, LocalCanonOwnerMatch,
    OwnerHealth, OwnerStatus,
};
#[allow(deprecated)]
pub use legacy_journal::{AckFuture, JournalActor, JournalHandle, ServiceError};
pub use reconcile::{
    OperatorQuestion, PlannedAction, ReconciliationState, RecoveryAction, RecoveryConfidence,
    RecoveryFacts, RecoveryFinding, RecoveryMode, RecoveryPlan, plan as plan_recovery,
};
pub use runtime_observation::{
    EventSequencer, NoopRuntimeObserver, OperationContext, RuntimeObservationSession,
    RuntimeObserver,
};
pub use scripture_node::{
    ScriptureNode, ScriptureNodeConfigError, ScriptureNodeError, ScriptureNodeHandoffError,
    ScriptureNodeStart, VerseKey,
};
pub use verse_runtime::{
    VerseAdmitError, VerseHandoffError, VerseHandoffFailure, VerseHandoffRequest, VerseRuntime,
    VerseRuntimeConfig, VerseRuntimeStartError, VerseTerminal, VerseUnavailable,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use holylog::atomic::AtomicLog;
    use holylog::drive::{DriveError, DriveFuture, LogDrive};
    use holylog::logdrive::{Address, ReferenceLogDrive, TailDescription};
    use holylog::memory::InMemoryLogDrive;
    use scripture::{
        AckLevel, AttributeValue, ChunkDriverActor, ChunkDriverHandle, ChunkLogWriter, ChunkPolicy,
        CohortId, JournalId, ProducerId, Record, RecordOffset, RecoveryBound,
        Submission as ChunkSubmission, SystemClock, WriterId,
    };

    use super::{ChunkJournalService, ChunkServiceError, OwnerStatus};

    fn record(number: i64) -> Record {
        Record::new(
            [("number".into(), AttributeValue::I64(number))],
            Bytes::from(format!("record-{number}")),
        )
    }

    fn policy() -> ChunkPolicy {
        ChunkPolicy {
            max_chunk_bytes: 64 * 1024,
            max_record_bytes: 16 * 1024,
            max_chunk_records: 8,
            max_chunk_age: Duration::from_secs(60),
            max_buffered_bytes: 64 * 1024,
            max_inflight_chunks: 1,
            max_uncommitted_age: Duration::from_secs(60),
            recovery_scan: RecoveryBound::new(16).expect("bound"),
        }
    }

    fn cohort() -> CohortId {
        CohortId::from_bytes(*b"svc-cohort!!!!!!")
    }

    fn writer_id() -> WriterId {
        WriterId::from_bytes(*b"svc-writer!!!!!!")
    }

    fn producer() -> ProducerId {
        ProducerId::from_bytes(*b"svc-producer!!!!")
    }

    fn chunk_submission(sequence: u64, value: i64) -> ChunkSubmission {
        ChunkSubmission {
            producer_id: producer(),
            producer_epoch: 1,
            sequence,
            records: vec![record(value)],
        }
    }

    fn build_owner(
        journal_id: JournalId,
        drive: Arc<dyn LogDrive>,
    ) -> (
        ChunkDriverHandle,
        ChunkDriverActor<SystemClock, scripture::SystemTimer>,
    ) {
        let log = AtomicLog::builder(drive, 0).build().expect("log");
        let writer = ChunkLogWriter::new(journal_id, cohort(), 1, log, RecordOffset::new(0));
        let (clock, timer) = SystemClock::pair();
        ChunkDriverActor::new(
            journal_id,
            cohort(),
            writer_id(),
            1,
            writer,
            &[],
            policy(),
            clock,
            timer,
            16,
        )
        .expect("actor")
    }

    #[tokio::test]
    async fn registered_owner_returns_committed_receipt() {
        let journal = JournalId::from_bytes(*b"chunk-svc-jour!!");
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let (handle, actor) = build_owner(journal, drive);
        let mut service = ChunkJournalService::new();
        service
            .register_owner(journal, 1, handle, actor)
            .expect("register");

        let receipt = service
            .submit(journal, chunk_submission(0, 1))
            .await
            .expect("admit");
        service.flush(journal).await.expect("flush");
        let receipt = receipt.await.expect("committed");
        assert_eq!(receipt.level, AckLevel::Committed);
        assert_eq!(receipt.first_offset, RecordOffset::new(0));
        assert_eq!(
            service.health(journal).expect("health").status,
            OwnerStatus::Running
        );
    }

    #[tokio::test]
    async fn unknown_journal_fails_with_no_append() {
        let service = ChunkJournalService::new();
        let journal = JournalId::from_bytes(*b"missing-journal!");
        let err = match service.submit(journal, chunk_submission(0, 1)).await {
            Err(error) => error,
            Ok(_) => panic!("unknown journal must not admit"),
        };
        assert!(matches!(
            err,
            ChunkServiceError::UnknownJournal { journal_id } if journal_id == journal
        ));
    }

    #[tokio::test]
    async fn two_journals_keep_independent_offsets() {
        let first_id = JournalId::from_bytes(*b"journal-one!!!!!");
        let second_id = JournalId::from_bytes(*b"journal-two!!!!!");
        let mut service = ChunkJournalService::new();
        let (h1, a1) = build_owner(
            first_id,
            Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
        );
        let (h2, a2) = build_owner(
            second_id,
            Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
        );
        service.register_owner(first_id, 1, h1, a1).expect("first");
        service
            .register_owner(second_id, 1, h2, a2)
            .expect("second");

        let r1 = service
            .submit(first_id, chunk_submission(0, 10))
            .await
            .expect("admit 1");
        let r2 = service
            .submit(second_id, chunk_submission(0, 20))
            .await
            .expect("admit 2");
        service.flush(first_id).await.expect("flush 1");
        service.flush(second_id).await.expect("flush 2");
        let r1 = r1.await.expect("receipt 1");
        let r2 = r2.await.expect("receipt 2");
        assert_eq!(r1.first_offset, RecordOffset::new(0));
        assert_eq!(r2.first_offset, RecordOffset::new(0));
        assert_eq!(r1.journal_id, first_id);
        assert_eq!(r2.journal_id, second_id);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("injected durable-then-error")]
    struct InjectedFailure;

    #[derive(Debug, Default)]
    struct FailAfterWriteDrive {
        model: std::sync::Mutex<ReferenceLogDrive>,
        armed: std::sync::atomic::AtomicBool,
    }

    impl FailAfterWriteDrive {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    impl LogDrive for FailAfterWriteDrive {
        fn write(&self, address: Address, value: Bytes) -> DriveFuture<'_, ()> {
            Box::pin(async move {
                self.model
                    .lock()
                    .map_err(|_| DriveError::backend(InjectedFailure))?
                    .write(address, value)?;
                if self.armed.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(DriveError::backend(InjectedFailure));
                }
                Ok(())
            })
        }

        fn read(&self, address: Address) -> DriveFuture<'_, Option<Bytes>> {
            Box::pin(async move {
                Ok(self
                    .model
                    .lock()
                    .map_err(|_| DriveError::backend(InjectedFailure))?
                    .read(address)
                    .cloned())
            })
        }

        fn weak_tail(&self, k: u64) -> DriveFuture<'_, TailDescription> {
            Box::pin(async move {
                Ok(self
                    .model
                    .lock()
                    .map_err(|_| DriveError::backend(InjectedFailure))?
                    .weak_tail(k))
            })
        }
    }

    #[tokio::test]
    async fn drive_failure_poisons_owner_and_blocks_later_submits() {
        let journal = JournalId::from_bytes(*b"poison-journal!!");
        let drive = FailAfterWriteDrive::new();
        drive.arm();
        let (handle, actor) = build_owner(journal, Arc::clone(&drive) as Arc<dyn LogDrive>);
        let mut service = ChunkJournalService::new();
        service
            .register_owner(journal, 1, handle, actor)
            .expect("register");

        let receipt = service
            .submit(journal, chunk_submission(0, 1))
            .await
            .expect("admit");
        let _ = service.flush(journal).await;
        let _ = receipt.await;
        tokio::task::yield_now().await;

        let later = service.submit(journal, chunk_submission(1, 2)).await;
        match later {
            Err(ChunkServiceError::OwnerUnavailable { status, .. }) => {
                assert_eq!(status, OwnerStatus::Poisoned);
            }
            Ok(future) => {
                assert!(future.await.is_err());
                assert_eq!(
                    service.health(journal).expect("health").status,
                    OwnerStatus::Poisoned
                );
            }
            Err(other) => panic!("unexpected later submit: {other:?}"),
        }
        assert_eq!(
            service.health(journal).expect("health").status,
            OwnerStatus::Poisoned
        );
    }

    #[tokio::test]
    async fn poison_is_visible_in_health_without_a_followup_request() {
        let journal = JournalId::from_bytes(*b"poison-health!!!");
        let drive = FailAfterWriteDrive::new();
        drive.arm();
        let (handle, actor) = build_owner(journal, Arc::clone(&drive) as Arc<dyn LogDrive>);
        let mut service = ChunkJournalService::new();
        service
            .register_owner(journal, 1, handle, actor)
            .expect("register");

        let receipt = service
            .submit(journal, chunk_submission(0, 1))
            .await
            .expect("admit");
        // Flush may itself surface the uncertain append; either way we only wait
        // for the first receipt's terminal result — no later submit/flush.
        let _ = service.flush(journal).await;
        assert!(receipt.await.is_err());
        tokio::task::yield_now().await;

        assert_eq!(
            service.health(journal).expect("health").status,
            OwnerStatus::Poisoned,
            "health must read DriverMetrics.poisoned without a follow-up request"
        );
    }

    #[tokio::test]
    async fn dropped_receipt_through_service_still_commits() {
        let journal = JournalId::from_bytes(*b"drop-receipt-jr!");
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let (handle, actor) = build_owner(journal, drive);
        let mut service = ChunkJournalService::new();
        service
            .register_owner(journal, 1, handle, actor)
            .expect("register");

        let receipt = service
            .submit(journal, chunk_submission(0, 7))
            .await
            .expect("admit");
        drop(receipt);
        service.flush(journal).await.expect("flush");

        let retry = service
            .submit(journal, chunk_submission(0, 7))
            .await
            .expect("retry admit");
        let retry = retry.await.expect("dedup receipt");
        assert!(retry.deduplicated);
        assert_eq!(retry.first_offset, RecordOffset::new(0));
    }

    #[tokio::test]
    async fn finished_actor_task_is_visible_in_health() {
        let journal = JournalId::from_bytes(*b"finished-owner!!");
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let (handle, actor) = build_owner(journal, drive);
        let mut service = ChunkJournalService::new();
        service
            .register_owner(journal, 1, handle, actor)
            .expect("register");
        assert_eq!(
            service.health(journal).expect("health").status,
            OwnerStatus::Running
        );

        service.stop_owner(journal).await.expect("stop");
        assert_eq!(
            service.health(journal).expect("health").status,
            OwnerStatus::TaskFinished
        );
        let err = match service.submit(journal, chunk_submission(0, 1)).await {
            Err(error) => error,
            Ok(_) => panic!("stopped owner must not admit"),
        };
        assert!(matches!(
            err,
            ChunkServiceError::OwnerUnavailable {
                status: OwnerStatus::TaskFinished,
                ..
            }
        ));
        // Same registry entry — no second writer was created.
        assert_eq!(
            service
                .health(journal)
                .expect("health")
                .registered_owner_generation,
            1
        );
    }
}
