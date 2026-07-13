//! Transport-neutral submission primitives for Scripture journals.
//!
//! The legacy [`JournalActor`] path remains for lab adapters still wired to the
//! v0 [`scripture::JournalWriter`]. New work targets [`ChunkJournalService`],
//! which routes Phase 1 [`scripture::ChunkDriverHandle`] owners without
//! duplicating admission or durability logic.
//!
//! Canon-authorized startup uses [`CanonNode::start`] (or
//! [`recover_canon_owner`] then [`ChunkJournalService::register_canon_owner`]).
//! Operator-directed A→B handoff uses [`ChunkJournalService::drain_owner`] then
//! [`publish_canon_transition`]. Clients discover who may serve a Line via
//! [`resolve_canon_route`]. [`ChunkJournalService::register_owner`] remains a
//! local lab registry only and cannot drain for Canon publish.

mod canon_node;
mod canon_owner;
mod canon_route;
mod canon_transition;
mod chunk_service;
pub mod reconcile;

pub use canon_node::{
    CanonNode, CanonNodeConfig, CanonNodeConfigError, CanonNodeStart, CanonNodeStartError,
};
pub use canon_owner::{
    CanonOwnerError, CanonOwnerRequest, RecoveredCanonOwner, recover_canon_owner,
};
pub use canon_route::{CanonRoute, CanonRouteError, resolve_canon_route};
pub use canon_transition::{
    CanonTransitionError, CanonTransitionOutcome, CanonTransitionRequest, PublishedCanon,
    publish_canon_transition,
};
pub use chunk_service::{
    ChunkJournalService, ChunkServiceError, DrainError, DrainedOwner, LocalCanonOwnerMatch,
    OwnerHealth, OwnerStatus,
};
pub use reconcile::{
    OperatorQuestion, PlannedAction, ReconciliationState, RecoveryAction, RecoveryConfidence,
    RecoveryFacts, RecoveryFinding, RecoveryMode, RecoveryPlan, plan as plan_recovery,
};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use scripture::{AppendAck, CodecError, JournalWriter, Record, WriteError};
use tokio::sync::{mpsc, oneshot};

/// Errors exposed by the legacy service submission boundary.
///
/// `Unavailable` is intentionally coarse. A kernel failure can leave a zombie
/// write durable while making the actor unable to assign another safe range;
/// callers receive no false acknowledgement and must recover at a later,
/// explicitly fenced generation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServiceError {
    /// A request did not name any records.
    #[error("cannot submit an empty record batch")]
    EmptyBatch,
    /// The submitted record cannot be represented by the durable format.
    #[error("invalid record submission")]
    InvalidRequest,
    /// The bounded submission queue is closed or the actor has terminally
    /// failed. A prior failed append may still be visible after recovery.
    #[error("journal service is unavailable")]
    Unavailable,
    /// The log is sealed. The named slot is informational only.
    #[error("journal is sealed after durable write at slot {slot}")]
    Sealed {
        /// Sequencer slot observed when the log sealed.
        slot: u64,
    },
}

/// Future returned by [`JournalHandle::submit`].
///
/// It resolves only after the containing batch is durably acknowledged, or
/// with a terminal service error. Dropping it never cancels the durable work.
#[must_use = "durability is learned by awaiting the acknowledgement"]
pub struct AckFuture {
    receiver: oneshot::Receiver<Result<AppendAck, ServiceError>>,
}

impl Future for AckFuture {
    type Output = Result<AppendAck, ServiceError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(context)
            .map(|result| result.unwrap_or(Err(ServiceError::Unavailable)))
    }
}

struct Submission {
    records: Vec<Record>,
    acknowledgement: oneshot::Sender<Result<AppendAck, ServiceError>>,
}

/// Cloneable bounded submission endpoint for one journal.
#[derive(Clone)]
pub struct JournalHandle {
    sender: mpsc::Sender<Submission>,
}

impl JournalHandle {
    /// Stages a non-empty record batch for durable append.
    ///
    /// Waiting to enqueue is the service's first backpressure mechanism. This
    /// v1 slice intentionally emits one durable batch per submission; batching
    /// policy will be introduced behind this boundary rather than in a wire
    /// protocol.
    pub async fn submit(&self, records: Vec<Record>) -> Result<AckFuture, ServiceError> {
        if records.is_empty() {
            return Err(ServiceError::EmptyBatch);
        }
        let (acknowledgement, receiver) = oneshot::channel();
        self.sender
            .send(Submission {
                records,
                acknowledgement,
            })
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        Ok(AckFuture { receiver })
    }
}

/// The single task that owns a v0 `JournalWriter`.
///
/// Run this future exactly once. On the first kernel failure it enters a
/// terminal state and resolves the failed request and every later submission
/// as unavailable (or sealed), so no client acknowledgement is left pending.
pub struct JournalActor {
    writer: JournalWriter,
    receiver: mpsc::Receiver<Submission>,
}

impl JournalActor {
    /// Creates a bounded actor and its cloneable client endpoint.
    #[must_use]
    pub fn new(writer: JournalWriter, queue_capacity: usize) -> (JournalHandle, Self) {
        let (sender, receiver) = mpsc::channel(queue_capacity);
        (JournalHandle { sender }, Self { writer, receiver })
    }

    /// Drives submissions until every handle is dropped.
    ///
    /// The actor deliberately does not attempt to restart its writer. The
    /// recovery helper's same-process preconditions are not a daemon recovery
    /// protocol; a future VirtualLog/fencing layer owns that transition.
    pub async fn run(mut self) {
        let mut terminal: Option<ServiceError> = None;
        while let Some(submission) = self.receiver.recv().await {
            if let Some(error) = &terminal {
                let _ = submission.acknowledgement.send(Err(error.clone()));
                continue;
            }
            match self.writer.append_batch(submission.records).await {
                Ok(acknowledgement) => {
                    let _ = submission.acknowledgement.send(Ok(acknowledgement));
                }
                Err(WriteError::Log(holylog::atomic::AtomicLogError::Sealed { address })) => {
                    let error = ServiceError::Sealed { slot: address };
                    terminal = Some(error.clone());
                    let _ = submission.acknowledgement.send(Err(error));
                }
                Err(WriteError::Log(_))
                | Err(WriteError::Poisoned)
                | Err(WriteError::Codec(CodecError::OffsetOverflow)) => {
                    terminal = Some(ServiceError::Unavailable);
                    let _ = submission
                        .acknowledgement
                        .send(Err(ServiceError::Unavailable));
                }
                Err(WriteError::EmptyBatch)
                | Err(WriteError::TooManyRecords)
                | Err(WriteError::Codec(_))
                | Err(WriteError::JournalMismatch { .. }) => {
                    let _ = submission
                        .acknowledgement
                        .send(Err(ServiceError::InvalidRequest));
                }
            }
        }
    }
}

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
        CohortId, JournalId, JournalWriter, ProducerId, Record, RecordOffset, RecoveryBound,
        Submission as ChunkSubmission, SystemClock, WriterId,
    };

    use super::{ChunkJournalService, ChunkServiceError, JournalActor, OwnerStatus, ServiceError};

    fn writer() -> JournalWriter {
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let log = AtomicLog::builder(drive, 4).build().expect("build log");
        JournalWriter::new(
            JournalId::from_bytes(*b"service-test!!!!"),
            log,
            RecordOffset::new(0),
        )
    }

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
    async fn concurrent_submitters_receive_dense_durable_acknowledgements() {
        let (handle, actor) = JournalActor::new(writer(), 4);
        let actor = tokio::spawn(actor.run());
        let first = handle.submit(vec![record(1)]).await.expect("enqueue first");
        let second = handle
            .submit(vec![record(2), record(3)])
            .await
            .expect("enqueue second");
        let first = first.await.expect("first durable");
        let second = second.await.expect("second durable");
        assert_eq!(first.first_offset.get(), 0);
        assert_eq!(first.next_offset.get(), 1);
        assert_eq!(second.first_offset.get(), 1);
        assert_eq!(second.next_offset.get(), 3);
        drop(handle);
        actor.await.expect("actor exits");
    }

    #[tokio::test]
    async fn sealed_failure_transitions_the_actor_and_never_strands_later_acknowledgements() {
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let log = AtomicLog::builder(drive, 4).build().expect("build log");
        log.seal().await.expect("seal");
        let writer = JournalWriter::new(
            JournalId::from_bytes(*b"service-test!!!!"),
            log,
            RecordOffset::new(0),
        );
        let (handle, actor) = JournalActor::new(writer, 4);
        let actor = tokio::spawn(actor.run());
        let first = handle.submit(vec![record(1)]).await.expect("enqueue first");
        let second = handle
            .submit(vec![record(2)])
            .await
            .expect("enqueue second");
        assert!(matches!(first.await, Err(ServiceError::Sealed { .. })));
        assert!(matches!(second.await, Err(ServiceError::Sealed { .. })));
        drop(handle);
        actor.await.expect("actor exits");
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
                    .weak_tail(k)?)
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
