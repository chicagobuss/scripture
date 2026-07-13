//! Operator-directed Canon drain → seal → publish transition.
//!
//! Pair with [`crate::recover_canon_owner`]: owner A drains and publishes; owner B
//! later constructs from durable evidence. Failed transitions stay visibly
//! drained — never auto-resume A.

use holylog::virtual_log::{LogletId, Reconfiguration, VirtualLog, VirtualLogError};
use scripture::{
    CanonAuthorityError, CanonFence, CanonOwner, JournalId, LineId, OwnerId,
    WitnessedCanonAuthority,
};

use crate::chunk_service::{ChunkJournalService, ChunkServiceError, DrainedOwner};

/// Inputs for one fenced Canon publish attempt after a successful local drain.
#[derive(Debug)]
pub struct CanonTransitionRequest {
    /// Exact validated observation/witness for current owner A.
    pub authority: WitnessedCanonAuthority,
    /// Local drain proof for A.
    pub drained: DrainedOwner,
    /// Private fresh successor Loglet (empty, open, resolvable).
    pub successor: LogletId,
    /// Desired successor Canon owner (B or explicit [`CanonOwner::Unowned`]).
    pub next_owner: CanonOwner,
    /// Expected journal identity.
    pub journal_id: JournalId,
    /// Expected physical Line identity.
    pub line_id: LineId,
}

/// Successful publication details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedCanon {
    /// Sealed predecessor Loglet.
    pub predecessor: LogletId,
    /// Published successor Loglet.
    pub successor: LogletId,
    /// Global cutover boundary.
    pub boundary: u64,
    /// Fence published with the successor membership.
    pub fence: CanonFence,
}

/// Typed outcome of one publish attempt.
///
/// Conflict and failure leave the local owner drained for inspect/reconcile.
#[derive(Debug)]
pub enum CanonTransitionOutcome {
    /// Membership and Canon fence published together; local A was stopped.
    Published(PublishedCanon),
    /// CAS lost to a competing transition; A's supplied fence was not published.
    ConflictNeedsInspect,
    /// Seal/CAS path failed after drain; A remains drained.
    FailedNeedsReconcile {
        /// Underlying Holylog failure.
        error: VirtualLogError,
    },
}

/// Failures that refuse to attempt Holylog reconfiguration.
#[derive(Debug, thiserror::Error)]
pub enum CanonTransitionError {
    /// Drain token does not match the supplied authority observation.
    #[error("drained owner does not match witnessed Canon authority")]
    DrainAuthorityMismatch {
        /// Drain journal.
        drained_journal: JournalId,
        /// Drain owner.
        drained_owner: OwnerId,
        /// Drain revision.
        drained_revision: u64,
        /// Authority revision.
        authority_revision: u64,
    },
    /// Journal or Line identity mismatch.
    #[error(transparent)]
    Authority(#[from] CanonAuthorityError),
    /// Next Canon revision would overflow.
    #[error("Canon revision overflow from {revision}")]
    RevisionOverflow {
        /// Current revision that cannot be incremented.
        revision: u64,
    },
    /// Local service failed while permanently stopping A after publish.
    #[error(transparent)]
    Service(#[from] ChunkServiceError),
}

/// Publishes a successor VirtualLog mapping plus CanonFence from a drained owner.
///
/// On [`CanonTransitionOutcome::Published`], permanently stops local A.
/// On conflict or Holylog error after drain, A stays drained and is never
/// auto-resumed.
pub async fn publish_canon_transition(
    service: &mut ChunkJournalService,
    virtual_log: &VirtualLog,
    request: CanonTransitionRequest,
) -> Result<CanonTransitionOutcome, CanonTransitionError> {
    // Refuse inconsistent witnesses before computing next_revision or sealing.
    request.authority.validate()?;
    validate_transition_inputs(&request)?;

    let next_revision = request.authority.revision().checked_add(1).ok_or(
        CanonTransitionError::RevisionOverflow {
            revision: request.authority.revision(),
        },
    )?;
    let next_fence = CanonFence::new(
        next_revision,
        request.journal_id,
        request.line_id,
        request.next_owner,
    );
    // Encode validates owner/endpoint schema before Holylog stores opaque bytes.
    let application_fence = next_fence.encode();

    let outcome = virtual_log
        .reconfigure_from_observation(
            request.authority.observed(),
            request.successor.clone(),
            application_fence,
        )
        .await;

    match outcome {
        Ok(Reconfiguration::Applied {
            predecessor,
            successor,
            boundary,
            revision,
        }) => {
            debug_assert_eq!(revision, next_revision);
            service.stop_owner(request.drained.journal_id()).await?;
            Ok(CanonTransitionOutcome::Published(PublishedCanon {
                predecessor,
                successor,
                boundary,
                fence: next_fence,
            }))
        }
        Ok(Reconfiguration::Conflict) => Ok(CanonTransitionOutcome::ConflictNeedsInspect),
        Err(error) => Ok(CanonTransitionOutcome::FailedNeedsReconcile { error }),
    }
}

fn validate_transition_inputs(
    request: &CanonTransitionRequest,
) -> Result<(), CanonTransitionError> {
    if request.drained.journal_id() != request.journal_id
        || request.drained.line_id() != request.line_id
        || request.drained.owner_id()
            != match &request.authority.fence().owner {
                CanonOwner::Owned { owner_id, .. } => *owner_id,
                CanonOwner::Unowned => {
                    return Err(CanonTransitionError::Authority(
                        CanonAuthorityError::Unowned {
                            revision: request.authority.revision(),
                            line_id: request.authority.fence().line_id,
                        },
                    ));
                }
            }
        || request.drained.revision() != request.authority.revision()
    {
        return Err(CanonTransitionError::DrainAuthorityMismatch {
            drained_journal: request.drained.journal_id(),
            drained_owner: request.drained.owner_id(),
            drained_revision: request.drained.revision(),
            authority_revision: request.authority.revision(),
        });
    }
    if request.authority.fence().journal_id != request.journal_id {
        return Err(CanonTransitionError::Authority(
            CanonAuthorityError::JournalMismatch {
                expected: request.journal_id,
                actual: request.authority.fence().journal_id,
            },
        ));
    }
    if request.authority.fence().line_id != request.line_id {
        return Err(CanonTransitionError::Authority(
            CanonAuthorityError::LineMismatch {
                expected: request.line_id,
                actual: request.authority.fence().line_id,
            },
        ));
    }
    match &request.authority.fence().owner {
        CanonOwner::Unowned => {
            return Err(CanonTransitionError::Authority(
                CanonAuthorityError::Unowned {
                    revision: request.authority.revision(),
                    line_id: request.line_id,
                },
            ));
        }
        CanonOwner::Owned { owner_id, .. } if *owner_id != request.drained.owner_id() => {
            return Err(CanonTransitionError::Authority(
                CanonAuthorityError::NotOwner {
                    revision: request.authority.revision(),
                    expected: request.drained.owner_id(),
                    actual: *owner_id,
                },
            ));
        }
        CanonOwner::Owned { .. } => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use holylog::atomic::AtomicLog;
    use holylog::drive::{DriveError, DriveFuture, LogDrive};
    use holylog::logdrive::{Address, ReferenceLogDrive, TailDescription};
    use holylog::memory::InMemoryLogDrive;
    use holylog::virtual_log::{
        ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver, ResolveFuture,
        VersionedState, VirtualLog, VirtualLogState,
    };
    use scripture::{
        CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, LineId, ManualClock, ManualTimer,
        OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound, Submission, SystemClock,
        WitnessedCanonAuthority, WriterId, observe_canon_authority_witnessed,
    };

    use super::{
        CanonTransitionError, CanonTransitionOutcome, CanonTransitionRequest,
        publish_canon_transition,
    };
    use crate::chunk_service::{
        ChunkJournalService, ChunkServiceError, DrainError, DrainedOwner, OwnerStatus,
    };
    use crate::{CanonOwnerRequest, recover_canon_owner};

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"transit-journal!")
    }

    fn line() -> LineId {
        LineId::from_bytes(*b"transit-line-id!")
    }

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"transit-owner-a!")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"transit-owner-b!")
    }

    fn cohort() -> CohortId {
        CohortId::from_bytes(*b"transit-cohort!!")
    }

    fn writer_id() -> WriterId {
        WriterId::from_bytes(*b"transit-writer!!")
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
            recovery_scan: RecoveryBound::new(8).expect("bound"),
        }
    }

    fn request(owner: OwnerId) -> CanonOwnerRequest {
        CanonOwnerRequest {
            journal_id: journal(),
            line_id: line(),
            owner_id: owner,
            cohort_id: cohort(),
            writer_id: writer_id(),
            policy: policy(),
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 16,
        }
    }

    fn owned(revision: u64, owner: OwnerId) -> CanonFence {
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

    fn owned_owner(owner: OwnerId) -> CanonOwner {
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
        second: LogletId,
        third: LogletId,
    }

    impl Harness {
        fn memory() -> Self {
            Self::with_first_drive(Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>)
        }

        fn with_first_drive(first_drive: Arc<dyn LogDrive>) -> Self {
            let resolver = Arc::new(Resolver::default());
            let first = LogletId::new("transit-first").expect("id");
            let second = LogletId::new("transit-second").expect("id");
            let third = LogletId::new("transit-third").expect("id");
            resolver.insert(
                first.clone(),
                Arc::new(AtomicLog::builder(first_drive, 0).build().expect("log")),
            );
            resolver.insert(
                second.clone(),
                Arc::new(
                    AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                        .build()
                        .expect("log"),
                ),
            );
            resolver.insert(
                third.clone(),
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
                second,
                third,
            }
        }

        fn virtual_log(&self) -> VirtualLog {
            VirtualLog::new(
                Arc::clone(&self.register),
                Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
            )
        }
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

    fn submission(sequence: u64, payload: &'static [u8]) -> Submission {
        Submission {
            producer_id: ProducerId::from_bytes(*b"transit-producer"),
            producer_epoch: 0,
            sequence,
            records: vec![Record::new([], Bytes::from_static(payload))],
        }
    }

    #[tokio::test]
    async fn drain_publish_recover_continues_dense_offsets() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");

        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover a");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");

        let pending = service
            .submit(journal(), submission(0, b"a0"))
            .await
            .expect("admit");
        service.flush(journal()).await.expect("flush");
        pending.await.expect("commit");

        let authority =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("witness");
        let drained = service
            .drain_owner(journal(), &authority)
            .await
            .expect("drain");
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::Draining
        );
        assert!(matches!(
            service.submit(journal(), submission(1, b"late")).await,
            Err(ChunkServiceError::OwnerDraining { .. })
        ));

        let published = publish_canon_transition(
            &mut service,
            &harness.virtual_log(),
            CanonTransitionRequest {
                authority,
                drained,
                successor: harness.second.clone(),
                next_owner: owned_owner(owner_b()),
                journal_id: journal(),
                line_id: line(),
            },
        )
        .await
        .expect("publish");
        assert!(matches!(
            published,
            CanonTransitionOutcome::Published(ref p)
                if p.fence.revision == 1
                    && matches!(
                        &p.fence.owner,
                        CanonOwner::Owned { owner_id, .. } if *owner_id == owner_b()
                    )
        ));
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::TaskFinished
        );

        let recovered_b = recover_canon_owner(
            request(owner_b()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover b");
        let mut service_b = ChunkJournalService::new();
        service_b
            .register_canon_owner(recovered_b)
            .expect("register b");
        let pending_b = service_b
            .submit(journal(), submission(1, b"b1"))
            .await
            .expect("admit b");
        service_b.flush(journal()).await.expect("flush b");
        let receipt = pending_b.await.expect("commit b");
        assert_eq!(receipt.first_offset.get(), 1);
    }

    #[tokio::test]
    async fn competing_transition_leaves_a_drained_without_loser_fence() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");

        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");

        let stale =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("stale witness");
        let drained = service.drain_owner(journal(), &stale).await.expect("drain");

        let winner_fence = owned(1, owner_b());
        harness
            .virtual_log()
            .reconfigure_with_application_fence(harness.second.clone(), winner_fence.encode())
            .await
            .expect("winner");

        let outcome = publish_canon_transition(
            &mut service,
            &harness.virtual_log(),
            CanonTransitionRequest {
                authority: stale,
                drained,
                successor: harness.third.clone(),
                next_owner: owned_owner(OwnerId::from_bytes(*b"transit-owner-c!")),
                journal_id: journal(),
                line_id: line(),
            },
        )
        .await
        .expect("conflict outcome");
        assert!(matches!(
            outcome,
            CanonTransitionOutcome::ConflictNeedsInspect
        ));
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::Draining
        );
        let state = harness.virtual_log().state().await.expect("state");
        assert_eq!(state.application_fence, winner_fence.encode());
        assert_eq!(state.active().expect("active").loglet_id, harness.second);
    }

    #[tokio::test]
    async fn publish_unowned_then_factory_refuses_owners() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");
        let authority =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("witness");
        let drained = service
            .drain_owner(journal(), &authority)
            .await
            .expect("drain");
        let outcome = publish_canon_transition(
            &mut service,
            &harness.virtual_log(),
            CanonTransitionRequest {
                authority,
                drained,
                successor: harness.second.clone(),
                next_owner: CanonOwner::Unowned,
                journal_id: journal(),
                line_id: line(),
            },
        )
        .await
        .expect("publish unowned");
        assert!(matches!(
            outcome,
            CanonTransitionOutcome::Published(ref p)
                if p.fence.owner == CanonOwner::Unowned && p.fence.revision == 1
        ));
        assert!(
            recover_canon_owner(
                request(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .is_err()
        );
        assert!(
            recover_canon_owner(
                request(owner_b()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn poison_flush_blocks_drain_and_publish() {
        let drive = FailAfterWriteDrive::new();
        let harness = Harness::with_first_drive(Arc::clone(&drive) as Arc<dyn LogDrive>);
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");
        drive.arm();
        let pending = service
            .submit(journal(), submission(0, b"poison"))
            .await
            .expect("admit");
        let authority =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("witness");
        let drain = service.drain_owner(journal(), &authority).await;
        let _ = pending.await;
        assert!(matches!(drain, Err(DrainError::DrainFailed { .. })));
        // No successful drain token ⇒ no publish path.
        assert_ne!(
            harness.virtual_log().state().await.expect("state").revision,
            1
        );
    }

    #[tokio::test]
    async fn stale_mismatch_and_non_fresh_fail_closed() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");

        let authority =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("witness");
        let drained = service
            .drain_owner(journal(), &authority)
            .await
            .expect("drain");

        assert!(matches!(
            publish_canon_transition(
                &mut service,
                &harness.virtual_log(),
                CanonTransitionRequest {
                    authority: authority.clone(),
                    drained: DrainedOwner::for_test(
                        JournalId::from_bytes(*b"other-journal!!!"),
                        line(),
                        owner_a(),
                        0,
                    ),
                    successor: harness.second.clone(),
                    next_owner: owned_owner(owner_b()),
                    journal_id: journal(),
                    line_id: line(),
                },
            )
            .await,
            Err(CanonTransitionError::DrainAuthorityMismatch { .. })
        ));

        assert!(matches!(
            publish_canon_transition(
                &mut service,
                &harness.virtual_log(),
                CanonTransitionRequest {
                    authority: authority.clone(),
                    drained: DrainedOwner::for_test(journal(), line(), owner_a(), 0),
                    successor: harness.second.clone(),
                    next_owner: owned_owner(owner_b()),
                    journal_id: journal(),
                    line_id: LineId::from_bytes(*b"other-line-id!!!"),
                },
            )
            .await,
            Err(CanonTransitionError::DrainAuthorityMismatch { .. })
        ));

        let outcome = publish_canon_transition(
            &mut service,
            &harness.virtual_log(),
            CanonTransitionRequest {
                authority,
                drained,
                successor: harness.first.clone(),
                next_owner: owned_owner(owner_b()),
                journal_id: journal(),
                line_id: line(),
            },
        )
        .await
        .expect("failed outcome");
        assert!(matches!(
            outcome,
            CanonTransitionOutcome::FailedNeedsReconcile { .. }
        ));
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::Draining
        );
    }

    #[tokio::test]
    async fn revision_overflow_fails_before_reconfigure() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");
        let authority =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("witness");
        let _drained = service
            .drain_owner(journal(), &authority)
            .await
            .expect("drain");
        // Replace drained revision via test-only constructor.
        let drained = DrainedOwner::for_test(journal(), line(), owner_a(), u64::MAX);
        let authority = WitnessedCanonAuthority::from_parts_for_test(
            VersionedState {
                token: authority.observed().token.clone(),
                state: VirtualLogState {
                    revision: u64::MAX,
                    generations: authority.observed().state.generations.clone(),
                    application_fence: owned(u64::MAX, owner_a()).encode(),
                },
            },
            owned(u64::MAX, owner_a()),
        );
        assert!(matches!(
            publish_canon_transition(
                &mut service,
                &harness.virtual_log(),
                CanonTransitionRequest {
                    authority,
                    drained,
                    successor: harness.second.clone(),
                    next_owner: owned_owner(owner_b()),
                    journal_id: journal(),
                    line_id: line(),
                },
            )
            .await,
            Err(CanonTransitionError::RevisionOverflow { revision: u64::MAX })
        ));
    }

    #[tokio::test]
    async fn lab_register_cannot_drain_for_publish() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        let (recovered_authority, recovered_handle, recovered_actor, _) =
            recovered.into_unmanaged();
        let mut service = ChunkJournalService::new();
        // Lab path: no Canon binding stored.
        service
            .register_owner(
                journal(),
                recovered_authority.revision(),
                recovered_handle,
                recovered_actor,
            )
            .expect("lab register");
        let authority =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("witness");
        assert!(matches!(
            service.drain_owner(journal(), &authority).await,
            Err(DrainError::NotCanonBound { .. })
        ));
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::Running
        );
    }

    #[tokio::test]
    async fn drain_rejects_authority_for_a_different_published_owner() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");

        harness
            .virtual_log()
            .reconfigure_with_application_fence(
                harness.second.clone(),
                owned(1, owner_b()).encode(),
            )
            .await
            .expect("publish B without draining A");
        let authority_b =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_b())
                .await
                .expect("witness B");
        assert!(matches!(
            service.drain_owner(journal(), &authority_b).await,
            Err(DrainError::BindingMismatch { .. })
        ));
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::Running
        );
    }

    #[tokio::test]
    async fn inconsistent_witness_refuses_publish_without_seal_or_stop() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");
        let authority =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("witness");
        let drained = service
            .drain_owner(journal(), &authority)
            .await
            .expect("drain");
        let bad = WitnessedCanonAuthority::from_parts_for_test(
            authority.observed().clone(),
            owned(0, owner_b()),
        );
        assert!(matches!(
            publish_canon_transition(
                &mut service,
                &harness.virtual_log(),
                CanonTransitionRequest {
                    authority: bad,
                    drained,
                    successor: harness.second.clone(),
                    next_owner: owned_owner(owner_b()),
                    journal_id: journal(),
                    line_id: line(),
                },
            )
            .await,
            Err(CanonTransitionError::Authority(
                scripture::CanonAuthorityError::InconsistentWitness
            ))
        ));
        assert_eq!(
            harness.virtual_log().state().await.expect("state").revision,
            0
        );
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::Draining
        );
    }

    #[tokio::test]
    async fn manual_clock_drain_still_flushes_open_chunk() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), owned(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let clock = Arc::new(ManualClock::new());
        let timer = ManualTimer::new(Arc::clone(&clock));
        let recovered =
            recover_canon_owner(request(owner_a()), harness.virtual_log(), clock, timer)
                .await
                .expect("recover");
        let mut service = ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");
        let pending = service
            .submit(journal(), submission(0, b"open"))
            .await
            .expect("admit");
        let authority =
            observe_canon_authority_witnessed(&harness.virtual_log(), journal(), line(), owner_a())
                .await
                .expect("witness");
        let drained = service
            .drain_owner(journal(), &authority)
            .await
            .expect("drain flushes");
        let receipt = pending.await.expect("committed by drain flush");
        assert_eq!(receipt.first_offset.get(), 0);
        let _ = drained;
    }
}
