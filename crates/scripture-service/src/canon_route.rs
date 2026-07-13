//! Transport-neutral Canon route resolution for client endpoint refresh.
//!
//! This is an explicit control-plane one-shot: it always performs a fresh
//! VirtualLog observation. It is not on the append hot path and never exposes
//! adapter-private compare tokens.

use holylog::virtual_log::{VirtualLog, VirtualLogError};
use scripture::{
    CanonAuthorityError, CanonFence, CanonFenceError, CanonOwner, JournalId, OwnerEndpoint,
    OwnerId, VerseId,
};

use crate::chunk_service::{ChunkJournalService, LocalCanonOwnerMatch};

/// Typed route answer derived from one fresh Canon observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonRoute {
    /// Fresh Canon names this node and a matching Canon-bound owner is Running.
    Serve {
        /// Observed Canon / VirtualLog revision.
        canon_revision: u64,
        /// Owner identity named by the fence (this node).
        owner_id: OwnerId,
        /// Advisory endpoint from the fence.
        endpoint: OwnerEndpoint,
    },
    /// Fresh Canon names another owner. Endpoint is advisory only.
    NotOwner {
        /// Observed Canon / VirtualLog revision (client routing cache key).
        canon_revision: u64,
        /// Owner identity named by the fence.
        owner_id: OwnerId,
        /// Advisory endpoint from the fence.
        endpoint: OwnerEndpoint,
    },
    /// Verse is Unowned, or this node is named but has no running Canon-bound owner.
    Recovering {
        /// Observed Canon / VirtualLog revision.
        canon_revision: u64,
    },
}

/// Failures that refuse to invent an optimistic route.
#[derive(Debug, thiserror::Error)]
pub enum CanonRouteError {
    /// Holylog register / VirtualLog observation failed.
    #[error(transparent)]
    VirtualLog(#[from] VirtualLogError),
    /// Opaque fence bytes failed to decode or bind.
    #[error(transparent)]
    Fence(#[from] CanonFenceError),
    /// Journal or Verse identity mismatch against the expected route request.
    #[error(transparent)]
    Authority(#[from] CanonAuthorityError),
}

/// Resolves whether `this_owner` may serve `journal_id`/`verse_id` right now.
///
/// Always observes membership freshly. All route fields come from that one
/// observation; [`holylog::virtual_log::CompareToken`] never appears in the
/// result. Does not create, start, stop, or resume local owners.
pub async fn resolve_canon_route(
    virtual_log: &VirtualLog,
    service: &ChunkJournalService,
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
        CanonOwner::Owned { owner_id, endpoint } if owner_id != this_owner => {
            Ok(CanonRoute::NotOwner {
                canon_revision: fence.revision,
                owner_id,
                endpoint,
            })
        }
        CanonOwner::Owned { owner_id, endpoint } => {
            match service.local_canon_owner_match(journal_id, verse_id, owner_id, fence.revision) {
                LocalCanonOwnerMatch::ServeReady => Ok(CanonRoute::Serve {
                    canon_revision: fence.revision,
                    owner_id,
                    endpoint,
                }),
                LocalCanonOwnerMatch::BoundNotRunning { .. }
                | LocalCanonOwnerMatch::Unavailable => Ok(CanonRoute::Recovering {
                    canon_revision: fence.revision,
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use holylog::atomic::AtomicLog;
    use holylog::drive::{DriveError, DriveFuture, LogDrive};
    use holylog::logdrive::{Address, ReferenceLogDrive, TailDescription};
    use holylog::memory::InMemoryLogDrive;
    use holylog::virtual_log::{
        ApplicationFence, ConditionalRegister, InMemoryConditionalRegister, LogletId,
        LogletResolver, ResolveFuture, VirtualLog,
    };
    use scripture::{
        CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId,
        ProducerId, Record, RecoveryBound, Submission, SystemClock, VerseId, WriterId,
        observe_canon_authority_witnessed,
    };

    use super::{CanonRoute, CanonRouteError, resolve_canon_route};
    use crate::canon_transition::{
        CanonTransitionOutcome, CanonTransitionRequest, publish_canon_transition,
    };
    use crate::chunk_service::{ChunkJournalService, OwnerStatus};
    use crate::{CanonOwnerRequest, recover_canon_owner};

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"route-journal-id")
    }

    fn verse() -> VerseId {
        VerseId::from_bytes(*b"route-line-id!!!")
    }

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"route-owner-a!!!")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"route-owner-b!!!")
    }

    fn cohort() -> CohortId {
        CohortId::from_bytes(*b"route-cohort!!!!")
    }

    fn writer_id() -> WriterId {
        WriterId::from_bytes(*b"route-writer!!!!")
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
            verse_id: verse(),
            owner_id: owner,
            cohort_id: cohort(),
            writer_id: writer_id(),
            policy: policy(),
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
    }

    impl Harness {
        fn memory() -> Self {
            Self::with_first_drive(Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>)
        }

        fn with_first_drive(first_drive: Arc<dyn LogDrive>) -> Self {
            let resolver = Arc::new(Resolver::default());
            let first = LogletId::new("route-first").expect("id");
            let second = LogletId::new("route-second").expect("id");
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
            Self {
                register: Arc::new(InMemoryConditionalRegister::new()),
                resolver,
                first,
                second,
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
        fn write(&self, address: Address, value: bytes::Bytes) -> DriveFuture<'_, ()> {
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

        fn read(&self, address: Address) -> DriveFuture<'_, Option<bytes::Bytes>> {
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
    async fn named_running_canon_owner_resolves_serve() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
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
        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &service,
                journal(),
                verse(),
                owner_a(),
            )
            .await
            .expect("route"),
            CanonRoute::Serve {
                canon_revision: 0,
                owner_id,
                ..
            } if owner_id == owner_a()
        ));
    }

    #[tokio::test]
    async fn handoff_route_moves_from_a_to_recovering_to_b_serve() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
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
        let mut service_a = ChunkJournalService::new();
        service_a
            .register_canon_owner(recovered)
            .expect("register a");

        let authority = observe_canon_authority_witnessed(
            &harness.virtual_log(),
            journal(),
            verse(),
            owner_a(),
        )
        .await
        .expect("witness");
        let drained = service_a
            .drain_owner(journal(), &authority)
            .await
            .expect("drain");
        assert!(matches!(
            publish_canon_transition(
                &mut service_a,
                &harness.virtual_log(),
                CanonTransitionRequest {
                    authority,
                    drained,
                    successor: harness.second.clone(),
                    next_owner: owned_owner(owner_b()),
                    journal_id: journal(),
                    verse_id: verse(),
                },
            )
            .await
            .expect("publish"),
            CanonTransitionOutcome::Published(_)
        ));

        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &service_a,
                journal(),
                verse(),
                owner_a(),
            )
            .await
            .expect("a route"),
            CanonRoute::NotOwner {
                canon_revision: 1,
                owner_id,
                ..
            } if owner_id == owner_b()
        ));

        let empty_b = ChunkJournalService::new();
        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &empty_b,
                journal(),
                verse(),
                owner_b(),
            )
            .await
            .expect("b before recover"),
            CanonRoute::Recovering { canon_revision: 1 }
        ));

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
        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &service_b,
                journal(),
                verse(),
                owner_b(),
            )
            .await
            .expect("b serve"),
            CanonRoute::Serve {
                canon_revision: 1,
                owner_id,
                ..
            } if owner_id == owner_b()
        ));
    }

    #[tokio::test]
    async fn unowned_resolves_recovering() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                CanonFence::new(0, journal(), verse(), CanonOwner::Unowned).encode(),
            )
            .await
            .expect("bootstrap");
        let service = ChunkJournalService::new();
        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &service,
                journal(),
                verse(),
                owner_a(),
            )
            .await
            .expect("unowned"),
            CanonRoute::Recovering { canon_revision: 0 }
        ));
    }

    #[tokio::test]
    async fn lab_register_never_resolves_serve() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
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
        let (authority, handle, actor, _) = recovered.into_unmanaged();
        let mut lab = ChunkJournalService::new();
        lab.register_owner(journal(), authority.revision(), handle, actor)
            .expect("lab");
        assert!(matches!(
            resolve_canon_route(&harness.virtual_log(), &lab, journal(), verse(), owner_a())
                .await
                .expect("lab never Serve"),
            CanonRoute::Recovering { canon_revision: 0 }
        ));
    }

    #[tokio::test]
    async fn draining_canon_owner_resolves_recovering() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
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
        let authority = observe_canon_authority_witnessed(
            &harness.virtual_log(),
            journal(),
            verse(),
            owner_a(),
        )
        .await
        .expect("witness");
        service
            .drain_owner(journal(), &authority)
            .await
            .expect("drain");
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::Draining
        );
        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &service,
                journal(),
                verse(),
                owner_a(),
            )
            .await
            .expect("draining"),
            CanonRoute::Recovering { canon_revision: 0 }
        ));
    }

    #[tokio::test]
    async fn poisoned_canon_owner_resolves_recovering() {
        let drive = FailAfterWriteDrive::new();
        let harness = Harness::with_first_drive(Arc::clone(&drive) as Arc<dyn LogDrive>);
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
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
            .submit(
                journal(),
                Submission {
                    producer_id: ProducerId::from_bytes(*b"route-producer!!"),
                    producer_epoch: 0,
                    sequence: 0,
                    records: vec![Record::new([], bytes::Bytes::from_static(b"poison"))],
                },
            )
            .await
            .expect("admit");
        let _ = service.flush(journal()).await;
        let _ = pending.await;
        tokio::task::yield_now().await;
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::Poisoned
        );
        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &service,
                journal(),
                verse(),
                owner_a(),
            )
            .await
            .expect("poisoned"),
            CanonRoute::Recovering { canon_revision: 0 }
        ));
    }

    #[tokio::test]
    async fn finished_canon_owner_resolves_recovering() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
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
        service.stop_owner(journal()).await.expect("stop");
        assert_eq!(
            service.health(journal()).expect("health").status,
            OwnerStatus::TaskFinished
        );
        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &service,
                journal(),
                verse(),
                owner_a(),
            )
            .await
            .expect("finished"),
            CanonRoute::Recovering { canon_revision: 0 }
        ));
    }

    #[tokio::test]
    async fn malformed_fence_and_uninitialized_register_are_typed_errors() {
        let harness = Harness::memory();
        assert!(matches!(
            resolve_canon_route(
                &harness.virtual_log(),
                &ChunkJournalService::new(),
                journal(),
                verse(),
                owner_a(),
            )
            .await,
            Err(CanonRouteError::VirtualLog(_))
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
            resolve_canon_route(
                &harness.virtual_log(),
                &ChunkJournalService::new(),
                journal(),
                verse(),
                owner_a(),
            )
            .await,
            Err(CanonRouteError::Fence(_))
        ));
    }
}
