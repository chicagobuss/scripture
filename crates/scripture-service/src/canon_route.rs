//! Transport-neutral Canon route resolution for client endpoint refresh.
//!
//! This is an explicit control-plane one-shot: it always performs a fresh
//! VirtualLog observation. It is not on the append hot path and never exposes
//! adapter-private compare tokens.

use holylog::remote_sequencer::SequencerEpoch;
use holylog::virtual_log::{VirtualLog, VirtualLogError};
use scripture::{
    CanonAuthorityError, CanonFence, CanonFenceError, CanonOwner, JournalId, OwnerEndpoint,
    OwnerId, VerseId,
};

use crate::chunk_service::{ChunkJournalService, LocalCanonOwnerMatch};

/// Admission disposition for a local owner against one observed Canon fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDisposition {
    /// Local `(owner_id, epoch)` matches the observed v2 fence and the owner is Running.
    Serving,
    /// Canon names a different owner or epoch; refuse writes.
    Standby,
    /// Local epoch is sealed or superseded; refuse writes.
    Fenced,
    /// Verse is Unowned or the local Canon-bound owner is not Running.
    RecoveryRequired,
}

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
        /// Remote sequencer epoch when the fence is v2 Owned.
        sequencer_epoch: Option<SequencerEpoch>,
        /// Advisory sequencer endpoint when the fence is v2 Owned.
        sequencer_endpoint: Option<OwnerEndpoint>,
    },
    /// Fresh Canon names another owner. Endpoint is advisory only.
    NotOwner {
        /// Observed Canon / VirtualLog revision (client routing cache key).
        canon_revision: u64,
        /// Owner identity named by the fence.
        owner_id: OwnerId,
        /// Advisory endpoint from the fence.
        endpoint: OwnerEndpoint,
        /// Remote sequencer epoch when the fence is v2 Owned.
        sequencer_epoch: Option<SequencerEpoch>,
        /// Advisory sequencer endpoint when the fence is v2 Owned.
        sequencer_endpoint: Option<OwnerEndpoint>,
    },
    /// This owner held a superseded epoch; refuse writes and surface the newer route.
    Fenced {
        /// Observed Canon / VirtualLog revision.
        canon_revision: u64,
        /// Owner identity named by the fence.
        owner_id: OwnerId,
        /// Advisory endpoint from the fence.
        endpoint: OwnerEndpoint,
        /// Active remote sequencer epoch from the observed fence.
        sequencer_epoch: SequencerEpoch,
        /// Advisory sequencer endpoint from the observed fence.
        sequencer_endpoint: OwnerEndpoint,
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

fn owned_route_fields(
    owner: &CanonOwner,
) -> Option<(
    OwnerId,
    OwnerEndpoint,
    Option<SequencerEpoch>,
    Option<OwnerEndpoint>,
)> {
    match owner {
        CanonOwner::Unowned => None,
        CanonOwner::Owned {
            owner_id,
            endpoint,
            sequencer,
        } => {
            let (sequencer_epoch, sequencer_endpoint) = sequencer
                .as_ref()
                .map(|binding| {
                    (
                        Some(binding.epoch),
                        Some(binding.sequencer_endpoint.clone()),
                    )
                })
                .unwrap_or((None, None));
            Some((
                *owner_id,
                endpoint.clone(),
                sequencer_epoch,
                sequencer_endpoint,
            ))
        }
    }
}

/// Pure admission check from local owner state and one observed Canon fence.
#[must_use]
pub fn admission_for(
    local_owner: OwnerId,
    local_epoch: Option<SequencerEpoch>,
    local_fenced: bool,
    fence: &CanonFence,
    owner_match: LocalCanonOwnerMatch,
) -> AdmissionDisposition {
    if local_fenced {
        return AdmissionDisposition::Fenced;
    }
    let Some((owner_id, _, fence_epoch, _)) = owned_route_fields(&fence.owner) else {
        return AdmissionDisposition::RecoveryRequired;
    };
    if owner_id != local_owner {
        return AdmissionDisposition::Standby;
    }
    if let Some(active_epoch) = fence_epoch
        && local_epoch != Some(active_epoch)
    {
        return AdmissionDisposition::Fenced;
    }
    match owner_match {
        LocalCanonOwnerMatch::ServeReady => AdmissionDisposition::Serving,
        LocalCanonOwnerMatch::BoundNotRunning { .. } | LocalCanonOwnerMatch::Unavailable => {
            AdmissionDisposition::RecoveryRequired
        }
    }
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
    resolve_canon_route_with_epoch(
        virtual_log,
        service,
        journal_id,
        verse_id,
        this_owner,
        None,
        false,
    )
    .await
}

/// Like [`resolve_canon_route`], but considers local epoch state for Fenced answers.
pub async fn resolve_canon_route_with_epoch(
    virtual_log: &VirtualLog,
    service: &ChunkJournalService,
    journal_id: JournalId,
    verse_id: VerseId,
    this_owner: OwnerId,
    local_epoch: Option<SequencerEpoch>,
    local_fenced: bool,
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

    let owner_match = match owned_route_fields(&fence.owner) {
        None => {
            return Ok(CanonRoute::Recovering {
                canon_revision: fence.revision,
            });
        }
        Some((owner_id, endpoint, sequencer_epoch, sequencer_endpoint))
            if owner_id != this_owner =>
        {
            return Ok(CanonRoute::NotOwner {
                canon_revision: fence.revision,
                owner_id,
                endpoint,
                sequencer_epoch,
                sequencer_endpoint,
            });
        }
        Some((owner_id, _, _, _)) => {
            service.local_canon_owner_match(journal_id, verse_id, owner_id, fence.revision)
        }
    };

    let disposition = admission_for(this_owner, local_epoch, local_fenced, &fence, owner_match);
    let Some((owner_id, endpoint, sequencer_epoch, sequencer_endpoint)) =
        owned_route_fields(&fence.owner)
    else {
        return Ok(CanonRoute::Recovering {
            canon_revision: fence.revision,
        });
    };

    Ok(match disposition {
        AdmissionDisposition::Serving => CanonRoute::Serve {
            canon_revision: fence.revision,
            owner_id,
            endpoint,
            sequencer_epoch,
            sequencer_endpoint,
        },
        AdmissionDisposition::Standby => CanonRoute::NotOwner {
            canon_revision: fence.revision,
            owner_id,
            endpoint,
            sequencer_epoch,
            sequencer_endpoint,
        },
        AdmissionDisposition::Fenced => match (sequencer_epoch, sequencer_endpoint) {
            (Some(sequencer_epoch), Some(sequencer_endpoint)) => CanonRoute::Fenced {
                canon_revision: fence.revision,
                owner_id,
                endpoint,
                sequencer_epoch,
                sequencer_endpoint,
            },
            // Legacy v1 has no epoch tip; surface recovery rather than inventing one.
            _ => CanonRoute::Recovering {
                canon_revision: fence.revision,
            },
        },
        AdmissionDisposition::RecoveryRequired => CanonRoute::Recovering {
            canon_revision: fence.revision,
        },
    })
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
    use holylog::remote_sequencer::{ActivateOutcome, InMemoryRemoteSequencer, SequencerEpoch};
    use holylog::virtual_log::{
        ApplicationFence, ConditionalRegister, InMemoryConditionalRegister, LogletId,
        LogletResolver, ResolveFuture, VirtualLog,
    };
    use scripture::{
        CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, OwnedSequencerBinding,
        OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound, Submission, SystemClock,
        VerseId, WriterId, observe_canon_authority_witnessed,
    };

    use super::{
        AdmissionDisposition, CanonRoute, CanonRouteError, admission_for, resolve_canon_route,
        resolve_canon_route_with_epoch,
    };
    use crate::canon_transition::{
        CanonTransitionOutcome, CanonTransitionRequest, publish_canon_transition,
    };
    use crate::chunk_service::{ChunkJournalService, LocalCanonOwnerMatch, OwnerStatus};
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

    fn owned_v2(owner: OwnerId, epoch: SequencerEpoch, endpoint_text: &str) -> CanonOwner {
        let endpoint = OwnerEndpoint::new(endpoint_text).expect("endpoint");
        CanonOwner::Owned {
            owner_id: owner,
            endpoint: endpoint.clone(),
            sequencer: Some(OwnedSequencerBinding {
                epoch,
                sequencer_endpoint: endpoint,
            }),
        }
    }

    fn fence(revision: u64, owner: OwnerId) -> CanonFence {
        CanonFence::new(revision, journal(), verse(), owned_legacy(owner))
    }

    fn owned_legacy(owner: OwnerId) -> CanonOwner {
        let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
        CanonOwner::Owned {
            owner_id: owner,
            endpoint,
            sequencer: None,
        }
    }

    fn fence_with_epoch(revision: u64, owner: OwnerId, epoch: SequencerEpoch) -> CanonFence {
        CanonFence::new(
            revision,
            journal(),
            verse(),
            owned_v2(owner, epoch, "tcp://owner.local:9000"),
        )
    }

    fn owned_owner(owner: OwnerId) -> CanonOwner {
        owned_legacy(owner)
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
                    .weak_tail(k))
            })
        }
    }

    #[test]
    fn admission_maps_serving_standby_fenced_and_recovery() {
        let owner = owner_a();
        let fence = fence_with_epoch(1, owner, SequencerEpoch::test(2));
        assert_eq!(
            admission_for(
                owner,
                Some(SequencerEpoch::test(2)),
                false,
                &fence,
                LocalCanonOwnerMatch::ServeReady,
            ),
            AdmissionDisposition::Serving
        );
        assert_eq!(
            admission_for(
                owner_b(),
                Some(SequencerEpoch::test(2)),
                false,
                &fence,
                LocalCanonOwnerMatch::Unavailable,
            ),
            AdmissionDisposition::Standby
        );
        assert_eq!(
            admission_for(
                owner,
                Some(SequencerEpoch::test(1)),
                false,
                &fence,
                LocalCanonOwnerMatch::ServeReady,
            ),
            AdmissionDisposition::Fenced
        );
        assert_eq!(
            admission_for(owner, None, false, &fence, LocalCanonOwnerMatch::ServeReady,),
            AdmissionDisposition::Fenced
        );
        assert_eq!(
            admission_for(
                owner,
                Some(SequencerEpoch::test(2)),
                true,
                &fence,
                LocalCanonOwnerMatch::ServeReady,
            ),
            AdmissionDisposition::Fenced
        );
    }

    #[test]
    fn legacy_v1_fence_cannot_activate_remote_sequencer() {
        let fence = CanonFence::new(
            0,
            journal(),
            verse(),
            CanonOwner::Owned {
                owner_id: owner_a(),
                endpoint: OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint"),
                sequencer: None,
            },
        );
        assert!(!fence.allows_remote_sequencer());
        assert_eq!(
            admission_for(
                owner_a(),
                None,
                false,
                &fence,
                LocalCanonOwnerMatch::ServeReady,
            ),
            AdmissionDisposition::Serving
        );
    }

    #[tokio::test]
    async fn v2_fence_requires_a_matching_local_epoch_before_serving() {
        let harness = Harness::memory();
        let epoch = SequencerEpoch::test(42);
        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                fence_with_epoch(0, owner_a(), epoch).encode(),
            )
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
            .expect("route without epoch"),
            CanonRoute::Fenced { sequencer_epoch, .. } if sequencer_epoch == epoch
        ));
        assert!(matches!(
            resolve_canon_route_with_epoch(
                &harness.virtual_log(),
                &service,
                journal(),
                verse(),
                owner_a(),
                Some(epoch),
                false,
            )
            .await
            .expect("route with matching epoch"),
            CanonRoute::Serve { sequencer_epoch: Some(observed), .. } if observed == epoch
        ));
    }

    #[test]
    fn canon_cas_loser_with_stale_epoch_does_not_become_serving() {
        let mut sequencer = InMemoryRemoteSequencer::new();
        let winner_epoch = SequencerEpoch::test(2);
        let loser_epoch = SequencerEpoch::test(1);
        assert_eq!(
            sequencer.activate(winner_epoch, 4, 0),
            ActivateOutcome::Active
        );
        let fence = fence_with_epoch(1, owner_a(), winner_epoch);
        assert!(!sequencer.is_active_epoch(loser_epoch));
        assert_eq!(
            admission_for(
                owner_a(),
                Some(loser_epoch),
                false,
                &fence,
                LocalCanonOwnerMatch::ServeReady,
            ),
            AdmissionDisposition::Fenced
        );
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
                sequencer_epoch: None,
                ..
            } if owner_id == owner_a()
        ));
    }

    #[tokio::test]
    async fn handoff_route_moves_from_a_to_recovering_to_b_serve_with_epoch() {
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
                sequencer_epoch: None,
                ..
            } if owner_id == owner_b()
        ));

        assert_eq!(
            admission_for(
                owner_a(),
                Some(SequencerEpoch::test(0)),
                false,
                &CanonFence::from_virtual_log_state(
                    &harness
                        .virtual_log()
                        .observe_membership()
                        .await
                        .expect("obs")
                        .state
                )
                .expect("fence"),
                LocalCanonOwnerMatch::ServeReady,
            ),
            AdmissionDisposition::Standby
        );

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
                sequencer_epoch: None,
                ..
            } if owner_id == owner_b()
        ));
    }

    #[tokio::test]
    async fn stale_owner_after_handoff_returns_not_owner_with_newer_epoch_route() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                fence_with_epoch(0, owner_a(), SequencerEpoch::test(10)).encode(),
            )
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
        let successor_owner = owned_v2(
            owner_b(),
            SequencerEpoch::test(11),
            "tcp://owner-b.local:9000",
        );
        assert!(matches!(
            publish_canon_transition(
                &mut service_a,
                &harness.virtual_log(),
                CanonTransitionRequest {
                    authority,
                    drained,
                    successor: harness.second.clone(),
                    next_owner: successor_owner,
                    journal_id: journal(),
                    verse_id: verse(),
                },
            )
            .await
            .expect("publish"),
            CanonTransitionOutcome::Published(_)
        ));

        assert!(matches!(
            resolve_canon_route_with_epoch(
                &harness.virtual_log(),
                &service_a,
                journal(),
                verse(),
                owner_a(),
                Some(SequencerEpoch::test(10)),
                false,
            )
            .await
            .expect("stale a"),
            CanonRoute::NotOwner {
                canon_revision: 1,
                owner_id,
                sequencer_epoch: Some(sequencer_epoch),
                ..
            } if owner_id == owner_b() && sequencer_epoch == SequencerEpoch::test(11)
        ));
    }

    #[tokio::test]
    async fn standby_refuses_writes_for_foreign_owner_epoch() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_b()).encode())
            .await
            .expect("bootstrap");
        let fence = CanonFence::from_virtual_log_state(
            &harness
                .virtual_log()
                .observe_membership()
                .await
                .expect("obs")
                .state,
        )
        .expect("fence");
        assert_eq!(
            admission_for(
                owner_a(),
                Some(SequencerEpoch::test(0)),
                false,
                &fence,
                LocalCanonOwnerMatch::Unavailable,
            ),
            AdmissionDisposition::Standby
        );
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
