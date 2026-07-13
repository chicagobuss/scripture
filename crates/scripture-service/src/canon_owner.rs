//! Construct a fenced Canon owner from durable VirtualLog recovery.
//!
//! This is the transport-neutral startup primitive for a Scripture Line after a
//! fenced handoff: observe Canon authority, recover a bounded cross-generation
//! suffix once, then build an unstarted [`ChunkDriverActor`] at the active
//! Canon revision.
//!
//! It is not election, discovery, owner replacement, or a restart loop.
//! [`crate::ChunkJournalService::register_canon_owner`] after recovery; the lab
//! [`crate::ChunkJournalService::register_owner`] path does not grant a Canon
//! binding for publish.

use holylog::virtual_log::VirtualLog;
use scripture::{
    CanonAuthoritySnapshot, ChunkDriverActor, ChunkDriverHandle, ChunkLogError, ChunkLogWriter,
    ChunkPolicy, Clock, CohortId, DriverError, JournalId, LineId, OwnerId, RecoveredChunk,
    RecoveryBound, Timer, WriterId,
};

/// Inputs for one Canon-authorized owner construction attempt.
#[derive(Debug, Clone)]
pub struct CanonOwnerRequest {
    /// Logical Scripture journal.
    pub journal_id: JournalId,
    /// Physical Line being recovered.
    pub line_id: LineId,
    /// Owner identity that must match the fresh Canon fence.
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

/// A recovered, unstarted Canon owner for one startup attempt.
///
/// [`Self::authority`] is the observation used to authorize this attempt. It is
/// not a forever lease: after the actor runs, stale ownership is rejected by
/// the Holylog seal fence on the VirtualLog-backed writer.
pub struct RecoveredCanonOwner<C, T> {
    /// Fresh Canon / VirtualLog observation for this attempt.
    pub authority: CanonAuthoritySnapshot,
    /// Cloneable submission endpoint.
    pub handle: ChunkDriverHandle,
    /// Unstarted actor. The caller owns lifecycle; this factory never spawns it.
    pub actor: ChunkDriverActor<C, T>,
    /// Bounded recovered suffix retained for diagnostics only.
    ///
    /// Dedup state already lives inside [`Self::actor`]; callers must not rebuild
    /// a second producer window from this vector.
    pub recovered_chunks: Vec<RecoveredChunk>,
}

/// Failures constructing a Canon-authorized owner.
#[derive(Debug, thiserror::Error)]
pub enum CanonOwnerError {
    /// Durable recovery or Canon authority observation failed.
    #[error(transparent)]
    Recovery(#[from] ChunkLogError),
    /// Actor construction failed after a successful recovery.
    #[error(transparent)]
    Driver(#[from] DriverError),
}

/// Recovers a VirtualLog-backed chunk writer and builds an unstarted driver.
///
/// Calls [`ChunkLogWriter::recover_virtual`] exactly once. On any authority,
/// stale-cutover, corruption, or construction error it returns no handle/actor.
///
/// The actor generation equals [`CanonAuthoritySnapshot::revision`]. This is the
/// only public construction route that combines Canon ownership with a
/// VirtualLog writer; [`ChunkDriverActor::new`] remains a generic composition
/// primitive for lab/AtomicLog paths.
pub async fn recover_canon_owner<C, T>(
    request: CanonOwnerRequest,
    virtual_log: VirtualLog,
    clock: C,
    timer: T,
) -> Result<RecoveredCanonOwner<C, T>, CanonOwnerError>
where
    C: Clock,
    T: Timer,
{
    let recovery = ChunkLogWriter::recover_virtual(
        request.journal_id,
        request.cohort_id,
        request.line_id,
        request.owner_id,
        virtual_log,
        request.recovery_bound,
    )
    .await?;
    let generation = recovery.authority.revision();
    let (handle, actor) = ChunkDriverActor::new(
        request.journal_id,
        request.cohort_id,
        request.writer_id,
        generation,
        recovery.writer,
        &recovery.chunks,
        request.policy,
        clock,
        timer,
        request.queue_capacity,
    )?;
    Ok(RecoveredCanonOwner {
        authority: recovery.authority,
        handle,
        actor,
        recovered_chunks: recovery.chunks,
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use holylog::atomic::AtomicLog;
    use holylog::memory::InMemoryLogDrive;
    use holylog::virtual_log::{
        CompareToken, ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver,
        RegisterFuture, ResolveFuture, VersionedState, VirtualLog, VirtualLogState,
    };
    use scripture::{
        CanonFence, CanonOwner, ChunkLogError, ChunkPolicy, CohortId, JournalId, LineId,
        ManualClock, ManualTimer, OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound,
        Submission, SystemClock, WriterId,
    };
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{CanonOwnerError, CanonOwnerRequest, recover_canon_owner};

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"factory-journal!")
    }

    fn line() -> LineId {
        LineId::from_bytes(*b"factory-line-id!")
    }

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"factory-owner-a!")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"factory-owner-b!")
    }

    fn cohort() -> CohortId {
        CohortId::from_bytes(*b"factory-cohort!!")
    }

    fn writer_id() -> WriterId {
        WriterId::from_bytes(*b"factory-writer!!")
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
            Self::with_register(Arc::new(InMemoryConditionalRegister::new()))
        }

        fn with_register(register: Arc<dyn ConditionalRegister>) -> Self {
            let resolver = Arc::new(Resolver::default());
            let first = LogletId::new("factory-first").expect("id");
            let second = LogletId::new("factory-second").expect("id");
            resolver.insert(
                first.clone(),
                Arc::new(
                    AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                        .build()
                        .expect("log"),
                ),
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
                register,
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

    struct FlipRegister {
        inner: InMemoryConditionalRegister,
        reads: std::sync::atomic::AtomicUsize,
        flip_at: usize,
        flipped: Mutex<Option<VirtualLogState>>,
    }

    impl FlipRegister {
        fn new(flip_at: usize) -> Self {
            Self {
                inner: InMemoryConditionalRegister::new(),
                reads: std::sync::atomic::AtomicUsize::new(0),
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
    async fn fresh_boot_constructs_an_actor_that_commits() {
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
        assert_eq!(recovered.authority.revision(), 0);
        assert!(recovered.recovered_chunks.is_empty());

        let mut service = crate::ChunkJournalService::new();
        service.register_canon_owner(recovered).expect("register");
        let pending = service
            .submit(
                journal(),
                Submission {
                    producer_id: ProducerId::from_bytes(*b"factory-producer"),
                    producer_epoch: 0,
                    sequence: 0,
                    records: vec![Record::new([], Bytes::from_static(b"fresh"))],
                },
            )
            .await
            .expect("admit");
        service.flush(journal()).await.expect("flush");
        let receipt = pending.await.expect("commit");
        assert_eq!(receipt.first_offset.get(), 0);
        assert_eq!(receipt.slot, 0);
    }

    #[tokio::test]
    async fn handoff_refuses_a_and_recovers_b_with_dense_offsets() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");

        let recovered_a = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("owner a");
        let mut service = crate::ChunkJournalService::new();
        service
            .register_canon_owner(recovered_a)
            .expect("register a");
        let pending_a = service
            .submit(
                journal(),
                Submission {
                    producer_id: ProducerId::from_bytes(*b"factory-producer"),
                    producer_epoch: 0,
                    sequence: 0,
                    records: vec![Record::new([], Bytes::from_static(b"a0"))],
                },
            )
            .await
            .expect("admit");
        service.flush(journal()).await.expect("flush");
        pending_a.await.expect("commit");
        service.stop_owner(journal()).await.expect("stop a");

        harness
            .virtual_log()
            .reconfigure_with_application_fence(
                harness.second.clone(),
                fence(1, owner_b()).encode(),
            )
            .await
            .expect("cutover");

        assert!(matches!(
            recover_canon_owner(
                request(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonOwnerError::Recovery(ChunkLogError::Authority(_)))
        ));

        let recovered_b = recover_canon_owner(
            request(owner_b()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("owner b");
        assert_eq!(recovered_b.authority.revision(), 1);
        assert_eq!(recovered_b.recovered_chunks.len(), 1);
        assert_eq!(recovered_b.recovered_chunks[0].first_offset.get(), 0);

        // A separate process-local registry — not an in-place owner replacement.
        let mut service_b = crate::ChunkJournalService::new();
        service_b
            .register_canon_owner(recovered_b)
            .expect("register b");
        let pending_b = service_b
            .submit(
                journal(),
                Submission {
                    producer_id: ProducerId::from_bytes(*b"factory-producer"),
                    producer_epoch: 0,
                    sequence: 1,
                    records: vec![Record::new([], Bytes::from_static(b"b1"))],
                },
            )
            .await
            .expect("admit");
        service_b.flush(journal()).await.expect("flush");
        let receipt = pending_b.await.expect("commit");
        assert_eq!(receipt.first_offset.get(), 1);
        assert_eq!(receipt.slot, 1);
    }

    #[tokio::test]
    async fn unowned_and_not_owner_yield_no_actor() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(
                harness.first.clone(),
                CanonFence::new(0, journal(), line(), CanonOwner::Unowned).encode(),
            )
            .await
            .expect("bootstrap");
        assert!(matches!(
            recover_canon_owner(
                request(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonOwnerError::Recovery(ChunkLogError::Authority(_)))
        ));

        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        assert!(matches!(
            recover_canon_owner(
                request(owner_b()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonOwnerError::Recovery(ChunkLogError::Authority(_)))
        ));
    }

    #[tokio::test]
    async fn mid_recovery_cutover_yields_no_actor() {
        let flip = Arc::new(FlipRegister::new(1));
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
            recover_canon_owner(
                request(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await,
            Err(CanonOwnerError::Recovery(
                ChunkLogError::StaleCanonRecovery { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn dropping_the_handle_terminates_the_unspawned_actor_task() {
        let harness = Harness::memory();
        harness
            .virtual_log()
            .bootstrap_with_application_fence(harness.first.clone(), fence(0, owner_a()).encode())
            .await
            .expect("bootstrap");
        let clock = Arc::new(ManualClock::new());
        let timer = ManualTimer::new(Arc::clone(&clock));
        let recovered =
            recover_canon_owner(request(owner_a()), harness.virtual_log(), clock, timer)
                .await
                .expect("recover");
        let task = tokio::spawn(recovered.actor.run());
        drop(recovered.handle);
        let _ = task.await.expect("terminates without restart");
    }
}
