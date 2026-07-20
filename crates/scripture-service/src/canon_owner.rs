//! Construct a fenced Canon owner from durable VirtualLog recovery.
//!
//! This is the transport-neutral startup primitive for a Scripture Verse after a
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
    BlobCommitSink, CanonAuthoritySnapshot, ChunkDriverActor, ChunkDriverHandle, ChunkLogError,
    ChunkLogWriter, ChunkPolicy, Clock, CohortId, DataRefBlobConfig, DriverError, JournalId,
    OwnerId, RecoveredChunk, RecoveryBound, Timer, VerseId, WriterId,
};

/// Inputs for one Canon-authorized owner construction attempt.
#[derive(Clone)]
pub struct CanonOwnerRequest {
    /// Logical Scripture journal.
    pub journal_id: JournalId,
    /// Physical Verse being recovered.
    pub verse_id: VerseId,
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
    /// When set, recovery resolves DataRefs and the driver emits them.
    pub dataref_blobs: Option<DataRefBlobConfig>,
    /// When set, sealed chunks buffer in a shared Scribe sink instead of depth-one PUTs.
    pub blob_sink: Option<std::sync::Arc<dyn BlobCommitSink>>,
    /// Assignment key for shared-sink routing.
    pub blob_verse_key: Option<String>,
}

impl std::fmt::Debug for CanonOwnerRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonOwnerRequest")
            .field("journal_id", &self.journal_id)
            .field("verse_id", &self.verse_id)
            .field("owner_id", &self.owner_id)
            .field("blob_sink", &self.blob_sink.is_some())
            .field("blob_verse_key", &self.blob_verse_key)
            .finish_non_exhaustive()
    }
}

/// A recovered, unstarted Canon owner for one startup attempt.
///
/// This value can only be created by [`recover_canon_owner`]. Its authority is
/// not a forever lease: after the actor runs, stale ownership is rejected by
/// the Holylog seal fence on the VirtualLog-backed writer.
pub struct RecoveredCanonOwner<C, T> {
    authority: CanonAuthoritySnapshot,
    handle: ChunkDriverHandle,
    actor: ChunkDriverActor<C, T>,
    recovered_chunks: Vec<RecoveredChunk>,
}

impl<C, T> RecoveredCanonOwner<C, T> {
    /// Fresh Canon / VirtualLog observation that authorized this startup attempt.
    #[must_use]
    pub const fn authority(&self) -> &CanonAuthoritySnapshot {
        &self.authority
    }

    /// Bounded durable suffix retained for diagnostics only.
    ///
    /// Dedup state already lives inside the actor; callers must not rebuild a
    /// second producer window from this slice.
    #[must_use]
    pub fn recovered_chunks(&self) -> &[RecoveredChunk] {
        &self.recovered_chunks
    }

    /// Consumes this factory-created owner for an unmanaged local runtime.
    ///
    /// An actor started this way can still be seal-fenced by Holylog, but it
    /// cannot later be upgraded into a Canon publish-capable service binding.
    #[must_use]
    pub fn into_unmanaged(
        self,
    ) -> (
        CanonAuthoritySnapshot,
        ChunkDriverHandle,
        ChunkDriverActor<C, T>,
        Vec<RecoveredChunk>,
    ) {
        (
            self.authority,
            self.handle,
            self.actor,
            self.recovered_chunks,
        )
    }

    pub(crate) fn into_canon_registration(
        self,
    ) -> (
        CanonAuthoritySnapshot,
        ChunkDriverHandle,
        ChunkDriverActor<C, T>,
    ) {
        (self.authority, self.handle, self.actor)
    }
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
    let blob_store = request
        .dataref_blobs
        .as_ref()
        .map(|config| config.store.as_ref());
    let recovery = ChunkLogWriter::recover_virtual(
        request.journal_id,
        request.cohort_id,
        request.verse_id,
        request.owner_id,
        virtual_log,
        request.recovery_bound,
        blob_store,
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
        request.dataref_blobs.clone(),
        request.blob_sink.clone(),
        request.blob_verse_key.clone(),
    )?;
    if let (Some(sink), Some(verse_key)) = (&request.blob_sink, &request.blob_verse_key) {
        sink.register_driver(verse_key, handle.clone());
    }
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
    use holylog::virtual_log::{
        CompareToken, ConditionalRegister, InMemoryConditionalRegister, RegisterFuture,
        VersionedState, VirtualLogState,
    };
    use scripture::{
        CanonFence, CanonOwner, ChunkLogError, ChunkPolicy, CohortId, JournalId, ManualClock,
        ManualTimer, OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound, Submission,
        SystemClock, VerseId, WriterId,
    };
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{CanonOwnerError, CanonOwnerRequest, recover_canon_owner};
    use crate::virtuallog_test_support::VirtualLogHarness;

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"factory-journal!")
    }

    fn verse() -> VerseId {
        VerseId::from_bytes(*b"factory-line-id!")
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
            verse_id: verse(),
            owner_id: owner,
            cohort_id: cohort(),
            writer_id: writer_id(),
            policy: policy(),
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 16,
            dataref_blobs: None,
            blob_sink: None,
            blob_verse_key: None,
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

    async fn factory_harness() -> VirtualLogHarness {
        VirtualLogHarness::with_ids(
            "factory-first",
            "factory-second",
            "factory-third",
            Arc::new(InMemoryConditionalRegister::new()),
        )
        .await
    }

    async fn factory_harness_with_register(
        register: Arc<dyn ConditionalRegister>,
    ) -> VirtualLogHarness {
        VirtualLogHarness::with_ids("factory-first", "factory-second", "factory-third", register)
            .await
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
        let harness = factory_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;

        let recovered = recover_canon_owner(
            request(owner_a()),
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("recover");
        assert_eq!(recovered.authority().revision(), 0);
        assert!(recovered.recovered_chunks().is_empty());

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
        assert_eq!(receipt.canon_revision, 0);
    }

    #[tokio::test]
    async fn handoff_refuses_a_and_recovers_b_with_dense_offsets() {
        let harness = factory_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;

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
        let receipt = pending_a.await.expect("commit");
        assert_eq!(receipt.canon_revision, 0);
        service.stop_owner(journal()).await.expect("stop a");

        harness
            .reconfigure_id(&harness.second, fence(1, owner_b()).encode())
            .await;

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
        assert_eq!(recovered_b.authority().revision(), 1);
        assert_eq!(recovered_b.recovered_chunks().len(), 1);
        assert_eq!(recovered_b.recovered_chunks()[0].first_offset.get(), 0);
        assert_eq!(recovered_b.recovered_chunks()[0].generation, 0);

        // A separate process-local registry — not an in-place owner replacement.
        let mut service_b = crate::ChunkJournalService::new();
        service_b
            .register_canon_owner(recovered_b)
            .expect("register b");
        let retry_a = service_b
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
            .expect("dedup admit");
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
        let retry = retry_a.await.expect("dedup receipt");
        assert!(retry.deduplicated);
        assert_eq!(retry.canon_revision, 0, "dedup must preserve generation-0");
        let receipt = pending_b.await.expect("commit");
        assert_eq!(receipt.first_offset.get(), 1);
        assert_eq!(receipt.slot, 1);
        assert_eq!(receipt.canon_revision, 1);
        assert!(!receipt.deduplicated);
    }

    #[tokio::test]
    async fn unowned_and_not_owner_yield_no_actor() {
        let harness = factory_harness().await;
        harness
            .bootstrap_first(CanonFence::new(0, journal(), verse(), CanonOwner::Unowned).encode())
            .await;
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

        let harness = factory_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;
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
        let harness =
            factory_harness_with_register(Arc::clone(&flip) as Arc<dyn ConditionalRegister>).await;
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
        let harness = factory_harness().await;
        harness.bootstrap_first(fence(0, owner_a()).encode()).await;
        let clock = Arc::new(ManualClock::new());
        let timer = ManualTimer::new(Arc::clone(&clock));
        let recovered =
            recover_canon_owner(request(owner_a()), harness.virtual_log(), clock, timer)
                .await
                .expect("recover");
        let (_, handle, actor, _) = recovered.into_unmanaged();
        let task = tokio::spawn(actor.run());
        drop(handle);
        let _ = task.await.expect("terminates without restart");
    }
}
