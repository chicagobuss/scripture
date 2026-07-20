//! The immutable codec exercised through the real Holylog append boundary.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::executor::block_on;
use holylog::atomic::{AtomicLog, InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{
    BindTag, InMemoryExclusiveClaimStore, LogletComponents, LogletObjectNamespaces,
    ProvisionAuthority, ProvisionerId, ResolvedLoglet, WritableLoglet,
};
use holylog::virtual_log::{
    CompareToken, ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver,
    RegisterFuture, ResolveFuture, VersionedState, VirtualLog, VirtualLogState,
};
use scripture::{
    AttributeValue, CanonAuthorityError, CanonFence, CanonOwner, ChunkHeader, ChunkId,
    ChunkLogError, ChunkLogWriter, CohortId, Frame, JournalId, OwnedSequencerBinding,
    OwnerEndpoint, OwnerId, ProducerId, Record, RecordOffset, RecoveryBound, SequencerEpoch,
    SubmissionRef, VerseId, WriterId, observe_canon_authority, seal_single_frame_chunk,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"journal-id-01234")
}

const CHUNKLOG_TEST_ROOT: &str = "scripture-chunklog-tests";

#[derive(Default)]
struct TestResolver {
    loglets: Mutex<BTreeMap<LogletId, ResolvedLoglet>>,
}

impl TestResolver {
    fn insert_writable(&self, id: LogletId, writable: Arc<WritableLoglet>) {
        self.loglets
            .lock()
            .expect("resolver lock")
            .insert(id, ResolvedLoglet::Writable(writable));
    }
}

impl LogletResolver for TestResolver {
    fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<ResolvedLoglet>> {
        let id = id.clone();
        Box::pin(async move {
            Ok(self
                .loglets
                .lock()
                .expect("resolver lock")
                .get(&id)
                .cloned())
        })
    }
}

#[test]
fn rejects_sealed_carrier_metadata_that_does_not_match_its_bytes() {
    block_on(async {
        let drive = Arc::new(InMemoryLogDrive::new());
        let log = AtomicLog::builder(drive, 0).build().expect("log");
        let mut writer = ChunkLogWriter::new(journal(), cohort(), 4, log, RecordOffset::new(0));
        let mut forged = chunk(1, 0, 1);
        forged.chunk_id = ChunkId::from_bytes([99; 16]);

        assert!(matches!(
            writer.append(&forged).await,
            Err(scripture::ChunkLogError::SealedMetadataMismatch)
        ));
        assert_eq!(writer.next_offset(), RecordOffset::new(0));
    });
}

fn cohort() -> CohortId {
    CohortId::from_bytes(*b"cohort-id-012345")
}

fn header(chunk: u8) -> ChunkHeader {
    ChunkHeader {
        chunk_id: ChunkId::from_bytes([chunk; 16]),
        cohort_id: cohort(),
        generation: 4,
        writer_id: WriterId::from_bytes(*b"writer-id-012345"),
        created_at_micros: 100,
    }
}

fn record(value: i64) -> Record {
    Record::new(
        [("value".into(), AttributeValue::I64(value))],
        Bytes::from(value.to_be_bytes().to_vec()),
    )
}

fn chunk(chunk: u8, base: u64, count: u32) -> scripture::SealedChunk {
    chunk_at_generation(chunk, base, count, 4)
}

fn chunk_at_generation(
    chunk: u8,
    base: u64,
    count: u32,
    generation: u64,
) -> scripture::SealedChunk {
    let records = (0..count).map(|value| record(i64::from(value))).collect();
    let submissions = (0..count)
        .map(|sequence| SubmissionRef {
            producer_id: ProducerId::from_bytes(*b"producer-id-0123"),
            producer_epoch: 1,
            sequence: u64::from(sequence),
            first_record: sequence,
            record_count: 1,
        })
        .collect();
    seal_single_frame_chunk(
        ChunkHeader {
            generation,
            ..header(chunk)
        },
        vec![Frame {
            journal_id: journal(),
            base_offset: RecordOffset::new(base),
            records,
            submissions,
        }],
    )
    .expect("valid test chunk")
}

#[test]
fn virtual_writer_is_fenced_by_a_canon_cutover() {
    block_on(async {
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new())).await;
        harness.bootstrap_first(owned(0).encode()).await;

        let mut old_owner = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            0,
            harness.virtual_log(),
            RecordOffset::new(0),
        );
        assert_eq!(
            old_owner
                .append(&chunk_at_generation(11, 0, 1, 0))
                .await
                .expect("old generation append")
                .slot,
            0
        );

        harness.reconfigure_second(owned(1).encode()).await;

        assert!(matches!(
            old_owner.append(&chunk_at_generation(12, 1, 1, 0)).await,
            Err(scripture::ChunkLogError::VirtualLog(
                holylog::virtual_log::VirtualLogError::StaleGeneration { .. }
            ))
        ));
        assert!(matches!(
            old_owner.append(&chunk_at_generation(12, 1, 1, 0)).await,
            Err(scripture::ChunkLogError::Poisoned)
        ));

        let mut successor_owner = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            1,
            harness.virtual_log(),
            RecordOffset::new(1),
        );
        assert_eq!(
            successor_owner
                .append(&chunk_at_generation(13, 1, 1, 1))
                .await
                .expect("successor append")
                .slot,
            1
        );
    });
}

#[test]
fn real_atomic_log_append_and_bounded_recovery_preserve_offsets() {
    block_on(async {
        let drive = Arc::new(InMemoryLogDrive::new());
        let log = AtomicLog::builder(drive, 0).build().expect("log");
        let mut writer =
            ChunkLogWriter::new(journal(), cohort(), 4, log.clone(), RecordOffset::new(0));

        let first = chunk(1, 0, 2);
        let first_ack = writer.append(&first).await.expect("first append");
        assert_eq!(first_ack.slot, 0);
        assert_eq!(first_ack.first_offset, RecordOffset::new(0));
        assert_eq!(first_ack.next_offset, RecordOffset::new(2));

        let second = chunk(2, 2, 3);
        let second_ack = writer.append(&second).await.expect("second append");
        assert_eq!(second_ack.slot, 1);
        assert_eq!(writer.next_offset(), RecordOffset::new(5));

        let recovery = ChunkLogWriter::recover(
            journal(),
            cohort(),
            4,
            log,
            RecoveryBound::new(2).expect("non-zero bound"),
            None,
        )
        .await
        .expect("recover");
        assert_eq!(recovery.writer.next_offset(), RecordOffset::new(5));
        assert_eq!(recovery.chunks.len(), 2);
        assert_eq!(recovery.chunks[0].chunk_id, first.chunk_id);
        assert_eq!(recovery.chunks[0].digest, first.digest);
        assert_eq!(recovery.chunks[0].frame.submissions.len(), 2);
        assert_eq!(
            recovery.chunks[0]
                .frame
                .offsets_for(ProducerId::from_bytes(*b"producer-id-0123"), 1, 0)
                .expect("span"),
            (RecordOffset::new(0), 1)
        );
        assert_eq!(recovery.chunks[1].chunk_id, second.chunk_id);
        assert_eq!(recovery.chunks[1].record_count, 3);
        let reconstructed_first = recovery.chunks[1].first_offset;
        let reconstructed_count = recovery.chunks[1].record_count;
        assert_eq!(reconstructed_first, RecordOffset::new(2));
        assert_eq!(reconstructed_count, 3);
    });
}

fn verse() -> VerseId {
    VerseId::from_bytes(*b"canon-line-id!!!")
}

fn owner_id() -> OwnerId {
    OwnerId::from_bytes(*b"canon-owner-id!!")
}

fn fence(revision: u64, owner: CanonOwner) -> CanonFence {
    CanonFence::new(revision, journal(), verse(), owner)
}

fn owned(revision: u64) -> CanonFence {
    let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
    fence(
        revision,
        CanonOwner::Owned {
            owner_id: owner_id(),
            endpoint: endpoint.clone(),
            sequencer: Some(OwnedSequencerBinding {
                epoch: SequencerEpoch::test(revision),
                sequencer_endpoint: endpoint,
            }),
            writer_term: None,
        },
    )
}

struct FlipOnSecondReadAfterArm {
    inner: InMemoryConditionalRegister,
    armed: std::sync::atomic::AtomicBool,
    reads_after_arm: AtomicUsize,
    flipped: Mutex<Option<VirtualLogState>>,
}

impl FlipOnSecondReadAfterArm {
    fn new() -> Self {
        Self {
            inner: InMemoryConditionalRegister::new(),
            armed: std::sync::atomic::AtomicBool::new(false),
            reads_after_arm: AtomicUsize::new(0),
            flipped: Mutex::new(None),
        }
    }

    fn arm_flip(&self, state: VirtualLogState) {
        *self.flipped.lock().expect("flip lock") = Some(state);
        self.armed.store(true, Ordering::Release);
        self.reads_after_arm.store(0, Ordering::Release);
    }
}

impl ConditionalRegister for FlipOnSecondReadAfterArm {
    fn read(&self) -> RegisterFuture<'_, Option<VersionedState>> {
        Box::pin(async {
            if self.armed.load(Ordering::Acquire) {
                let n = self.reads_after_arm.fetch_add(1, Ordering::SeqCst);
                if n >= 1
                    && let Some(state) = self.flipped.lock().expect("flip lock").clone()
                {
                    return Ok(Some(VersionedState {
                        token: CompareToken::from_revision(state.revision),
                        state,
                    }));
                }
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

struct VirtualHarness {
    register: Arc<dyn ConditionalRegister>,
    authority: ProvisionAuthority,
    resolver: Arc<TestResolver>,
    first: LogletId,
    second: LogletId,
    second_writable: Arc<Mutex<Option<Arc<WritableLoglet>>>>,
}

impl VirtualHarness {
    async fn new(register: Arc<dyn ConditionalRegister>) -> Self {
        let first = LogletId::new("recover-line-first").expect("first id");
        let second = LogletId::new("recover-line-second").expect("second id");
        Self {
            register,
            authority: ProvisionAuthority::new(
                Arc::new(InMemoryExclusiveClaimStore::new()),
                ProvisionerId::new("chunklog-virtual"),
            ),
            resolver: Arc::new(TestResolver::default()),
            first,
            second,
            second_writable: Arc::new(Mutex::new(None)),
        }
    }

    fn bind(id: &LogletId) -> BindTag {
        BindTag::new(id.as_str().as_bytes().to_vec())
    }

    fn components(k: u64) -> LogletComponents {
        LogletComponents::new(
            Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
            Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
            Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
            k,
        )
    }

    async fn provision(
        &self,
        id: &LogletId,
        k: u64,
    ) -> (
        BindTag,
        Arc<WritableLoglet>,
        holylog::provision::FreshWritableProvisionReceipt,
    ) {
        let bind = Self::bind(id);
        let (receipt, writable) = self
            .authority
            .provision_fresh(
                id.clone(),
                LogletObjectNamespaces::under_root(CHUNKLOG_TEST_ROOT, id),
                bind.clone(),
                Self::components(k),
            )
            .await
            .expect("provision fresh");
        let writable = Arc::new(writable);
        if id == &self.second {
            *self.second_writable.lock().expect("lock") = Some(Arc::clone(&writable));
        }
        self.resolver
            .insert_writable(id.clone(), Arc::clone(&writable));
        (bind, writable, receipt)
    }

    fn virtual_log(&self) -> VirtualLog {
        VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        )
    }

    async fn bootstrap_first(&self, fence: holylog::virtual_log::ApplicationFence) {
        let (bind, writable, receipt) = self.provision(&self.first, 0).await;
        self.virtual_log()
            .bootstrap_with_receipt(receipt, writable.as_ref(), &bind, fence)
            .await
            .expect("bootstrap");
    }

    async fn reconfigure_second(&self, fence: holylog::virtual_log::ApplicationFence) {
        let (bind, writable, receipt) = self.provision(&self.second, 0).await;
        let log = self.virtual_log();
        let observed = log.observe_membership().await.expect("observe");
        log.reconfigure_with_receipt(&observed, receipt, writable.as_ref(), &bind, fence)
            .await
            .expect("reconfigure");
    }

    async fn reconfigure_id(&self, id: &LogletId, fence: holylog::virtual_log::ApplicationFence) {
        let (bind, writable, receipt) = self.provision(id, 0).await;
        let log = self.virtual_log();
        let observed = log.observe_membership().await.expect("observe");
        log.reconfigure_with_receipt(&observed, receipt, writable.as_ref(), &bind, fence)
            .await
            .expect("reconfigure");
    }

    fn second_writable(&self) -> Arc<WritableLoglet> {
        self.second_writable
            .lock()
            .expect("lock")
            .clone()
            .expect("second writable")
    }
}

#[test]
fn virtual_recovery_rebuilds_suffix_across_canon_cutover() {
    block_on(async {
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new())).await;
        harness.bootstrap_first(owned(0).encode()).await;
        let mut gen0 = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            0,
            harness.virtual_log(),
            RecordOffset::new(0),
        );
        let first = chunk_at_generation(1, 0, 1, 0);
        let second = chunk_at_generation(2, 1, 1, 0);
        assert_eq!(gen0.append(&first).await.expect("first").slot, 0);
        assert_eq!(gen0.append(&second).await.expect("second").slot, 1);

        harness.reconfigure_second(owned(1).encode()).await;

        let mut recovery = ChunkLogWriter::recover_virtual(
            journal(),
            cohort(),
            verse(),
            owner_id(),
            harness.virtual_log(),
            RecoveryBound::new(8).expect("bound"),
            None,
        )
        .await
        .expect("recover");
        assert_eq!(recovery.authority.revision(), 1);
        assert_eq!(recovery.chunks.len(), 2);
        assert_eq!(recovery.chunks[0].chunk_id, first.chunk_id);
        assert_eq!(recovery.chunks[1].chunk_id, second.chunk_id);
        assert_eq!(recovery.writer.next_offset(), RecordOffset::new(2));

        let ack = recovery
            .writer
            .append(&chunk_at_generation(3, 2, 1, 1))
            .await
            .expect("gen1 append");
        assert_eq!(ack.slot, 2);
    });
}

#[test]
fn virtual_recovery_preserves_mixed_generation_suffix_order() {
    block_on(async {
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new())).await;
        harness.bootstrap_first(owned(0).encode()).await;
        let mut gen0 = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            0,
            harness.virtual_log(),
            RecordOffset::new(0),
        );
        let a = chunk_at_generation(10, 0, 1, 0);
        let b = chunk_at_generation(11, 1, 1, 0);
        gen0.append(&a).await.expect("a");
        gen0.append(&b).await.expect("b");

        harness.reconfigure_second(owned(1).encode()).await;

        let mut gen1 = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            1,
            harness.virtual_log(),
            RecordOffset::new(2),
        );
        let c = chunk_at_generation(12, 2, 1, 1);
        gen1.append(&c).await.expect("c");

        let recovery = ChunkLogWriter::recover_virtual(
            journal(),
            cohort(),
            verse(),
            owner_id(),
            harness.virtual_log(),
            RecoveryBound::new(8).expect("bound"),
            None,
        )
        .await
        .expect("recover");
        assert_eq!(
            recovery
                .chunks
                .iter()
                .map(|chunk| chunk.chunk_id)
                .collect::<Vec<_>>(),
            vec![a.chunk_id, b.chunk_id, c.chunk_id]
        );
        assert_eq!(recovery.writer.next_offset(), RecordOffset::new(3));
        assert_eq!(
            recovery.chunks[0].frame.submissions[0].producer_id,
            ProducerId::from_bytes(*b"producer-id-0123")
        );
    });
}

#[test]
fn virtual_recovery_rejects_future_chunk_generation() {
    block_on(async {
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new())).await;
        harness.bootstrap_first(owned(0).encode()).await;
        let mut gen0 = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            0,
            harness.virtual_log(),
            RecordOffset::new(0),
        );
        gen0.append(&chunk_at_generation(1, 0, 1, 0))
            .await
            .expect("gen0");

        harness.reconfigure_second(owned(1).encode()).await;

        // Plant a corrupt future-generation payload into the active Loglet.
        let forged = chunk_at_generation(99, 1, 1, 9);
        harness
            .second_writable()
            .append(forged.bytes.clone())
            .await
            .expect("plant forged");

        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                verse(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(8).expect("bound"),
                None,
            )
            .await,
            Err(ChunkLogError::FutureChunkGeneration {
                active: 1,
                actual: 9
            })
        ));
    });
}

#[test]
fn virtual_recovery_rejects_generation_regression_in_logical_order() {
    block_on(async {
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new())).await;
        harness.bootstrap_first(owned(0).encode()).await;
        let mut gen0 = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            0,
            harness.virtual_log(),
            RecordOffset::new(0),
        );
        gen0.append(&chunk_at_generation(20, 0, 1, 0))
            .await
            .expect("gen0 append");

        harness.reconfigure_second(owned(1).encode()).await;
        let mut gen1 = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            1,
            harness.virtual_log(),
            RecordOffset::new(1),
        );
        gen1.append(&chunk_at_generation(21, 1, 1, 1))
            .await
            .expect("gen1 append");

        // A generation-0 chunk after a visible generation-1 chunk cannot be
        // correct fenced history. Plant it directly to model stale/corrupt
        // object-store evidence that recovery must not normalize.
        let regressed = chunk_at_generation(22, 2, 1, 0);
        harness
            .second_writable()
            .append(regressed.bytes.clone())
            .await
            .expect("plant regressed chunk");

        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                verse(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(8).expect("bound"),
                None,
            )
            .await,
            Err(ChunkLogError::RecoveredGenerationRegression {
                previous: 1,
                actual: 0
            })
        ));
    });
}

#[test]
fn virtual_recovery_validates_canon_identity_before_returning_a_writer() {
    block_on(async {
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new())).await;
        harness.bootstrap_first(owned(0).encode()).await;

        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                JournalId::from_bytes(*b"other-journal-id"),
                cohort(),
                verse(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(1).expect("bound"),
                None,
            )
            .await,
            Err(ChunkLogError::Authority(
                CanonAuthorityError::JournalMismatch { .. }
            ))
        ));
        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                VerseId::from_bytes(*b"other-line-id!!!"),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(1).expect("bound"),
                None,
            )
            .await,
            Err(ChunkLogError::Authority(
                CanonAuthorityError::VerseMismatch { .. }
            ))
        ));
        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                verse(),
                OwnerId::from_bytes(*b"other-owner-id!!"),
                harness.virtual_log(),
                RecoveryBound::new(1).expect("bound"),
                None,
            )
            .await,
            Err(ChunkLogError::Authority(
                CanonAuthorityError::NotOwner { .. }
            ))
        ));

        harness
            .reconfigure_id(&harness.second, fence(1, CanonOwner::Unowned).encode())
            .await;
        assert!(matches!(
            observe_canon_authority(&harness.virtual_log(), journal(), verse(), owner_id()).await,
            Err(CanonAuthorityError::Unowned { .. })
        ));
        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                verse(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(1).expect("bound"),
                None,
            )
            .await,
            Err(ChunkLogError::Authority(
                CanonAuthorityError::Unowned { .. }
            ))
        ));
    });
}

#[test]
fn virtual_recovery_fails_closed_when_canon_advances_mid_recovery() {
    block_on(async {
        let flip = Arc::new(FlipOnSecondReadAfterArm::new());
        let harness = VirtualHarness::new(Arc::clone(&flip) as Arc<dyn ConditionalRegister>).await;
        harness.bootstrap_first(owned(0).encode()).await;
        let mut gen0 = ChunkLogWriter::new_virtual(
            journal(),
            cohort(),
            0,
            harness.virtual_log(),
            RecordOffset::new(0),
        );
        gen0.append(&chunk_at_generation(1, 0, 1, 0))
            .await
            .expect("append");

        // After the first linearizable read (observe), later reads see a forged
        // advanced Canon revision so recovery cannot return a writer.
        flip.arm_flip(VirtualLogState {
            revision: 1,
            generations: vec![holylog::virtual_log::GenerationDescriptor {
                loglet_id: harness.first.clone(),
                start: 0,
            }],
            application_fence: owned(1).encode(),
        });

        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                verse(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(8).expect("bound"),
                None,
            )
            .await,
            Err(ChunkLogError::StaleCanonRecovery {
                expected: 0,
                observed: 1
            })
        ));
    });
}

#[test]
fn append_data_ref_writes_pointer_not_chunk_bytes() {
    use scripture::{ChunkDigest, DataRef, LogPayload, decode_log_payload};
    block_on(async {
        let drive: Arc<dyn LogDrive> = Arc::new(InMemoryLogDrive::new());
        let log = AtomicLog::builder(Arc::clone(&drive), 0)
            .build()
            .expect("log");
        let mut writer = ChunkLogWriter::new(journal(), cohort(), 4, log, RecordOffset::new(0));
        let sealed = chunk(1, 0, 2);
        let data_ref = DataRef {
            blob_key: "blobs/v1/demo".into(),
            offset: 32,
            length: sealed.bytes.len() as u64,
            record_count: 2,
            chunk_id: sealed.chunk_id,
            chunk_digest: sealed.digest,
            blob_digest: ChunkDigest::of(b"blob evidence"),
        };
        let ack = writer
            .append_data_ref(&sealed, &data_ref)
            .await
            .expect("append dataref");
        assert_eq!(ack.record_count, 2);
        assert_eq!(ack.first_offset, RecordOffset::new(0));
        assert_eq!(writer.next_offset(), RecordOffset::new(2));

        let log = AtomicLog::builder(drive, 0).build().expect("log2");
        let entry = log.read_next(0, 1).await.expect("read");
        match decode_log_payload(&entry.payload).expect("dispatch") {
            LogPayload::DataRef(decoded) => assert_eq!(decoded, data_ref),
            LogPayload::InlineChunk(_) => panic!("expected DataRef payload"),
        }
    });
}

/// Superseding a chunk that is **not** the latest must report that chunk's own
/// offsets.
///
/// `append_superseding_data_ref` derives `first_offset` by subtracting the
/// pointer's record count from the writer's current `next_offset`. That is only
/// the superseded chunk's range when it happens to be the most recent append.
/// A background rewrite runs while the Verse keeps taking writes, so the
/// ordinary case is superseding an older chunk with newer ones already behind
/// it.
#[test]
fn superseding_an_earlier_chunk_reports_that_chunks_offsets() {
    block_on(async {
        let drive = Arc::new(InMemoryLogDrive::new());
        let log = AtomicLog::builder(drive, 0).build().expect("log");
        let mut writer = ChunkLogWriter::new(journal(), cohort(), 4, log, RecordOffset::new(0));

        // Two staging chunks: records 0..2, then 2..5.
        let first = chunk(1, 0, 2);
        let first_ack = writer.append(&first).await.expect("append first");
        assert_eq!(first_ack.first_offset, RecordOffset::new(0));
        let second = chunk(2, 2, 3);
        writer.append(&second).await.expect("append second");
        assert_eq!(writer.next_offset(), RecordOffset::new(5));

        // Rewrite supersedes the *first* chunk while the second is already
        // durable behind it.
        let superseding = scripture::DataRef {
            blob_key: "verses/v1/deadbeef/cafebabe".into(),
            offset: 0,
            length: first.bytes.len() as u64,
            record_count: 2,
            chunk_id: first.chunk_id,
            chunk_digest: first.digest,
            blob_digest: scripture::ChunkDigest::of(&first.bytes),
        };
        let ack = writer
            .append_superseding_data_ref(
                WriterId::from_bytes(*b"writer-id-012345"),
                &superseding,
                RecordOffset::new(0),
            )
            .await
            .expect("supersede");

        assert_eq!(
            ack.first_offset,
            RecordOffset::new(0),
            "superseding pointer must report the offsets of the chunk it replaces, \
             not the tail of the log"
        );
        assert_eq!(
            writer.next_offset(),
            RecordOffset::new(5),
            "a superseding append must not advance the dense offset"
        );
    });
}
