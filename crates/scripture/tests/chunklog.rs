//! The immutable codec exercised through the real Holylog append boundary.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::executor::block_on;
use holylog::atomic::AtomicLog;
use holylog::memory::InMemoryLogDrive;
use holylog::virtual_log::{
    CompareToken, ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver,
    RegisterFuture, ResolveFuture, VersionedState, VirtualLog, VirtualLogState,
};
use scripture::{
    AttributeValue, CanonAuthorityError, CanonFence, CanonOwner, ChunkHeader, ChunkId,
    ChunkLogError, ChunkLogWriter, CohortId, Frame, JournalId, LineId, OwnerEndpoint, OwnerId,
    ProducerId, Record, RecordOffset, RecoveryBound, SubmissionRef, WriterId,
    observe_canon_authority, seal_single_frame_chunk,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"journal-id-01234")
}

#[derive(Default)]
struct TestResolver {
    loglets: Mutex<BTreeMap<LogletId, Arc<AtomicLog>>>,
}

impl TestResolver {
    fn insert(&self, id: LogletId, log: Arc<AtomicLog>) {
        self.loglets.lock().expect("resolver lock").insert(id, log);
    }
}

impl LogletResolver for TestResolver {
    fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<Arc<AtomicLog>>> {
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
        let resolver = Arc::new(TestResolver::default());
        let register = Arc::new(InMemoryConditionalRegister::new());
        let first = LogletId::new("canon-line-first").expect("first id");
        let second = LogletId::new("canon-line-second").expect("second id");
        let first_log = Arc::new(
            AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                .build()
                .expect("first log"),
        );
        let second_log = Arc::new(
            AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                .build()
                .expect("second log"),
        );
        resolver.insert(first.clone(), first_log);
        resolver.insert(second.clone(), second_log);

        let owner_log = VirtualLog::new(
            Arc::clone(&register) as Arc<dyn ConditionalRegister>,
            Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        );
        let reconfigurer = VirtualLog::new(
            Arc::clone(&register) as Arc<dyn ConditionalRegister>,
            Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        );
        owner_log.bootstrap(first).await.expect("bootstrap");

        let mut old_owner =
            ChunkLogWriter::new_virtual(journal(), cohort(), 0, owner_log, RecordOffset::new(0));
        assert_eq!(
            old_owner
                .append(&chunk_at_generation(11, 0, 1, 0))
                .await
                .expect("old generation append")
                .slot,
            0
        );

        reconfigurer.reconfigure(second).await.expect("cutover");

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

        let mut successor_owner =
            ChunkLogWriter::new_virtual(journal(), cohort(), 1, reconfigurer, RecordOffset::new(1));
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

fn line() -> LineId {
    LineId::from_bytes(*b"canon-line-id!!!")
}

fn owner_id() -> OwnerId {
    OwnerId::from_bytes(*b"canon-owner-id!!")
}

fn fence(revision: u64, owner: CanonOwner) -> CanonFence {
    CanonFence::new(revision, journal(), line(), owner)
}

fn owned(revision: u64) -> CanonFence {
    fence(
        revision,
        CanonOwner::Owned {
            owner_id: owner_id(),
            endpoint: OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint"),
        },
    )
}

struct FlipOnLaterRead {
    inner: InMemoryConditionalRegister,
    reads: AtomicUsize,
    flip_at: usize,
    flipped: Mutex<Option<VirtualLogState>>,
}

impl FlipOnLaterRead {
    fn new(flip_at: usize) -> Self {
        Self {
            inner: InMemoryConditionalRegister::new(),
            reads: AtomicUsize::new(0),
            flip_at,
            flipped: Mutex::new(None),
        }
    }

    fn arm_flip(&self, state: VirtualLogState) {
        *self.flipped.lock().expect("flip lock") = Some(state);
    }
}

impl ConditionalRegister for FlipOnLaterRead {
    fn read(&self) -> RegisterFuture<'_, Option<VersionedState>> {
        Box::pin(async {
            let n = self.reads.fetch_add(1, Ordering::SeqCst);
            if n >= self.flip_at
                && let Some(state) = self.flipped.lock().expect("flip lock").clone()
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

struct VirtualHarness {
    resolver: Arc<TestResolver>,
    register: Arc<dyn ConditionalRegister>,
    first: LogletId,
    second: LogletId,
    second_log: Arc<AtomicLog>,
}

impl VirtualHarness {
    fn new(register: Arc<dyn ConditionalRegister>) -> Self {
        let resolver = Arc::new(TestResolver::default());
        let first = LogletId::new("recover-line-first").expect("first id");
        let second = LogletId::new("recover-line-second").expect("second id");
        let first_log = Arc::new(
            AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                .build()
                .expect("first log"),
        );
        let second_log = Arc::new(
            AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                .build()
                .expect("second log"),
        );
        resolver.insert(first.clone(), Arc::clone(&first_log));
        resolver.insert(second.clone(), Arc::clone(&second_log));
        Self {
            resolver,
            register,
            first,
            second,
            second_log,
        }
    }

    fn virtual_log(&self) -> VirtualLog {
        VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        )
    }
}

#[test]
fn virtual_recovery_rebuilds_suffix_across_canon_cutover() {
    block_on(async {
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new()));
        let bootstrap = harness.virtual_log();
        bootstrap
            .bootstrap_with_application_fence(harness.first.clone(), owned(0).encode())
            .await
            .expect("bootstrap");

        let mut gen0 =
            ChunkLogWriter::new_virtual(journal(), cohort(), 0, bootstrap, RecordOffset::new(0));
        let first = chunk_at_generation(1, 0, 1, 0);
        let second = chunk_at_generation(2, 1, 1, 0);
        assert_eq!(gen0.append(&first).await.expect("first").slot, 0);
        assert_eq!(gen0.append(&second).await.expect("second").slot, 1);

        let cutter = harness.virtual_log();
        cutter
            .reconfigure_with_application_fence(harness.second.clone(), owned(1).encode())
            .await
            .expect("cutover");

        let mut recovery = ChunkLogWriter::recover_virtual(
            journal(),
            cohort(),
            line(),
            owner_id(),
            harness.virtual_log(),
            RecoveryBound::new(8).expect("bound"),
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
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new()));
        let bootstrap = harness.virtual_log();
        bootstrap
            .bootstrap_with_application_fence(harness.first.clone(), owned(0).encode())
            .await
            .expect("bootstrap");
        let mut gen0 =
            ChunkLogWriter::new_virtual(journal(), cohort(), 0, bootstrap, RecordOffset::new(0));
        let a = chunk_at_generation(10, 0, 1, 0);
        let b = chunk_at_generation(11, 1, 1, 0);
        gen0.append(&a).await.expect("a");
        gen0.append(&b).await.expect("b");

        let cutter = harness.virtual_log();
        cutter
            .reconfigure_with_application_fence(harness.second.clone(), owned(1).encode())
            .await
            .expect("cutover");

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
            line(),
            owner_id(),
            harness.virtual_log(),
            RecoveryBound::new(8).expect("bound"),
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
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new()));
        let bootstrap = harness.virtual_log();
        bootstrap
            .bootstrap_with_application_fence(harness.first.clone(), owned(0).encode())
            .await
            .expect("bootstrap");
        let mut gen0 =
            ChunkLogWriter::new_virtual(journal(), cohort(), 0, bootstrap, RecordOffset::new(0));
        gen0.append(&chunk_at_generation(1, 0, 1, 0))
            .await
            .expect("gen0");

        let cutter = harness.virtual_log();
        cutter
            .reconfigure_with_application_fence(harness.second.clone(), owned(1).encode())
            .await
            .expect("cutover");

        // Plant a corrupt future-generation payload into the active Loglet.
        let forged = chunk_at_generation(99, 1, 1, 9);
        harness
            .second_log
            .append(forged.bytes.clone())
            .await
            .expect("plant forged");

        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                line(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(8).expect("bound"),
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
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new()));
        let bootstrap = harness.virtual_log();
        bootstrap
            .bootstrap_with_application_fence(harness.first.clone(), owned(0).encode())
            .await
            .expect("bootstrap");
        let mut gen0 =
            ChunkLogWriter::new_virtual(journal(), cohort(), 0, bootstrap, RecordOffset::new(0));
        gen0.append(&chunk_at_generation(20, 0, 1, 0))
            .await
            .expect("gen0 append");

        let cutter = harness.virtual_log();
        cutter
            .reconfigure_with_application_fence(harness.second.clone(), owned(1).encode())
            .await
            .expect("cutover");
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
            .second_log
            .append(regressed.bytes.clone())
            .await
            .expect("plant regressed chunk");

        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                line(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(8).expect("bound"),
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
        let harness = VirtualHarness::new(Arc::new(InMemoryConditionalRegister::new()));
        let bootstrap = harness.virtual_log();
        bootstrap
            .bootstrap_with_application_fence(harness.first.clone(), owned(0).encode())
            .await
            .expect("bootstrap");

        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                JournalId::from_bytes(*b"other-journal-id"),
                cohort(),
                line(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(1).expect("bound"),
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
                LineId::from_bytes(*b"other-line-id!!!"),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(1).expect("bound"),
            )
            .await,
            Err(ChunkLogError::Authority(
                CanonAuthorityError::LineMismatch { .. }
            ))
        ));
        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                line(),
                OwnerId::from_bytes(*b"other-owner-id!!"),
                harness.virtual_log(),
                RecoveryBound::new(1).expect("bound"),
            )
            .await,
            Err(ChunkLogError::Authority(
                CanonAuthorityError::NotOwner { .. }
            ))
        ));

        let unowned = harness.virtual_log();
        // Replace fence with Unowned via cutover onto a fresh loglet.
        let third = LogletId::new("recover-line-unowned").expect("third");
        harness.resolver.insert(
            third.clone(),
            Arc::new(
                AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                    .build()
                    .expect("third"),
            ),
        );
        // Need an owned appendable log first — reconfigure from current.
        // Current is still first at rev 0. Cutover to second with Unowned fence.
        unowned
            .reconfigure_with_application_fence(
                harness.second.clone(),
                fence(1, CanonOwner::Unowned).encode(),
            )
            .await
            .expect("unowned cutover");
        assert!(matches!(
            observe_canon_authority(&harness.virtual_log(), journal(), line(), owner_id()).await,
            Err(CanonAuthorityError::Unowned { .. })
        ));
        assert!(matches!(
            ChunkLogWriter::recover_virtual(
                journal(),
                cohort(),
                line(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(1).expect("bound"),
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
        let flip = Arc::new(FlipOnLaterRead::new(1));
        let harness = VirtualHarness::new(Arc::clone(&flip) as Arc<dyn ConditionalRegister>);
        let bootstrap = harness.virtual_log();
        bootstrap
            .bootstrap_with_application_fence(harness.first.clone(), owned(0).encode())
            .await
            .expect("bootstrap");
        let mut gen0 =
            ChunkLogWriter::new_virtual(journal(), cohort(), 0, bootstrap, RecordOffset::new(0));
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
                line(),
                owner_id(),
                harness.virtual_log(),
                RecoveryBound::new(8).expect("bound"),
            )
            .await,
            Err(ChunkLogError::StaleCanonRecovery {
                expected: 0,
                observed: 1
            })
        ));
    });
}
