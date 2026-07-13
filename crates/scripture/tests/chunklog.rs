//! The immutable codec exercised through the real Holylog append boundary.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::executor::block_on;
use holylog::atomic::AtomicLog;
use holylog::memory::InMemoryLogDrive;
use holylog::virtual_log::{
    ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver, ResolveFuture,
    VirtualLog,
};
use scripture::{
    AttributeValue, ChunkHeader, ChunkId, ChunkLogWriter, CohortId, Frame, JournalId, ProducerId,
    Record, RecordOffset, RecoveryBound, SubmissionRef, WriterId, seal_single_frame_chunk,
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
