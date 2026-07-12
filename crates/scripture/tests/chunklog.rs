//! The immutable codec exercised through the real Holylog append boundary.

use std::sync::Arc;

use bytes::Bytes;
use futures::executor::block_on;
use holylog::atomic::AtomicLog;
use holylog::memory::InMemoryLogDrive;
use scripture::{
    AttributeValue, ChunkHeader, ChunkId, ChunkLogWriter, CohortId, Frame, JournalId, ProducerId,
    Record, RecordOffset, RecoveryBound, SubmissionRef, WriterId, seal_single_frame_chunk,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"journal-id-01234")
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
        header(chunk),
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
        assert_eq!(recovery.chunks[1].chunk_id, second.chunk_id);
    });
}
