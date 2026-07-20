//! Shared fixture values for real-actor integration tests.

use std::time::Duration;

use bytes::Bytes;
use holylog::logdrive::Address;
use scripture::{
    AttributeValue, ChunkPolicy, CohortId, JournalId, ProducerId, Record, RecoveryBound, WriterId,
};

pub(crate) fn journal() -> JournalId {
    JournalId::from_bytes(*b"driver-journal!!")
}

pub(crate) fn cohort() -> CohortId {
    CohortId::from_bytes(*b"driver-cohort!!!")
}

pub(crate) fn writer_id() -> WriterId {
    WriterId::from_bytes(*b"driver-writer!!!")
}

pub(crate) fn producer() -> ProducerId {
    ProducerId::from_bytes(*b"driver-producer!")
}

pub(crate) fn record(n: i64) -> Record {
    Record::new(
        [("n".into(), AttributeValue::I64(n))],
        Bytes::from(format!("payload-{n}")),
    )
}

pub(crate) fn address(slot: u64) -> Address {
    Address::new(slot).expect("address")
}

pub(crate) fn policy() -> ChunkPolicy {
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

pub(crate) fn tiny_policy() -> ChunkPolicy {
    ChunkPolicy {
        max_chunk_bytes: 512,
        max_record_bytes: 256,
        max_chunk_records: 2,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    }
}

/// Policy tuned for age, reservation, and depth-one hostile schedules.
pub(crate) fn hostile_policy() -> ChunkPolicy {
    ChunkPolicy {
        max_chunk_bytes: 2048,
        max_record_bytes: 512,
        max_chunk_records: 4,
        max_chunk_age: Duration::from_millis(8),
        max_buffered_bytes: 768,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_millis(8),
        recovery_scan: RecoveryBound::new(32).expect("bound"),
    }
}
