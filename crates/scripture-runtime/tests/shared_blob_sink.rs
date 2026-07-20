//! Shared Scribe blob sink acceptance tests.

use std::sync::Arc;

use object_store::memory::InMemory;
use scripture::DEFAULT_STAGING_BLOB_PREFIX;
use scripture::{BlobCommitSink, BlobSinkSubmit, DriverError, PendingBlobEnvelope};
use scripture_runtime::{BlobWriterConfig, SharedBlobSink, SharedBlobSinkConfig};

fn tiny_envelope(verse: &str, chunk: u8) -> PendingBlobEnvelope {
    use scripture::{
        AttributeValue, ChunkId, CohortId, JournalId, ProducerId, Record, RecordOffset,
        SubmissionRef,
    };
    PendingBlobEnvelope {
        verse_key: verse.into(),
        chunk_id: ChunkId::from_bytes([chunk; 16]),
        base_offset: RecordOffset::new(0),
        journal_id: JournalId::from_bytes([chunk; 16]),
        cohort_id: CohortId::from_bytes(*b"shared-sink-test"),
        records: vec![Record::new(
            [("k".into(), AttributeValue::I64(1))],
            bytes::Bytes::from_static(b"v"),
        )],
        submissions: vec![SubmissionRef {
            producer_id: ProducerId::from_bytes(*b"producer-shared!"),
            producer_epoch: 1,
            sequence: chunk as u64,
            first_record: 0,
            record_count: 1,
        }],
    }
}

#[tokio::test]
async fn one_hot_verse_cannot_starve_idle_assignments_out_of_the_shared_buffer() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let sink: Arc<dyn BlobCommitSink> = Arc::new(
        SharedBlobSink::spawn(
            store,
            SharedBlobSinkConfig {
                writer: BlobWriterConfig {
                    target_blob_bytes: 1024 * 1024,
                    max_linger: std::time::Duration::from_secs(60),
                    blob_prefix: DEFAULT_STAGING_BLOB_PREFIX.into(),
                },
                max_buffer_bytes: 256,
                per_assignment_max_bytes: 128,
            },
        )
        .expect("sink"),
    );

    let hot = Arc::clone(&sink);
    let idle = Arc::clone(&sink);

    let (tx, _rx) = futures::channel::oneshot::channel();
    BlobCommitSink::submit(
        Arc::clone(&hot),
        BlobSinkSubmit {
            envelope: tiny_envelope("hot", 1),
            encoded_bytes: 64,
            completion: tx,
        },
    )
    .await
    .expect("hot first chunk accepted");

    let (tx, _rx) = futures::channel::oneshot::channel();
    let second = BlobCommitSink::submit(
        Arc::clone(&hot),
        BlobSinkSubmit {
            envelope: tiny_envelope("hot", 2),
            encoded_bytes: 80,
            completion: tx,
        },
    )
    .await;
    assert!(
        matches!(second, Err(DriverError::BlobSinkBufferFull)),
        "hot verse must hit per-assignment fair share before monopolizing the node buffer"
    );

    let (tx, _rx) = futures::channel::oneshot::channel();
    BlobCommitSink::submit(
        Arc::clone(&idle),
        BlobSinkSubmit {
            envelope: tiny_envelope("idle", 3),
            encoded_bytes: 64,
            completion: tx,
        },
    )
    .await
    .expect("idle assignment must retain its fair share while hot is capped");
}

#[tokio::test]
async fn reaching_the_memory_ceiling_backpressures_without_acknowledging_buffered_data() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let sink: Arc<dyn BlobCommitSink> = Arc::new(
        SharedBlobSink::spawn(
            store,
            SharedBlobSinkConfig {
                writer: BlobWriterConfig {
                    target_blob_bytes: 1024 * 1024,
                    max_linger: std::time::Duration::from_secs(60),
                    blob_prefix: DEFAULT_STAGING_BLOB_PREFIX.into(),
                },
                max_buffer_bytes: 100,
                per_assignment_max_bytes: 100,
            },
        )
        .expect("sink"),
    );

    let shared = Arc::clone(&sink);
    let (tx, _rx) = futures::channel::oneshot::channel();
    BlobCommitSink::submit(
        Arc::clone(&shared),
        BlobSinkSubmit {
            envelope: tiny_envelope("a", 1),
            encoded_bytes: 60,
            completion: tx,
        },
    )
    .await
    .expect("first envelope fits");

    let (tx2, _rx2) = futures::channel::oneshot::channel();
    let rejected = BlobCommitSink::submit(
        Arc::clone(&shared),
        BlobSinkSubmit {
            envelope: tiny_envelope("a", 2),
            encoded_bytes: 60,
            completion: tx2,
        },
    )
    .await;
    assert!(matches!(rejected, Err(DriverError::BlobSinkBufferFull)));
}
