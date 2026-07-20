//! Acceptance tests for mounting DataRefs on the ChunkDriverActor path.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::executor::LocalPool;
use futures::future::BoxFuture;
use futures::task::SpawnExt;
use holylog::atomic::AtomicLog;
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use scripture::{
    ChunkBlobStore, ChunkDigest, ChunkDriverActor, ChunkLogError, ChunkLogWriter,
    DataRefBlobConfig, LogPayload, RecordOffset, RecoveryBound, Submission, decode_log_payload,
};

#[path = "support/mod.rs"]
mod support;

use support::{cohort, journal, policy, producer, record, writer_id};

#[derive(Default)]
struct MemoryBlobStore {
    objects: Mutex<HashMap<String, Bytes>>,
}

impl ChunkBlobStore for MemoryBlobStore {
    fn put_verified<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        digest: ChunkDigest,
    ) -> BoxFuture<'a, Result<(), ChunkLogError>> {
        Box::pin(async move {
            if ChunkDigest::of(&bytes) != digest {
                return Err(ChunkLogError::BlobStore("digest mismatch".into()));
            }
            self.objects
                .lock()
                .expect("objects")
                .insert(key.to_owned(), bytes);
            Ok(())
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Bytes, ChunkLogError>> {
        Box::pin(async move {
            self.objects
                .lock()
                .expect("objects")
                .get(key)
                .cloned()
                .ok_or_else(|| ChunkLogError::DataRefBlobMissing { key: key.into() })
        })
    }
}

fn submission(sequence: u64, values: &[i64]) -> Submission {
    Submission {
        producer_id: producer(),
        producer_epoch: 1,
        sequence,
        records: values.iter().copied().map(record).collect(),
    }
}

fn build_driver(
    drive: Arc<dyn LogDrive>,
    dataref: Option<DataRefBlobConfig>,
) -> (
    scripture::ChunkDriverHandle,
    ChunkDriverActor<Arc<scripture::ManualClock>, scripture::ManualTimer>,
) {
    let log = AtomicLog::builder(drive, 0).build().expect("log");
    let writer = ChunkLogWriter::new(journal(), cohort(), 1, log, RecordOffset::new(0));
    let clock = Arc::new(scripture::ManualClock::new());
    let timer = scripture::ManualTimer::new(Arc::clone(&clock));
    ChunkDriverActor::new(
        journal(),
        cohort(),
        writer_id(),
        1,
        writer,
        &[],
        policy(),
        clock,
        timer,
        8,
        dataref,
    )
    .expect("actor")
}

#[test]
fn driver_append_emits_dataref_and_record_reads_end_to_end() {
    let store = Arc::new(MemoryBlobStore::default());
    let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
    let log_for_read = AtomicLog::builder(Arc::clone(&drive), 0)
        .build()
        .expect("log");
    let (handle, actor) = build_driver(
        Arc::clone(&drive),
        Some(DataRefBlobConfig::new(
            store.clone() as Arc<dyn ChunkBlobStore>
        )),
    );

    let mut pool = LocalPool::new();
    pool.spawner()
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn");

    let receipt = pool.run_until(async {
        let future = handle.submit(submission(0, &[42])).await.expect("admit");
        handle.flush().await.expect("flush");
        future.await.expect("committed")
    });
    assert_eq!(receipt.first_offset, RecordOffset::new(0));
    assert_eq!(receipt.next_offset, RecordOffset::new(1));

    let entry = pool.run_until(log_for_read.read_next(0, 1)).expect("read");
    match decode_log_payload(&entry.payload).expect("dispatch") {
        LogPayload::DataRef(data_ref) => {
            assert_eq!(data_ref.record_count, 1);
            assert_eq!(data_ref.chunk_id, receipt.chunk_id);
            let bytes = pool
                .run_until(store.get(&data_ref.blob_key))
                .expect("blob bytes");
            let chunk = scripture::decode_chunk(&bytes).expect("chunk");
            assert_eq!(chunk.frames[0].records.len(), 1);
            assert_eq!(chunk.frames[0].records[0], record(42));
        }
        LogPayload::InlineChunk(_) => panic!("expected DataRef payload on live path"),
        LogPayload::ReferenceBatch(_) => panic!("expected single DataRef payload on live path"),
    }
}

#[test]
fn mixed_log_inline_then_dataref_reads_in_sequence() {
    let store = Arc::new(MemoryBlobStore::default());
    let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
    let log = AtomicLog::builder(Arc::clone(&drive), 0)
        .build()
        .expect("log");

    // First entry: legacy inline chunk via a plain writer.
    let mut inline_writer =
        ChunkLogWriter::new(journal(), cohort(), 1, log.clone(), RecordOffset::new(0));
    let sealed = {
        use scripture::{ChunkHeader, Frame, SubmissionRef, seal_single_frame_chunk};
        seal_single_frame_chunk(
            ChunkHeader {
                chunk_id: scripture::ChunkId::from_bytes([1; 16]),
                cohort_id: cohort(),
                generation: 1,
                writer_id: writer_id(),
                created_at_micros: 1,
            },
            vec![Frame {
                journal_id: journal(),
                base_offset: RecordOffset::new(0),
                records: vec![record(1)],
                submissions: vec![SubmissionRef {
                    producer_id: producer(),
                    producer_epoch: 1,
                    sequence: 0,
                    first_record: 0,
                    record_count: 1,
                }],
            }],
        )
        .expect("seal")
    };
    let mut pool = LocalPool::new();
    pool.run_until(inline_writer.append(&sealed))
        .expect("inline append");

    // Second entry: DataRef via the driver commit helper.
    let sealed2 = {
        use scripture::{ChunkHeader, Frame, SubmissionRef, seal_single_frame_chunk};
        seal_single_frame_chunk(
            ChunkHeader {
                chunk_id: scripture::ChunkId::from_bytes([2; 16]),
                cohort_id: cohort(),
                generation: 1,
                writer_id: writer_id(),
                created_at_micros: 2,
            },
            vec![Frame {
                journal_id: journal(),
                base_offset: RecordOffset::new(1),
                records: vec![record(2)],
                submissions: vec![SubmissionRef {
                    producer_id: producer(),
                    producer_epoch: 1,
                    sequence: 1,
                    first_record: 0,
                    record_count: 1,
                }],
            }],
        )
        .expect("seal")
    };
    pool.run_until(scripture::commit_sealed_as_data_ref(
        &mut inline_writer,
        store.as_ref(),
        "blobs/v1",
        &sealed2,
    ))
    .expect("dataref append");

    let first = pool.run_until(log.read_next(0, 2)).expect("read0");
    let second = pool.run_until(log.read_next(1, 2)).expect("read1");
    assert!(matches!(
        decode_log_payload(&first.payload).expect("d0"),
        LogPayload::InlineChunk(_)
    ));
    let LogPayload::DataRef(data_ref) = decode_log_payload(&second.payload).expect("d1") else {
        panic!("expected DataRef as second entry");
    };
    assert_eq!(data_ref.chunk_id, sealed2.chunk_id);

    let recovery = pool
        .run_until(ChunkLogWriter::recover(
            journal(),
            cohort(),
            1,
            log,
            RecoveryBound::new(8).expect("bound"),
            Some(store.as_ref() as &dyn ChunkBlobStore),
        ))
        .expect("recover mixed");
    assert_eq!(recovery.chunks.len(), 2);
    assert_eq!(recovery.chunks[0].first_offset, RecordOffset::new(0));
    assert_eq!(recovery.chunks[1].first_offset, RecordOffset::new(1));
    assert_eq!(recovery.chunks[0].frame.submissions[0].sequence, 0);
    assert_eq!(recovery.chunks[1].frame.submissions[0].sequence, 1);
}

#[test]
fn recovery_over_dataref_restores_producer_dedup_state_equivalent_to_inline() {
    let store = Arc::new(MemoryBlobStore::default());
    let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
    let log = AtomicLog::builder(Arc::clone(&drive), 0)
        .build()
        .expect("log");
    let mut writer = ChunkLogWriter::new(journal(), cohort(), 1, log.clone(), RecordOffset::new(0));

    let sealed = {
        use scripture::{ChunkHeader, Frame, SubmissionRef, seal_single_frame_chunk};
        seal_single_frame_chunk(
            ChunkHeader {
                chunk_id: scripture::ChunkId::from_bytes([9; 16]),
                cohort_id: cohort(),
                generation: 1,
                writer_id: writer_id(),
                created_at_micros: 1,
            },
            vec![Frame {
                journal_id: journal(),
                base_offset: RecordOffset::new(0),
                records: vec![record(7), record(8)],
                submissions: vec![SubmissionRef {
                    producer_id: producer(),
                    producer_epoch: 1,
                    sequence: 3,
                    first_record: 0,
                    record_count: 2,
                }],
            }],
        )
        .expect("seal")
    };

    let mut pool = LocalPool::new();
    pool.run_until(scripture::commit_sealed_as_data_ref(
        &mut writer,
        store.as_ref(),
        "blobs/v1",
        &sealed,
    ))
    .expect("commit");

    let recovery = pool
        .run_until(ChunkLogWriter::recover(
            journal(),
            cohort(),
            1,
            log,
            RecoveryBound::new(4).expect("bound"),
            Some(store.as_ref() as &dyn ChunkBlobStore),
        ))
        .expect("recover");

    assert_eq!(recovery.chunks.len(), 1);
    let chunk = &recovery.chunks[0];
    assert_eq!(chunk.chunk_id, sealed.chunk_id);
    assert_eq!(chunk.first_offset, RecordOffset::new(0));
    assert_eq!(chunk.record_count, 2);
    assert_eq!(chunk.frame.submissions.len(), 1);
    assert_eq!(chunk.frame.submissions[0].producer_id, producer());
    assert_eq!(chunk.frame.submissions[0].producer_epoch, 1);
    assert_eq!(chunk.frame.submissions[0].sequence, 3);
    assert_eq!(chunk.frame.submissions[0].record_count, 2);
}

#[test]
fn recovery_over_dataref_with_missing_blob_fails_loudly() {
    let store = Arc::new(MemoryBlobStore::default());
    let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
    let log = AtomicLog::builder(Arc::clone(&drive), 0)
        .build()
        .expect("log");
    let mut writer = ChunkLogWriter::new(journal(), cohort(), 1, log.clone(), RecordOffset::new(0));
    let sealed = {
        use scripture::{ChunkHeader, Frame, SubmissionRef, seal_single_frame_chunk};
        seal_single_frame_chunk(
            ChunkHeader {
                chunk_id: scripture::ChunkId::from_bytes([3; 16]),
                cohort_id: cohort(),
                generation: 1,
                writer_id: writer_id(),
                created_at_micros: 1,
            },
            vec![Frame {
                journal_id: journal(),
                base_offset: RecordOffset::new(0),
                records: vec![record(1)],
                submissions: vec![SubmissionRef {
                    producer_id: producer(),
                    producer_epoch: 1,
                    sequence: 0,
                    first_record: 0,
                    record_count: 1,
                }],
            }],
        )
        .expect("seal")
    };
    let mut pool = LocalPool::new();
    pool.run_until(scripture::commit_sealed_as_data_ref(
        &mut writer,
        store.as_ref(),
        "blobs/v1",
        &sealed,
    ))
    .expect("commit");

    // Delete the blob after the pointer is durable.
    store.objects.lock().expect("objects").clear();

    let err = pool
        .run_until(ChunkLogWriter::recover(
            journal(),
            cohort(),
            1,
            log,
            RecoveryBound::new(4).expect("bound"),
            Some(store.as_ref() as &dyn ChunkBlobStore),
        ))
        .expect_err("missing blob must fail recovery");
    assert!(matches!(err, ChunkLogError::DataRefBlobMissing { .. }));
}
