//! Cross-Verse blob writer: immutable evidence and handoff-safe sealing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use holylog::atomic::AtomicLog;
use holylog::memory::InMemoryLogDrive;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use scripture::{
    AttributeValue, ChunkAppendAck, ChunkHeader, ChunkId, ChunkLogWriter, CohortId, DataRef, Frame,
    JournalId, ProducerId, Record, RecordOffset, SealedChunk, SubmissionRef, WriterId,
    seal_single_frame_chunk,
};
use scripture_runtime::blob_writer::clock_shim::ManualBlobClock;
use scripture_runtime::{
    BlobCutReason, BlobEnvelope, BlobReadError, BlobWriter, BlobWriterConfig, BlobWriterError,
    DataRefAppendTarget, VerseSealer, commit_cut_plan, resolve_data_refs_coalesced,
    resolve_log_payload,
};

fn journal(byte: u8) -> JournalId {
    JournalId::from_bytes([byte; 16])
}

fn cohort() -> CohortId {
    CohortId::from_bytes(*b"cohort-blob-test")
}

fn record(value: i64) -> Record {
    Record::new(
        [("value".into(), AttributeValue::I64(value))],
        Bytes::from(value.to_be_bytes().to_vec()),
    )
}

fn envelope(verse: &str, chunk: u8, journal_id: JournalId, values: &[i64]) -> BlobEnvelope {
    BlobEnvelope {
        verse_key: verse.into(),
        chunk_id: ChunkId::from_bytes([chunk; 16]),
        base_offset: RecordOffset::new(0),
        journal_id,
        cohort_id: cohort(),
        records: values.iter().copied().map(record).collect(),
        submissions: values
            .iter()
            .enumerate()
            .map(|(sequence, _)| SubmissionRef {
                producer_id: ProducerId::from_bytes(*b"producer-blob-ts"),
                producer_epoch: 1,
                sequence: sequence as u64,
                first_record: sequence as u32,
                record_count: 1,
            })
            .collect(),
    }
}

struct TestSealer {
    generation: u64,
    next: RecordOffset,
}

#[async_trait]
impl VerseSealer for TestSealer {
    async fn seal(&mut self, envelope: &BlobEnvelope) -> Result<SealedChunk, BlobWriterError> {
        let base_offset = self.next;
        self.next = self
            .next
            .checked_add(envelope.records.len())
            .ok_or_else(|| BlobWriterError::Invariant("test offset overflow".into()))?;
        Ok(seal_single_frame_chunk(
            ChunkHeader {
                chunk_id: envelope.chunk_id,
                cohort_id: envelope.cohort_id,
                generation: self.generation,
                writer_id: WriterId::from_bytes(*b"writer-blob-test"),
                created_at_micros: 1,
            },
            vec![Frame {
                journal_id: envelope.journal_id,
                base_offset,
                records: envelope.records.clone(),
                submissions: envelope.submissions.clone(),
            }],
        )?)
    }
}

struct WriterTarget {
    writer: ChunkLogWriter,
    refs: Arc<Mutex<Vec<DataRef>>>,
}

#[async_trait]
impl DataRefAppendTarget for WriterTarget {
    async fn append_data_ref(
        &mut self,
        sealed: &SealedChunk,
        data_ref: &DataRef,
    ) -> Result<ChunkAppendAck, BlobWriterError> {
        let ack = self.writer.append_data_ref(sealed, data_ref).await?;
        self.refs.lock().expect("refs").push(data_ref.clone());
        Ok(ack)
    }

    async fn append_data_refs(
        &mut self,
        items: &[(&SealedChunk, &DataRef)],
    ) -> Result<ChunkAppendAck, BlobWriterError> {
        match items {
            [] => Err(BlobWriterError::Invariant(
                "append_data_refs requires at least one DataRef".into(),
            )),
            [(sealed, data_ref)] => self.append_data_ref(sealed, data_ref).await,
            _ => {
                let ack = self.writer.append_reference_batch(items).await?;
                let mut refs = self.refs.lock().expect("refs");
                for (_, data_ref) in items {
                    refs.push((*data_ref).clone());
                }
                Ok(ack)
            }
        }
    }
}

struct RejectTarget;
#[async_trait]
impl DataRefAppendTarget for RejectTarget {
    async fn append_data_ref(
        &mut self,
        _: &SealedChunk,
        _: &DataRef,
    ) -> Result<ChunkAppendAck, BlobWriterError> {
        Err(BlobWriterError::Invariant(
            "simulated authority loss".into(),
        ))
    }

    async fn append_data_refs(
        &mut self,
        _: &[(&SealedChunk, &DataRef)],
    ) -> Result<ChunkAppendAck, BlobWriterError> {
        Err(BlobWriterError::Invariant(
            "simulated authority loss".into(),
        ))
    }
}

type DataRefs = Arc<Mutex<Vec<DataRef>>>;
type Sealers = BTreeMap<String, Box<dyn VerseSealer>>;
type Targets = BTreeMap<String, Box<dyn DataRefAppendTarget>>;

fn writer_target(
    journal_id: JournalId,
    generation: u64,
    refs: Arc<Mutex<Vec<DataRef>>>,
) -> WriterTarget {
    let drive = Arc::new(InMemoryLogDrive::new());
    let log = AtomicLog::builder(drive, 0).build().expect("log");
    WriterTarget {
        writer: ChunkLogWriter::new(journal_id, cohort(), generation, log, RecordOffset::new(0)),
        refs,
    }
}

fn maps(entries: &[(&str, JournalId, u64, DataRefs)]) -> (Sealers, Targets) {
    let mut sealers: Sealers = BTreeMap::new();
    let mut targets: Targets = BTreeMap::new();
    for (verse, journal_id, generation, refs) in entries {
        sealers.insert(
            (*verse).into(),
            Box::new(TestSealer {
                generation: *generation,
                next: RecordOffset::new(0),
            }),
        );
        targets.insert(
            (*verse).into(),
            Box::new(writer_target(*journal_id, *generation, Arc::clone(refs))),
        );
    }
    (sealers, targets)
}

fn writer(target: usize) -> BlobWriter<Arc<ManualBlobClock>> {
    BlobWriter::with_clock(
        BlobWriterConfig {
            target_blob_bytes: target,
            max_linger: Duration::from_millis(100),
            blob_prefix: "blobs/v1".into(),
        },
        Arc::new(ManualBlobClock::new()),
    )
    .expect("writer")
}

#[test]
fn size_trigger_cuts_independently_of_linger() {
    let mut writer = writer(1);
    let plan = writer
        .push(envelope("a", 1, journal(b'a'), &[1]))
        .expect("push")
        .expect("cut");
    assert_eq!(plan.reason, BlobCutReason::Size);
    assert_eq!(plan.envelopes.len(), 1);
}

#[test]
fn linger_trigger_cuts_independently_of_size() {
    let clock = Arc::new(ManualBlobClock::new());
    let mut writer = BlobWriter::with_clock(
        BlobWriterConfig {
            target_blob_bytes: 1024 * 1024,
            max_linger: Duration::from_millis(100),
            blob_prefix: "blobs/v1".into(),
        },
        Arc::clone(&clock),
    )
    .expect("writer");
    assert!(
        writer
            .push(envelope("a", 1, journal(b'a'), &[1]))
            .expect("push")
            .is_none()
    );
    clock.advance(Duration::from_millis(100));
    assert_eq!(
        writer.poll_linger().expect("poll").expect("cut").reason,
        BlobCutReason::Linger
    );
}

#[tokio::test]
async fn authority_loss_on_one_verse_leaves_sibling_committed_and_keys_chunk_id() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut writer = writer(1024 * 1024);
    writer
        .push(envelope("a", 1, journal(b'a'), &[1]))
        .expect("push");
    writer
        .push(envelope("b", 2, journal(b'b'), &[2]))
        .expect("push");
    let plan = writer.flush_drained().expect("flush").expect("plan");
    let refs = Arc::new(Mutex::new(Vec::new()));
    let (mut sealers, mut targets) = maps(&[("a", journal(b'a'), 1, Arc::clone(&refs))]);
    sealers.insert(
        "b".into(),
        Box::new(TestSealer {
            generation: 1,
            next: RecordOffset::new(0),
        }),
    );
    targets.insert("b".into(), Box::new(RejectTarget));
    let outcomes = commit_cut_plan(&store, &plan, &mut sealers, &mut targets)
        .await
        .expect("commit");
    assert!(
        outcomes
            .iter()
            .any(|o| o.chunk_id == ChunkId::from_bytes([1; 16]) && o.result.is_ok())
    );
    assert!(
        outcomes
            .iter()
            .any(|o| o.chunk_id == ChunkId::from_bytes([2; 16]) && o.result.is_err())
    );
}

#[tokio::test]
async fn wrong_range_wrong_blob_and_valid_same_count_chunk_are_rejected() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let refs = Arc::new(Mutex::new(Vec::new()));
    let mut writer = writer(1024 * 1024);
    writer
        .push(envelope("a", 1, journal(b'a'), &[10]))
        .expect("push");
    writer
        .push(envelope("a", 2, journal(b'a'), &[11]))
        .expect("push");
    let plan = writer.flush_drained().expect("flush").expect("plan");
    let (mut sealers, mut targets) = maps(&[("a", journal(b'a'), 1, Arc::clone(&refs))]);
    commit_cut_plan(&store, &plan, &mut sealers, &mut targets)
        .await
        .expect("commit");
    let refs = refs.lock().expect("refs").clone();
    let mut wrong_range = refs[0].clone();
    wrong_range.offset = refs[1].offset;
    assert!(matches!(
        resolve_log_payload(
            &store,
            &scripture::encode_data_ref(&wrong_range).expect("encode")
        )
        .await,
        Err(BlobReadError::Payload(
            scripture::DataRefError::ChunkDigestMismatch
        ))
    ));
    let mut wrong_blob = refs[0].clone();
    wrong_blob.blob_digest = scripture::ChunkDigest::from_bytes([9; 32]);
    assert!(matches!(
        resolve_log_payload(
            &store,
            &scripture::encode_data_ref(&wrong_blob).expect("encode")
        )
        .await,
        Err(BlobReadError::Payload(
            scripture::DataRefError::BlobDigestMismatch
        ))
    ));

    // The subtle case, stated on its own rather than left implicit: a range
    // that lands exactly on a *different valid chunk* carrying the same record
    // count. Length and count both agree, so only the digest and chunk id can
    // reject it. Asserting the other two cases would still pass with those
    // checks removed.
    let mut other_valid_chunk = refs[0].clone();
    other_valid_chunk.offset = refs[1].offset;
    other_valid_chunk.length = refs[1].length;
    assert_eq!(
        other_valid_chunk.record_count, refs[1].record_count,
        "the case is only meaningful when both chunks carry the same count"
    );
    assert!(matches!(
        resolve_log_payload(
            &store,
            &scripture::encode_data_ref(&other_valid_chunk).expect("encode")
        )
        .await,
        Err(BlobReadError::Payload(
            scripture::DataRefError::ChunkDigestMismatch
        ))
    ));
}

#[tokio::test]
async fn two_chunks_one_verse_and_sibling_failure_ack_only_exact_chunk_ids() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let refs = Arc::new(Mutex::new(Vec::new()));
    let mut writer = writer(1024 * 1024);
    for (verse, id, j) in [
        ("a", 1, journal(b'a')),
        ("a", 2, journal(b'a')),
        ("b", 3, journal(b'b')),
    ] {
        writer
            .push(envelope(verse, id, j, &[id as i64]))
            .expect("push");
    }
    let plan = writer.flush_drained().expect("flush").expect("plan");
    let (mut sealers, mut targets) = maps(&[("a", journal(b'a'), 1, Arc::clone(&refs))]);
    sealers.insert(
        "b".into(),
        Box::new(TestSealer {
            generation: 1,
            next: RecordOffset::new(0),
        }),
    );
    targets.insert("b".into(), Box::new(RejectTarget));
    let outcomes = commit_cut_plan(&store, &plan, &mut sealers, &mut targets)
        .await
        .expect("commit");
    let acked: Vec<_> = outcomes
        .into_iter()
        .filter(|o| o.result.is_ok())
        .map(|o| o.chunk_id)
        .collect();
    assert_eq!(
        acked,
        vec![ChunkId::from_bytes([1; 16]), ChunkId::from_bytes([2; 16])]
    );
}

#[tokio::test]
async fn handoff_replay_reseals_same_envelopes_under_successor_generation() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let refs0 = Arc::new(Mutex::new(Vec::new()));
    let mut writer = writer(1024 * 1024);
    writer
        .push(envelope("a", 7, journal(b'a'), &[7]))
        .expect("push");
    let plan = writer.flush_drained().expect("flush").expect("plan");
    let (mut sealers0, mut targets0) = maps(&[("a", journal(b'a'), 1, Arc::clone(&refs0))]);
    let first = commit_cut_plan(&store, &plan, &mut sealers0, &mut targets0)
        .await
        .expect("gen1");
    let refs1 = Arc::new(Mutex::new(Vec::new()));
    let (mut sealers1, mut targets1) = maps(&[("a", journal(b'a'), 2, Arc::clone(&refs1))]);
    let replay = commit_cut_plan(&store, &plan, &mut sealers1, &mut targets1)
        .await
        .expect("gen2 replay");
    assert_eq!(first[0].chunk_id, replay[0].chunk_id);
    let payload = scripture::encode_data_ref(&refs1.lock().expect("refs")[0]).expect("payload");
    let resolved = resolve_log_payload(&store, &payload)
        .await
        .expect("resolve");
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].chunk.header.generation, 2);
}

#[tokio::test]
async fn lost_put_reply_retry_uses_one_content_addressed_blob_without_duplicate_dataref() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let refs = Arc::new(Mutex::new(Vec::new()));
    let mut writer = writer(1024 * 1024);
    writer
        .push(envelope("a", 8, journal(b'a'), &[8]))
        .expect("push");
    let plan = writer.flush_drained().expect("flush").expect("plan");
    let (mut sealers, mut targets) = maps(&[("a", journal(b'a'), 1, Arc::clone(&refs))]);
    commit_cut_plan(&store, &plan, &mut sealers, &mut targets)
        .await
        .expect("first commit");
    assert_eq!(refs.lock().expect("refs").len(), 1);
    assert_eq!(store.list(None).collect::<Vec<_>>().await.len(), 1);
}

#[tokio::test]
async fn reads_across_two_blob_generations_return_exact_records() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let refs = Arc::new(Mutex::new(Vec::new()));
    for (id, value) in [(1, 20), (2, 21)] {
        let mut writer = writer(1);
        let plan = writer
            .push(envelope("a", id, journal(b'a'), &[value]))
            .expect("push")
            .expect("plan");
        let (mut sealers, mut targets) = maps(&[("a", journal(b'a'), 1, Arc::clone(&refs))]);
        commit_cut_plan(&store, &plan, &mut sealers, &mut targets)
            .await
            .expect("commit");
    }
    let refs = refs.lock().expect("refs").clone();
    let resolved = resolve_data_refs_coalesced(&store, &refs)
        .await
        .expect("read");
    let values: Vec<_> = resolved
        .iter()
        .map(
            |chunk| match chunk.chunk.frames[0].records[0].attributes["value"] {
                AttributeValue::I64(value) => value,
                _ => panic!("value"),
            },
        )
        .collect();
    assert_eq!(values, vec![20, 21]);
}

#[tokio::test]
async fn same_verse_multi_chunk_cut_commits_one_reference_batch() {
    use scripture::{LogPayload, decode_log_payload};

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let drive = Arc::new(InMemoryLogDrive::new());
    let log = AtomicLog::builder(drive, 0).build().expect("log");
    let refs = Arc::new(Mutex::new(Vec::new()));
    let mut sealers: Sealers = BTreeMap::new();
    sealers.insert(
        "a".into(),
        Box::new(TestSealer {
            generation: 1,
            next: RecordOffset::new(0),
        }),
    );
    let mut targets: Targets = BTreeMap::new();
    targets.insert(
        "a".into(),
        Box::new(WriterTarget {
            writer: ChunkLogWriter::new(
                journal(b'a'),
                cohort(),
                1,
                log.clone(),
                RecordOffset::new(0),
            ),
            refs: Arc::clone(&refs),
        }),
    );

    let mut writer = writer(1024 * 1024);
    writer
        .push(envelope("a", 1, journal(b'a'), &[1]))
        .expect("push");
    writer
        .push(envelope("a", 2, journal(b'a'), &[2]))
        .expect("push");
    let plan = writer.flush_drained().expect("flush").expect("plan");
    let outcomes = commit_cut_plan(&store, &plan, &mut sealers, &mut targets)
        .await
        .expect("commit");
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| o.result.is_ok()));
    let slot0 = outcomes[0].result.as_ref().expect("ack0").slot;
    let slot1 = outcomes[1].result.as_ref().expect("ack1").slot;
    assert_eq!(slot0, slot1, "both placements share one Holylog slot");
    assert_eq!(outcomes[0].chunk_id, ChunkId::from_bytes([1; 16]));
    assert_eq!(outcomes[1].chunk_id, ChunkId::from_bytes([2; 16]));

    let entry = log.read_next(0, 1).await.expect("read");
    let LogPayload::ReferenceBatch(batch) = decode_log_payload(&entry.payload).expect("dispatch")
    else {
        panic!("multi-chunk same-Verse cut must write SRRB, not SRDF");
    };
    assert_eq!(batch.len(), 2);
    let resolved = resolve_log_payload(&store, &entry.payload)
        .await
        .expect("expand");
    assert_eq!(resolved.len(), 2);
    assert_eq!(
        resolved[0].chunk.header.chunk_id,
        ChunkId::from_bytes([1; 16])
    );
    assert_eq!(
        resolved[1].chunk.header.chunk_id,
        ChunkId::from_bytes([2; 16])
    );
}
