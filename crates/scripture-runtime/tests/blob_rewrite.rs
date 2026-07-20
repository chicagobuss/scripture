//! Two-format rewrite: staging → per-Verse objects, superseding pointers, retention.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use holylog::atomic::AtomicLog;
use holylog::memory::InMemoryLogDrive;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use scripture::{
    AttributeValue, ChunkAppendAck, ChunkHeader, ChunkId, ChunkLogWriter, CohortId, DataRef, Frame,
    JournalId, LogPayload, ProducerId, Record, RecordOffset, SealedChunk, SubmissionRef, WriterId,
    encode_data_ref, seal_single_frame_chunk,
};
use scripture_runtime::{
    BlobEnvelope, BlobWriter, BlobWriterConfig, DataRefAppendTarget, RewriteConfig,
    StagingBlobContents, StagingPointer, SupersedingAppendTarget, VerseRewriteProgress,
    VerseSealer, commit_cut_plan, is_rewritten_blob_key, rewrite_verse_staging, scan_log_deduped,
    staging_blob_collectable, superseded_chunk_ids,
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
        bytes::Bytes::from(value.to_be_bytes().to_vec()),
    )
}

fn envelope(verse: &str, chunk: u8, journal_id: JournalId, values: &[i64]) -> BlobEnvelope {
    BlobEnvelope {
        verse_key: verse.into(),
        chunk_id: ChunkId::from_bytes([chunk; 16]),
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
    async fn seal(
        &mut self,
        envelope: &BlobEnvelope,
    ) -> Result<SealedChunk, scripture_runtime::BlobWriterError> {
        let base_offset = self.next;
        self.next = self
            .next
            .checked_add(envelope.records.len())
            .ok_or_else(|| {
                scripture_runtime::BlobWriterError::Invariant("test offset overflow".into())
            })?;
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

type SharedWriter = Arc<tokio::sync::Mutex<ChunkLogWriter>>;

struct WriterTarget {
    writer: SharedWriter,
    refs: Arc<Mutex<Vec<DataRef>>>,
    payloads: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl DataRefAppendTarget for WriterTarget {
    async fn append_data_ref(
        &mut self,
        sealed: &SealedChunk,
        data_ref: &DataRef,
    ) -> Result<ChunkAppendAck, scripture_runtime::BlobWriterError> {
        let ack = self
            .writer
            .lock()
            .await
            .append_data_ref(sealed, data_ref)
            .await?;
        self.refs.lock().expect("refs").push(data_ref.clone());
        self.payloads
            .lock()
            .expect("payloads")
            .push(encode_data_ref(data_ref).expect("encode").to_vec());
        Ok(ack)
    }
}

struct SupersedingTarget {
    writer: SharedWriter,
    payloads: Arc<Mutex<Vec<Vec<u8>>>>,
    fail_after: Option<usize>,
    appended: Arc<Mutex<usize>>,
}

#[async_trait]
impl SupersedingAppendTarget for SupersedingTarget {
    async fn append_superseding(
        &mut self,
        writer_id: WriterId,
        data_ref: &DataRef,
        superseded_first_offset: RecordOffset,
    ) -> Result<ChunkAppendAck, scripture_runtime::RewriteError> {
        let fail = {
            let mut count = self.appended.lock().expect("count");
            *count += 1;
            self.fail_after.is_some_and(|limit| *count > limit)
        };
        if fail {
            return Err(scripture_runtime::RewriteError::Invariant(
                "simulated authority loss".into(),
            ));
        }
        let ack = self
            .writer
            .lock()
            .await
            .append_superseding_data_ref(writer_id, data_ref, superseded_first_offset)
            .await?;
        self.payloads
            .lock()
            .expect("payloads")
            .push(encode_data_ref(data_ref).expect("encode").to_vec());
        Ok(ack)
    }
}

struct RejectSuperseding;
#[async_trait]
impl SupersedingAppendTarget for RejectSuperseding {
    async fn append_superseding(
        &mut self,
        _: WriterId,
        _: &DataRef,
        _: RecordOffset,
    ) -> Result<ChunkAppendAck, scripture_runtime::RewriteError> {
        Err(scripture_runtime::RewriteError::Invariant(
            "simulated authority loss".into(),
        ))
    }
}

struct StagingCommit {
    staging_refs: Vec<DataRef>,
    log_payloads: Arc<Mutex<Vec<Vec<u8>>>>,
    writers: BTreeMap<String, SharedWriter>,
}

async fn commit_staging(
    store: &Arc<dyn ObjectStore>,
    verses: &[(&str, u8, JournalId, &[i64])],
) -> StagingCommit {
    let mut blob_writer = BlobWriter::new(BlobWriterConfig::default()).expect("writer");
    for (verse, chunk, journal_id, values) in verses {
        blob_writer
            .push(envelope(verse, *chunk, *journal_id, values))
            .expect("push");
    }
    let plan = blob_writer.flush_drained().expect("flush").expect("plan");

    let refs = Arc::new(Mutex::new(Vec::new()));
    let payloads = Arc::new(Mutex::new(Vec::new()));
    let mut sealers: BTreeMap<String, Box<dyn VerseSealer>> = BTreeMap::new();
    let mut targets: BTreeMap<String, Box<dyn DataRefAppendTarget>> = BTreeMap::new();
    let mut writers: BTreeMap<String, SharedWriter> = BTreeMap::new();

    for (verse, _, journal_id, _) in verses {
        sealers.insert(
            (*verse).into(),
            Box::new(TestSealer {
                generation: 1,
                next: RecordOffset::new(0),
            }),
        );
        let drive = Arc::new(InMemoryLogDrive::new());
        let log = AtomicLog::builder(drive, 0).build().expect("log");
        let shared = Arc::new(tokio::sync::Mutex::new(ChunkLogWriter::new(
            *journal_id,
            cohort(),
            1,
            log,
            RecordOffset::new(0),
        )));
        writers.insert((*verse).into(), Arc::clone(&shared));
        targets.insert(
            (*verse).into(),
            Box::new(WriterTarget {
                writer: shared,
                refs: Arc::clone(&refs),
                payloads: Arc::clone(&payloads),
            }),
        );
    }

    commit_cut_plan(store, &plan, &mut sealers, &mut targets)
        .await
        .expect("commit");

    StagingCommit {
        staging_refs: refs.lock().expect("refs").clone(),
        log_payloads: payloads,
        writers,
    }
}

fn record_values(resolved: &[scripture_runtime::ResolvedChunk]) -> Vec<i64> {
    resolved
        .iter()
        .flat_map(|item| {
            item.chunk.frames[0]
                .records
                .iter()
                .map(|r| match r.attributes.get("value") {
                    Some(scripture::AttributeValue::I64(v)) => *v,
                    _ => panic!("expected i64 value"),
                })
        })
        .collect()
}

#[tokio::test]
async fn rewrite_produces_per_verse_object_with_matching_digests() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = RewriteConfig::default();
    let commit = commit_staging(
        &store,
        &[
            ("alpha", 1, journal(b'a'), &[10]),
            ("alpha", 2, journal(b'a'), &[11]),
        ],
    )
    .await;

    let pointers: Vec<StagingPointer> = commit
        .staging_refs
        .iter()
        .enumerate()
        .map(|(index, data_ref)| StagingPointer {
            first_offset: RecordOffset::new(index as u64),
            verse_key: "alpha".into(),
            data_ref: data_ref.clone(),
        })
        .collect();

    let mut progress = VerseRewriteProgress::default();
    let payloads = Arc::clone(&commit.log_payloads);
    let mut target = SupersedingTarget {
        writer: Arc::clone(commit.writers.get("alpha").expect("writer")),
        payloads: Arc::clone(&payloads),
        fail_after: None,
        appended: Arc::new(Mutex::new(0)),
    };

    let outcome = rewrite_verse_staging(&store, &config, &pointers, &mut progress, &mut target)
        .await
        .expect("rewrite");
    assert!(outcome.append_outcomes.values().all(|r| r.is_ok()));
    let rewritten_key = progress.rewritten_blob_key.expect("rewritten key");
    assert!(is_rewritten_blob_key(&rewritten_key, &config));

    let all_payloads = payloads.lock().expect("payloads").clone();
    let resolved = scan_log_deduped(&store, &all_payloads, &config)
        .await
        .expect("scan");
    assert_eq!(record_values(&resolved), vec![10, 11]);
    for item in &resolved {
        assert!(
            item.data_ref
                .as_ref()
                .is_some_and(|r| is_rewritten_blob_key(&r.blob_key, &config))
        );
    }
}

#[tokio::test]
async fn reader_sees_every_record_exactly_once_across_transition() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = RewriteConfig::default();
    let commit = commit_staging(
        &store,
        &[
            ("alpha", 1, journal(b'a'), &[1]),
            ("alpha", 2, journal(b'a'), &[2]),
            ("beta", 3, journal(b'b'), &[3]),
        ],
    )
    .await;

    let payloads = Arc::clone(&commit.log_payloads);
    let mid = payloads.lock().expect("payloads").clone();
    let mid_scan = scan_log_deduped(&store, &mid, &config).await.expect("mid");
    assert_eq!(record_values(&mid_scan), vec![1, 2, 3]);

    for (verse, chunk_ids) in [("alpha", vec![1_u8, 2]), ("beta", vec![3])] {
        let pointers: Vec<StagingPointer> = commit
            .staging_refs
            .iter()
            .filter(|r| chunk_ids.contains(&r.chunk_id.as_bytes()[0]))
            .map(|data_ref| StagingPointer {
                first_offset: RecordOffset::new(0),
                verse_key: verse.into(),
                data_ref: data_ref.clone(),
            })
            .collect();
        let mut progress = VerseRewriteProgress::default();
        let mut target = SupersedingTarget {
            writer: Arc::clone(commit.writers.get(verse).expect("writer")),
            payloads: Arc::clone(&payloads),
            fail_after: None,
            appended: Arc::new(Mutex::new(0)),
        };
        rewrite_verse_staging(&store, &config, &pointers, &mut progress, &mut target)
            .await
            .expect("rewrite");
    }

    let final_payloads = payloads.lock().expect("payloads").clone();
    let final_scan = scan_log_deduped(&store, &final_payloads, &config)
        .await
        .expect("final");
    assert_eq!(record_values(&final_scan), vec![1, 2, 3]);
    assert_eq!(final_scan.len(), 3);
}

#[tokio::test]
async fn interrupted_rewrite_is_resumable_and_staging_survives() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = RewriteConfig::default();
    let commit = commit_staging(
        &store,
        &[
            ("alpha", 1, journal(b'a'), &[100]),
            ("alpha", 2, journal(b'a'), &[101]),
        ],
    )
    .await;
    let staging_key = commit.staging_refs[0].blob_key.clone();
    let pointers: Vec<StagingPointer> = commit
        .staging_refs
        .iter()
        .enumerate()
        .map(|(index, data_ref)| StagingPointer {
            first_offset: RecordOffset::new(index as u64),
            verse_key: "alpha".into(),
            data_ref: data_ref.clone(),
        })
        .collect();

    let payloads = Arc::clone(&commit.log_payloads);
    let mut progress = VerseRewriteProgress::default();
    let appended = Arc::new(Mutex::new(0));
    let mut target = SupersedingTarget {
        writer: Arc::clone(commit.writers.get("alpha").expect("writer")),
        payloads: Arc::clone(&payloads),
        fail_after: Some(1),
        appended: Arc::clone(&appended),
    };
    let partial = rewrite_verse_staging(&store, &config, &pointers, &mut progress, &mut target)
        .await
        .expect("partial");
    assert_eq!(partial.append_outcomes.len(), 2);
    assert!(partial.append_outcomes[&ChunkId::from_bytes([1; 16])].is_ok());
    assert!(partial.append_outcomes[&ChunkId::from_bytes([2; 16])].is_err());
    assert!(progress.rewritten_blob_key.is_some());

    store
        .head(&object_store::path::Path::from(staging_key.as_str()))
        .await
        .expect("staging still exists");

    let mut target = SupersedingTarget {
        writer: Arc::clone(commit.writers.get("alpha").expect("writer")),
        payloads: Arc::clone(&payloads),
        fail_after: None,
        appended: Arc::new(Mutex::new(1)),
    };
    rewrite_verse_staging(&store, &config, &pointers, &mut progress, &mut target)
        .await
        .expect("resume");

    let all = payloads.lock().expect("payloads").clone();
    assert!(staging_blob_collectable(
        &StagingBlobContents {
            blob_key: staging_key.clone(),
            chunk_ids: commit.staging_refs.iter().map(|r| r.chunk_id).collect(),
        },
        &superseded_chunk_ids(&all, &config).expect("superseded"),
    ));
    store
        .head(&object_store::path::Path::from(staging_key.as_str()))
        .await
        .expect("staging bytes not deleted before explicit reclamation");
    let resolved = scan_log_deduped(&store, &all, &config).await.expect("scan");
    assert_eq!(record_values(&resolved), vec![100, 101]);
}

#[tokio::test]
async fn staging_collectable_only_after_all_superseding_pointers_exist() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = RewriteConfig::default();
    let commit = commit_staging(
        &store,
        &[
            ("alpha", 1, journal(b'a'), &[7]),
            ("alpha", 2, journal(b'a'), &[8]),
        ],
    )
    .await;
    let staging_key = commit.staging_refs[0].blob_key.clone();
    let base = commit.log_payloads.lock().expect("payloads").clone();
    assert!(!staging_blob_collectable(
        &StagingBlobContents {
            blob_key: staging_key.clone(),
            chunk_ids: commit.staging_refs.iter().map(|r| r.chunk_id).collect(),
        },
        &superseded_chunk_ids(&base, &config).expect("superseded"),
    ));

    let pointers: Vec<StagingPointer> = commit
        .staging_refs
        .iter()
        .enumerate()
        .map(|(index, data_ref)| StagingPointer {
            first_offset: RecordOffset::new(index as u64),
            verse_key: "alpha".into(),
            data_ref: data_ref.clone(),
        })
        .collect();
    let payloads = Arc::clone(&commit.log_payloads);
    let mut progress = VerseRewriteProgress::default();
    let mut target = SupersedingTarget {
        writer: Arc::clone(commit.writers.get("alpha").expect("writer")),
        payloads: Arc::clone(&payloads),
        fail_after: None,
        appended: Arc::new(Mutex::new(0)),
    };
    rewrite_verse_staging(&store, &config, &pointers, &mut progress, &mut target)
        .await
        .expect("rewrite");

    let all = payloads.lock().expect("payloads").clone();
    assert!(staging_blob_collectable(
        &StagingBlobContents {
            blob_key: staging_key.clone(),
            chunk_ids: commit.staging_refs.iter().map(|r| r.chunk_id).collect(),
        },
        &superseded_chunk_ids(&all, &config).expect("superseded"),
    ));
}

#[tokio::test]
async fn authority_loss_mid_rewrite_leaves_sibling_committed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = RewriteConfig::default();
    let commit = commit_staging(
        &store,
        &[
            ("alpha", 1, journal(b'a'), &[20]),
            ("beta", 2, journal(b'b'), &[21]),
        ],
    )
    .await;

    let alpha_pointers: Vec<StagingPointer> = commit
        .staging_refs
        .iter()
        .filter(|r| r.chunk_id == ChunkId::from_bytes([1; 16]))
        .enumerate()
        .map(|(index, data_ref)| StagingPointer {
            first_offset: RecordOffset::new(index as u64),
            verse_key: "alpha".into(),
            data_ref: data_ref.clone(),
        })
        .collect();
    let beta_pointers: Vec<StagingPointer> = commit
        .staging_refs
        .iter()
        .filter(|r| r.chunk_id == ChunkId::from_bytes([2; 16]))
        .map(|data_ref| StagingPointer {
            first_offset: RecordOffset::new(0),
            verse_key: "beta".into(),
            data_ref: data_ref.clone(),
        })
        .collect();

    let payloads = Arc::clone(&commit.log_payloads);
    let mut alpha_progress = VerseRewriteProgress::default();
    let mut alpha_target = SupersedingTarget {
        writer: Arc::clone(commit.writers.get("alpha").expect("writer")),
        payloads: Arc::clone(&payloads),
        fail_after: None,
        appended: Arc::new(Mutex::new(0)),
    };
    rewrite_verse_staging(
        &store,
        &config,
        &alpha_pointers,
        &mut alpha_progress,
        &mut alpha_target,
    )
    .await
    .expect("alpha rewrite");

    let mut beta_progress = VerseRewriteProgress::default();
    let mut beta_target = RejectSuperseding;
    let beta_outcome = rewrite_verse_staging(
        &store,
        &config,
        &beta_pointers,
        &mut beta_progress,
        &mut beta_target,
    )
    .await
    .expect("beta attempt");
    assert!(beta_outcome.append_outcomes.values().all(|r| r.is_err()));
    assert!(beta_progress.rewritten_blob_key.is_some());

    let all = payloads.lock().expect("payloads").clone();
    let resolved = scan_log_deduped(&store, &all, &config).await.expect("scan");
    assert_eq!(record_values(&resolved), vec![20, 21]);
}

#[tokio::test]
async fn rewrite_measurement_per_verse_objects_and_single_get() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = RewriteConfig::default();
    let commit = commit_staging(
        &store,
        &[
            ("alpha", 1, journal(b'a'), &[1]),
            ("beta", 2, journal(b'b'), &[2]),
        ],
    )
    .await;
    let staging_keys: BTreeMap<String, String> = commit
        .staging_refs
        .iter()
        .map(|r| {
            (
                format!("{:02x}", r.chunk_id.as_bytes()[0]),
                r.blob_key.clone(),
            )
        })
        .collect();
    assert_eq!(staging_keys.len(), 2);
    assert_eq!(
        staging_keys.values().collect::<BTreeSet<_>>().len(),
        1,
        "one shared staging object for both Verses"
    );

    let payloads = Arc::clone(&commit.log_payloads);
    for (verse, chunk) in [("alpha", 1_u8), ("beta", 2_u8)] {
        let pointers: Vec<StagingPointer> = commit
            .staging_refs
            .iter()
            .filter(|r| r.chunk_id.as_bytes()[0] == chunk)
            .map(|data_ref| StagingPointer {
                first_offset: RecordOffset::new(0),
                verse_key: verse.into(),
                data_ref: data_ref.clone(),
            })
            .collect();
        let mut progress = VerseRewriteProgress::default();
        let mut target = SupersedingTarget {
            writer: Arc::clone(commit.writers.get(verse).expect("writer")),
            payloads: Arc::clone(&payloads),
            fail_after: None,
            appended: Arc::new(Mutex::new(0)),
        };
        rewrite_verse_staging(&store, &config, &pointers, &mut progress, &mut target)
            .await
            .expect("rewrite");
        assert!(progress.rewritten_blob_key.is_some());
    }

    let all = payloads.lock().expect("payloads").clone();
    let rewritten_keys: BTreeSet<String> = all
        .iter()
        .filter_map(|payload| {
            let LogPayload::DataRef(data_ref) = scripture::decode_log_payload(payload).ok()? else {
                return None;
            };
            is_rewritten_blob_key(&data_ref.blob_key, &config).then_some(data_ref.blob_key.clone())
        })
        .collect();
    assert_eq!(rewritten_keys.len(), 2, "one rewritten object per Verse");

    let resolved = scan_log_deduped(&store, &all, &config).await.expect("scan");
    assert_eq!(record_values(&resolved), vec![1, 2]);
    for item in resolved {
        assert!(
            item.data_ref
                .as_ref()
                .is_some_and(|r| is_rewritten_blob_key(&r.blob_key, &config))
        );
    }
}

/// A staging blob shared by two Verses must not look collectable when only one
/// of them has been rewritten.
///
/// Logs are per-Verse, so the natural thing for a caller to hold is one Verse's
/// payloads. If the predicate answers from that alone it will report a blob
/// collectable while a sibling Verse still points into it, and deleting it
/// loses the sibling's records.
#[tokio::test]
async fn shared_staging_blob_is_not_collectable_from_one_verses_payloads() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = RewriteConfig::default();
    let commit = commit_staging(
        &store,
        &[
            ("alpha", 1, journal(b'a'), &[7]),
            ("beta", 2, journal(b'b'), &[8]),
        ],
    )
    .await;
    let staging_key = commit.staging_refs[0].blob_key.clone();
    assert!(
        commit
            .staging_refs
            .iter()
            .all(|r| r.blob_key == staging_key),
        "both Verses must share one staging blob for this case to mean anything"
    );

    // Rewrite alpha only; beta still points into the shared blob.
    let alpha_pointers: Vec<StagingPointer> = commit
        .staging_refs
        .iter()
        .zip(["alpha", "beta"])
        .filter(|(_, verse)| *verse == "alpha")
        .map(|(data_ref, verse)| StagingPointer {
            verse_key: verse.to_string(),
            data_ref: data_ref.clone(),
            first_offset: RecordOffset::new(0),
        })
        .collect();
    let payloads = Arc::clone(&commit.log_payloads);
    let mut progress = VerseRewriteProgress::default();
    let mut target = SupersedingTarget {
        writer: Arc::clone(commit.writers.get("alpha").expect("writer")),
        payloads: Arc::clone(&payloads),
        fail_after: None,
        appended: Arc::new(Mutex::new(0)),
    };
    rewrite_verse_staging(&store, &config, &alpha_pointers, &mut progress, &mut target)
        .await
        .expect("rewrite alpha");

    // What a caller holding only alpha's per-Verse log would pass.
    let all = payloads.lock().expect("payloads").clone();
    let alpha_only: Vec<Vec<u8>> = all
        .iter()
        .filter(|payload| match scripture::decode_log_payload(payload) {
            Ok(scripture::LogPayload::DataRef(data_ref)) => {
                data_ref.chunk_id == ChunkId::from_bytes([1; 16])
            }
            _ => false,
        })
        .cloned()
        .collect();

    assert!(
        !staging_blob_collectable(
            &StagingBlobContents {
                blob_key: staging_key.clone(),
                chunk_ids: commit.staging_refs.iter().map(|r| r.chunk_id).collect(),
            },
            &superseded_chunk_ids(&alpha_only, &config).expect("superseded"),
        ),
        "must not report collectable while a sibling Verse still references the blob"
    );
}
