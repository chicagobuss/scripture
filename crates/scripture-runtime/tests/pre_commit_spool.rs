//! Pre-commit spool WAL: named acceptance cases from the work package.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use holylog::atomic::AtomicLog;
use holylog::memory::InMemoryLogDrive;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use scripture::{
    AchievedProfile, AttributeValue, ChunkAppendAck, ChunkHeader, ChunkId, ChunkLogWriter,
    CohortId, Frame, InMemorySpoolStorage, JournalId, ProducerId, ProducerReceipt,
    ProgressIdentity, ReceiptRequirement, Record, RecordOffset, ScanTail, ScribeSpoolCapability,
    SealedChunk, SpoolConfig, SpoolError, SpoolFsyncPolicy, SpoolOnFull, SpoolStorage,
    SpoolStorageFaults, SubmissionRef, ValidFrame, VerseReceiptPolicy, WriterId, plan_admission,
    seal_single_frame_chunk,
};
use scripture_runtime::{
    AdmitOrderLog, BlobEnvelope, BlobEnvelopeSource, BlobWriter, BlobWriterConfig,
    DataRefAppendTarget, PreCommitSpool, PreCommitSpoolConfig, VerseSealer, commit_cut_plan,
    committed_receipt_for, plan_without_spool, reset_drain_markers,
};

fn journal(byte: u8) -> JournalId {
    JournalId::from_bytes([byte; 16])
}

fn cohort() -> CohortId {
    CohortId::from_bytes(*b"cohort-precommit")
}

fn capability() -> ScribeSpoolCapability {
    ScribeSpoolCapability {
        path: "/tmp/spool-test".into(),
        max_bytes: 64 * 1024,
        fsync: SpoolFsyncPolicy::EveryRecord,
        on_full: SpoolOnFull::Reject,
        loss_budget: Duration::from_secs(30),
        scribe_id: "scribe-a".into(),
    }
}

fn config(max_wal_bytes: usize) -> PreCommitSpoolConfig {
    PreCommitSpoolConfig {
        wal: SpoolConfig {
            max_wal_bytes,
            max_frames: 64,
            max_frame_bytes: 64 * 1024,
            max_inflight_completions: 8,
        },
        capability: capability(),
    }
}

fn record(value: i64) -> Record {
    Record::new(
        [("value".into(), AttributeValue::I64(value))],
        bytes::Bytes::from(value.to_be_bytes().to_vec()),
    )
}

fn envelope(verse: &str, chunk: u8, sequence: u64, value: i64) -> BlobEnvelope {
    BlobEnvelope {
        verse_key: verse.into(),
        chunk_id: ChunkId::from_bytes([chunk; 16]),
        journal_id: journal(b'a'),
        cohort_id: cohort(),
        records: vec![record(value)],
        submissions: vec![SubmissionRef {
            producer_id: ProducerId::from_bytes(*b"producer-precomt"),
            producer_epoch: 1,
            sequence,
            first_record: 0,
            record_count: 1,
        }],
    }
}

fn identity_of(env: &BlobEnvelope) -> ProgressIdentity {
    let submission = &env.submissions[0];
    ProgressIdentity {
        journal_id: env.journal_id,
        producer_id: submission.producer_id,
        producer_epoch: submission.producer_epoch,
        sequence: submission.sequence,
    }
}

/// Shared durable bytes so a "crash" can reopen the same WAL.
#[derive(Clone, Default)]
struct SharedStorage {
    inner: Arc<Mutex<InMemorySpoolStorage>>,
}

impl SpoolStorage for SharedStorage {
    fn append_frame(&mut self, frame: &scripture::SpoolFrame) -> Result<(), SpoolError> {
        self.inner.lock().expect("storage").append_frame(frame)
    }

    fn sync(&mut self) -> Result<(), SpoolError> {
        self.inner.lock().expect("storage").sync()
    }

    fn scan_valid_frames(&self) -> Result<(Vec<ValidFrame>, ScanTail), SpoolError> {
        self.inner.lock().expect("storage").scan_valid_frames()
    }

    fn used_bytes(&self) -> usize {
        self.inner.lock().expect("storage").used_bytes()
    }

    fn frame_count(&self) -> usize {
        self.inner.lock().expect("storage").frame_count()
    }

    fn set_faults(&mut self, faults: SpoolStorageFaults) {
        self.inner.lock().expect("storage").set_faults(faults);
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
                scripture_runtime::BlobWriterError::Invariant("offset overflow".into())
            })?;
        Ok(seal_single_frame_chunk(
            ChunkHeader {
                chunk_id: envelope.chunk_id,
                cohort_id: envelope.cohort_id,
                generation: self.generation,
                writer_id: WriterId::from_bytes(*b"writer-precommit"),
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
}

#[async_trait]
impl DataRefAppendTarget for WriterTarget {
    async fn append_data_ref(
        &mut self,
        sealed: &SealedChunk,
        data_ref: &scripture::DataRef,
    ) -> Result<ChunkAppendAck, scripture_runtime::BlobWriterError> {
        Ok(self.writer.append_data_ref(sealed, data_ref).await?)
    }
}

async fn commit_one(
    store: &Arc<dyn ObjectStore>,
    env: BlobEnvelope,
    generation: u64,
) -> ChunkAppendAck {
    let verse = env.verse_key.clone();
    let mut writer = BlobWriter::new(BlobWriterConfig::default()).expect("writer");
    writer.push(env).expect("push");
    let plan = writer.flush_drained().expect("flush").expect("plan");
    let drive = Arc::new(InMemoryLogDrive::new());
    let log = AtomicLog::builder(drive, 0).build().expect("log");
    let mut sealers: BTreeMap<String, Box<dyn VerseSealer>> = BTreeMap::new();
    sealers.insert(
        verse.clone(),
        Box::new(TestSealer {
            generation,
            next: RecordOffset::new(0),
        }),
    );
    let mut targets: BTreeMap<String, Box<dyn DataRefAppendTarget>> = BTreeMap::new();
    targets.insert(
        verse,
        Box::new(WriterTarget {
            writer: ChunkLogWriter::new(
                journal(b'a'),
                cohort(),
                generation,
                log,
                RecordOffset::new(0),
            ),
        }),
    );
    let outcomes = commit_cut_plan(store, &plan, &mut sealers, &mut targets)
        .await
        .expect("commit");
    outcomes
        .into_iter()
        .next()
        .expect("one")
        .result
        .expect("ok")
}

#[test]
fn fsync_precedes_the_ack() {
    let order = AdmitOrderLog::new();
    let spool = PreCommitSpool::open(config(64 * 1024), SharedStorage::default())
        .expect("open")
        .with_order_log(order.clone());
    let policy = VerseReceiptPolicy {
        minimum: ReceiptRequirement::Spooled,
        default: ReceiptRequirement::Spooled,
        allow_spooled: true,
    };
    let receipt = spool
        .admit_for_receipt(
            &policy,
            Some(ReceiptRequirement::Spooled),
            envelope("v", 1, 1, 7),
        )
        .expect("admit")
        .expect("spooled");
    assert_eq!(receipt.profile(), AchievedProfile::Spooled);
    assert_eq!(order.snapshot(), vec!["append", "sync", "ack"]);
}

#[tokio::test]
async fn restart_recovery_spooled_before_crash_is_replayed_and_reaches_committed() {
    let shared = SharedStorage::default();
    let cfg = config(64 * 1024);
    let env = envelope("alpha", 1, 1, 42);
    let identity = identity_of(&env);

    {
        let spool = PreCommitSpool::open(cfg.clone(), shared.clone()).expect("open");
        let receipt = spool.persist_and_ack_spooled(env.clone()).expect("spooled");
        assert_eq!(receipt.profile, AchievedProfile::Spooled);
        assert!(ProducerReceipt::Spooled(receipt).canon_offsets().is_none());
    }

    let mut restarted = PreCommitSpool::open(cfg, shared).expect("restart");
    assert_eq!(restarted.pending_count(), 1);
    let replayed = restarted.next_envelope().await.expect("next").expect("env");
    assert_eq!(replayed.chunk_id, env.chunk_id);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ack = commit_one(&store, replayed, 1).await;
    assert_eq!(ack.chunk_id, env.chunk_id);
    restarted.observe_committed(identity).expect("retire");
    assert!(!restarted.is_pending(&identity));
}

#[tokio::test]
async fn handoff_replay_entries_captured_under_generation_n_resealed_under_n_plus_1() {
    let spool = PreCommitSpool::open(config(64 * 1024), SharedStorage::default()).expect("open");
    let env = envelope("alpha", 9, 3, 99);
    spool.persist_and_ack_spooled(env.clone()).expect("spooled");

    let mut source = spool;
    let replayed = source.next_envelope().await.expect("next").expect("env");
    assert_eq!(replayed.chunk_id, env.chunk_id);
    assert_eq!(replayed.submissions[0].sequence, 3);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // Captured under generation-free envelope; seal under successor generation 2.
    let ack = commit_one(&store, replayed, 2).await;
    assert_eq!(ack.chunk_id, env.chunk_id);
}

#[tokio::test]
async fn idempotent_drain_draining_twice_yields_one_committed_record() {
    let mut spool =
        PreCommitSpool::open(config(64 * 1024), SharedStorage::default()).expect("open");
    let env = envelope("alpha", 2, 1, 5);
    spool.persist_and_ack_spooled(env.clone()).expect("spooled");

    let first = spool.next_envelope().await.expect("a").expect("env");
    assert!(spool.next_envelope().await.expect("b").is_some()); // still pending, re-yield
    reset_drain_markers(&spool);
    let second = spool.next_envelope().await.expect("c").expect("env");
    assert_eq!(first.chunk_id, second.chunk_id);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ack = commit_one(&store, first, 1).await;
    spool.observe_committed(identity_of(&env)).expect("retire");
    // Second drain after committed must not invent another commit.
    reset_drain_markers(&spool);
    assert!(spool.next_envelope().await.expect("empty").is_none());
    assert_eq!(ack.record_count, 1);
}

#[test]
fn deletion_ordering_entry_present_after_replay_before_committed_gone_after() {
    let mut spool =
        PreCommitSpool::open(config(64 * 1024), SharedStorage::default()).expect("open");
    let env = envelope("alpha", 3, 1, 8);
    let identity = identity_of(&env);
    spool.persist_and_ack_spooled(env).expect("spooled");

    futures::executor::block_on(async {
        let _ = spool.next_envelope().await.expect("replay");
    });
    assert!(
        spool.is_pending(&identity),
        "entry must remain after replay until committed is observed"
    );
    spool.observe_committed(identity).expect("retire");
    assert!(
        !spool.is_pending(&identity),
        "entry must be gone after committed"
    );
}

#[test]
fn verse_floor_wins_minimum_committed_never_receives_spooled_ack() {
    let spool = PreCommitSpool::open(config(64 * 1024), SharedStorage::default()).expect("open");
    let policy = VerseReceiptPolicy {
        minimum: ReceiptRequirement::Committed,
        default: ReceiptRequirement::Committed,
        allow_spooled: true,
    };
    let plan = plan_admission(
        &policy,
        Some(ReceiptRequirement::Spooled),
        Some(&capability()),
    )
    .expect("plan");
    assert!(matches!(plan, scripture::AdmitPlan::WaitForCommitted));
    let receipt = spool
        .admit_for_receipt(
            &policy,
            Some(ReceiptRequirement::Spooled),
            envelope("v", 1, 1, 1),
        )
        .expect("admit");
    assert!(
        receipt.is_none(),
        "floor raised to committed: no early spooled ack"
    );
}

#[test]
fn requirement_is_not_a_preference_committed_request_never_gets_spooled() {
    let spool = PreCommitSpool::open(config(64 * 1024), SharedStorage::default()).expect("open");
    let policy = VerseReceiptPolicy {
        minimum: ReceiptRequirement::Spooled,
        default: ReceiptRequirement::Spooled,
        allow_spooled: true,
    };
    let receipt = spool
        .admit_for_receipt(
            &policy,
            Some(ReceiptRequirement::Committed),
            envelope("v", 1, 1, 1),
        )
        .expect("admit");
    assert!(receipt.is_none(), "must wait for committed, never spooled");
}

#[test]
fn stronger_satisfies_spooled_request_that_reaches_committed_reports_committed() {
    let env = envelope("v", 1, 1, 1);
    let receipt = committed_receipt_for(&env, RecordOffset::new(0), RecordOffset::new(1), 7, 1)
        .expect("committed");
    assert_eq!(receipt.profile(), AchievedProfile::Committed);
    assert!(receipt.satisfies(ReceiptRequirement::Spooled));
    assert!(matches!(receipt, ProducerReceipt::Committed(_)));
}

#[test]
fn no_spool_still_serves_spooled_permitting_verse_returns_committed_plan() {
    let policy = VerseReceiptPolicy {
        minimum: ReceiptRequirement::Spooled,
        default: ReceiptRequirement::Spooled,
        allow_spooled: true,
    };
    let plan = plan_without_spool(&policy, Some(ReceiptRequirement::Spooled)).expect("plan");
    assert!(matches!(plan, scripture::AdmitPlan::WaitForCommitted));
}

#[test]
fn no_offset_at_spooled() {
    let spool = PreCommitSpool::open(config(64 * 1024), SharedStorage::default()).expect("open");
    let receipt = ProducerReceipt::Spooled(
        spool
            .persist_and_ack_spooled(envelope("v", 1, 1, 1))
            .expect("spooled"),
    );
    assert!(receipt.canon_offsets().is_none());
}

#[test]
fn capacity_on_full_reject_rejects_rather_than_acknowledging_and_evicting() {
    let tiny = PreCommitSpoolConfig {
        wal: SpoolConfig {
            max_wal_bytes: 64,
            max_frames: 64,
            max_frame_bytes: 64 * 1024,
            max_inflight_completions: 8,
        },
        capability: capability(),
    };
    let spool = PreCommitSpool::open(tiny, SharedStorage::default()).expect("open");
    let err = spool
        .persist_and_ack_spooled(envelope("v", 1, 1, 1))
        .expect_err("must reject");
    assert!(matches!(
        err,
        scripture_runtime::PreCommitSpoolError::Spool(SpoolError::CapacityExceeded)
    ));
    assert_eq!(spool.pending_count(), 0);
}
