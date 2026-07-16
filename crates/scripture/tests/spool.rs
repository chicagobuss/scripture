//! S1a spool proofs: frames, WAL ordering, progress, recovery, file survival.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::executor::LocalPool;
use futures::future::poll_fn;
use futures::task::SpawnExt;
use holylog::atomic::AtomicLog;
use holylog::drive::LogDrive;
use scripture::{
    AttributeValue, ChunkDriverActor, ChunkLogWriter, FileSpoolStorage, FrameClassification,
    InMemorySpoolStorage, ManualClock, ManualTimer, ProducerId, ProgressIdentity, Record,
    RecordOffset, RecoveryClassification, ScanTail, SpoolCell, SpoolCellHandle, SpoolConfig,
    SpoolError, SpoolFrame, SpoolPoisonCause, SpoolStorage, SpoolStorageFaults, Submission,
    ValidFrame, classify_frames, decode_spool_frame, encode_spool_frame, scan_and_classify,
};

#[path = "support/mod.rs"]
mod support;

use support::{ScriptedLogDrive, address, cohort, journal, policy, producer, record, writer_id};

fn submission(sequence: u64) -> Submission {
    Submission {
        producer_id: producer(),
        producer_epoch: 1,
        sequence,
        records: vec![record(sequence as i64)],
    }
}

fn frame_for(sequence: u64) -> SpoolFrame {
    SpoolFrame::Submission {
        journal_id: journal(),
        submission: submission(sequence),
    }
}

#[derive(Default)]
struct OrderStorage {
    inner: InMemorySpoolStorage,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl SpoolStorage for OrderStorage {
    fn append_frame(&mut self, frame: &SpoolFrame) -> Result<(), SpoolError> {
        self.events.lock().expect("events").push("append");
        self.inner.append_frame(frame)
    }

    fn sync(&mut self) -> Result<(), SpoolError> {
        self.events.lock().expect("events").push("sync");
        self.inner.sync()
    }

    fn scan_valid_frames(&self) -> Result<(Vec<ValidFrame>, ScanTail), SpoolError> {
        self.inner.scan_valid_frames()
    }

    fn used_bytes(&self) -> usize {
        self.inner.used_bytes()
    }

    fn frame_count(&self) -> usize {
        self.inner.frame_count()
    }

    fn set_faults(&mut self, faults: SpoolStorageFaults) {
        self.inner.set_faults(faults);
    }
}

fn spawn_driver() -> (
    LocalPool,
    scripture::ChunkDriverHandle,
    Arc<ScriptedLogDrive>,
) {
    let drive = ScriptedLogDrive::new();
    let drive_arc = Arc::clone(&drive);
    let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
        .build()
        .expect("log");
    let writer = ChunkLogWriter::new(journal(), cohort(), 1, log, RecordOffset::new(0));
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    let (handle, actor) = ChunkDriverActor::new(
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
    )
    .expect("actor");
    let pool = LocalPool::new();
    pool.spawner()
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn");
    (pool, handle, drive_arc)
}

fn open_cell<S: SpoolStorage + 'static>(pool: &LocalPool, storage: S) -> SpoolCellHandle<S> {
    let (handle, cell) = SpoolCell::open(journal(), SpoolConfig::default(), storage).expect("open");
    pool.spawner()
        .spawn(async move {
            cell.run().await;
        })
        .expect("spawn spool");
    handle
}

#[test]
fn wal_sync_occurs_before_wrapped_driver_submission_returns() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let storage = OrderStorage {
        inner: InMemorySpoolStorage::new(),
        events: Arc::clone(&events),
    };
    let (mut pool, handle, drive) = spawn_driver();
    let cell = open_cell(&pool, storage);
    let gate = drive.gate_write(address(0));

    pool.run_until(async {
        let _receipt = cell.submit(&handle, submission(0)).await.expect("admit");
        let saw = events.lock().expect("events").clone();
        assert_eq!(
            saw,
            vec!["append", "sync"],
            "WAL must sync before forward returns"
        );
    });
    gate.open();
    pool.run_until_stalled();
}

#[test]
fn no_successful_receipt_before_durable_progress() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let storage = OrderStorage {
        inner: InMemorySpoolStorage::new(),
        events: Arc::clone(&events),
    };
    let (mut pool, handle, _) = spawn_driver();
    let cell = open_cell(&pool, storage);

    pool.run_until(async {
        let receipt = cell.submit(&handle, submission(0)).await.expect("admit");
        handle.flush().await.expect("flush");
        let committed = receipt.await.expect("commit+progress");
        assert_eq!(committed.first_offset, RecordOffset::new(0));
        let saw = events.lock().expect("events").clone();
        assert_eq!(
            saw,
            vec!["append", "sync", "append", "sync"],
            "progress append+sync must precede success"
        );
    });
}

#[test]
fn progress_sync_failure_poisons_and_never_emits_success() {
    let events = Arc::new(Mutex::new(Vec::new()));
    struct FailSecondSync {
        inner: InMemorySpoolStorage,
        syncs: usize,
        events: Arc<Mutex<Vec<&'static str>>>,
    }
    impl SpoolStorage for FailSecondSync {
        fn append_frame(&mut self, frame: &SpoolFrame) -> Result<(), SpoolError> {
            self.events.lock().expect("e").push("append");
            self.inner.append_frame(frame)
        }
        fn sync(&mut self) -> Result<(), SpoolError> {
            self.syncs += 1;
            self.events.lock().expect("e").push("sync");
            if self.syncs >= 2 {
                return Err(SpoolError::Io(std::io::Error::other("progress sync boom")));
            }
            self.inner.sync()
        }
        fn scan_valid_frames(&self) -> Result<(Vec<ValidFrame>, ScanTail), SpoolError> {
            self.inner.scan_valid_frames()
        }
        fn used_bytes(&self) -> usize {
            self.inner.used_bytes()
        }
        fn frame_count(&self) -> usize {
            self.inner.frame_count()
        }
        fn set_faults(&mut self, faults: SpoolStorageFaults) {
            self.inner.set_faults(faults);
        }
    }
    let storage = FailSecondSync {
        inner: InMemorySpoolStorage::new(),
        syncs: 0,
        events: Arc::clone(&events),
    };
    let (mut pool, handle, _) = spawn_driver();
    let cell = open_cell(&pool, storage);
    pool.run_until(async {
        let receipt = cell.submit(&handle, submission(0)).await.expect("admit");
        handle.flush().await.expect("flush");
        let err = receipt.await.expect_err("must not succeed");
        assert!(matches!(err, SpoolError::ProgressFailed));
        assert!(matches!(
            cell.state(),
            scripture::SpoolCellState::Poisoned {
                cause: SpoolPoisonCause::ProgressFailed
            }
        ));
        let refused = cell.submit(&handle, submission(1)).await;
        assert!(matches!(
            refused,
            Err(SpoolError::Poisoned {
                cause: SpoolPoisonCause::ProgressFailed
            })
        ));
    });
}

#[test]
fn progress_required_for_committed_locally_classification() {
    let mut storage = InMemorySpoolStorage::new();
    storage.append_frame(&frame_for(0)).expect("append");
    storage.sync().expect("sync");
    let report = scan_and_classify(&storage).expect("scan");
    assert!(report.recovery_required());
    assert_eq!(report.pending_unclassified, 1);
    assert_eq!(report.committed_locally, 0);

    storage
        .append_frame(&SpoolFrame::Progress(ProgressIdentity {
            journal_id: journal(),
            producer_id: producer(),
            producer_epoch: 1,
            sequence: 0,
        }))
        .expect("progress");
    storage.sync().expect("sync");
    let report = scan_and_classify(&storage).expect("scan");
    assert_eq!(report.committed_locally, 1);
    assert_eq!(report.pending_unclassified, 0);
    assert!(matches!(
        report.classification,
        RecoveryClassification::RecoveryRequired { .. }
    ));
}

#[test]
fn torn_terminal_and_corrupt_middle() {
    let mut storage = InMemorySpoolStorage::new();
    storage.append_frame(&frame_for(0)).expect("append");
    storage.set_faults(SpoolStorageFaults {
        tear_after_bytes: Some(8),
        ..Default::default()
    });
    storage.append_frame(&frame_for(1)).expect("tear");
    let (frames, tail) = storage.scan_valid_frames().expect("scan");
    assert_eq!(frames.len(), 1);
    assert!(matches!(tail, ScanTail::TornTerminal { .. }));
    assert!(classify_frames(&frames, &tail).torn_terminal);

    let dir = tempfile_dir("corrupt-middle");
    let mut file = FileSpoolStorage::open(&dir).expect("open");
    file.append_frame(&frame_for(0)).expect("a");
    file.sync().expect("s");
    drop(file);
    let segment = dir.join("segment-000001.wal");
    let mut on_disk = std::fs::read(&segment).expect("read");
    let mut bad = encode_spool_frame(&frame_for(1)).expect("enc").to_vec();
    let last = bad.len() - 1;
    bad[last] ^= 0xff;
    let third = encode_spool_frame(&frame_for(2)).expect("enc");
    on_disk.extend_from_slice(&bad);
    on_disk.extend_from_slice(&third);
    std::fs::write(&segment, &on_disk).expect("write");
    let (frames, tail) = FileSpoolStorage::inspect(&dir).expect("inspect");
    assert_eq!(frames.len(), 1);
    assert!(matches!(tail, ScanTail::CorruptMiddle { .. }));
    assert!(classify_frames(&frames, &tail).corrupt_history);
}

#[test]
fn submission_plus_progress_budget_enforced_before_forward() {
    // Encode once to size a budget that fits submission alone but not +progress.
    let sub = encode_spool_frame(&frame_for(0)).expect("enc");
    let prog = encode_spool_frame(&SpoolFrame::Progress(ProgressIdentity {
        journal_id: journal(),
        producer_id: producer(),
        producer_epoch: 1,
        sequence: 0,
    }))
    .expect("enc");
    assert!(sub.len() + prog.len() > sub.len());
    let config = SpoolConfig {
        max_wal_bytes: sub.len() + prog.len() - 1,
        max_frames: 8,
        max_frame_bytes: 1024 * 1024,
        max_inflight_completions: 8,
    };
    let (mut pool, handle, _) = spawn_driver();
    let (cell, completer) =
        SpoolCell::open(journal(), config, InMemorySpoolStorage::new()).expect("open");
    pool.spawner()
        .spawn(async move {
            completer.run().await;
        })
        .expect("spawn");
    pool.run_until(async {
        let err = match cell.submit(&handle, submission(0)).await {
            Ok(_) => panic!("expected capacity error before forward"),
            Err(error) => error,
        };
        assert!(matches!(err, SpoolError::CapacityExceeded));
        assert!(cell.is_serving(), "pre-WAL reject must not poison");
    });
}

#[test]
fn file_spool_survives_reconstruction_and_blocks_serving() {
    let dir = tempfile_dir("file-rebuild");
    {
        let storage = FileSpoolStorage::open(&dir).expect("open");
        let (mut pool, handle, _) = spawn_driver();
        let cell = open_cell(&pool, storage);
        assert!(cell.is_serving());
        pool.run_until(async {
            let receipt = cell.submit(&handle, submission(0)).await.expect("submit");
            handle.flush().await.expect("flush");
            let committed = receipt.await.expect("commit+progress");
            assert_eq!(committed.first_offset, RecordOffset::new(0));
        });
    }
    let storage = FileSpoolStorage::open(&dir).expect("reopen");
    let (cell, _completer) =
        SpoolCell::open(journal(), SpoolConfig::default(), storage).expect("cell");
    assert!(!cell.is_serving());
    let report = cell.recovery_report().expect("report");
    assert_eq!(report.committed_locally, 1);
    assert!(report.recovery_required());
}

#[test]
fn dropped_receipt_still_records_committed_locally_progress() {
    let dir = tempfile_dir("drop-receipt-progress");
    let storage = FileSpoolStorage::open(&dir).expect("open");
    let (mut pool, handle, drive) = spawn_driver();
    let cell = open_cell(&pool, storage);
    let gate = drive.gate_write(address(0));

    pool.run_until(async {
        let receipt = cell.submit(&handle, submission(0)).await.expect("wal");
        drop(receipt);
        gate.open();
        handle.flush().await.expect("flush");
    });
    // Quiesce cell-owned completion (progress sync).
    for _ in 0..128 {
        pool.run_until_stalled();
    }
    drop(cell);
    // Dropping the handle closes the work queue eventually — give run() a moment.
    for _ in 0..32 {
        pool.run_until_stalled();
    }

    let storage = FileSpoolStorage::open(&dir).expect("reopen");
    let report = scan_and_classify(&storage).expect("scan");
    assert!(report.recovery_required());
    assert_eq!(report.committed_locally, 1);
    assert_eq!(report.pending_unclassified, 0);
}

#[test]
fn empty_reopen_still_serving() {
    let dir = tempfile_dir("empty-serving");
    {
        let storage = FileSpoolStorage::open(&dir).expect("open");
        let (cell, _completer) =
            SpoolCell::open(journal(), SpoolConfig::default(), storage).expect("cell");
        assert!(cell.is_serving());
    }
    let storage = FileSpoolStorage::open(&dir).expect("reopen");
    let (cell, _completer) =
        SpoolCell::open(journal(), SpoolConfig::default(), storage).expect("cell");
    assert!(cell.is_serving());
}

#[test]
fn frame_round_trip_and_arbitrary_bytes() {
    let frame = SpoolFrame::Submission {
        journal_id: journal(),
        submission: Submission {
            producer_id: ProducerId::from_bytes(*b"producer-fixed!!"),
            producer_epoch: 2,
            sequence: 4,
            records: vec![Record::new(
                [("a".into(), AttributeValue::String("x".into()))],
                Bytes::from_static(b"y"),
            )],
        },
    };
    let encoded = encode_spool_frame(&frame).expect("encode");
    let (decoded, n) = decode_spool_frame(&encoded).expect("decode");
    assert_eq!(n, encoded.len());
    assert_eq!(decoded, frame);
    assert!(decode_spool_frame(&[0xff; 32]).is_err());
}

#[test]
fn pending_receipt_stays_pending_until_progress() {
    let (mut pool, handle, drive) = spawn_driver();
    let cell = open_cell(&pool, InMemorySpoolStorage::new());
    let gate = drive.gate_write(address(0));

    pool.run_until(async {
        let mut receipt = cell.submit(&handle, submission(0)).await.expect("admit");
        let pending = poll_fn(|context| match Pin::new(&mut receipt).poll(context) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await;
        assert!(pending, "must not resolve before kernel + progress");
        gate.open();
        handle.flush().await.expect("flush");
        let committed = receipt.await.expect("ok");
        assert_eq!(committed.first_offset, RecordOffset::new(0));
    });
}

#[test]
fn post_wal_receipt_failure_poisons_second_submit() {
    let (mut pool, handle, drive) = spawn_driver();
    let cell = open_cell(&pool, InMemorySpoolStorage::new());
    drive.fail_before_write(address(0));

    pool.run_until(async {
        let receipt = cell
            .submit(&handle, submission(0))
            .await
            .expect("wal+admit");
        // Seal path hits the injected LogDrive failure; do not require flush OK.
        let _ = handle.flush().await;
        let err = receipt.await.expect_err("driver fail");
        assert!(matches!(err, SpoolError::Forward(_)));
        assert!(matches!(
            cell.state(),
            scripture::SpoolCellState::Poisoned {
                cause: SpoolPoisonCause::ReceiptFailed
            }
        ));
        let second = cell.submit(&handle, submission(1)).await;
        assert!(matches!(
            second,
            Err(SpoolError::Poisoned {
                cause: SpoolPoisonCause::ReceiptFailed
            })
        ));
    });
}

#[test]
fn forward_failure_poison_is_visible_before_submit_returns() {
    let (mut pool, _handle, _) = spawn_driver();
    let cell = open_cell(&pool, InMemorySpoolStorage::new());

    pool.run_until(async {
        let result = cell
            .submit_forwarded(submission(0), |_submission| async {
                Err(SpoolError::Forward("injected forwarding failure".into()))
            })
            .await;
        let error = match result {
            Ok(_) => panic!("forward must fail"),
            Err(error) => error,
        };
        assert!(matches!(error, SpoolError::Forward(_)));
        assert!(matches!(
            cell.state(),
            scripture::SpoolCellState::Poisoned {
                cause: SpoolPoisonCause::ForwardFailed
            }
        ));
        let refused = cell
            .submit_forwarded(submission(1), |_submission| async {
                unreachable!("poisoned cell must not forward")
            })
            .await;
        assert!(matches!(
            refused,
            Err(SpoolError::Poisoned {
                cause: SpoolPoisonCause::ForwardFailed
            })
        ));
    });
}

fn tempfile_dir(tag: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "scripture-spool-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).expect("mkdir");
    path
}

#[test]
fn concurrent_duplicate_identity_admits_exactly_one() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let forwards = Arc::new(AtomicUsize::new(0));
    let dir = tempfile_dir("dup-race");
    let storage = FileSpoolStorage::open(&dir).expect("open");
    let (mut pool, _handle, _) = spawn_driver();
    let cell = open_cell(&pool, storage);
    let cell_a = cell.clone();
    let cell_b = cell.clone();
    let forwards_a = Arc::clone(&forwards);
    let forwards_b = Arc::clone(&forwards);

    pool.run_until(async {
        let left = cell_a.submit_forwarded(submission(0), |_submission| {
            let forwards = Arc::clone(&forwards_a);
            async move {
                forwards.fetch_add(1, Ordering::SeqCst);
                let (_tx, rx) = futures::channel::oneshot::channel();
                Ok(scripture::ReceiptFuture::from_receiver(rx))
            }
        });
        let right = cell_b.submit_forwarded(submission(0), |_submission| {
            let forwards = Arc::clone(&forwards_b);
            async move {
                forwards.fetch_add(1, Ordering::SeqCst);
                let (_tx, rx) = futures::channel::oneshot::channel();
                Ok(scripture::ReceiptFuture::from_receiver(rx))
            }
        });
        let (left, right) = futures::future::join(left, right).await;
        let outcomes = [left.map(|_| ()), right.map(|_| ())];
        let oks = outcomes.iter().filter(|r| r.is_ok()).count();
        let dups = outcomes
            .iter()
            .filter(|r| matches!(r, Err(SpoolError::DuplicateIdentity)))
            .count();
        assert_eq!(oks, 1, "exactly one admit");
        assert_eq!(dups, 1, "other is DuplicateIdentity");
        assert_eq!(forwards.load(Ordering::SeqCst), 1, "exactly one forward");
    });

    drop(cell);
    for _ in 0..32 {
        pool.run_until_stalled();
    }
    let storage = FileSpoolStorage::open(&dir).expect("reopen");
    let report = scan_and_classify(&storage).expect("scan");
    assert!(
        !report.corrupt_history,
        "duplicate race must not corrupt WAL history"
    );
    assert_eq!(report.valid_frames, 1);
    assert_eq!(report.pending_unclassified, 1);
}

#[test]
fn completion_queue_capacity_refuses_before_wal() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let wal_appends = Arc::new(AtomicUsize::new(0));
    struct CountingStorage {
        inner: InMemorySpoolStorage,
        appends: Arc<AtomicUsize>,
    }
    impl SpoolStorage for CountingStorage {
        fn append_frame(&mut self, frame: &SpoolFrame) -> Result<(), SpoolError> {
            self.appends.fetch_add(1, Ordering::SeqCst);
            self.inner.append_frame(frame)
        }
        fn sync(&mut self) -> Result<(), SpoolError> {
            self.inner.sync()
        }
        fn scan_valid_frames(&self) -> Result<(Vec<ValidFrame>, ScanTail), SpoolError> {
            self.inner.scan_valid_frames()
        }
        fn used_bytes(&self) -> usize {
            self.inner.used_bytes()
        }
        fn frame_count(&self) -> usize {
            self.inner.frame_count()
        }
        fn set_faults(&mut self, faults: SpoolStorageFaults) {
            self.inner.set_faults(faults);
        }
    }

    let config = SpoolConfig {
        max_inflight_completions: 1,
        ..SpoolConfig::default()
    };
    let storage = CountingStorage {
        inner: InMemorySpoolStorage::new(),
        appends: Arc::clone(&wal_appends),
    };
    let (mut pool, handle, _) = spawn_driver();
    let (cell, completer) = SpoolCell::open(journal(), config, storage).expect("open");
    pool.spawner()
        .spawn(async move {
            completer.run().await;
        })
        .expect("spawn");

    pool.run_until(async {
        let first = cell
            .submit_forwarded(submission(0), |_submission| async {
                let (_tx, rx) = futures::channel::oneshot::channel();
                Ok(scripture::ReceiptFuture::from_receiver(rx))
            })
            .await
            .expect("first admit");
        let after_first = wal_appends.load(Ordering::SeqCst);
        assert!(after_first >= 1, "first submission must WAL");

        let refused = cell.submit(&handle, submission(1)).await;
        assert!(matches!(refused, Err(SpoolError::CapacityExceeded)));
        assert_eq!(
            wal_appends.load(Ordering::SeqCst),
            after_first,
            "refused submit must not append"
        );
        assert!(cell.is_serving(), "queue-full refusal must not poison");
        drop(first);
    });
}

#[test]
fn classification_labels_exist() {
    let _ = FrameClassification::CommittedLocally;
    let _ = FrameClassification::PendingUnclassified;
}
