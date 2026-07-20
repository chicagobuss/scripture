//! Real-path ChunkDriverActor tests over a scripted Holylog LogDrive.
//!
//! The actor owns a real [`ChunkLogWriter`] and a real [`AtomicLog`]. Faults are
//! injected only at the LogDrive boundary. The fourth ambiguous cutover outcome
//! (durable-but-unobserved revealed to a VirtualLog successor) remains a later
//! fleet-plan integration test — see holylog folio
//! `scripture-cross-cloud-real-driver-and-fleet-plan.md`.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::channel::oneshot;
use futures::executor::LocalPool;
use futures::future::poll_fn;
use futures::task::SpawnExt;
use holylog::atomic::AtomicLog;
use holylog::drive::LogDrive;
use proptest::prelude::*;
use scripture::{
    ChunkDriverActor, ChunkLogWriter, ChunkPolicy, DriverError, Frame, ManualClock, ManualTimer,
    PolicyError, ProducerId, Record, RecordOffset, RecoveryBound, Submission, SubmissionRef,
    encoded_chunk_len,
};

#[path = "support/mod.rs"]
mod support;

use support::{
    ScriptedLogDrive, address, cohort, journal, policy, producer, record, tiny_policy, writer_id,
};

fn submission(sequence: u64, values: &[i64]) -> Submission {
    Submission {
        producer_id: producer(),
        producer_epoch: 1,
        sequence,
        records: values.iter().copied().map(record).collect(),
    }
}

#[test]
fn receipt_is_released_only_after_kernel_acknowledges() {
    let drive = ScriptedLogDrive::new();
    let gate = drive.gate_write(address(0));
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
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let (done_tx, done_rx) = oneshot::channel();
    let handle2 = handle.clone();
    spawner
        .spawn(async move {
            let future = handle2.submit(submission(0, &[1])).await.expect("enqueue");
            let flush = handle2.flush();
            let _ = ready_tx.send(());
            // Race flush against receipt; both wait on the gated write.
            let (flush_result, receipt) = futures::future::join(flush, future).await;
            let _ = done_tx.send((flush_result, receipt));
        })
        .expect("spawn client");

    pool.run_until(ready_rx).expect("ready");
    pool.run_until_stalled();
    assert_eq!(drive.write_count(), 0);

    gate.open();
    let (flush_result, receipt) = pool.run_until(done_rx).expect("done");
    flush_result.expect("flush ok");
    let receipt = receipt.expect("receipt");
    assert_eq!(receipt.first_offset, RecordOffset::new(0));
    assert_eq!(receipt.slot, 0);
    assert!(!receipt.deduplicated);
    assert_eq!(drive.write_count(), 1);
}

#[test]
fn cancelled_submitter_still_commits() {
    let drive = ScriptedLogDrive::new();
    let gate = drive.gate_write(address(0));
    let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
        .build()
        .expect("log");
    let writer = ChunkLogWriter::new(journal(), cohort(), 1, log.clone(), RecordOffset::new(0));
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
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let handle2 = handle.clone();
    spawner
        .spawn(async move {
            let future = handle2.submit(submission(0, &[7])).await.expect("enqueue");
            drop(future);
            let flush = handle2.flush();
            let _ = ready_tx.send(());
            let _ = flush.await;
        })
        .expect("spawn");

    pool.run_until(ready_rx).expect("ready");
    pool.run_until_stalled();
    gate.open();
    pool.run_until_stalled();

    assert!(drive.contains(address(0)));
    let recovery = pool
        .run_until(ChunkLogWriter::recover(
            journal(),
            cohort(),
            1,
            log,
            RecoveryBound::new(4).expect("bound"),
            None,
        ))
        .expect("recover");
    assert_eq!(recovery.chunks.len(), 1);
    assert_eq!(recovery.chunks[0].record_count, 1);
}

#[test]
fn failed_or_ambiguous_append_poisons_and_never_retries() {
    let drive = ScriptedLogDrive::new();
    drive.fail_after_durable_write(address(0));
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
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    let first = handle.clone();
    let first_result = pool.run_until(async move {
        let future = first.submit(submission(0, &[1])).await.expect("enqueue");
        let flush = first.flush();
        let (flush_result, receipt) = futures::future::join(flush, future).await;
        (flush_result, receipt)
    });
    assert!(matches!(first_result.1, Err(DriverError::Uncertain { .. })));
    assert!(matches!(first_result.0, Err(DriverError::Poisoned)));

    let later = handle.clone();
    let later_result = pool.run_until(async move { later.submit(submission(1, &[2])).await });
    assert!(matches!(later_result, Err(DriverError::Poisoned)));
    assert_eq!(drive.write_count(), 1);
}

#[test]
fn record_and_flush_bounds_emit_single_frame_chunks() {
    let drive = ScriptedLogDrive::new();
    let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
        .build()
        .expect("log");
    let writer = ChunkLogWriter::new(journal(), cohort(), 1, log.clone(), RecordOffset::new(0));
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    let (handle, actor) = ChunkDriverActor::new(
        journal(),
        cohort(),
        writer_id(),
        1,
        writer,
        &[],
        tiny_policy(),
        clock,
        timer,
        8,
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            actor.run().await.expect("run");
        })
        .expect("spawn");

    let (a, b, c) = pool.run_until(async move {
        let a = handle.submit(submission(0, &[1])).await.expect("a");
        let b = handle.submit(submission(1, &[2])).await.expect("b");
        let c = handle.submit(submission(2, &[3])).await.expect("c");
        handle.flush().await.expect("flush");
        (
            a.await.expect("a"),
            b.await.expect("b"),
            c.await.expect("c"),
        )
    });
    assert_eq!(a.first_offset, RecordOffset::new(0));
    assert_eq!(b.first_offset, RecordOffset::new(1));
    assert_eq!(c.first_offset, RecordOffset::new(2));
    assert_eq!(a.slot, 0);
    assert_eq!(b.slot, 0);
    assert_eq!(c.slot, 1);

    let recovery = pool
        .run_until(ChunkLogWriter::recover(
            journal(),
            cohort(),
            1,
            log,
            RecoveryBound::new(8).expect("bound"),
            None,
        ))
        .expect("recover");
    assert_eq!(recovery.chunks.len(), 2);
    assert_eq!(recovery.chunks[0].frame.submissions.len(), 2);
    assert_eq!(recovery.chunks[1].frame.submissions.len(), 1);
}

#[test]
fn dropped_response_retry_returns_original_receipt() {
    let drive = ScriptedLogDrive::new();
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
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            actor.run().await.expect("run");
        })
        .expect("spawn");

    let first = pool.run_until({
        let handle = handle.clone();
        async move {
            let future = handle.submit(submission(0, &[9])).await.expect("enqueue");
            handle.flush().await.expect("flush");
            future.await.expect("first")
        }
    });

    let retry = pool.run_until(async move {
        let future = handle
            .submit(submission(0, &[9]))
            .await
            .expect("retry enqueue");
        future.await.expect("retry")
    });
    assert!(retry.deduplicated);
    assert_eq!(retry.first_offset, first.first_offset);
    assert_eq!(retry.slot, first.slot);
    assert_eq!(retry.chunk_id, first.chunk_id);
    assert_eq!(drive.write_count(), 1);
}

#[test]
fn policy_rejects_record_that_cannot_fit_a_chunk() {
    let err = ChunkPolicy {
        max_chunk_bytes: 64,
        max_record_bytes: 64,
        max_chunk_records: 1,
        max_chunk_age: Duration::from_millis(10),
        max_buffered_bytes: 64,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_millis(10),
        recovery_scan: RecoveryBound::new(1).expect("bound"),
    }
    .validate();
    assert!(err.is_err());
}

#[test]
fn policy_rejects_inflight_depth_above_one() {
    let err = ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 8,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 64 * 1024,
        max_inflight_chunks: 2,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(16).expect("bound"),
    }
    .validate();
    assert!(matches!(
        err,
        Err(PolicyError::PhaseOneRequiresInflightOne {
            max_inflight_chunks: 2
        })
    ));
}

#[test]
fn policy_rejects_zero_max_uncommitted_age() {
    let err = ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 8,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 64 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::ZERO,
        recovery_scan: RecoveryBound::new(16).expect("bound"),
    }
    .validate();
    assert!(matches!(err, Err(PolicyError::InvalidLimit)));
}

#[test]
fn policy_rejects_bytes_at_risk_overflow() {
    let err = ChunkPolicy {
        max_chunk_bytes: usize::MAX / 2 + 1,
        max_record_bytes: 1,
        max_chunk_records: 1,
        max_chunk_age: Duration::from_secs(1),
        max_buffered_bytes: usize::MAX / 2 + 1,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(1),
        recovery_scan: RecoveryBound::new(1).expect("bound"),
    }
    .validate();
    assert!(matches!(err, Err(PolicyError::InvalidLimit)));
}

fn solo_submission_encoded_bytes(submission: &Submission) -> usize {
    let frame = Frame {
        journal_id: journal(),
        base_offset: RecordOffset::new(0),
        records: submission.records.clone(),
        submissions: vec![SubmissionRef {
            producer_id: submission.producer_id,
            producer_epoch: submission.producer_epoch,
            sequence: submission.sequence,
            first_record: 0,
            record_count: u32::try_from(submission.records.len()).expect("count"),
        }],
    };
    encoded_chunk_len(std::slice::from_ref(&frame)).expect("len")
}

fn record_encoded_contribution(record: &Record) -> usize {
    let solo = {
        let frame = Frame {
            journal_id: journal(),
            base_offset: RecordOffset::new(0),
            records: vec![record.clone()],
            submissions: vec![SubmissionRef {
                producer_id: ProducerId::from_bytes([0; 16]),
                producer_epoch: 0,
                sequence: 0,
                first_record: 0,
                record_count: 1,
            }],
        };
        encoded_chunk_len(std::slice::from_ref(&frame)).expect("solo")
    };
    let empty = {
        let frame = Frame {
            journal_id: journal(),
            base_offset: RecordOffset::new(0),
            records: vec![Record::new([], Bytes::new())],
            submissions: vec![SubmissionRef {
                producer_id: ProducerId::from_bytes([0; 16]),
                producer_epoch: 0,
                sequence: 0,
                first_record: 0,
                record_count: 1,
            }],
        };
        encoded_chunk_len(std::slice::from_ref(&frame)).expect("empty")
    };
    solo.saturating_sub(empty)
}

#[test]
fn reservation_pressure_parks_admission_until_commit() {
    let first = submission(0, &[1]);
    let first_bytes = solo_submission_encoded_bytes(&first);
    let tight = ChunkPolicy {
        max_chunk_bytes: first_bytes * 4,
        max_record_bytes: first_bytes,
        max_chunk_records: 8,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: first_bytes,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    };
    tight.validate().expect("policy");

    let drive = ScriptedLogDrive::new();
    let gate = drive.gate_write(address(0));
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
        tight,
        clock,
        timer,
        8,
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    let metrics = handle.metrics();
    assert_eq!(metrics.bytes_at_risk, tight.bytes_at_risk());
    assert_eq!(metrics.reserved_bytes, 0);

    let first_receipt = pool.run_until({
        let handle = handle.clone();
        #[allow(clippy::async_yields_async)]
        async move {
            handle.submit(first).await.expect("first admission")
        }
    });
    assert_eq!(handle.metrics().reserved_bytes, first_bytes);
    assert_eq!(
        handle.metrics().bytes_at_risk,
        tight.bytes_at_risk(),
        "bytes_at_risk stays the declared policy bound"
    );

    let (pending_tx, pending_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let handle2 = handle.clone();
    spawner
        .spawn(async move {
            let admit = handle2.submit(submission(1, &[2]));
            futures::pin_mut!(admit);
            // Prove admission has not resolved under reservation pressure.
            let stalled = poll_fn(|cx| match admit.as_mut().poll(cx) {
                Poll::Ready(_) => Poll::Ready(false),
                Poll::Pending => Poll::Ready(true),
            })
            .await;
            let _ = pending_tx.send(stalled);
            let receipt_future = admit.await.expect("second admission after drain");
            let _ = done_tx.send(receipt_future);
        })
        .expect("spawn blocked submit");

    pool.run_until_stalled();
    assert!(
        pool.run_until(pending_rx).expect("pending signal"),
        "submit must stay pending on admission, not error"
    );

    let (flush_tx, flush_rx) = oneshot::channel();
    let handle3 = handle.clone();
    let first_receipt_task = first_receipt;
    spawner
        .spawn(async move {
            let flush = handle3.flush();
            let (flush_result, first) = futures::future::join(flush, first_receipt_task).await;
            let _ = flush_tx.send((flush_result, first));
        })
        .expect("spawn flush");

    pool.run_until_stalled();
    assert_eq!(drive.write_count(), 0);
    gate.open();

    let (flush_result, first) = pool.run_until(flush_rx).expect("flush done");
    flush_result.expect("flush ok");
    let first = first.expect("first receipt");
    assert_eq!(first.first_offset, RecordOffset::new(0));
    assert_eq!(first.slot, 0);

    let second_future = pool.run_until(done_rx).expect("second admitted");
    let second = pool.run_until(async move {
        handle.flush().await.expect("second flush");
        second_future.await.expect("second receipt")
    });
    assert_eq!(second.first_offset, RecordOffset::new(1));
    assert_eq!(second.slot, 1);
    assert_eq!(drive.write_count(), 2);
}

#[test]
fn parked_duplicate_retries_join_instead_of_double_encode() {
    // Filler occupies the buffer under a gated append. Two same-identity
    // retries park (neither is open/pending yet). After the filler commits,
    // drain must admit the first and join the second — not encode twice.
    let filler = Submission {
        producer_id: ProducerId::from_bytes(*b"filler-producer!"),
        producer_epoch: 1,
        sequence: 0,
        records: vec![record(1)],
    };
    let retry = submission(0, &[2]);
    let filler_bytes = solo_submission_encoded_bytes(&filler);
    let retry_bytes = solo_submission_encoded_bytes(&retry);
    let tight = ChunkPolicy {
        max_chunk_bytes: filler_bytes.max(retry_bytes) * 4,
        max_record_bytes: filler_bytes.max(retry_bytes),
        max_chunk_records: 8,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: filler_bytes,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    };
    tight.validate().expect("policy");

    let drive = ScriptedLogDrive::new();
    let gate = drive.gate_write(address(0));
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
        tight,
        clock,
        timer,
        8,
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    let filler_receipt = pool.run_until({
        let handle = handle.clone();
        #[allow(clippy::async_yields_async)]
        async move {
            handle.submit(filler).await.expect("filler admission")
        }
    });

    let (first_pending_tx, first_pending_rx) = oneshot::channel();
    let (first_done_tx, first_done_rx) = oneshot::channel();
    let handle_a = handle.clone();
    let first_retry = retry.clone();
    spawner
        .spawn(async move {
            let admit = handle_a.submit(first_retry);
            futures::pin_mut!(admit);
            let stalled = poll_fn(|cx| match admit.as_mut().poll(cx) {
                Poll::Ready(_) => Poll::Ready(false),
                Poll::Pending => Poll::Ready(true),
            })
            .await;
            let _ = first_pending_tx.send(stalled);
            let receipt_future = admit.await.expect("first retry admission");
            let _ = first_done_tx.send(receipt_future);
        })
        .expect("spawn first retry");

    pool.run_until_stalled();
    assert!(
        pool.run_until(first_pending_rx).expect("first pending"),
        "first same-identity retry must park behind filler reservation"
    );

    let (second_pending_tx, second_pending_rx) = oneshot::channel();
    let (second_done_tx, second_done_rx) = oneshot::channel();
    let handle_b = handle.clone();
    spawner
        .spawn(async move {
            let admit = handle_b.submit(retry);
            futures::pin_mut!(admit);
            let stalled = poll_fn(|cx| match admit.as_mut().poll(cx) {
                Poll::Ready(_) => Poll::Ready(false),
                Poll::Pending => Poll::Ready(true),
            })
            .await;
            let _ = second_pending_tx.send(stalled);
            let receipt_future = admit.await.expect("second retry admission");
            let _ = second_done_tx.send(receipt_future);
        })
        .expect("spawn second retry");

    pool.run_until_stalled();
    assert!(
        pool.run_until(second_pending_rx).expect("second pending"),
        "second same-identity retry must also park"
    );

    let (flush_tx, flush_rx) = oneshot::channel();
    let handle_flush = handle.clone();
    spawner
        .spawn(async move {
            let flush = handle_flush.flush();
            let (flush_result, filler) = futures::future::join(flush, filler_receipt).await;
            let _ = flush_tx.send((flush_result, filler));
        })
        .expect("spawn flush");

    pool.run_until_stalled();
    assert_eq!(drive.write_count(), 0);
    gate.open();

    let (flush_result, filler) = pool.run_until(flush_rx).expect("flush done");
    flush_result.expect("flush ok");
    let filler = filler.expect("filler receipt");
    assert_eq!(filler.first_offset, RecordOffset::new(0));
    assert_eq!(filler.slot, 0);

    let first_future = pool.run_until(first_done_rx).expect("first admitted");
    let second_future = pool.run_until(second_done_rx).expect("second admitted");
    let (first, second) = pool.run_until(async {
        handle.flush().await.expect("retry flush");
        futures::future::join(first_future, second_future).await
    });
    let first = first.expect("first receipt");
    let second = second.expect("second receipt");

    assert_eq!(first.first_offset, RecordOffset::new(1));
    assert_eq!(second.first_offset, first.first_offset);
    assert_eq!(second.next_offset, first.next_offset);
    assert_eq!(second.slot, first.slot);
    assert_eq!(second.chunk_id, first.chunk_id);
    assert!(!first.deduplicated);
    assert!(
        !second.deduplicated,
        "parked retry must join, not re-encode"
    );
    assert_eq!(
        drive.write_count(),
        2,
        "filler chunk + one durable retry chunk"
    );
}

#[test]
fn auto_seal_durable_then_error_poisons_without_flush() {
    let drive = ScriptedLogDrive::new();
    drive.fail_after_durable_write(address(0));
    let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
        .build()
        .expect("log");
    let writer = ChunkLogWriter::new(journal(), cohort(), 1, log, RecordOffset::new(0));
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    let auto_seal = ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 1,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 64 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    };
    let (handle, actor) = ChunkDriverActor::new(
        journal(),
        cohort(),
        writer_id(),
        1,
        writer,
        &[],
        auto_seal,
        clock,
        timer,
        8,
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    let first = pool.run_until({
        let handle = handle.clone();
        async move {
            let future = handle.submit(submission(0, &[1])).await.expect("admitted");
            future.await
        }
    });
    assert!(matches!(first, Err(DriverError::Uncertain { .. })));
    assert_eq!(drive.write_count(), 1);

    let later = pool.run_until(async move { handle.submit(submission(1, &[2])).await });
    assert!(
        matches!(later, Err(DriverError::Poisoned)),
        "later submit must see Poisoned at admission"
    );
    assert_eq!(drive.write_count(), 1);
}

#[test]
fn age_bound_seals_and_commits_via_manual_timer() {
    let drive = ScriptedLogDrive::new();
    let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
        .build()
        .expect("log");
    let writer = ChunkLogWriter::new(journal(), cohort(), 1, log, RecordOffset::new(0));
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    let age_policy = ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 64,
        max_chunk_age: Duration::from_millis(5),
        max_buffered_bytes: 64 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    };
    let (handle, actor) = ChunkDriverActor::new(
        journal(),
        cohort(),
        writer_id(),
        1,
        writer,
        &[],
        age_policy,
        Arc::clone(&clock),
        timer.clone(),
        8,
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    let receipt_future = pool.run_until({
        let handle = handle.clone();
        #[allow(clippy::async_yields_async)]
        async move {
            handle.submit(submission(0, &[42])).await.expect("admitted")
        }
    });
    pool.run_until_stalled();
    assert_eq!(drive.write_count(), 0);

    timer.advance(Duration::from_millis(5));
    pool.run_until_stalled();

    let receipt = pool.run_until(receipt_future).expect("age-sealed receipt");
    assert_eq!(receipt.first_offset, RecordOffset::new(0));
    assert_eq!(receipt.slot, 0);
    assert_eq!(drive.write_count(), 1);
}

#[test]
fn multi_record_submission_admits_when_each_record_under_limit() {
    let left = record(1);
    let right = record(2);
    let left_contrib = record_encoded_contribution(&left);
    let right_contrib = record_encoded_contribution(&right);
    let per_record = left_contrib.max(right_contrib);
    let combined = submission(0, &[1, 2]);
    let combined_bytes = solo_submission_encoded_bytes(&combined);
    assert!(
        combined_bytes > per_record,
        "aggregate solo size must exceed per-record ceiling for this test"
    );

    let policy = ChunkPolicy {
        max_chunk_bytes: combined_bytes * 2,
        max_record_bytes: per_record,
        max_chunk_records: 8,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: combined_bytes * 2,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    };
    policy.validate().expect("policy");

    let drive = ScriptedLogDrive::new();
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
        policy,
        clock,
        timer,
        8,
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            actor.run().await.expect("run");
        })
        .expect("spawn");

    let receipt = pool.run_until(async move {
        let future = handle.submit(combined).await.expect("admitted");
        handle.flush().await.expect("flush");
        future.await.expect("receipt")
    });
    assert_eq!(receipt.first_offset, RecordOffset::new(0));
    assert_eq!(receipt.next_offset, RecordOffset::new(2));
    assert_eq!(drive.write_count(), 1);
}

#[test]
fn command_winning_select_does_not_leak_timer_sleepers() {
    let drive = ScriptedLogDrive::new();
    let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
        .build()
        .expect("log");
    let writer = ChunkLogWriter::new(journal(), cohort(), 1, log, RecordOffset::new(0));
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    let age_policy = ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 64,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 64 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    };
    let (handle, actor) = ChunkDriverActor::new(
        journal(),
        cohort(),
        writer_id(),
        1,
        writer,
        &[],
        age_policy,
        clock,
        timer.clone(),
        8,
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    pool.run_until({
        let handle = handle.clone();
        async move {
            drop(handle.submit(submission(0, &[1])).await.expect("admitted"));
        }
    });
    pool.run_until_stalled();
    let baseline = timer.sleeper_count();
    assert!(baseline >= 1, "open chunk should register one age sleeper");

    for sequence in 10..60 {
        let handle = handle.clone();
        let result = pool.run_until(async move {
            handle
                .submit(submission(sequence, &[sequence as i64]))
                .await
        });
        assert!(matches!(result, Err(DriverError::OutOfSequence { .. })));
    }
    pool.run_until_stalled();
    assert_eq!(
        timer.sleeper_count(),
        baseline,
        "command-winning select must retain one age sleep, not leak registrations"
    );
}

#[test]
fn max_uncommitted_age_parks_new_admission() {
    let drive = ScriptedLogDrive::new();
    let gate = drive.gate_write(address(0));
    let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
        .build()
        .expect("log");
    let writer = ChunkLogWriter::new(journal(), cohort(), 1, log, RecordOffset::new(0));
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    let policy = ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 1, // first submit seals immediately
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 64 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_millis(5),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    };
    let (handle, actor) = ChunkDriverActor::new(
        journal(),
        cohort(),
        writer_id(),
        1,
        writer,
        &[],
        policy,
        Arc::clone(&clock),
        timer.clone(),
        8,
        None,
    )
    .expect("actor");

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    spawner
        .spawn(async move {
            let _ = actor.run().await;
        })
        .expect("spawn actor");

    let first = handle.clone();
    let first_done = spawner
        .spawn_with_handle(async move {
            let future = first
                .submit(submission(0, &[1]))
                .await
                .expect("first admitted");
            future.await
        })
        .expect("spawn first");
    pool.run_until_stalled();
    assert_eq!(drive.write_count(), 0);

    // Oldest uncommitted work is the sealed in-flight chunk; age past the limit.
    timer.advance(Duration::from_millis(5));
    pool.run_until_stalled();

    let (started_tx, started_rx) = oneshot::channel::<()>();
    let (second_tx, mut second_rx) = oneshot::channel();
    let second = handle.clone();
    spawner
        .spawn(async move {
            let _ = started_tx.send(());
            let result = second.submit(submission(1, &[2])).await;
            let _ = second_tx.send(result);
        })
        .expect("spawn second");

    pool.run_until(started_rx).expect("second started");
    pool.run_until_stalled();
    assert!(
        second_rx.try_recv().expect("open").is_none(),
        "admission must park once max_uncommitted_age is exceeded"
    );

    gate.open();
    let first_receipt = pool.run_until(first_done).expect("first committed");
    assert_eq!(first_receipt.slot, 0);

    let second_admission = pool
        .run_until(second_rx)
        .expect("second admission resolved");
    let second_future = second_admission.expect("second admitted after commit");
    let flush = handle.flush();
    let (flush_result, second_receipt) =
        pool.run_until(async move { futures::future::join(flush, second_future).await });
    flush_result.expect("flush");
    let second_receipt = second_receipt.expect("second receipt");
    assert_eq!(second_receipt.first_offset, RecordOffset::new(1));
    assert_eq!(drive.write_count(), 2);
}

// A small generated schedule over the *real* actor and AtomicLog. The pure
// model remains the broad oracle; this catches implementation drift in the
// normal admission/flush/timer path before fault scheduling is added.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn generated_real_actor_schedules_preserve_dense_unique_receipts(
        operations in prop::collection::vec((any::<bool>(), 1_usize..3), 1..32),
    ) {
        let drive = ScriptedLogDrive::new();
        let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
            .build()
            .expect("log");
        let writer = ChunkLogWriter::new(journal(), cohort(), 1, log.clone(), RecordOffset::new(0));
        let clock = Arc::new(ManualClock::new());
        let timer = ManualTimer::new(Arc::clone(&clock));
        let (handle, actor) = ChunkDriverActor::new(
            journal(), cohort(), writer_id(), 1, writer, &[], policy(), clock, timer, 32, None,
        ).expect("actor");
        let mut pool = LocalPool::new();
        pool.spawner().spawn(async move { let _ = actor.run().await; }).expect("spawn actor");

        let mut next_sequence = 0_u64;
        let mut committed_sequences = Vec::new();
        let mut pending = Vec::new();
        let mut receipts = Vec::new();
        for (index, (retry, records)) in operations.into_iter().enumerate() {
            let sequence = if retry && !committed_sequences.is_empty() {
                *committed_sequences.last().expect("non-empty")
            } else {
                let sequence = next_sequence;
                next_sequence += 1;
                sequence
            };
            let submission = Submission {
                producer_id: producer(), producer_epoch: 1, sequence,
                records: (0..records).map(|value| record(i64::try_from(value).expect("small"))).collect(),
            };
            let receipt = pool.run_until(handle.submit(submission)).expect("admitted");
            pending.push((sequence, receipt));
            if index % 3 == 2 {
                pool.run_until(handle.flush()).expect("flush");
                for (sequence, receipt) in std::mem::take(&mut pending) {
                    let receipt = pool.run_until(receipt).expect("committed receipt");
                    if !receipt.deduplicated { committed_sequences.push(sequence); }
                    receipts.push(receipt);
                }
            }
        }
        pool.run_until(handle.flush()).expect("final flush");
        for (sequence, receipt) in pending {
            let receipt = pool.run_until(receipt).expect("committed receipt");
            if !receipt.deduplicated { committed_sequences.push(sequence); }
            receipts.push(receipt);
        }

        let recovery = pool.run_until(ChunkLogWriter::recover(
            journal(), cohort(), 1, log, RecoveryBound::new(64).expect("bound"), None,
        )).expect("recover");
        let mut expected = 0_u64;
        let mut identities = BTreeSet::new();
        for chunk in &recovery.chunks {
            prop_assert_eq!(chunk.first_offset, RecordOffset::new(expected));
            expected += u64::from(chunk.record_count);
            for submission in &chunk.frame.submissions {
                prop_assert!(identities.insert((submission.producer_id, submission.producer_epoch, submission.sequence)));
            }
        }
        for receipt in receipts {
            prop_assert_eq!(receipt.level, scripture::AckLevel::Committed);
            prop_assert!(receipt.next_offset.get() > receipt.first_offset.get());
            prop_assert!(receipt.first_offset.get() < expected);
        }
        prop_assert_eq!(handle.ledger().logical_count(scripture::Effect::ChunkCommitted), recovery.chunks.len());
    }
}
