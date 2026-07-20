//! Generated adversarial schedules over the real [`ChunkDriverActor`].
//!
//! The pure model in `driver_model.rs` remains the broad oracle. These schedules
//! stress fault injection, timer age bounds, receipt drops, and poison at the
//! real AtomicLog / scripted LogDrive boundary.

use std::collections::{BTreeSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::channel::oneshot;
use futures::executor::LocalPool;
use futures::task::{SpawnExt, noop_waker};
use holylog::atomic::AtomicLog;
use holylog::drive::LogDrive;
use proptest::prelude::*;
use scripture::{
    AckLevel, ChunkDriverActor, ChunkDriverHandle, ChunkLogWriter, DriverError, Event, Ledger,
    ManualClock, ManualTimer, Receipt, RecordOffset, RecoveryBound, Submission, TerminalOutcome,
    decode_chunk,
};

#[path = "support/mod.rs"]
mod support;

use support::{
    PollGate, ScriptedLogDrive, address, cohort, hostile_policy, journal, producer, record,
    writer_id,
};

/// Schedule operations over protocol boundaries the hand tests already cover.
#[derive(Debug, Clone, Copy)]
enum HostileOp {
    SubmitNew(u8),
    RetryCommitted,
    RetryInFlight,
    Flush,
    AdvanceAge(u8),
    GateNextWrite,
    ReleaseWrite,
    DurableThenErrorNext,
    FailBeforeNext,
    DropLastReceipt,
    Quiesce,
}

fn hostile_op_strategy() -> impl Strategy<Value = HostileOp> {
    prop_oneof![
        6 => (1_u8..3).prop_map(HostileOp::SubmitNew),
        2 => Just(HostileOp::RetryCommitted),
        2 => Just(HostileOp::RetryInFlight),
        3 => Just(HostileOp::Flush),
        2 => (1_u8..12).prop_map(HostileOp::AdvanceAge),
        1 => Just(HostileOp::GateNextWrite),
        1 => Just(HostileOp::ReleaseWrite),
        1 => Just(HostileOp::DurableThenErrorNext),
        1 => Just(HostileOp::FailBeforeNext),
        1 => Just(HostileOp::DropLastReceipt),
        1 => Just(HostileOp::Quiesce),
    ]
}

fn is_admission_reject(error: &DriverError) -> bool {
    matches!(
        error,
        DriverError::OutOfSequence { .. }
            | DriverError::FencedProducer { .. }
            | DriverError::Indeterminate { .. }
            | DriverError::RecordTooLarge { .. }
            | DriverError::EmptySubmission
    )
}

struct RealActorHarness {
    pool: LocalPool,
    handle: ChunkDriverHandle,
    drive: Arc<ScriptedLogDrive>,
    log: AtomicLog,
    timer: ManualTimer,
    next_sequence: u64,
    committed: Vec<u64>,
    in_flight: BTreeSet<u64>,
    pending_gates: VecDeque<Arc<PollGate>>,
    pending_receipts: Vec<(u64, scripture::ReceiptFuture)>,
    collected: Vec<(u64, Receipt)>,
    dropped: BTreeSet<u64>,
    blocked_flush: Option<oneshot::Receiver<Result<(), DriverError>>>,
    blocked_admits: Vec<(
        u64,
        oneshot::Receiver<Result<scripture::ReceiptFuture, DriverError>>,
    )>,
    writes_at_poison: Option<u64>,
}

impl RealActorHarness {
    fn new() -> Self {
        let drive = ScriptedLogDrive::new();
        let log = AtomicLog::builder(Arc::clone(&drive) as Arc<dyn LogDrive>, 0)
            .build()
            .expect("log");
        let writer = ChunkLogWriter::new(journal(), cohort(), 1, log.clone(), RecordOffset::new(0));
        let clock = Arc::new(ManualClock::new());
        let timer = ManualTimer::new(Arc::clone(&clock));
        let policy = hostile_policy();
        let (handle, actor) = ChunkDriverActor::new(
            journal(),
            cohort(),
            writer_id(),
            1,
            writer,
            &[],
            policy,
            clock,
            timer.clone(),
            64,
            None,
            None,
            None,
        )
        .expect("actor");
        let pool = LocalPool::new();
        pool.spawner()
            .spawn(async move {
                let _ = actor.run().await;
            })
            .expect("spawn actor");
        Self {
            pool,
            handle,
            drive,
            log,
            timer,
            next_sequence: 0,
            committed: Vec::new(),
            in_flight: BTreeSet::new(),
            pending_gates: VecDeque::new(),
            pending_receipts: Vec::new(),
            collected: Vec::new(),
            dropped: BTreeSet::new(),
            blocked_flush: None,
            blocked_admits: Vec::new(),
            writes_at_poison: None,
        }
    }

    fn note_poison(&mut self) {
        if self.writes_at_poison.is_none() {
            self.writes_at_poison = Some(self.drive.write_count());
        }
    }

    fn metrics_ok(&self) -> Result<(), String> {
        let metrics = self.handle.metrics();
        if metrics.reserved_bytes > metrics.bytes_at_risk {
            return Err(format!(
                "reserved {} exceeds declared bytes_at_risk {}",
                metrics.reserved_bytes, metrics.bytes_at_risk
            ));
        }
        Ok(())
    }

    fn poll_async(&mut self) {
        self.pool.run_until_stalled();
    }

    fn release_all_gates(&mut self) {
        while let Some(gate) = self.pending_gates.pop_front() {
            gate.open();
        }
    }

    fn drain_blocked_flush(&mut self) -> Result<(), String> {
        if let Some(rx) = self.blocked_flush.take() {
            match self.pool.run_until(rx) {
                Ok(Ok(())) => {}
                Ok(Err(DriverError::Poisoned)) => self.note_poison(),
                Ok(Err(error)) => return Err(format!("flush failed: {error:?}")),
                Err(_) => return Err("flush channel dropped".into()),
            }
        }
        Ok(())
    }

    fn poll_blocked_admits(&mut self) -> Result<bool, String> {
        let mut remaining = Vec::new();
        let mut progressed = false;
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        for (sequence, rx) in std::mem::take(&mut self.blocked_admits) {
            let mut rx = rx;
            match Pin::new(&mut rx).poll(&mut context) {
                Poll::Ready(Ok(Ok(receipt_future))) => {
                    progressed = true;
                    self.in_flight.insert(sequence);
                    self.pending_receipts.push((sequence, receipt_future));
                }
                Poll::Ready(Ok(Err(DriverError::Poisoned))) => {
                    progressed = true;
                    self.note_poison();
                }
                Poll::Ready(Ok(Err(error))) if is_admission_reject(&error) => {
                    progressed = true;
                }
                Poll::Ready(Ok(Err(error))) => {
                    return Err(format!("admission failed: {error:?}"));
                }
                Poll::Ready(Err(_)) => return Err("admission channel dropped".into()),
                Poll::Pending => remaining.push((sequence, rx)),
            }
        }
        self.blocked_admits = remaining;
        Ok(progressed)
    }

    fn should_use_async_submit(&self) -> bool {
        !self.pending_gates.is_empty()
            || self.blocked_flush.is_some()
            || self.handle.metrics().reserved_bytes > 0
    }

    fn should_use_async_flush(&self) -> bool {
        !self.pending_gates.is_empty() || self.blocked_flush.is_some()
    }

    fn submit(&mut self, sequence: u64, records: u8) -> Result<(), String> {
        if self.should_use_async_submit() {
            self.submit_async(sequence, records);
        } else {
            self.submit_sync(sequence, records)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.blocked_flush.is_some() {
            self.poll_async();
            return Ok(());
        }
        if self.should_use_async_flush() {
            self.flush_async();
        } else {
            self.flush_sync()?;
            self.poll_async();
            for _ in 0..8 {
                if self.poll_blocked_admits()? && self.blocked_admits.is_empty() {
                    break;
                }
                self.poll_async();
            }
        }
        Ok(())
    }

    fn submit_sync(&mut self, sequence: u64, records: u8) -> Result<(), String> {
        let submission = submission(sequence, records);
        match self.pool.run_until(self.handle.submit(submission)) {
            Ok(receipt_future) => {
                self.in_flight.insert(sequence);
                self.pending_receipts.push((sequence, receipt_future));
            }
            Err(DriverError::Poisoned) => self.note_poison(),
            Err(error) if is_admission_reject(&error) => {}
            Err(error) => return Err(format!("submit rejected: {error:?}")),
        }
        Ok(())
    }

    fn submit_async(&mut self, sequence: u64, records: u8) {
        let submission = submission(sequence, records);
        let handle = self.handle.clone();
        let (tx, rx) = oneshot::channel();
        self.pool
            .spawner()
            .spawn(async move {
                let _ = tx.send(handle.submit(submission).await);
            })
            .expect("spawn submit");
        self.blocked_admits.push((sequence, rx));
    }

    fn flush_sync(&mut self) -> Result<(), String> {
        match self.pool.run_until(self.handle.flush()) {
            Ok(()) => {}
            Err(DriverError::Poisoned) => self.note_poison(),
            Err(error) => return Err(format!("flush failed: {error:?}")),
        }
        Ok(())
    }

    fn flush_async(&mut self) {
        let handle = self.handle.clone();
        let (tx, rx) = oneshot::channel();
        self.pool
            .spawner()
            .spawn(async move {
                let _ = tx.send(handle.flush().await);
            })
            .expect("spawn flush");
        self.blocked_flush = Some(rx);
    }

    fn poll_pending_receipts(&mut self) -> Result<bool, String> {
        let mut remaining = Vec::new();
        let mut progressed = false;
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        for (sequence, future) in std::mem::take(&mut self.pending_receipts) {
            let mut future = future;
            match Pin::new(&mut future).poll(&mut context) {
                Poll::Ready(Ok(receipt)) => {
                    progressed = true;
                    self.in_flight.remove(&sequence);
                    if !receipt.deduplicated && !self.committed.contains(&sequence) {
                        self.committed.push(sequence);
                    }
                    self.collected.push((sequence, receipt));
                }
                Poll::Ready(Err(DriverError::Uncertain { .. })) => {
                    progressed = true;
                    self.note_poison();
                }
                Poll::Ready(Err(DriverError::Poisoned)) => {
                    progressed = true;
                    self.note_poison();
                }
                Poll::Ready(Err(error)) => {
                    return Err(format!("receipt for seq {sequence}: {error:?}"));
                }
                Poll::Pending => remaining.push((sequence, future)),
            }
        }
        self.pending_receipts = remaining;
        Ok(progressed)
    }

    fn collect_pending_receipts(&mut self) -> Result<(), String> {
        for _ in 0..64 {
            let _ = self.poll_pending_receipts()?;
            if self.pending_receipts.is_empty() {
                return Ok(());
            }
            let _ = self.flush_sync();
            self.poll_async();
            self.drain_blocked_flush()?;
        }
        if !self.pending_receipts.is_empty() {
            return Err(format!(
                "{} receipts still pending at quiesce",
                self.pending_receipts.len()
            ));
        }
        Ok(())
    }

    fn step(&mut self, op: HostileOp) -> Result<(), String> {
        match op {
            HostileOp::SubmitNew(records) => {
                let sequence = self.next_sequence;
                self.next_sequence += 1;
                self.submit(sequence, records)?;
            }
            HostileOp::RetryCommitted => {
                if let Some(&sequence) = self.committed.last() {
                    self.submit(sequence, 1)?;
                }
            }
            HostileOp::RetryInFlight => {
                if let Some(&sequence) = self.in_flight.iter().next() {
                    self.submit(sequence, 1)?;
                }
            }
            HostileOp::Flush => {
                self.flush()?;
            }
            HostileOp::AdvanceAge(millis) => {
                self.timer.advance(Duration::from_millis(u64::from(millis)));
                self.poll_async();
            }
            HostileOp::GateNextWrite => {
                let next = self.drive.next_write_address(address);
                self.pending_gates.push_back(self.drive.gate_write(next));
            }
            HostileOp::ReleaseWrite => {
                if let Some(gate) = self.pending_gates.pop_front() {
                    gate.open();
                }
                self.poll_async();
                self.drain_blocked_flush()?;
                let _ = self.poll_blocked_admits()?;
            }
            HostileOp::DurableThenErrorNext => {
                let next = self.drive.next_write_address(address);
                self.drive.fail_after_durable_write(next);
            }
            HostileOp::FailBeforeNext => {
                let next = self.drive.next_write_address(address);
                self.drive.fail_before_write(next);
            }
            HostileOp::DropLastReceipt => {
                if let Some((sequence, _)) = self.pending_receipts.pop() {
                    self.dropped.insert(sequence);
                    self.in_flight.remove(&sequence);
                }
            }
            HostileOp::Quiesce => {
                self.release_all_gates();
                self.poll_async();
                self.drain_blocked_flush()?;
                for _ in 0..64 {
                    let _ = self.flush_sync();
                    self.poll_async();
                    self.drain_blocked_flush()?;
                    let _ = self.poll_blocked_admits()?;
                    let _ = self.poll_pending_receipts()?;
                    if self.blocked_admits.is_empty() && self.pending_receipts.is_empty() {
                        break;
                    }
                }
                if !self.blocked_admits.is_empty() {
                    return Err(format!(
                        "{} admissions still parked at quiesce",
                        self.blocked_admits.len()
                    ));
                }
                self.collect_pending_receipts()?;
                self.poll_async();
                if self.timer.sleeper_count() != 0 {
                    return Err(format!(
                        "ManualTimer still has {} sleepers after quiescence",
                        self.timer.sleeper_count()
                    ));
                }
            }
        }
        self.metrics_ok()
    }
}

fn submission(sequence: u64, records: u8) -> Submission {
    Submission {
        producer_id: producer(),
        producer_epoch: 1,
        sequence,
        records: (0..records).map(|value| record(i64::from(value))).collect(),
    }
}

fn check_durable_payloads(drive: &ScriptedLogDrive) -> Result<(), String> {
    for slot in 0..drive.write_count() {
        let addr = address(slot);
        if !drive.contains(addr) {
            continue;
        }
        let bytes = drive.read(addr).expect("durable payload");
        let chunk = decode_chunk(&bytes).map_err(|error| format!("decode slot {slot}: {error}"))?;
        if chunk.frames.len() != 1 {
            return Err(format!(
                "slot {slot}: expected one Phase-1 frame, got {}",
                chunk.frames.len()
            ));
        }
    }
    Ok(())
}

type SubmissionIdentity = (scripture::ProducerId, u32, u64);

fn check_recovery(
    recovery: &scripture::ChunkLogRecovery,
) -> Result<(u64, BTreeSet<SubmissionIdentity>), String> {
    let mut expected = 0_u64;
    let mut identities = BTreeSet::new();
    for chunk in &recovery.chunks {
        if chunk.first_offset != RecordOffset::new(expected) {
            return Err(format!(
                "non-dense recovery: expected offset {expected}, got {}",
                chunk.first_offset.get()
            ));
        }
        expected += u64::from(chunk.record_count);
        for submission in &chunk.frame.submissions {
            if !identities.insert((
                submission.producer_id,
                submission.producer_epoch,
                submission.sequence,
            )) {
                return Err(format!(
                    "duplicate identity {:?} in recovery",
                    (
                        submission.producer_id,
                        submission.producer_epoch,
                        submission.sequence
                    )
                ));
            }
        }
    }
    Ok((expected, identities))
}

/// Refinement mapping used by the schedule test:
///
/// - `SubmissionAdmitted` / `ChunkSealed` / `AppendIssued` / `AppendAcknowledged`
///   / `ReceiptReleased` / `WaiterFailed` / `OwnerPoisoned` are the shared
///   vocabulary with the pure model (`driver_model.rs`).
/// - We assert ordering and presence, not byte-for-byte trace equality.
fn receipt_follows_ack(ledger: &Ledger, receipt: &Receipt, sequence: u64) -> Result<(), String> {
    let mut ack_seen = false;
    let mut receipt_seen = false;
    for event in ledger.events() {
        if let Event::AppendAcknowledged { chunk_id, .. } = event
            && receipt.chunk_id == *chunk_id
        {
            ack_seen = true;
        }
        if let Event::ReceiptReleased {
            producer_id,
            producer_epoch,
            sequence: released,
            first_offset,
            records,
        } = event
            && *released == sequence
            && *producer_id == producer()
            && *producer_epoch == 1
            && *first_offset == receipt.first_offset
            && u64::from(*records) == receipt.next_offset.get() - receipt.first_offset.get()
        {
            if !ack_seen {
                return Err(format!(
                    "ReceiptReleased for seq {sequence} before AppendAcknowledged"
                ));
            }
            receipt_seen = true;
        }
    }
    if !receipt_seen {
        return Err(format!(
            "no ReceiptReleased ledger event for seq {sequence}"
        ));
    }
    Ok(())
}

fn check_poison_contract(harness: &RealActorHarness, ledger: &Ledger) -> Result<(), String> {
    let poison_index = ledger
        .events()
        .iter()
        .position(|event| matches!(event, Event::OwnerPoisoned));
    if let Some(index) = poison_index {
        let appends_after = ledger.events()[index + 1..]
            .iter()
            .filter(|event| matches!(event, Event::AppendIssued { .. }))
            .count();
        if appends_after != 0 {
            return Err(format!("AppendIssued after OwnerPoisoned: {appends_after}"));
        }
        if let Some(at_poison) = harness.writes_at_poison
            && harness.drive.write_count() != at_poison
        {
            return Err(format!(
                "write_count {} after poison boundary {} (expected no further writes)",
                harness.drive.write_count(),
                at_poison
            ));
        }
        for event in &ledger.events()[index..] {
            if let Event::WaiterFailed { outcome, .. } = event
                && !matches!(
                    outcome,
                    TerminalOutcome::Uncertain | TerminalOutcome::NotWritten
                )
            {
                return Err(format!(
                    "unexpected terminal outcome after poison: {outcome:?}"
                ));
            }
        }
    }
    Ok(())
}

fn waiter_outcome(ledger: &Ledger, sequence: u64) -> Option<TerminalOutcome> {
    ledger.events().iter().find_map(|event| {
        if let Event::WaiterFailed {
            producer_id,
            producer_epoch,
            sequence: failed,
            outcome,
        } = event
            && *producer_id == producer()
            && *producer_epoch == 1
            && *failed == sequence
        {
            Some(*outcome)
        } else {
            None
        }
    })
}

fn check_dropped_receipts(
    ledger: &Ledger,
    recovery_identities: &BTreeSet<SubmissionIdentity>,
    dropped: &BTreeSet<u64>,
) -> Result<(), String> {
    let poisoned = ledger
        .events()
        .iter()
        .any(|event| matches!(event, Event::OwnerPoisoned));

    for sequence in dropped {
        let identity = (producer(), 1, *sequence);
        let recovered = recovery_identities.contains(&identity);
        if poisoned {
            if recovered {
                continue;
            }
            match waiter_outcome(ledger, *sequence) {
                Some(TerminalOutcome::Uncertain | TerminalOutcome::NotWritten) => {}
                None => {
                    return Err(format!(
                        "dropped seq {sequence} neither recovered nor resolved with WaiterFailed"
                    ));
                }
            }
        } else if !recovered {
            return Err(format!(
                "dropped admitted seq {sequence} missing from recovery without poison"
            ));
        }
    }
    Ok(())
}

#[test]
fn hand_hostile_gate_flush_release_quiesces() {
    let mut harness = RealActorHarness::new();
    harness.step(HostileOp::SubmitNew(1)).expect("submit");
    harness.step(HostileOp::GateNextWrite).expect("gate");
    harness.step(HostileOp::Flush).expect("flush");
    harness.step(HostileOp::ReleaseWrite).expect("release");
    harness.step(HostileOp::Quiesce).expect("quiesce");
}

#[test]
fn dropped_receipt_future_still_leaves_durable_identity() {
    let mut harness = RealActorHarness::new();
    harness.step(HostileOp::SubmitNew(1)).expect("submit");
    assert!(harness.dropped.is_empty());
    assert_eq!(harness.pending_receipts.len(), 1);
    harness.step(HostileOp::DropLastReceipt).expect("drop");
    assert_eq!(harness.dropped, BTreeSet::from([0]));
    assert!(harness.pending_receipts.is_empty());
    harness.step(HostileOp::Quiesce).expect("quiesce");

    let recovery = harness
        .pool
        .run_until(ChunkLogWriter::recover(
            journal(),
            cohort(),
            1,
            harness.log.clone(),
            RecoveryBound::new(64).expect("bound"),
            None,
        ))
        .expect("recover");
    let (_, recovery_identities) = check_recovery(&recovery).expect("recovery");
    let identity = (producer(), 1, 0);
    assert!(
        recovery_identities.contains(&identity),
        "dropped receipt must not remove durable record"
    );
    let ledger = harness.handle.ledger();
    check_dropped_receipts(&ledger, &recovery_identities, &harness.dropped).expect("dropped");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn generated_hostile_real_actor_schedules_preserve_invariants(
        operations in prop::collection::vec(hostile_op_strategy(), 1..36),
    ) {
        let mut harness = RealActorHarness::new();
        for (step, op) in operations.iter().copied().enumerate() {
            if let Err(violation) = harness.step(op) {
                return Err(TestCaseError::fail(format!("step {step} ({op:?}): {violation}")));
            }
        }
        harness.step(HostileOp::Quiesce).map_err(|violation| {
            TestCaseError::fail(format!("final quiesce: {violation}"))
        })?;

        check_durable_payloads(&harness.drive).map_err(TestCaseError::fail)?;
        let recovery = harness.pool.run_until(ChunkLogWriter::recover(
            journal(),
            cohort(),
            1,
            harness.log.clone(),
            RecoveryBound::new(64).expect("bound"),
            None,
        )).expect("recover");
        let (record_ceiling, recovery_identities) =
            check_recovery(&recovery).map_err(TestCaseError::fail)?;

        let ledger = harness.handle.ledger();
        for (sequence, receipt) in &harness.collected {
            prop_assert_eq!(receipt.level, AckLevel::Committed);
            prop_assert!(receipt.next_offset.get() > receipt.first_offset.get());
            prop_assert!(receipt.first_offset.get() < record_ceiling);
            receipt_follows_ack(&ledger, receipt, *sequence).map_err(TestCaseError::fail)?;
            prop_assert!(recovery.chunks.iter().any(|chunk| chunk.slot == receipt.slot));
        }

        check_poison_contract(&harness, &ledger).map_err(TestCaseError::fail)?;
        check_dropped_receipts(&ledger, &recovery_identities, &harness.dropped)
            .map_err(TestCaseError::fail)?;
        prop_assert_eq!(
            ledger.logical_count(scripture::Effect::ChunkCommitted),
            recovery.chunks.len()
        );
    }
}
