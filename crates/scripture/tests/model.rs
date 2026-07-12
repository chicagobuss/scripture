//! Generated semantic schedules against the pure reference model.
//!
//! The nine invariants of scripture 0011, checked after *every* protocol step,
//! over schedules proptest generates and shrinks. Three of them are the ones a
//! plausible implementation will violate — loss beyond budget, excluded ≠
//! committed, and loss honesty — and none of the three can be reached by a test
//! a human wrote by hand, because all three depend on the interleaving.

#[path = "driver_model.rs"]
mod model;

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use scripture::{Effect, Event, ProducerId, RecordOffset, TerminalOutcome};

use model::{Decision, Policy, Submission, World};

fn producer(tag: u8) -> ProducerId {
    ProducerId::from_bytes([tag; 16])
}

/// Every invariant, checked after every step. A checker that asserts nothing
/// passes everything, so this is the actual deliverable — not the harness.
fn check_invariants(world: &World, policy: &Policy, new_events_from: usize) -> Result<(), String> {
    let visible = world.log.visible_submissions();

    // 1. Density: visible offsets form a gapless prefix from zero.
    let mut expected = 0_u64;
    for placed in &visible {
        if placed.first_offset.get() != expected {
            return Err(format!(
                "density: expected offset {expected}, found {} (producer {}, seq {})",
                placed.first_offset.get(),
                placed.submission.producer_id,
                placed.submission.sequence
            ));
        }
        expected += u64::from(placed.submission.records);
    }

    // 2. No duplicates: a (producer, epoch, sequence) appears at most once in
    //    the visible log.
    let mut seen = BTreeSet::new();
    for placed in &visible {
        let key = (
            placed.submission.producer_id,
            placed.submission.producer_epoch,
            placed.submission.sequence,
        );
        if !seen.insert(key) {
            return Err(format!(
                "duplicate: producer {} epoch {} sequence {} is committed twice",
                key.0, key.1, key.2
            ));
        }
    }

    // 3. Loss budget: bytes at risk never exceed the declared HARD bound.
    let at_risk = world.bytes_at_risk();
    if at_risk > policy.bytes_at_risk() {
        return Err(format!(
            "loss budget: {at_risk} bytes at risk exceeds the declared bound of {}",
            policy.bytes_at_risk()
        ));
    }

    // 4. Receipt soundness: every released receipt names records that are
    //    visible at exactly the offsets it claims.
    let by_offset: BTreeMap<u64, (ProducerId, u32, u64, u32)> = visible
        .iter()
        .map(|p| {
            (
                p.first_offset.get(),
                (
                    p.submission.producer_id,
                    p.submission.producer_epoch,
                    p.submission.sequence,
                    p.submission.records,
                ),
            )
        })
        .collect();
    for event in world.ledger.events() {
        if let Event::ReceiptReleased {
            producer_id,
            producer_epoch,
            sequence,
            first_offset,
            records,
        } = event
        {
            match by_offset.get(&first_offset.get()) {
                Some((pid, epoch, seq, count))
                    if pid == producer_id
                        && epoch == producer_epoch
                        && seq == sequence
                        && count == records => {}
                Some(other) => {
                    return Err(format!(
                        "receipt soundness: receipt for producer {producer_id} seq {sequence} \
                         claims offset {} but that offset holds {other:?}",
                        first_offset.get()
                    ));
                }
                None => {
                    return Err(format!(
                        "receipt soundness: receipt for producer {producer_id} seq {sequence} \
                         claims offset {} which is not visible in the log",
                        first_offset.get()
                    ));
                }
            }
        }
    }

    // 5. Per-producer order: committed sequences increase with offset.
    let mut last: BTreeMap<(ProducerId, u32), (u64, u64)> = BTreeMap::new();
    for placed in &visible {
        let key = (
            placed.submission.producer_id,
            placed.submission.producer_epoch,
        );
        if let Some((prev_seq, prev_offset)) = last.get(&key)
            && (placed.submission.sequence <= *prev_seq
                || placed.first_offset.get() <= *prev_offset)
        {
            return Err(format!(
                "per-producer order: producer {} epoch {} committed sequence {} at offset {} \
                 after sequence {prev_seq} at offset {prev_offset}",
                key.0,
                key.1,
                placed.submission.sequence,
                placed.first_offset.get()
            ));
        }
        last.insert(key, (placed.submission.sequence, placed.first_offset.get()));
    }

    // 7. Excluded is never committed.
    //
    //    Stated by CHUNK IDENTITY, not by offset — and the distinction is a
    //    finding, not a technicality. After a cutover, record offsets are
    //    *reused*: recovery rebuilds `next_offset` from committed bytes only, so
    //    the offsets an excluded chunk occupied are re-allocated to whatever
    //    commits next. A retry of an excluded submission therefore lands at the
    //    same offset its unmapped copy holds — legitimately, because the unmapped
    //    copy is unreachable. Only one copy is ever visible, which invariant 2
    //    enforces.
    //
    //    So the real claim is: a chunk the kernel excluded never becomes visible.
    let committed_ids: BTreeSet<_> = world
        .log
        .committed
        .iter()
        .map(|chunk| chunk.chunk_id)
        .collect();
    for excluded in &world.log.excluded {
        if committed_ids.contains(&excluded.chunk_id) {
            return Err(format!(
                "excluded-not-committed: chunk {} is unmapped by a cutover, yet it is visible \
                 in the log",
                excluded.chunk_id
            ));
        }
    }

    // 9. Loss honesty — and this is a TEMPORAL property, not a state invariant.
    //
    //    The naive version ("nothing reported NotWritten may be visible") is
    //    wrong, and the model proved it: a caller told NotWritten *retries*, and
    //    the retry legitimately commits. That is the whole point of telling it
    //    NotWritten rather than Uncertain.
    //
    //    The honest claim is about the instant: nothing reported NotWritten was
    //    in the log **at the moment it was reported**. So only events emitted by
    //    the step we just took are checked, against the log as it is now.
    for event in world.ledger.events().iter().skip(new_events_from) {
        if let Event::WaiterFailed {
            producer_id,
            producer_epoch,
            sequence,
            outcome: TerminalOutcome::NotWritten,
        } = event
        {
            // Identity is (producer, epoch, sequence). Epoch 0 seq 0 and epoch 1
            // seq 0 are DIFFERENT submissions, and conflating them made this
            // check report a false violation.
            let target = (*producer_id, *producer_epoch, *sequence);
            let is_visible = visible.iter().any(|p| {
                p.submission.producer_id == target.0
                    && p.submission.producer_epoch == target.1
                    && p.submission.sequence == target.2
            });
            if is_visible {
                return Err(format!(
                    "loss honesty: producer {producer_id} seq {sequence} was told NotWritten \
                     while it was committed and visible"
                ));
            }
        }
    }

    Ok(())
}

/// 6. No dangling waiter: every admitted submission eventually resolves. Checked
///    at the END of a history, because "eventually" is not a per-step property.
///
/// This is the closest a simulator can honestly get to the liveness claim in
/// scripture 0011 ("no caller may hang forever"). It shows that nothing hung in
/// the schedules we sampled. It is evidence, not proof — which is exactly why
/// the folio recommends a formal model for this one.
fn check_no_dangling_waiters(world: &World) -> Result<(), String> {
    for submission in &world.admitted {
        let key = (
            submission.producer_id,
            submission.producer_epoch,
            submission.sequence,
        );
        let resolved = world.ledger.events().iter().any(|event| match event {
            Event::ReceiptReleased {
                producer_id,
                producer_epoch,
                sequence,
                ..
            }
            | Event::WaiterFailed {
                producer_id,
                producer_epoch,
                sequence,
                ..
            } => *producer_id == key.0 && *producer_epoch == key.1 && *sequence == key.2,
            _ => false,
        });
        if !resolved {
            return Err(format!(
                "dangling waiter: producer {} epoch {} sequence {} was admitted and never \
                 resolved — the caller hangs forever",
                key.0, key.1, key.2
            ));
        }
    }
    Ok(())
}

/// The Phase-1 cost contract, as an invariant rather than an integration metric.
///
/// Cost is this family's differentiator, and no model checker can prove a
/// provider bill — but the *adapter* effects are ours to control, and they are
/// where the trim LIST-per-read defect lived. So they are asserted here.
fn check_cost_contract(world: &World) -> Result<(), String> {
    let commits = world.ledger.logical_count(Effect::ChunkCommitted);
    let puts = world.ledger.adapter_count(Effect::DataPut);

    // One PUT per append issued — never one per record. This is the economic
    // thesis of the whole layer, and it is checkable without a provider.
    let appends = world
        .ledger
        .count_events(|event| matches!(event, Event::AppendIssued { .. }));
    if puts != appends {
        return Err(format!(
            "cost: {puts} data PUTs for {appends} appends — a chunk must cost exactly one PUT"
        ));
    }
    if commits > appends {
        return Err(format!(
            "cost: {commits} chunks committed from only {appends} appends"
        ));
    }

    // A steady-state append path must never issue a tail scan. Scans belong to
    // recovery, and only to recovery.
    let recoveries = world
        .ledger
        .count_events(|event| matches!(event, Event::OwnerRecovered { .. }));
    let scans = world.ledger.adapter_count(Effect::TailScan);
    if scans > recoveries {
        return Err(format!(
            "cost: {scans} tail scans for {recoveries} recoveries — a warm path issued a LIST"
        ));
    }
    Ok(())
}

fn policy() -> Policy {
    Policy {
        max_chunk_records: 4,
        max_chunk_bytes: 100,
        max_record_bytes: 40,
        max_buffered_bytes: 100,
        max_inflight_chunks: 2,
        recovery_scan_chunks: 8,
    }
}

/// A workload of submissions from a few producers, with per-producer sequences
/// that mostly advance and sometimes repeat (a retry).
fn submission_strategy() -> impl Strategy<Value = (u8, u32, u64, u32, usize)> {
    (
        0_u8..3,     // producer tag
        0_u32..2,    // epoch
        0_u64..6,    // sequence
        1_u32..3,    // records
        1_usize..45, // bytes (some above max_record_bytes, to exercise rejection)
    )
}

fn decision_strategy() -> impl Strategy<Value = Decision> {
    prop_oneof![
        6 => submission_strategy().prop_map(|(p, e, s, r, b)| Decision::Submit(Submission {
            producer_id: producer(p),
            producer_epoch: e,
            sequence: s,
            records: r,
            bytes: b,
        })),
        3 => Just(Decision::IssueAppend),
        4 => Just(Decision::AppendAcknowledged),
        1 => Just(Decision::SealOpenChunk),
        1 => Just(Decision::AppendUncertain),
        1 => Just(Decision::AppendExcludedByCutover),
        1 => Just(Decision::OwnerCrashAndRecover),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// The whole ladder's first rung: generated semantic schedules, every
    /// invariant after every step, and proptest shrinking the decision vector to
    /// a minimal failing history when one breaks.
    #[test]
    fn generated_schedules_preserve_every_invariant(
        decisions in proptest::collection::vec(decision_strategy(), 1..40),
    ) {
        let policy = policy();
        let mut world = World::new(policy);

        for (step, decision) in decisions.iter().enumerate() {
            let events_before = world.ledger.events().len();
            world.step(*decision);

            if let Err(violation) = check_invariants(&world, &policy, events_before) {
                return Err(TestCaseError::fail(format!(
                    "after step {step} ({decision:?}): {violation}"
                )));
            }
            if let Err(violation) = check_cost_contract(&world) {
                return Err(TestCaseError::fail(format!(
                    "after step {step} ({decision:?}): {violation}"
                )));
            }
        }

        // Liveness, conditionally: give the driver a fair chance to finish its
        // work, then require that every admitted submission resolved. A schedule
        // that simply stopped mid-flight has not violated the contract.
        let events_before = world.ledger.events().len();
        world.quiesce();
        if let Err(violation) = check_invariants(&world, &policy, events_before) {
            return Err(TestCaseError::fail(format!("after quiescence: {violation}")));
        }
        if let Err(violation) = check_no_dangling_waiters(&world) {
            return Err(TestCaseError::fail(violation));
        }
    }
}

/// A hand-written scenario that pins the case the whole design turns on: an
/// append whose outcome is unknown, followed by recovery.
#[test]
fn an_uncertain_append_poisons_drains_and_recovers() {
    let policy = policy();
    let mut world = World::new(policy);

    let first = Submission {
        producer_id: producer(1),
        producer_epoch: 0,
        sequence: 0,
        records: 2,
        bytes: 10,
    };
    world.step(Decision::Submit(first));
    world.step(Decision::SealOpenChunk);
    world.step(Decision::IssueAppend);

    // A second submission is buffered behind the in-flight append.
    let second = Submission {
        producer_id: producer(1),
        producer_epoch: 0,
        sequence: 1,
        records: 1,
        bytes: 10,
    };
    world.step(Decision::Submit(second));

    // The append's outcome is unknown.
    world.step(Decision::AppendUncertain);

    // The in-flight chunk's submitter is told Uncertain — its record may or may
    // not be in the log, and only recovery can say.
    let uncertain = world.ledger.events().iter().any(|event| {
        matches!(
            event,
            Event::WaiterFailed {
                sequence: 0,
                outcome: TerminalOutcome::Uncertain,
                ..
            }
        )
    });
    assert!(uncertain, "the in-flight submitter must be told Uncertain");

    // The buffered submitter is told NotWritten — a DIFFERENT outcome, because
    // the driver KNOWS no append was issued for it. Collapsing the two would
    // force a caller to reconcile a record we know was never written.
    let not_written = world.ledger.events().iter().any(|event| {
        matches!(
            event,
            Event::WaiterFailed {
                sequence: 1,
                outcome: TerminalOutcome::NotWritten,
                ..
            }
        )
    });
    assert!(
        not_written,
        "a buffered submitter must be told NotWritten, not Uncertain"
    );

    // Nobody hangs.
    world.quiesce();
    check_no_dangling_waiters(&world).expect("every admitted submission resolved");
    check_invariants(&world, &policy, 0).expect("invariants hold through the poison drain");
}

/// A retry after a crash must be answered with the ORIGINAL offsets, not a
/// guess — which is only possible because the chunk stores each submission's
/// record span (decision 0009).
#[test]
fn a_retry_after_recovery_returns_the_original_offsets() {
    let policy = policy();
    let mut world = World::new(policy);

    let submission = Submission {
        producer_id: producer(1),
        producer_epoch: 0,
        sequence: 0,
        records: 3,
        bytes: 10,
    };
    world.step(Decision::Submit(submission));
    world.step(Decision::SealOpenChunk);
    world.step(Decision::IssueAppend);
    world.step(Decision::AppendAcknowledged);
    world.step(Decision::OwnerCrashAndRecover);

    // The producer never saw its receipt (the response was lost) and retries.
    world.step(Decision::Submit(submission));

    let deduped = world.ledger.events().iter().any(|event| {
        matches!(
            event,
            Event::SubmissionDeduplicated { sequence: 0, first_offset, .. }
                if *first_offset == RecordOffset::new(0)
        )
    });
    assert!(
        deduped,
        "the retry must be deduplicated against the rebuilt window and given offset 0"
    );

    let visible = world.log.visible_submissions();
    assert_eq!(
        visible.len(),
        1,
        "the retry must not create a second record"
    );
    check_invariants(&world, &policy, 0).expect("invariants hold across recovery and retry");
}
