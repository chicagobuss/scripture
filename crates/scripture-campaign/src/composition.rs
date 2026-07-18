//! Composition / AtomicLog / VirtualLog campaign scenarios (WP05 families 3–5, 7–11).

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use holylog::atomic::{
    AtomicLog, AtomicLogError, InMemorySeal, InMemorySequencer, InMemoryTrimPoint, Seal,
    SealStatus, Sequencer, TrimPoint,
};
use holylog::drive::LogDrive;
use holylog::logdrive::{Address, ReferenceLogDrive, TailDescription};
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{
    BindTag, InMemoryExclusiveClaimStore, LogletComponents, LogletObjectNamespaces,
    ProvisionAuthority, ProvisionerId, ResolvedLoglet, WritableLoglet,
};
use holylog::quorum::{QuorumError, QuorumLogDrive, ReplicaOrder};
use holylog::striped::StripedLogDrive;
use holylog::virtual_log::{
    ApplicationFence, InMemoryConditionalRegister, LogletId, LogletResolver,
    ReceiptReconfiguration, ResolveFuture, VirtualLog, VirtualLogError,
};
use holylog_correctness::{RecordingSink, RunId, Verdict};

use crate::scripted_drive::ScriptedDrive;
use crate::{CampaignError, CampaignReport, Scenario};

/// Runs a Holylog-layer composition/core scenario and packages a campaign report.
pub(crate) async fn run_composition(
    run_id: &str,
    scenario: Scenario,
) -> Result<CampaignReport, CampaignError> {
    let (events, final_root, oracle_ok, detail) = match scenario {
        Scenario::KWindowDelayedCompletion => k_window_delayed_completion(run_id).await?,
        Scenario::KWindowPermanentWedgeSeal => k_window_permanent_wedge_seal(run_id).await?,
        Scenario::PermanentWedgeSealSuccessor => permanent_wedge_seal_successor(run_id).await?,
        Scenario::SealTailRace => seal_tail_race(run_id).await?,
        Scenario::StripedModuloMapping => striped_modulo_mapping(run_id).await?,
        Scenario::StripedLaggingScanReconstruction => {
            striped_lagging_scan_reconstruction(run_id).await?
        }
        Scenario::QuorumPartialWriteNotGlobal => quorum_partial_write_not_global(run_id).await?,
        Scenario::QuorumRepairUnavailability => quorum_repair_unavailability(run_id).await?,
        Scenario::NestedStripeQuorumSchedules => nested_stripe_quorum_schedules(run_id).await?,
        other => {
            return Err(CampaignError::Scenario(format!(
                "not a composition scenario: {}",
                other.as_str()
            )));
        }
    };

    // Direct-oracle only: RecordingSink is not bridged into these schedules, so
    // check_trace on the (empty) event vector would be a vacuous pass. Verdict
    // follows the independent Rust oracle; the seeded negative checker test
    // below remains the checker control.
    let verdict = if oracle_ok {
        Verdict::Pass
    } else {
        Verdict::Fail {
            invariant: holylog_correctness::Invariant::UniqueCommittedOffset,
            evidence_slice: vec![detail.clone()],
        }
    };

    Ok(CampaignReport {
        run_id: run_id.to_owned(),
        scenario: scenario.as_str(),
        backend: "memory",
        environment: serde_json::json!({
            "run_id": run_id,
            "scenario": scenario.as_str(),
            "evidence_class": "direct-oracle-test",
            "backend": { "kind": "memory", "layer": "holylog-composition" },
            "oracle": detail,
            "trace_event_count": events.len(),
            "claims": [
                "deterministic Holylog AtomicLog/composition schedule judged by an independent ReferenceLogDrive / Rust oracle"
            ],
            "non_claims": [
                "not a Holylog semantic-trace / checker pass (sink not attached; events are not transition-point traces)",
                "not a Scripture HA/process-separated proof",
                "not an object-store or cloud-backend attestation"
            ],
        }),
        events,
        final_root,
        final_authority: serde_json::Value::Null,
        verdict,
    })
}

type ScenarioParts = (
    Vec<holylog_correctness::TraceEvent>,
    serde_json::Value,
    bool,
    String,
);

fn atomic_log(drive: Arc<dyn LogDrive>, k: u64) -> Result<AtomicLog, CampaignError> {
    AtomicLog::new(
        drive,
        Arc::new(InMemorySequencer::new(k)) as Arc<dyn Sequencer>,
        Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
        Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
        k,
    )
    .map_err(|error| CampaignError::Scenario(format!("atomic log: {error}")))
}

fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

async fn k_window_delayed_completion(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let drive = ScriptedDrive::available();
    let zero =
        Address::new(0).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    let one = Address::new(1).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    let two = Address::new(2).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    let log = atomic_log(Arc::clone(&drive) as Arc<dyn LogDrive>, 2)?;

    let gate0 = drive.gate_write(zero);
    let mut append0 = Box::pin(log.append(Bytes::from_static(b"zero")));
    if !poll(Pin::as_mut(&mut append0)).is_pending() {
        return Err(CampaignError::Scenario(
            "append0 should block on gate".into(),
        ));
    }

    drive.gate_write(one).open();
    let mut append1 = Box::pin(log.append(Bytes::from_static(b"one")));
    if !poll(Pin::as_mut(&mut append1)).is_pending() {
        return Err(CampaignError::Scenario(
            "append1 should await slot 0".into(),
        ));
    }

    let mut append2 = Box::pin(log.append(Bytes::from_static(b"two")));
    if !poll(Pin::as_mut(&mut append2)).is_pending() {
        return Err(CampaignError::Scenario(
            "append2 should block on K-window".into(),
        ));
    }

    gate0.open();
    match poll(Pin::as_mut(&mut append0)) {
        Poll::Ready(Ok(addr)) if addr == zero => {}
        other => {
            return Err(CampaignError::Scenario(format!(
                "append0 expected Ok(0), got {other:?}"
            )));
        }
    }
    match poll(Pin::as_mut(&mut append1)) {
        Poll::Ready(Ok(addr)) if addr == one => {}
        other => {
            return Err(CampaignError::Scenario(format!(
                "append1 expected Ok(1), got {other:?}"
            )));
        }
    }

    drive.gate_write(two).open();
    match poll(Pin::as_mut(&mut append2)) {
        Poll::Ready(Ok(addr)) if addr == two => {}
        other => {
            return Err(CampaignError::Scenario(format!(
                "append2 expected Ok(2), got {other:?}"
            )));
        }
    }

    Ok((
        sink.events(),
        serde_json::json!({ "layer": "atomic", "k": 2, "completed_tail": 3 }),
        true,
        "k-window delayed completion unblocked after slot 0 finished".into(),
    ))
}

async fn k_window_permanent_wedge_seal(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let drive = ScriptedDrive::available();
    let zero =
        Address::new(0).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    let log = atomic_log(Arc::clone(&drive) as Arc<dyn LogDrive>, 2)?;

    drive.fail_after_write(zero);
    let gate0 = drive.gate_write(zero);
    let mut append0 = Box::pin(log.append(Bytes::from_static(b"zero")));
    if !poll(Pin::as_mut(&mut append0)).is_pending() {
        return Err(CampaignError::Scenario("append0 should block".into()));
    }

    let mut append1 = Box::pin(log.append(Bytes::from_static(b"one")));
    if !poll(Pin::as_mut(&mut append1)).is_pending() {
        return Err(CampaignError::Scenario(
            "append1 should await completion frontier".into(),
        ));
    }

    let mut append2 = Box::pin(log.append(Bytes::from_static(b"two")));
    if !poll(Pin::as_mut(&mut append2)).is_pending() {
        return Err(CampaignError::Scenario(
            "append2 should block on full K-window".into(),
        ));
    }

    gate0.open();
    match (&mut append0).await {
        Err(AtomicLogError::Drive(_)) => {}
        other => {
            return Err(CampaignError::Scenario(format!(
                "append0 expected drive error after durable payload, got {other:?}"
            )));
        }
    }
    if !poll(Pin::as_mut(&mut append2)).is_pending() {
        return Err(CampaignError::Scenario(
            "append2 must remain blocked while slot 0 is uncompleted".into(),
        ));
    }
    drop(append2);

    log.seal()
        .await
        .map_err(|error| CampaignError::Scenario(format!("seal: {error}")))?;
    let checked = log
        .check_tail()
        .await
        .map_err(|error| CampaignError::Scenario(format!("check_tail: {error}")))?;
    if checked.tail != 2 || checked.seal_status != SealStatus::Sealed {
        return Err(CampaignError::Scenario(format!(
            "expected sealed boundary 2, got tail={} status={:?}",
            checked.tail, checked.seal_status
        )));
    }
    if !drive.contains(zero) {
        return Err(CampaignError::Scenario(
            "payload 0 must remain durable after post-write failure".into(),
        ));
    }

    Ok((
        sink.events(),
        serde_json::json!({
            "layer": "atomic",
            "sealed_boundary": checked.tail,
            "seal_status": "sealed",
            "note": "successor publication is owned above AtomicLog; this row proves seal boundary only"
        }),
        true,
        "permanent K-window wedge sealed at physical boundary 2".into(),
    ))
}

async fn striped_modulo_mapping(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let stripes: Vec<Arc<dyn LogDrive>> = (0..3)
        .map(|_| Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>)
        .collect();
    let striped = StripedLogDrive::new(stripes)
        .map_err(|error| CampaignError::Scenario(format!("striped: {error}")))?;
    let mut reference = ReferenceLogDrive::new();

    for index in 0..6u64 {
        let address = Address::new(index)
            .map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
        let payload = Bytes::from(format!("stripe-{index}"));
        striped
            .write(address, payload.clone())
            .await
            .map_err(|error| CampaignError::Scenario(format!("striped write: {error}")))?;
        reference
            .write(address, payload)
            .map_err(|error| CampaignError::Scenario(format!("reference write: {error}")))?;
    }

    for index in 0..6u64 {
        let address = Address::new(index)
            .map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
        let got = striped
            .read(address)
            .await
            .map_err(|error| CampaignError::Scenario(format!("striped read: {error}")))?;
        let expected = reference.read(address).cloned();
        if got != expected {
            return Err(CampaignError::Scenario(format!(
                "stripe/reference mismatch at {index}: {got:?} vs {expected:?}"
            )));
        }
    }

    let striped_tail = striped
        .weak_tail(0)
        .await
        .map_err(|error| CampaignError::Scenario(format!("striped tail: {error}")))?;
    let reference_tail = reference.weak_tail(0);
    if striped_tail.non_contiguous_tail() != reference_tail.non_contiguous_tail()
        || striped_tail.holes() != reference_tail.holes()
    {
        return Err(CampaignError::Scenario(format!(
            "tail mismatch striped={striped_tail:?} reference={reference_tail:?}"
        )));
    }

    Ok((
        sink.events(),
        serde_json::json!({
            "layer": "striped",
            "stripes": 3,
            "addresses": 6,
            "non_contiguous_tail": striped_tail.non_contiguous_tail()
        }),
        true,
        "striped modulo mapping matched ReferenceLogDrive for writes/reads/tail".into(),
    ))
}

async fn quorum_partial_write_not_global(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let drives = [
        ScriptedDrive::available(),
        ScriptedDrive::available(),
        ScriptedDrive::available(),
    ];
    let address =
        Address::new(0).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;

    // Seed only replica 0 — below write quorum, so not globally written.
    drives[0]
        .write(address, Bytes::from_static(b"partial"))
        .await
        .map_err(|error| CampaignError::Scenario(format!("seed partial: {error}")))?;

    // Prefer replicas 1 and 2 first so the tail quorum never sees the lone write.
    let order = ReplicaOrder::new(vec![1, 2, 0], 3)
        .map_err(|error| CampaignError::Scenario(format!("replica order: {error}")))?;
    let replicas: Vec<Arc<dyn LogDrive>> = drives
        .iter()
        .cloned()
        .map(|drive| drive as Arc<dyn LogDrive>)
        .collect();
    let quorum = QuorumLogDrive::with_order(replicas, 2, order)
        .map_err(|error| CampaignError::Scenario(format!("quorum: {error}")))?;
    let tail = quorum
        .weak_tail(1)
        .await
        .map_err(|error| CampaignError::Scenario(format!("weak_tail: {error}")))?;
    if tail.non_contiguous_tail() != 0 || tail.contiguous_tail() != 0 || !tail.holes().is_empty() {
        return Err(CampaignError::Scenario(format!(
            "partial single-replica write must not advance global tail; got {tail:?}"
        )));
    }

    Ok((
        sink.events(),
        serde_json::json!({
            "layer": "quorum",
            "n": 3,
            "qw": 2,
            "seeded_replicas": 1,
            "observed_non_contiguous_tail": tail.non_contiguous_tail(),
            "claim": "one replica is not globally written"
        }),
        true,
        "quorum partial write is not globally written under Qf responders without the copy".into(),
    ))
}

async fn permanent_wedge_seal_successor(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let fleet = CompositionFleet::new("family4-wedge-successor");
    let first = loglet("wedge-pred")?;
    let second = loglet("wedge-succ")?;
    let zero =
        Address::new(0).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    let one = Address::new(1).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;

    let drive0 = fleet.provision_scripted(&first, 2).await?;
    let log = fleet.virtual_log();
    fleet
        .bootstrap(&log, &first, ApplicationFence::default())
        .await?;

    drive0.fail_after_write(zero);
    let gate0 = drive0.gate_write(zero);
    let mut first_append = Box::pin(log.append(Bytes::from_static(b"wedge-zero")));
    if !poll(Pin::as_mut(&mut first_append)).is_pending() {
        return Err(CampaignError::Scenario(
            "first append should block on write gate".into(),
        ));
    }
    let mut second_append = Box::pin(log.append(Bytes::from_static(b"wedge-one")));
    if !poll(Pin::as_mut(&mut second_append)).is_pending() {
        return Err(CampaignError::Scenario(
            "second append should await completion frontier".into(),
        ));
    }
    gate0.open();
    match first_append.await {
        Err(VirtualLogError::Atomic(AtomicLogError::Drive(_))) => {}
        other => {
            return Err(CampaignError::Scenario(format!(
                "expected durable-then-fail on first append, got {other:?}"
            )));
        }
    }
    if !drive0.written_addresses().contains(&zero) || !drive0.written_addresses().contains(&one) {
        return Err(CampaignError::Scenario(
            "both payloads must be durable before successor cutover".into(),
        ));
    }
    if !poll(Pin::as_mut(&mut second_append)).is_pending() {
        return Err(CampaignError::Scenario(
            "second append must remain unacknowledged behind the hole".into(),
        ));
    }
    drop(second_append);

    let _ = fleet.provision_scripted(&second, 2).await?;
    let cutover = fleet.preserve(&log, &second).await?;
    match cutover {
        ReceiptReconfiguration::Applied { boundary: 2, .. } => {}
        other => {
            return Err(CampaignError::Scenario(format!(
                "expected Applied boundary 2, got {other:?}"
            )));
        }
    }

    let after = log
        .append(Bytes::from_static(b"after-recovery"))
        .await
        .map_err(|error| CampaignError::Scenario(format!("successor append: {error}")))?;
    if after.position != 2 || after.loglet_id != second {
        return Err(CampaignError::Scenario(format!(
            "successor must start at logical 2 on {:?}, got position={} loglet={:?}",
            second, after.position, after.loglet_id
        )));
    }

    Ok((
        sink.events(),
        serde_json::json!({
            "layer": "virtual_log",
            "sealed_boundary": 2,
            "successor_first_position": after.position,
            "predecessor": first.as_str(),
            "successor": second.as_str()
        }),
        true,
        "wedged K-window sealed at boundary 2; VirtualLog successor continues at logical 2".into(),
    ))
}

async fn seal_tail_race(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let drive = ScriptedDrive::available();
    let zero =
        Address::new(0).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    let one = Address::new(1).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    let log = atomic_log(Arc::clone(&drive) as Arc<dyn LogDrive>, 2)?;

    drive.gate_write(zero).open();
    drive.gate_write(one).open();
    log.append(Bytes::from_static(b"zero"))
        .await
        .map_err(|error| CampaignError::Scenario(format!("append0: {error}")))?;
    log.append(Bytes::from_static(b"one"))
        .await
        .map_err(|error| CampaignError::Scenario(format!("append1: {error}")))?;

    let open_tail = log
        .check_tail()
        .await
        .map_err(|error| CampaignError::Scenario(format!("pre-seal check_tail: {error}")))?;
    if open_tail.seal_status != SealStatus::Open {
        return Err(CampaignError::Scenario(format!(
            "pre-seal tail must be Open, got {open_tail:?}"
        )));
    }

    log.seal()
        .await
        .map_err(|error| CampaignError::Scenario(format!("seal: {error}")))?;

    // Dual post-seal observers (deterministic "fast" then "slow") must agree on
    // Sealed boundary 2 and must not regress to Open or a smaller sealed tail.
    let fast = log
        .check_tail()
        .await
        .map_err(|error| CampaignError::Scenario(format!("fast check_tail: {error}")))?;
    let slow = log
        .check_tail()
        .await
        .map_err(|error| CampaignError::Scenario(format!("slow check_tail: {error}")))?;
    for (label, tail) in [("fast", &fast), ("slow", &slow)] {
        if tail.seal_status != SealStatus::Sealed || tail.tail != 2 {
            return Err(CampaignError::Scenario(format!(
                "{label} observer must see sealed boundary 2, got {tail:?}"
            )));
        }
    }
    if slow.tail < fast.tail {
        return Err(CampaignError::Scenario(
            "slow sealed observation must not regress below the fast sealed tail".into(),
        ));
    }
    match log.append(Bytes::from_static(b"after-seal")).await {
        Err(_) => {}
        Ok(_) => {
            return Err(CampaignError::Scenario(
                "append after seal must fail closed".into(),
            ));
        }
    }
    let after_denied = log
        .check_tail()
        .await
        .map_err(|error| CampaignError::Scenario(format!("post-deny check_tail: {error}")))?;
    if after_denied.seal_status != SealStatus::Sealed {
        return Err(CampaignError::Scenario(format!(
            "tail must remain Sealed after denied append, got {after_denied:?}"
        )));
    }
    if after_denied.tail < 2 {
        return Err(CampaignError::Scenario(format!(
            "sealed tail must not regress below boundary 2 after denied append, got {after_denied:?}"
        )));
    }

    Ok((
        sink.events(),
        serde_json::json!({
            "layer": "atomic",
            "pre_seal_status": "open",
            "fast_tail": fast.tail,
            "slow_tail": slow.tail,
            "post_deny_tail": after_denied.tail,
            "seal_status": "sealed",
            "schedule": "open-observe → seal → dual sealed observe → denied append → sealed re-observe",
            "note": "denied append may leave a durable zombie that advances sealed tail; Open regression is forbidden"
        }),
        true,
        "seal/check-tail schedule: dual sealed observers agree at boundary 2; denied append cannot reopen the log".into(),
    ))
}

async fn striped_lagging_scan_reconstruction(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    // Deterministic schedule: 2 stripes, initial {0,1}, advance stripe 0
    // before its snapshot, release order [1, 0], K=2.
    let stripe_count = 2_u64;
    let k = 2_u64;
    let initial: BTreeSet<u64> = [0, 1].into_iter().collect();
    let advance_stripe0 = 2_u64; // next local*stripes+stripe for stripe 0

    let drives = [ScriptedDrive::available(), ScriptedDrive::available()];
    for raw in &initial {
        let stripe = (*raw % stripe_count) as usize;
        let local = *raw / stripe_count;
        let address = Address::new(local)
            .map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
        drives[stripe]
            .write(address, Bytes::copy_from_slice(&raw.to_be_bytes()))
            .await
            .map_err(|error| CampaignError::Scenario(format!("seed: {error}")))?;
    }

    let gates = [
        drives[0].gate_next_tail_scan(),
        drives[1].gate_next_tail_scan(),
    ];
    let striped = StripedLogDrive::new(
        drives
            .iter()
            .cloned()
            .map(|drive| drive as Arc<dyn LogDrive>)
            .collect(),
    )
    .map_err(|error| CampaignError::Scenario(format!("striped: {error}")))?;

    let mut pending = Box::pin(striped.weak_tail(k));
    if !poll(Pin::as_mut(&mut pending)).is_pending() {
        return Err(CampaignError::Scenario(
            "composed scan should await gated stripe snapshots".into(),
        ));
    }

    // Stripe 0 advances before its snapshot; stripe 1 does not.
    let advance_addr = Address::new(advance_stripe0 / stripe_count)
        .map_err(|error| CampaignError::Scenario(format!("advance addr: {error}")))?;
    drives[0]
        .write(
            advance_addr,
            Bytes::copy_from_slice(&advance_stripe0.to_be_bytes()),
        )
        .await
        .map_err(|error| CampaignError::Scenario(format!("advance before scan: {error}")))?;

    // Release order: stripe 1 then stripe 0 (lagging release of the advanced stripe).
    gates[1].open();
    if !poll(Pin::as_mut(&mut pending)).is_pending() {
        return Err(CampaignError::Scenario(
            "scan must remain pending until all stripe gates open".into(),
        ));
    }
    gates[0].open();
    let observed = pending
        .await
        .map_err(|error| CampaignError::Scenario(format!("composed weak_tail: {error}")))?;

    let claims = globalize_scan_claims(&drives, stripe_count);
    let oracle = oracle_from_scan_claims(claims.clone(), k);
    if observed.non_contiguous_tail() != oracle.non_contiguous_tail()
        || observed.contiguous_tail() != oracle.contiguous_tail()
        || observed.holes() != oracle.holes()
    {
        return Err(CampaignError::Scenario(format!(
            "lagging-scan oracle mismatch observed={observed:?} oracle={oracle:?}"
        )));
    }

    Ok((
        sink.events(),
        serde_json::json!({
            "layer": "striped",
            "schedule": {
                "stripes": stripe_count,
                "initial": [0, 1],
                "advance_before_scan": [advance_stripe0],
                "release_order": [1, 0],
                "k": k
            },
            "observed_non_contiguous_tail": observed.non_contiguous_tail(),
            "oracle_non_contiguous_tail": oracle.non_contiguous_tail(),
            "scan_claims": claims.iter().map(|a| a.get()).collect::<Vec<_>>()
        }),
        true,
        "striped lagging-scan matched independent scan-claim ReferenceLogDrive oracle".into(),
    ))
}

async fn quorum_repair_unavailability(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let address =
        Address::new(0).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    let drives = [
        ScriptedDrive::available(),
        ScriptedDrive::available(),
        ScriptedDrive::available(),
    ];
    drives[0]
        .write(address, Bytes::from_static(b"partial"))
        .await
        .map_err(|error| CampaignError::Scenario(format!("seed: {error}")))?;
    drives[1].fail_write(address);
    drives[2].set_available(false);

    let replicas: Vec<Arc<dyn LogDrive>> = drives
        .iter()
        .cloned()
        .map(|drive| drive as Arc<dyn LogDrive>)
        .collect();
    let quorum = QuorumLogDrive::new(replicas, 2)
        .map_err(|error| CampaignError::Scenario(format!("quorum: {error}")))?;
    match quorum.weak_tail(1).await {
        Err(QuorumError::Unavailable {
            operation: "repair write",
            ..
        }) => {}
        other => {
            return Err(CampaignError::Scenario(format!(
                "expected repair-write unavailability, got {other:?}"
            )));
        }
    }

    Ok((
        sink.events(),
        serde_json::json!({
            "layer": "quorum",
            "n": 3,
            "qw": 2,
            "outcome": "unavailable",
            "operation": "repair write",
            "claim": "repair failure is never a successful tail"
        }),
        true,
        "quorum repair unavailability fails closed without inventing a successful tail".into(),
    ))
}

async fn nested_stripe_quorum_schedules(run_id: &str) -> Result<ScenarioParts, CampaignError> {
    let _run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    // Nested: striped over two quorum stripes (N=3,Qw=2 each). Seed a partial
    // replica write on stripe 0 that must not become a global claim under a
    // responder order that never queries the seeded replica first.
    let make_quorum =
        |prefer_without_seed: bool| -> Result<(QuorumLogDrive, [Arc<ScriptedDrive>; 3]), CampaignError> {
            let drives = [
                ScriptedDrive::available(),
                ScriptedDrive::available(),
                ScriptedDrive::available(),
            ];
            let replicas: Vec<Arc<dyn LogDrive>> = drives
                .iter()
                .cloned()
                .map(|drive| drive as Arc<dyn LogDrive>)
                .collect();
            let quorum = if prefer_without_seed {
                let order = ReplicaOrder::new(vec![1, 2, 0], 3)
                    .map_err(|error| CampaignError::Scenario(format!("replica order: {error}")))?;
                QuorumLogDrive::with_order(replicas, 2, order)
                    .map_err(|error| CampaignError::Scenario(format!("quorum: {error}")))?
            } else {
                QuorumLogDrive::new(replicas, 2)
                    .map_err(|error| CampaignError::Scenario(format!("quorum: {error}")))?
            };
            Ok((quorum, drives))
        };
    let (quorum0, drives0) = make_quorum(true)?;
    let (quorum1, _drives1) = make_quorum(false)?;

    let local0 =
        Address::new(0).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    quorum0
        .write(local0, Bytes::from_static(b"g0"))
        .await
        .map_err(|error| CampaignError::Scenario(format!("quorum0 write: {error}")))?;
    quorum1
        .write(local0, Bytes::from_static(b"g1"))
        .await
        .map_err(|error| CampaignError::Scenario(format!("quorum1 write: {error}")))?;

    let local1 =
        Address::new(1).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?;
    drives0[0]
        .write(local1, Bytes::from_static(b"partial-g2"))
        .await
        .map_err(|error| CampaignError::Scenario(format!("partial seed: {error}")))?;

    let striped = StripedLogDrive::new(vec![
        Arc::new(quorum0) as Arc<dyn LogDrive>,
        Arc::new(quorum1) as Arc<dyn LogDrive>,
    ])
    .map_err(|error| CampaignError::Scenario(format!("striped: {error}")))?;

    let mut reference = ReferenceLogDrive::new();
    reference
        .write(
            Address::new(0).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?,
            Bytes::from_static(b"g0"),
        )
        .map_err(|error| CampaignError::Scenario(format!("ref: {error}")))?;
    reference
        .write(
            Address::new(1).map_err(|error| CampaignError::Scenario(format!("addr: {error}")))?,
            Bytes::from_static(b"g1"),
        )
        .map_err(|error| CampaignError::Scenario(format!("ref: {error}")))?;

    let observed = striped
        .weak_tail(2)
        .await
        .map_err(|error| CampaignError::Scenario(format!("nested weak_tail: {error}")))?;
    let oracle = reference.weak_tail(2);
    if observed.non_contiguous_tail() != oracle.non_contiguous_tail()
        || observed.contiguous_tail() != oracle.contiguous_tail()
        || observed.holes() != oracle.holes()
    {
        return Err(CampaignError::Scenario(format!(
            "nested stripe/quorum mismatch observed={observed:?} oracle={oracle:?}"
        )));
    }
    if observed.non_contiguous_tail() >= 3 {
        return Err(CampaignError::Scenario(
            "partial quorum replica must not advance nested global tail to include global 2".into(),
        ));
    }

    Ok((
        sink.events(),
        serde_json::json!({
            "layer": "striped-over-quorum",
            "stripes": 2,
            "quorum": { "n": 3, "qw": 2 },
            "durable_globals": [0, 1],
            "partial_global_candidate": 2,
            "responder_order_stripe0": [1, 2, 0],
            "observed_non_contiguous_tail": observed.non_contiguous_tail()
        }),
        true,
        "nested striped-over-quorum schedule matched reference; partial replica is not global"
            .into(),
    ))
}

fn globalize_scan_claims(
    stripes: &[Arc<ScriptedDrive>; 2],
    stripe_count: u64,
) -> BTreeSet<Address> {
    let mut observed = BTreeSet::new();
    for (stripe, drive) in stripes.iter().enumerate() {
        for local in drive.last_tail_scan_written() {
            if let Some(global) = local
                .get()
                .checked_mul(stripe_count)
                .and_then(|base| base.checked_add(stripe as u64))
                && let Ok(address) = Address::new(global)
            {
                observed.insert(address);
            }
        }
    }
    observed
}

fn oracle_from_scan_claims(claims: BTreeSet<Address>, k: u64) -> TailDescription {
    let mut model = ReferenceLogDrive::new();
    for address in claims {
        let _ = model.write(address, Bytes::new());
    }
    model.weak_tail(k)
}

fn loglet(name: &str) -> Result<LogletId, CampaignError> {
    LogletId::new(name).map_err(|error| CampaignError::Scenario(format!("loglet id: {error}")))
}

struct CompositionFleet {
    authority: ProvisionAuthority,
    resolver: Arc<CompositionResolver>,
    receipts: std::sync::Mutex<
        std::collections::BTreeMap<LogletId, holylog::provision::FreshWritableProvisionReceipt>,
    >,
    writables: std::sync::Mutex<std::collections::BTreeMap<LogletId, Arc<WritableLoglet>>>,
    register: Arc<InMemoryConditionalRegister>,
}

struct CompositionResolver {
    loglets: std::sync::Mutex<std::collections::BTreeMap<LogletId, ResolvedLoglet>>,
}

impl CompositionResolver {
    fn insert(&self, id: LogletId, loglet: ResolvedLoglet) {
        self.loglets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, loglet);
    }
}

impl LogletResolver for CompositionResolver {
    fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<ResolvedLoglet>> {
        let id = id.clone();
        Box::pin(async move {
            Ok(self
                .loglets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&id)
                .cloned())
        })
    }
}

impl CompositionFleet {
    fn new(provisioner: &str) -> Self {
        let resolver = Arc::new(CompositionResolver {
            loglets: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        });
        Self {
            authority: ProvisionAuthority::new(
                Arc::new(InMemoryExclusiveClaimStore::new()),
                ProvisionerId::new(provisioner),
            ),
            resolver,
            receipts: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            writables: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            register: Arc::new(InMemoryConditionalRegister::new()),
        }
    }

    fn virtual_log(&self) -> VirtualLog {
        VirtualLog::new(
            Arc::clone(&self.register) as Arc<_>,
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        )
    }

    fn bind(id: &LogletId) -> BindTag {
        BindTag::new(id.as_str().as_bytes().to_vec())
    }

    async fn provision_scripted(
        &self,
        id: &LogletId,
        k: u64,
    ) -> Result<Arc<ScriptedDrive>, CampaignError> {
        let drive = ScriptedDrive::available();
        let (receipt, writable) = self
            .authority
            .provision_fresh(
                id.clone(),
                LogletObjectNamespaces::under_root("campaign-composition", id),
                Self::bind(id),
                LogletComponents::new(
                    Arc::clone(&drive) as Arc<dyn LogDrive>,
                    Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
                    Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
                    k,
                ),
            )
            .await
            .map_err(|error| CampaignError::Scenario(format!("provision: {error}")))?;
        let writable = Arc::new(writable);
        self.resolver
            .insert(id.clone(), ResolvedLoglet::Writable(Arc::clone(&writable)));
        self.writables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id.clone(), writable);
        self.receipts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id.clone(), receipt);
        Ok(drive)
    }

    async fn bootstrap(
        &self,
        log: &VirtualLog,
        id: &LogletId,
        fence: ApplicationFence,
    ) -> Result<(), CampaignError> {
        let receipt = self
            .receipts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(id)
            .ok_or_else(|| CampaignError::Scenario("missing provision receipt".into()))?;
        let writable = self
            .writables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(id)
            .cloned()
            .ok_or_else(|| CampaignError::Scenario("missing writable".into()))?;
        log.bootstrap_with_receipt(receipt, writable.as_ref(), &Self::bind(id), fence)
            .await
            .map_err(|error| CampaignError::Scenario(format!("bootstrap: {error}")))
    }

    async fn preserve(
        &self,
        log: &VirtualLog,
        successor: &LogletId,
    ) -> Result<ReceiptReconfiguration, CampaignError> {
        let observed = log
            .observe_membership()
            .await
            .map_err(|error| CampaignError::Scenario(format!("observe: {error}")))?;
        let receipt = self
            .receipts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(successor)
            .ok_or_else(|| CampaignError::Scenario("missing successor receipt".into()))?;
        let writable = self
            .writables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(successor)
            .cloned()
            .ok_or_else(|| CampaignError::Scenario("missing successor writable".into()))?;
        log.reconfigure_with_receipt(
            &observed,
            receipt,
            writable.as_ref(),
            &Self::bind(successor),
            observed.state.application_fence.clone(),
        )
        .await
        .map_err(|error| CampaignError::Scenario(format!("preserve/reconfigure: {error}")))
    }
}

/// Seeded negative control: checker must reject a duplicated conflicting ACK.
#[cfg(test)]
mod tests {
    use holylog_correctness::{
        ActorId, EventKind, RecordingSink, RunId, TraceEvent, TraceSink, check_trace,
    };

    #[test]
    fn negative_trace_is_rejected_by_checker() {
        let sink = RecordingSink::new().shared();
        let run = RunId::new("neg");
        let actor = holylog_correctness::ActorTrace::new(
            run,
            ActorId::new("a"),
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn TraceSink>,
        );
        actor.emit(
            None,
            EventKind::ScriptureCommittedAck {
                logical_offset: 0,
                digest: "aaa".into(),
                size: 1,
                loglet_id: "g0".into(),
            },
        );
        actor.emit(
            None,
            EventKind::ScriptureCommittedAck {
                logical_offset: 0,
                digest: "bbb".into(),
                size: 1,
                loglet_id: "g0".into(),
            },
        );
        let events: Vec<TraceEvent> = sink.events();
        assert!(matches!(
            check_trace(&events),
            holylog_correctness::Verdict::Fail { .. }
        ));
    }
}
