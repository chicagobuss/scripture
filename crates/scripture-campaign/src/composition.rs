//! Composition / AtomicLog campaign scenarios (WP05 families 3–4, 7, 9).

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
use holylog::logdrive::{Address, ReferenceLogDrive};
use holylog::memory::InMemoryLogDrive;
use holylog::quorum::{QuorumLogDrive, ReplicaOrder};
use holylog::striped::StripedLogDrive;
use holylog_correctness::{RecordingSink, RunId, Verdict, check_trace};

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
        Scenario::StripedModuloMapping => striped_modulo_mapping(run_id).await?,
        Scenario::QuorumPartialWriteNotGlobal => quorum_partial_write_not_global(run_id).await?,
        other => {
            return Err(CampaignError::Scenario(format!(
                "not a composition scenario: {}",
                other.as_str()
            )));
        }
    };

    let mut verdict = check_trace(&events);
    if !oracle_ok {
        verdict = Verdict::Fail {
            invariant: holylog_correctness::Invariant::UniqueCommittedOffset,
            evidence_slice: vec![detail.clone()],
        };
    }

    Ok(CampaignReport {
        run_id: run_id.to_owned(),
        scenario: scenario.as_str(),
        backend: "memory",
        environment: serde_json::json!({
            "run_id": run_id,
            "scenario": scenario.as_str(),
            "backend": { "kind": "memory", "layer": "holylog-composition" },
            "oracle": detail,
            "claims": [
                "exercises Holylog AtomicLog/composition adapters with an independent ReferenceLogDrive oracle where applicable"
            ],
            "non_claims": [
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
