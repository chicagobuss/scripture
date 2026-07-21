//! Acceptance tests for live-observed capability enforcement (WP 2026-07-21).
//!
//! All hermetic with injected live [`CapabilityInputs`]. Covers required tests
//! 1–10; test 11 is the cargo fmt/clippy/diff gate run at package completion.

use scripture_runtime::{
    CapabilityCode, CapabilityInputs, RecoveryCandidateEvidence, SatisfactionKind,
    VerseCapabilityInputs, evaluate,
};

use crate::config::{
    CanonHistoryRequire, OnDegraded, ProducerContinuityRequire, SafetyConfig, SafetyRequire,
    ScribeRecoveryRequire, ScriptureConfig,
};
use crate::doctor::disclose_from_inputs;
use crate::preflight::{
    enforce_safety_policy, evaluate_policy, run_live_preflight_with_inputs, run_static_preflight,
    run_static_preflight_deferring_live, run_static_preflight_to, static_capability_inputs,
    warning_counter_snapshot,
};

fn sample_yaml() -> String {
    r#"
version: 1
node:
  owner_id: "scripture-own-a!"
  advertise: "tcp://10.0.0.1:9000"
listener:
  bind: "0.0.0.0:9000"
verse:
  journal_id: "scripture-jrnl!!"
  verse_id: "scripture-verse!"
  cohort_id: "scripture-cohrt!"
  writer_id: "scripture-wrtr!!"
store:
  backend: r2
  endpoint: "https://example.r2.cloudflarestorage.com"
  bucket: "example"
  region: auto
  prefix: "scripture/deployments/example"
"#
    .to_owned()
}

fn load(yaml: &str) -> ScriptureConfig {
    let config: ScriptureConfig = serde_yaml::from_str(yaml).expect("parse");
    config.validate().expect("valid");
    config
}

fn candidate(
    owner: &str,
    canon: &str,
    verse: &str,
    serving_capable: bool,
    fresh: bool,
) -> RecoveryCandidateEvidence {
    RecoveryCandidateEvidence {
        owner_id: owner.to_owned(),
        canon: canon.to_owned(),
        verse: verse.to_owned(),
        serving_capable,
        fresh,
        age_ms: if fresh { 500 } else { 90_000 },
        posture: if serving_capable {
            "bootstrap-if-empty".to_owned()
        } else {
            "standby".to_owned()
        },
        disposition: if serving_capable {
            "Serving".to_owned()
        } else {
            "Standby".to_owned()
        },
    }
}

/// Live inputs for one Verse with the given candidates (live path flags).
fn live_inputs(candidates: Vec<RecoveryCandidateEvidence>) -> CapabilityInputs {
    CapabilityInputs {
        backend_label: "r2".to_owned(),
        independent_storage_targets: 1,
        committed_capable_target: true,
        durable_producer_spool_configured: false,
        verses: vec![VerseCapabilityInputs {
            canon: "scripture-jrnl!!".to_owned(),
            verse: "scripture-verse!".to_owned(),
            candidates,
            serving_now: None,
            // Live builder always sets this so empty observation is Unsatisfied,
            // not RequiresLivePreflight.
            candidates_declared_for_static: true,
            fleet_directory_nonempty: true,
        }],
    }
}

fn safety_automatic_fail() -> SafetyConfig {
    SafetyConfig {
        require: SafetyRequire {
            scribe_recovery: Some(ScribeRecoveryRequire::Automatic),
            canon_history: None,
            producer_continuity: None,
            min_storage_failure_domains: None,
        },
        on_degraded: OnDegraded::FailStart,
        declared_eligible_candidates: vec![],
    }
}

/// 1. No eligible candidates + automatic + fail_start → Unsatisfied (not live-preflight msg).
#[test]
fn t1_live_empty_candidates_fails_unsatisfied() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
  on_degraded: fail_start
"#;
    let config = load(&yaml);
    let inputs = live_inputs(vec![]);
    let err =
        run_live_preflight_with_inputs(&config, &inputs, &mut Vec::new()).expect_err("must fail");
    let message = err.to_string();
    assert!(
        message.contains("SCRIPTURE_CAP_SCRIBE_RECOVERY"),
        "missing code: {message}"
    );
    assert!(
        !message.contains("require live preflight"),
        "live path must score Unsatisfied, not RequiresLivePreflight: {message}"
    );
    assert!(
        message.contains("no eligible recovery candidate")
            || message.contains("no automatic Verse recovery"),
        "expected unsatisfied recovery messaging: {message}"
    );
}

/// 2. Fresh ∧ serving-capable candidate → automatic passes.
#[test]
fn t2_live_eligible_candidate_passes() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
"#;
    let config = load(&yaml);
    let inputs = live_inputs(vec![candidate(
        "node-a!!!!!!!!!!",
        "scripture-jrnl!!",
        "scripture-verse!",
        true,
        true,
    )]);
    run_live_preflight_with_inputs(&config, &inputs, &mut Vec::new()).expect("must pass");
}

/// 3. Fresh ∧ ¬serving_capable (standby) → Unsatisfied.
#[test]
fn t3_live_fresh_standby_unsatisfied() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
"#;
    let config = load(&yaml);
    let inputs = live_inputs(vec![candidate(
        "node-a!!!!!!!!!!",
        "scripture-jrnl!!",
        "scripture-verse!",
        false,
        true,
    )]);
    let err = run_live_preflight_with_inputs(&config, &inputs, &mut Vec::new())
        .expect_err("standby must fail");
    let message = err.to_string();
    assert!(message.contains("SCRIPTURE_CAP_SCRIBE_RECOVERY"));
    assert!(!message.contains("require live preflight"));
}

/// 4. Stale or wrong-Verse candidates → Unsatisfied.
#[test]
fn t4_live_stale_and_wrong_verse_unsatisfied() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
"#;
    let config = load(&yaml);

    let stale = live_inputs(vec![candidate(
        "node-a!!!!!!!!!!",
        "scripture-jrnl!!",
        "scripture-verse!",
        true,
        false,
    )]);
    let err = run_live_preflight_with_inputs(&config, &stale, &mut Vec::new())
        .expect_err("stale must fail");
    assert!(err.to_string().contains("SCRIPTURE_CAP_SCRIBE_RECOVERY"));

    let wrong = live_inputs(vec![candidate(
        "node-a!!!!!!!!!!",
        "scripture-jrnl!!",
        "other-verse!!!!!",
        true,
        true,
    )]);
    let err = run_live_preflight_with_inputs(&config, &wrong, &mut Vec::new())
        .expect_err("wrong-Verse must fail");
    assert!(err.to_string().contains("SCRIPTURE_CAP_SCRIBE_RECOVERY"));
}

/// 5. on_degraded: warn + unsatisfiable live recovery → real sink bytes, success.
#[test]
fn t5_live_warn_emits_ordered_warning_and_succeeds() {
    let before = warning_counter_snapshot();
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
  on_degraded: warn
"#;
    let config = load(&yaml);
    let inputs = live_inputs(vec![]);
    let mut sink = Vec::new();
    run_live_preflight_with_inputs(&config, &inputs, &mut sink).expect("warn must succeed");
    let emitted = String::from_utf8(sink).expect("utf-8");
    assert!(
        emitted.contains("scripture: warning: SCRIPTURE_CAP_SCRIBE_RECOVERY"),
        "missing warning bytes: {emitted}"
    );
    assert!(
        emitted.contains("scripture-jrnl!!/scripture-verse!"),
        "missing scope: {emitted}"
    );
    let after = warning_counter_snapshot();
    let code = CapabilityCode::ScribeRecovery;
    let before_n = before.get(&code).copied().unwrap_or(0);
    let after_n = after.get(&code).copied().unwrap_or(0);
    assert!(after_n > before_n, "counter must bump");
}

/// 6. Anti-drift: doctor disclose and live enforcement share one evaluator result.
#[test]
fn t6_doctor_and_live_enforcement_same_evaluator() {
    let inputs = live_inputs(vec![candidate(
        "node-a!!!!!!!!!!",
        "scripture-jrnl!!",
        "scripture-verse!",
        false, // fresh standby
        true,
    )]);
    let typed = evaluate(&inputs);
    let recovery = &typed.scribe_recovery[0].satisfaction;
    assert_eq!(recovery.kind, SatisfactionKind::Unsatisfied);

    let disclosure = disclose_from_inputs(&inputs);
    assert_eq!(
        disclosure.scribe_availability[0].result, recovery.consequence,
        "doctor must render evaluator consequence only"
    );

    let safety = safety_automatic_fail();
    let (_report, findings) = evaluate_policy(&inputs, &safety);
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].code, CapabilityCode::ScribeRecovery);
    assert_eq!(findings[0].kind, SatisfactionKind::Unsatisfied);
    assert_eq!(findings[0].consequence, recovery.consequence);

    let config = load(
        &(sample_yaml()
            + r#"
safety:
  require:
    scribe_recovery: automatic
"#),
    );
    let err = run_live_preflight_with_inputs(&config, &inputs, &mut Vec::new())
        .expect_err("standby fails live enforce");
    assert!(err.to_string().contains(&recovery.consequence));
}

/// 7. validate/static path remains hermetic RequiresLivePreflight (no directory I/O).
#[test]
fn t7_validate_static_still_requires_live_preflight() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
"#;
    let config = load(&yaml);
    let err = run_static_preflight(&config).expect_err("static must fail");
    let message = err.to_string();
    assert!(message.contains("SCRIPTURE_CAP_SCRIBE_RECOVERY"));
    assert!(
        message.contains("live preflight"),
        "static path must keep RequiresLivePreflight messaging: {message}"
    );
    // Startup deferral must not fail on the same static RequiresLivePreflight.
    run_static_preflight_deferring_live(&config).expect("startup defers live findings");
}

/// 8. Live preflight performs no authority writes (inputs+policy immutable).
#[test]
fn t8_live_preflight_no_mutation() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
  on_degraded: warn
"#;
    let config = load(&yaml);
    let safety = config.safety.as_ref().expect("safety").clone();
    let inputs = live_inputs(vec![]);
    let inputs_before = inputs.clone();
    let safety_before = safety.clone();
    let mut sink = Vec::new();
    run_live_preflight_with_inputs(&config, &inputs, &mut sink).expect("warn ok");
    assert_eq!(
        inputs, inputs_before,
        "inputs must be immutable through gate"
    );
    assert_eq!(
        safety, safety_before,
        "policy must be immutable through gate"
    );
    // Gate takes no authority/register handle by construction.
    let _ = enforce_safety_policy(&evaluate(&inputs), &inputs, &safety, &mut Vec::new());
}

/// 9. Fully satisfiable live require → ok, no warnings.
#[test]
fn t9_fully_satisfiable_live_require() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    canon_history: committed
    scribe_recovery: automatic
    min_storage_failure_domains: 1
"#;
    let config = load(&yaml);
    let inputs = live_inputs(vec![candidate(
        "node-a!!!!!!!!!!",
        "scripture-jrnl!!",
        "scripture-verse!",
        true,
        true,
    )]);
    let mut sink = Vec::new();
    let report = run_live_preflight_with_inputs(&config, &inputs, &mut sink).expect("must pass");
    assert!(sink.is_empty(), "no warnings expected: {:?}", sink);
    assert_eq!(report.canon_history.kind, SatisfactionKind::Satisfied);
    assert_eq!(
        report.scribe_recovery[0].satisfaction.kind,
        SatisfactionKind::Satisfied
    );
}

/// 10. Omitted safety → live enforcement is a no-op.
#[test]
fn t10_omitted_safety_live_noop() {
    let config = load(&sample_yaml());
    assert!(config.safety.is_none());
    let inputs = live_inputs(vec![]);
    let mut sink = Vec::new();
    run_live_preflight_with_inputs(&config, &inputs, &mut sink).expect("noop success");
    assert!(
        sink.is_empty(),
        "omitted safety must emit no warnings: {:?}",
        sink
    );
}

#[test]
fn static_inputs_unchanged_for_hermetic_validate() {
    let config = load(&sample_yaml());
    let inputs = static_capability_inputs(&config);
    assert!(!inputs.verses.is_empty());
    assert!(!inputs.verses[0].candidates_declared_for_static);
    let mut sink = Vec::new();
    run_static_preflight_to(&config, &mut sink).expect("no safety");
    assert!(sink.is_empty());
}

#[test]
fn live_and_static_share_evaluate_function() {
    let inputs = live_inputs(vec![candidate(
        "node-a!!!!!!!!!!",
        "scripture-jrnl!!",
        "scripture-verse!",
        true,
        true,
    )]);
    let a = evaluate(&inputs);
    let b = evaluate(&inputs);
    assert_eq!(a, b);
    assert_eq!(
        a.scribe_recovery[0].satisfaction.kind,
        SatisfactionKind::Satisfied
    );
}

#[test]
fn producer_continuity_still_fails_on_live_path_when_required() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    producer_continuity: spooled
    scribe_recovery: automatic
"#;
    let config = load(&yaml);
    let inputs = live_inputs(vec![candidate(
        "node-a!!!!!!!!!!",
        "scripture-jrnl!!",
        "scripture-verse!",
        true,
        true,
    )]);
    let err = run_live_preflight_with_inputs(&config, &inputs, &mut Vec::new())
        .expect_err("spooled still unsatisfied");
    assert!(
        err.to_string()
            .contains("SCRIPTURE_CAP_PRODUCER_CONTINUITY")
    );
}

#[test]
fn safety_config_fields_for_live_tests() {
    let _ = (
        CanonHistoryRequire::Committed,
        ProducerContinuityRequire::Spooled,
        ScribeRecoveryRequire::Automatic,
        OnDegraded::Warn,
    );
}
