//! Acceptance tests for safety.require static preflight (WP 2026-07-21).
//!
//! All hermetic with injected evidence. Covers required tests 1–11; test 12 is
//! the cargo fmt/clippy/diff gate run at package completion.

use scripture_runtime::{
    CapabilityCode, CapabilityInputs, RecoveryCandidateEvidence, SatisfactionKind,
    VerseCapabilityInputs, evaluate, is_eligible_recovery_candidate,
};

use crate::config::{
    CanonHistoryRequire, DeclaredEligibleCandidate, OnDegraded, ProducerContinuityRequire,
    SafetyConfig, SafetyRequire, ScribeRecoveryRequire, ScriptureConfig,
};
use crate::doctor::disclose_from_inputs;
use crate::preflight::{
    enforce_safety_policy, evaluate_policy, format_validate_ok, run_static_preflight,
    run_static_preflight_to, static_capability_inputs, warning_counter_snapshot,
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

/// 1. producer_continuity: spooled required, no spool → fail with code + remediation.
#[test]
fn t1_producer_continuity_spooled_fails_without_spool() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    producer_continuity: spooled
  on_degraded: fail_start
"#;
    let config = load(&yaml);
    let err = run_static_preflight(&config).expect_err("must fail");
    let message = err.to_string();
    assert!(
        message.contains("SCRIPTURE_CAP_PRODUCER_CONTINUITY"),
        "missing code: {message}"
    );
    assert!(
        message.to_lowercase().contains("remediation")
            || message.contains("edge spool")
            || message.contains("outbox"),
        "missing remediation: {message}"
    );
}

/// 2. min_storage_failure_domains: 2 with one target → failure-domain code.
#[test]
fn t2_min_storage_failure_domains_two_fails_with_one_target() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    min_storage_failure_domains: 2
"#;
    let config = load(&yaml);
    let err = run_static_preflight(&config).expect_err("must fail");
    let message = err.to_string();
    assert!(
        message.contains("SCRIPTURE_CAP_STORAGE_FAILURE_DOMAINS"),
        "missing code: {message}"
    );
}

/// 3. canon_history: committed with committed-capable target → passes.
#[test]
fn t3_canon_history_committed_passes() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    canon_history: committed
"#;
    let config = load(&yaml);
    run_static_preflight(&config).expect("must pass");
}

/// 4. scribe_recovery: automatic with no declared candidates → requires live preflight.
#[test]
fn t4_scribe_recovery_automatic_requires_live_preflight() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
"#;
    let config = load(&yaml);
    let err = run_static_preflight(&config).expect_err("must fail");
    let message = err.to_string();
    assert!(
        message.contains("SCRIPTURE_CAP_SCRIBE_RECOVERY"),
        "missing code: {message}"
    );
    assert!(
        message.contains("live preflight"),
        "expected live-preflight messaging: {message}"
    );
}

/// 5. Candidate predicate fixtures.
#[test]
fn t5_eligible_recovery_candidate_predicate_fixtures() {
    let canon = "telemetry-jrnl!!";
    let verse = "telemetry-host-a";
    let active = candidate("node-a!!!!!!!!!!", canon, verse, true, true);
    assert!(is_eligible_recovery_candidate(&active, canon, verse));

    let dormant = candidate("node-a!!!!!!!!!!", canon, verse, false, true);
    assert!(!is_eligible_recovery_candidate(&dormant, canon, verse));

    let stale = candidate("node-a!!!!!!!!!!", canon, verse, true, false);
    assert!(!is_eligible_recovery_candidate(&stale, canon, verse));

    let wrong_verse = candidate("node-a!!!!!!!!!!", canon, "other-verse!!!!!", true, true);
    assert!(!is_eligible_recovery_candidate(&wrong_verse, canon, verse));
}

/// 6. on_degraded: warn → real emitted bytes with stable codes/scopes in (code, scope) order.
#[test]
fn t6_on_degraded_warn_emits_ordered_warning_and_succeeds() {
    let before = warning_counter_snapshot();
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    producer_continuity: spooled
    scribe_recovery: automatic
    min_storage_failure_domains: 2
  on_degraded: warn
"#;
    let config = load(&yaml);
    let mut sink = Vec::new();
    run_static_preflight_to(&config, &mut sink).expect("warn must succeed");
    let emitted = String::from_utf8(sink).expect("warning bytes are utf-8");
    let lines: Vec<&str> = emitted.lines().filter(|l| !l.is_empty()).collect();

    assert!(
        lines.len() >= 3,
        "expected multiple warning lines, got: {lines:?}"
    );
    for line in &lines {
        assert!(
            line.starts_with("scripture: warning: SCRIPTURE_CAP_"),
            "missing stable code prefix: {line}"
        );
        assert!(line.contains("scope="), "missing scope: {line}");
    }

    // Deterministic (code, scope) emission order — same as collect_requirement_findings sort.
    let parsed: Vec<(String, String)> = lines
        .iter()
        .map(|line| {
            // "scripture: warning: SCRIPTURE_CAP_FOO scope=...: ..."
            let after_prefix = line
                .strip_prefix("scripture: warning: ")
                .expect("warning prefix");
            let mut parts = after_prefix.splitn(2, ' ');
            let code = parts.next().expect("code").to_owned();
            let rest = parts.next().unwrap_or("");
            let scope = rest
                .strip_prefix("scope=")
                .and_then(|s| s.split(':').next())
                .expect("scope=")
                .to_owned();
            (code, scope)
        })
        .collect();
    let mut sorted = parsed.clone();
    sorted.sort();
    assert_eq!(
        parsed, sorted,
        "warning lines must be in deterministic (code, scope) order: {lines:?}"
    );

    // Also assert the expected codes appear.
    assert!(emitted.contains("SCRIPTURE_CAP_PRODUCER_CONTINUITY"));
    assert!(emitted.contains("SCRIPTURE_CAP_SCRIBE_RECOVERY"));
    assert!(emitted.contains("SCRIPTURE_CAP_STORAGE_FAILURE_DOMAINS"));
    assert!(emitted.contains("scope=deployment") || emitted.contains("scope=scripture-jrnl!!"));

    let after = warning_counter_snapshot();
    for code in [
        CapabilityCode::ProducerContinuity,
        CapabilityCode::ScribeRecovery,
        CapabilityCode::StorageFailureDomains,
    ] {
        let b = before.get(&code).copied().unwrap_or(0);
        let a = after.get(&code).copied().unwrap_or(0);
        assert!(
            a > b,
            "expected counter bump for {code:?}: before={b} after={a}"
        );
    }
}

/// 7. Fully satisfiable static safety.require → validate ok, no findings.
#[test]
fn t7_fully_satisfiable_static_require_ok() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    canon_history: committed
    min_storage_failure_domains: 1
  on_degraded: fail_start
"#;
    let config = load(&yaml);
    let report = run_static_preflight(&config).expect("must pass");
    assert_eq!(report.canon_history.kind, SatisfactionKind::Satisfied);
    let inputs = static_capability_inputs(&config);
    let safety = config.safety.as_ref().expect("safety");
    let (_, findings) = evaluate_policy(&inputs, safety);
    assert!(findings.is_empty(), "unexpected findings: {findings:?}");
}

/// 8. Doctor real render path vs enforcement on fresh-but-NOT-serving-capable
///    (standby) candidate — both conclude not recoverable. Fails if drift returns.
#[test]
fn t8_doctor_render_agrees_enforcement_on_fresh_standby() {
    let inputs = CapabilityInputs {
        backend_label: "rustfs".to_owned(),
        independent_storage_targets: 1,
        committed_capable_target: true,
        durable_producer_spool_configured: false,
        verses: vec![VerseCapabilityInputs {
            canon: "telemetry-jrnl!!".to_owned(),
            verse: "telemetry-host-a".to_owned(),
            candidates: vec![candidate(
                "node-a!!!!!!!!!!",
                "telemetry-jrnl!!",
                "telemetry-host-a",
                false, // standby: NOT serving_capable
                true,  // fresh
            )],
            serving_now: None,
            candidates_declared_for_static: true,
            fleet_directory_nonempty: true,
        }],
    };

    // Enforcement path: shared pure evaluator.
    let enforce = evaluate(&inputs);
    assert_eq!(enforce.scribe_recovery.len(), 1);
    assert_eq!(
        enforce.scribe_recovery[0].satisfaction.kind,
        SatisfactionKind::Unsatisfied,
        "enforcement must treat fresh-standby as not recoverable"
    );
    assert!(
        enforce.scribe_recovery[0]
            .satisfaction
            .consequence
            .contains("no automatic Verse recovery"),
        "enforcement consequence: {}",
        enforce.scribe_recovery[0].satisfaction.consequence
    );
    assert_eq!(enforce.scribe_recovery[0].eligible_candidates, 0);
    assert_eq!(
        enforce.scribe_recovery[0].candidates_observed_heartbeating,
        1
    );

    // Doctor path: real render (evaluate + render_disclosure), not a evaluate wrapper.
    let doctor = disclose_from_inputs(&inputs);
    assert_eq!(doctor.scribe_availability.len(), 1);
    let rendered = &doctor.scribe_availability[0];
    assert!(
        rendered.result.contains("no automatic Verse recovery"),
        "doctor must conclude not recoverable: {}",
        rendered.result
    );
    assert!(
        !rendered.result.contains("can recover"),
        "doctor must not claim recovery for fresh-standby: {}",
        rendered.result
    );
    // Drift guard: doctor result bytes == evaluator consequence.
    assert_eq!(
        rendered.result, enforce.scribe_recovery[0].satisfaction.consequence,
        "doctor recovery result must derive from evaluator Satisfaction (drift)"
    );
    assert_eq!(
        rendered.candidates_observed_heartbeating,
        enforce.scribe_recovery[0].candidates_observed_heartbeating
    );
}

/// 9. Policy-doc validation: unknown key, retired vocab, invalid on_degraded, floor < 1.
#[test]
fn t9_policy_document_validation_rejects_bad_keys_and_values() {
    let unknown = sample_yaml()
        + r#"
safety:
  require:
    not_a_real_key: true
"#;
    let err = serde_yaml::from_str::<ScriptureConfig>(&unknown).expect_err("unknown key");
    assert!(
        err.to_string()
            .contains("unknown safety.require key 'not_a_real_key'"),
        "unexpected: {err}"
    );

    let retired_history = sample_yaml()
        + r#"
safety:
  require:
    canon_history: canon_committed
"#;
    let err = serde_yaml::from_str::<ScriptureConfig>(&retired_history).expect_err("retired");
    assert!(
        err.to_string().contains("canon_committed") && err.to_string().contains("committed"),
        "unexpected: {err}"
    );

    let retired_continuity = sample_yaml()
        + r#"
safety:
  require:
    producer_continuity: local_durable
"#;
    let err = serde_yaml::from_str::<ScriptureConfig>(&retired_continuity).expect_err("retired");
    assert!(
        err.to_string().contains("local_durable") && err.to_string().contains("spooled"),
        "unexpected: {err}"
    );

    let bad_degraded = sample_yaml()
        + r#"
safety:
  require:
    canon_history: committed
  on_degraded: explode
"#;
    let err = serde_yaml::from_str::<ScriptureConfig>(&bad_degraded).expect_err("on_degraded");
    assert!(
        err.to_string().contains("on_degraded") && err.to_string().contains("fail_start"),
        "unexpected: {err}"
    );

    let floor = sample_yaml()
        + r#"
safety:
  require:
    min_storage_failure_domains: 0
"#;
    let err = serde_yaml::from_str::<ScriptureConfig>(&floor).expect_err("floor");
    assert!(
        err.to_string()
            .contains("min_storage_failure_domains must be an integer >= 1"),
        "unexpected: {err}"
    );
}

/// 10. Omitted safety → byte-identical validate output vs pre-change baseline.
#[test]
fn t10_omitted_safety_preserves_behavior() {
    let config = load(&sample_yaml());
    assert!(config.safety.is_none());

    let mut sink = Vec::new();
    run_static_preflight_to(&config, &mut sink).expect("omitted safety is no-op success");
    assert!(
        sink.is_empty(),
        "omitted safety must emit no warnings: {:?}",
        String::from_utf8_lossy(&sink)
    );

    // Exact validate-ok line equals the pre-change baseline (no safety fields).
    let actual = format_validate_ok(&config);
    let baseline = "scripture: validate ok version=1 owner=scripture-own-a! advertise=tcp://10.0.0.1:9000 backend=r2 prefix=scripture/deployments/example";
    assert_eq!(
        actual, baseline,
        "validate ok line must be byte-identical to baseline"
    );
    assert!(!actual.contains("safety"));
    assert!(!actual.contains("SCRIPTURE_CAP_"));
    assert!(!actual.contains("warning"));
}

/// 11. Static gate leaves CapabilityInputs / SafetyConfig byte-identical.
///
/// The static gate takes no authority/register handle by construction, so it
/// structurally cannot perform a CAS. This property checks that the full gate
/// path does not interior-mutate the cloned inputs either.
#[test]
fn t11_static_gate_does_not_mutate_inputs() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    producer_continuity: spooled
    scribe_recovery: automatic
    min_storage_failure_domains: 2
    canon_history: committed
  on_degraded: warn
"#;
    let config = load(&yaml);
    let safety = config.safety.as_ref().expect("safety").clone();
    let inputs = static_capability_inputs(&config);
    let inputs_before = inputs.clone();
    let safety_before = safety.clone();
    // Tie the no-mutation assertion to the FULL gate's own input. The gate takes
    // `&ScriptureConfig` and holds no authority/register handle by construction,
    // so it structurally cannot perform a CAS; assert it also does not
    // interior-mutate the config it was given.
    let config_before = format!("{config:?}");

    let mut sink = Vec::new();
    run_static_preflight_to(&config, &mut sink).expect("warn path succeeds");
    assert!(!sink.is_empty(), "expected warn emissions");
    assert_eq!(
        format!("{config:?}"),
        config_before,
        "the full static gate (run_static_preflight) must not mutate its config input"
    );

    let report = evaluate(&inputs);
    enforce_safety_policy(&report, &inputs, &safety, &mut Vec::new()).expect("warn");
    let (_report, findings) = evaluate_policy(&inputs, &safety);
    assert!(!findings.is_empty());

    assert_eq!(
        inputs, inputs_before,
        "static gate must not interior-mutate CapabilityInputs"
    );
    assert_eq!(
        safety, safety_before,
        "static gate must not interior-mutate SafetyConfig"
    );
}

/// Declared eligible candidate satisfies automatic recovery on the static path.
#[test]
fn declared_eligible_candidate_satisfies_automatic_recovery() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    scribe_recovery: automatic
  declared_eligible_candidates:
    - canon: "scripture-jrnl!!"
      verse: "scripture-verse!"
      owner_id: "node-a!!!!!!!!!!"
      serving_capable: true
      fresh: true
"#;
    let config = load(&yaml);
    run_static_preflight(&config).expect("declared eligible candidate satisfies");
}

#[test]
fn enforce_warn_orders_findings_by_code_then_scope() {
    let inputs = CapabilityInputs {
        backend_label: "rustfs".to_owned(),
        independent_storage_targets: 1,
        committed_capable_target: true,
        durable_producer_spool_configured: false,
        verses: vec![
            VerseCapabilityInputs {
                canon: "canon-bbbbbbbbb".to_owned(),
                verse: "verse-bbbbbbbbb".to_owned(),
                candidates: vec![],
                serving_now: None,
                candidates_declared_for_static: false,
                fleet_directory_nonempty: false,
            },
            VerseCapabilityInputs {
                canon: "canon-aaaaaaaaa".to_owned(),
                verse: "verse-aaaaaaaaa".to_owned(),
                candidates: vec![],
                serving_now: None,
                candidates_declared_for_static: false,
                fleet_directory_nonempty: false,
            },
        ],
    };
    let safety = SafetyConfig {
        require: SafetyRequire {
            producer_continuity: Some(ProducerContinuityRequire::Spooled),
            scribe_recovery: Some(ScribeRecoveryRequire::Automatic),
            canon_history: Some(CanonHistoryRequire::Committed),
            min_storage_failure_domains: Some(2),
        },
        on_degraded: OnDegraded::Warn,
        declared_eligible_candidates: vec![],
    };
    let report = evaluate(&inputs);
    let out =
        enforce_safety_policy(&report, &inputs, &safety, &mut Vec::new()).expect("warn succeeds");
    assert_eq!(out.canon_history.kind, SatisfactionKind::Satisfied);
    let (_report, findings) = evaluate_policy(&inputs, &safety);
    assert!(findings.len() >= 3);
    for window in findings.windows(2) {
        let left = (
            window[0].code,
            window[0].scope.canon.as_str(),
            window[0].scope.verse.as_str(),
        );
        let right = (
            window[1].code,
            window[1].scope.canon.as_str(),
            window[1].scope.verse.as_str(),
        );
        assert!(left <= right, "findings not ordered: {findings:?}");
    }
}

#[test]
fn safety_config_round_trip_fields() {
    let yaml = sample_yaml()
        + r#"
safety:
  require:
    canon_history: committed
    producer_continuity: spooled
    scribe_recovery: automatic
    min_storage_failure_domains: 1
  on_degraded: warn
  declared_eligible_candidates:
    - canon: "scripture-jrnl!!"
      verse: "scripture-verse!"
      owner_id: "node-a!!!!!!!!!!"
      serving_capable: true
      fresh: false
"#;
    let config = load(&yaml);
    let safety = config.safety.as_ref().expect("safety");
    assert_eq!(safety.on_degraded, OnDegraded::Warn);
    assert_eq!(
        safety.require.canon_history,
        Some(CanonHistoryRequire::Committed)
    );
    assert_eq!(
        safety.require.producer_continuity,
        Some(ProducerContinuityRequire::Spooled)
    );
    assert_eq!(
        safety.require.scribe_recovery,
        Some(ScribeRecoveryRequire::Automatic)
    );
    assert_eq!(safety.require.min_storage_failure_domains, Some(1));
    assert_eq!(
        safety.declared_eligible_candidates,
        vec![DeclaredEligibleCandidate {
            canon: "scripture-jrnl!!".to_owned(),
            verse: "scripture-verse!".to_owned(),
            owner_id: "node-a!!!!!!!!!!".to_owned(),
            serving_capable: true,
            fresh: false,
        }]
    );
}
