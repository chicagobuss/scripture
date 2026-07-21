//! Acceptance tests for producer spooled enablement (WP 2026-07-21).
//!
//! Covers capability-truth and shared-evaluator anti-drift. Outbox path cases
//! (fsync-before-receipt, retain/replay/reclaim, capacity) live in
//! `scripture::producer_outbox` unit tests.

use scripture_runtime::{SatisfactionKind, evaluate};

use crate::config::ScriptureConfig;
use crate::doctor::disclose_from_inputs;
use crate::preflight::{evaluate_policy, run_static_preflight, static_capability_inputs};

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

fn spool_yaml() -> String {
    sample_yaml()
        + r#"
producer_spool:
  enabled: true
  kind: local
  path: ".scripture-producer-spool"
  max_bytes: 1048576
  fsync: every_record
  on_full: reject
  loss_budget: 30s
  scribe_id: "node-a"
"#
}

fn load(yaml: &str) -> ScriptureConfig {
    let config: ScriptureConfig = serde_yaml::from_str(yaml).expect("parse");
    config.validate().expect("valid");
    config
}

/// WP AT1: configured local spool → durable_producer_spool_configured + satisfied
/// producer_continuity; doctor + safety.require share the same evidence.
#[test]
fn t1_configured_capability_truth() {
    let yaml = spool_yaml()
        + r#"
safety:
  require:
    producer_continuity: spooled
  on_degraded: fail_start
"#;
    let config = load(&yaml);
    assert!(config.durable_producer_spool_configured());

    let inputs = static_capability_inputs(&config);
    assert!(inputs.durable_producer_spool_configured);
    assert_eq!(inputs.producer_spool_loss_budget, "30s");
    assert_eq!(inputs.producer_spool_scribe_id, "node-a");

    let typed = evaluate(&inputs);
    assert_eq!(
        typed.producer_continuity.kind,
        SatisfactionKind::Satisfied,
        "{}",
        typed.producer_continuity.observed
    );
    assert!(
        typed.producer_continuity.observed.contains("node-a"),
        "observed must name one-disk scope: {}",
        typed.producer_continuity.observed
    );
    assert!(
        typed.producer_continuity.observed.contains("30s")
            || typed.producer_continuity.observed.contains("loss_budget"),
        "observed must publish loss_budget: {}",
        typed.producer_continuity.observed
    );

    let doctor = disclose_from_inputs(&inputs);
    assert_eq!(
        doctor.producer_continuity.durable_local_outbox,
        "CONFIGURED"
    );
    assert_eq!(doctor.producer_continuity.spooled_loss_budget, "30s");
    assert!(doctor.producer_continuity.evidence.contains("node-a"));

    run_static_preflight(&config).expect("safety.require spooled must pass");
    let safety = config.safety.as_ref().expect("safety");
    let (_, findings) = evaluate_policy(&inputs, safety);
    assert!(
        findings.is_empty(),
        "unexpected findings with configured spool: {findings:?}"
    );
}

/// WP AT2: absent / memory / invalid → not configured; no hard-coded true.
#[test]
fn t2_missing_invalid_capability_truth() {
    let absent = load(&sample_yaml());
    assert!(!absent.durable_producer_spool_configured());
    let inputs = static_capability_inputs(&absent);
    assert!(!inputs.durable_producer_spool_configured);
    assert!(inputs.producer_spool_loss_budget.is_empty());
    assert_eq!(
        evaluate(&inputs).producer_continuity.kind,
        SatisfactionKind::Unsatisfied
    );

    let memory = sample_yaml()
        + r#"
producer_spool:
  kind: memory
  path: ".scripture-producer-spool"
  max_bytes: 1048576
  fsync: every_record
  on_full: reject
  loss_budget: 30s
  scribe_id: "node-a"
"#;
    let mem: ScriptureConfig = serde_yaml::from_str(&memory).expect("parse");
    assert!(mem.validate().is_err());
    assert!(!mem.durable_producer_spool_configured());

    let bad_budget = sample_yaml()
        + r#"
producer_spool:
  kind: local
  path: ".scripture-producer-spool"
  max_bytes: 1048576
  fsync: every_record
  on_full: reject
  loss_budget: 0s
  scribe_id: "node-a"
"#;
    let bad: ScriptureConfig = serde_yaml::from_str(&bad_budget).expect("parse");
    assert!(bad.validate().is_err());
    assert!(!bad.durable_producer_spool_configured());

    let empty_path = sample_yaml()
        + r#"
producer_spool:
  kind: local
  path: ""
  max_bytes: 1048576
  fsync: every_record
  on_full: reject
  loss_budget: 30s
  scribe_id: "node-a"
"#;
    let empty: ScriptureConfig = serde_yaml::from_str(&empty_path).expect("parse");
    assert!(empty.validate().is_err());
    assert!(!empty.durable_producer_spool_configured());

    // Path-only inference must not flip the bit: absent config stays false even
    // if a directory string appears elsewhere.
    assert!(!static_capability_inputs(&absent).durable_producer_spool_configured);

    // A valid durable path is dormant until the executable Producer Wire
    // lifecycle is explicitly mounted. Presence alone cannot claim spooled.
    let disabled = load(
        &(sample_yaml()
            + r#"
producer_spool:
  enabled: false
  kind: local
  path: ".scripture-producer-spool"
  max_bytes: 1048576
  fsync: every_record
  on_full: reject
  loss_budget: 30s
  scribe_id: "node-a"
"#),
    );
    let disabled_inputs = static_capability_inputs(&disabled);
    assert!(!disabled_inputs.durable_producer_spool_configured);
    assert_eq!(
        evaluate(&disabled_inputs).producer_continuity.kind,
        SatisfactionKind::Unsatisfied
    );
}

/// WP AT11: doctor + safety.require call the same pure evaluator on identical inputs.
#[test]
fn t11_shared_evaluator_anti_drift() {
    let config = load(&spool_yaml());
    let inputs = static_capability_inputs(&config);
    let typed = evaluate(&inputs);
    let doctor = disclose_from_inputs(&inputs);

    assert_eq!(typed.producer_continuity.kind, SatisfactionKind::Satisfied);
    assert_eq!(
        doctor.producer_continuity.durable_local_outbox,
        "CONFIGURED"
    );
    assert_eq!(
        doctor.producer_continuity.spooled_loss_budget,
        inputs.producer_spool_loss_budget
    );
    assert_eq!(
        doctor.producer_continuity.result,
        typed.producer_continuity.consequence
    );

    // Unconfigured inputs: both conclude unsatisfied / NOT CONFIGURED.
    let absent = static_capability_inputs(&load(&sample_yaml()));
    let typed_abs = evaluate(&absent);
    let doctor_abs = disclose_from_inputs(&absent);
    assert_eq!(
        typed_abs.producer_continuity.kind,
        SatisfactionKind::Unsatisfied
    );
    assert_eq!(
        doctor_abs.producer_continuity.durable_local_outbox,
        "NOT CONFIGURED"
    );
    assert!(
        doctor_abs
            .producer_continuity
            .spooled_loss_budget
            .is_empty()
    );
}

/// AT10 companion: doctor/capability evidence reports the same effective bound.
#[test]
fn doctor_publishes_one_disk_loss_budget_bound() {
    let config = load(&spool_yaml());
    let inputs = static_capability_inputs(&config);
    let doctor = disclose_from_inputs(&inputs);
    assert_eq!(doctor.producer_continuity.spooled_loss_budget, "30s");
    assert!(
        doctor
            .producer_continuity
            .evidence
            .contains("one named Scribe local disk")
    );
}
