//! Static and live safety.require preflight gates.
//!
//! Both paths call the shared pure [`scripture_runtime::evaluate`]. They differ
//! only in how [`CapabilityInputs`] are built:
//! - **static** (`validate`): config-derived; hermetic; live-only requirements
//!   without declared candidates yield [`SatisfactionKind::RequiresLivePreflight`].
//! - **live** (`preflight --live`, `serve` / `scribe run`): directory +
//!   authority-root observation; live-only boundaries become real
//!   Satisfied / Unsatisfied verdicts.
//!
//! Startup (`serve` / `scribe run`) runs static with live findings deferred,
//! then live enforcement after the shared store is connected — before listener
//! bind / lifecycle assembly where possible.

use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use scripture_runtime::{
    CapabilityCode, CapabilityFinding, CapabilityInputs, CapabilityReport,
    RecoveryCandidateEvidence, RequiredGuarantee, SatisfactionKind, VerseCapabilityInputs,
    collect_requirement_findings, evaluate,
};

use crate::assemble::SharedStore;
use crate::config::{
    CanonHistoryRequire, OnDegraded, ProducerContinuityRequire, SafetyConfig, SafetyRequire,
    ScribeRecoveryRequire, ScriptureConfig,
};
use crate::doctor;

/// Process-local warn-mode emission counts keyed by capability code.
///
/// Surfaced sink for `on_degraded: warn` (not a full metrics facade). Bumped
/// once per emitted warning line. Scrapable via [`warning_counter_snapshot`].
static WARNING_COUNTERS: Mutex<Option<HashMap<CapabilityCode, AtomicU64>>> = Mutex::new(None);

/// Snapshot of process-local warn-mode per-code counters.
///
/// This is a public counter accessor for tests and lightweight ops visibility,
/// not an OpenTelemetry/Prometheus integration.
#[must_use]
#[allow(dead_code)] // Surfaced sink; scraped by tests / ops, not the binary main path.
pub fn warning_counter_snapshot() -> HashMap<CapabilityCode, u64> {
    let guard = WARNING_COUNTERS.lock().unwrap_or_else(|p| p.into_inner());
    guard
        .as_ref()
        .map(|map| {
            map.iter()
                .map(|(code, counter)| (*code, counter.load(Ordering::Relaxed)))
                .collect()
        })
        .unwrap_or_default()
}

fn bump_warning_counter(code: CapabilityCode) {
    let mut guard = WARNING_COUNTERS.lock().unwrap_or_else(|p| p.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    map.entry(code)
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed);
}

/// Build statically-derivable capability inputs from config (hermetic; no I/O).
///
/// - Storage targets: one configured object-store backend today.
/// - Producer spool: always unset until producer-outbox enablement (follow-up).
/// - Recovery candidates: only `safety.declared_eligible_candidates`.
#[must_use]
pub fn static_capability_inputs(config: &ScriptureConfig) -> CapabilityInputs {
    let backend_label = config
        .backend()
        .map(|b| b.label().to_owned())
        .unwrap_or_else(|_| config.store.backend.clone());
    let verses = verse_scopes_for_static(config);
    CapabilityInputs {
        backend_label,
        independent_storage_targets: 1,
        committed_capable_target: true,
        // No producer-outbox config exists yet (WP out of scope).
        durable_producer_spool_configured: false,
        verses,
    }
}

fn verse_scopes_for_static(config: &ScriptureConfig) -> Vec<VerseCapabilityInputs> {
    let declared = config
        .safety
        .as_ref()
        .map(|s| s.declared_eligible_candidates.as_slice())
        .unwrap_or(&[]);
    let any_declared = !declared.is_empty();
    let scopes: Vec<(String, String)> = if config.is_multi_assignment() {
        config
            .scribe
            .as_ref()
            .map(|scribe| {
                scribe
                    .assignments
                    .iter()
                    .map(|a| (a.canon.clone(), a.verse.clone()))
                    .collect()
            })
            .unwrap_or_default()
    } else if let Some(verse) = &config.verse {
        vec![(verse.journal_id.clone(), verse.verse_id.clone())]
    } else {
        Vec::new()
    };

    scopes
        .into_iter()
        .map(|(canon, verse)| {
            let candidates: Vec<RecoveryCandidateEvidence> = declared
                .iter()
                .filter(|c| c.canon == canon && c.verse == verse)
                .map(|c| RecoveryCandidateEvidence {
                    owner_id: c.owner_id.clone(),
                    canon: c.canon.clone(),
                    verse: c.verse.clone(),
                    serving_capable: c.serving_capable,
                    fresh: c.fresh,
                    age_ms: if c.fresh { 0 } else { u64::MAX },
                    posture: String::new(),
                    disposition: String::new(),
                })
                .collect();
            VerseCapabilityInputs {
                canon,
                verse,
                candidates,
                serving_now: None,
                candidates_declared_for_static: any_declared,
                fleet_directory_nonempty: false,
            }
        })
        .collect()
}

fn requirements_from_safety(require: &SafetyRequire) -> Vec<RequiredGuarantee> {
    let mut out = Vec::new();
    if matches!(require.canon_history, Some(CanonHistoryRequire::Committed)) {
        out.push(RequiredGuarantee::CanonHistoryCommitted);
    }
    if matches!(
        require.producer_continuity,
        Some(ProducerContinuityRequire::Spooled)
    ) {
        out.push(RequiredGuarantee::ProducerContinuitySpooled);
    }
    if matches!(
        require.scribe_recovery,
        Some(ScribeRecoveryRequire::Automatic)
    ) {
        out.push(RequiredGuarantee::ScribeRecoveryAutomatic);
    }
    if let Some(min) = require.min_storage_failure_domains {
        out.push(RequiredGuarantee::MinStorageFailureDomains(min));
    }
    out
}

/// Exact `validate ok` stderr line (pre-safety baseline shape).
///
/// When `safety` is omitted, validate emits this line and nothing
/// safety-related — byte-identical to the pre-change baseline.
#[must_use]
pub fn format_validate_ok(config: &ScriptureConfig) -> String {
    if config.is_multi_assignment() {
        let scribe = config.scribe.as_ref().expect("multi-assignment");
        format!(
            "scripture: validate ok version={} owner={} advertise={} backend={} prefix={} assignments={}",
            config.version,
            config.node.owner_id,
            config.node.advertise,
            config.store.backend,
            config.store.prefix.trim_end_matches('/'),
            scribe.assignments.len(),
        )
    } else {
        format!(
            "scripture: validate ok version={} owner={} advertise={} backend={} prefix={}",
            config.version,
            config.node.owner_id,
            config.node.advertise,
            config.store.backend,
            config.store.prefix.trim_end_matches('/'),
        )
    }
}

/// Exact live-preflight ok stderr line.
#[must_use]
pub fn format_live_preflight_ok(config: &ScriptureConfig) -> String {
    format!(
        "scripture: live preflight ok version={} owner={} backend={}",
        config.version, config.node.owner_id, config.store.backend,
    )
}

/// Run the shared evaluator and enforce `safety.require` when present.
///
/// When `safety` is omitted, this is a no-op (exact prior behavior).
/// Warn-mode lines go to stderr. Live-only findings fail closed (validate path).
pub fn run_static_preflight(config: &ScriptureConfig) -> Result<CapabilityReport, Box<dyn Error>> {
    run_static_preflight_to(config, &mut std::io::stderr())
}

/// Like [`run_static_preflight`], writing warn-mode lines to `sink`.
///
/// Production passes stderr; tests pass a `Vec<u8>` to assert on real emitted bytes.
pub fn run_static_preflight_to(
    config: &ScriptureConfig,
    sink: &mut dyn Write,
) -> Result<CapabilityReport, Box<dyn Error>> {
    run_static_preflight_to_with_options(config, sink, StaticPreflightOptions { defer_live: false })
}

/// Startup static gate: config-only checks, but defer
/// [`SatisfactionKind::RequiresLivePreflight`] so the live gate can score them.
pub fn run_static_preflight_deferring_live(
    config: &ScriptureConfig,
) -> Result<CapabilityReport, Box<dyn Error>> {
    run_static_preflight_to_with_options(
        config,
        &mut std::io::stderr(),
        StaticPreflightOptions { defer_live: true },
    )
}

/// Options for the hermetic static gate.
#[derive(Debug, Clone, Copy)]
pub struct StaticPreflightOptions {
    /// When true, drop findings whose kind is [`SatisfactionKind::RequiresLivePreflight`]
    /// so `serve` / `scribe run` can observe the cluster and enforce live.
    pub defer_live: bool,
}

/// Static gate with explicit options (tests + startup).
pub fn run_static_preflight_to_with_options(
    config: &ScriptureConfig,
    sink: &mut dyn Write,
    options: StaticPreflightOptions,
) -> Result<CapabilityReport, Box<dyn Error>> {
    let inputs = static_capability_inputs(config);
    let report = evaluate(&inputs);
    let Some(safety) = &config.safety else {
        return Ok(report);
    };
    enforce_safety_policy_with_options(&report, &inputs, safety, sink, options.defer_live)
}

/// Enforce a safety policy against an already-evaluated report.
///
/// Warn-mode findings are written to `sink` (stderr in production).
/// Does not defer live findings (fail-closed for validate).
#[allow(dead_code)] // Used by acceptance tests; production uses run_static_preflight.
pub fn enforce_safety_policy(
    report: &CapabilityReport,
    inputs: &CapabilityInputs,
    safety: &SafetyConfig,
    sink: &mut dyn Write,
) -> Result<CapabilityReport, Box<dyn Error>> {
    enforce_safety_policy_with_options(report, inputs, safety, sink, false)
}

/// Enforce policy; optionally defer RequiresLivePreflight findings.
pub fn enforce_safety_policy_with_options(
    report: &CapabilityReport,
    inputs: &CapabilityInputs,
    safety: &SafetyConfig,
    sink: &mut dyn Write,
    defer_live: bool,
) -> Result<CapabilityReport, Box<dyn Error>> {
    let requirements = requirements_from_safety(&safety.require);
    if requirements.is_empty() {
        return Ok(report.clone());
    }
    let mut findings = collect_requirement_findings(report, inputs, &requirements);
    if defer_live {
        findings.retain(|f| f.kind != SatisfactionKind::RequiresLivePreflight);
    }
    if findings.is_empty() {
        return Ok(report.clone());
    }
    match safety.on_degraded {
        OnDegraded::FailStart => Err(format_fail_start(&findings).into()),
        OnDegraded::Warn => {
            emit_warnings(&findings, sink);
            Ok(report.clone())
        }
    }
}

fn format_fail_start(findings: &[CapabilityFinding]) -> String {
    let mut lines = vec!["safety.require preflight failed:".to_owned()];
    for finding in findings {
        lines.push(format!("  {}", finding.format_line()));
    }
    lines.join("\n")
}

fn emit_warnings(findings: &[CapabilityFinding], sink: &mut dyn Write) {
    for finding in findings {
        bump_warning_counter(finding.code);
        let _ = writeln!(sink, "scripture: warning: {}", finding.format_line());
    }
}

/// Evaluate policy against injected inputs (pure evaluate + policy check).
///
/// The static gate takes no authority/register handle by construction.
#[must_use]
#[allow(dead_code)] // Used by acceptance tests.
pub fn evaluate_policy(
    inputs: &CapabilityInputs,
    safety: &SafetyConfig,
) -> (CapabilityReport, Vec<CapabilityFinding>) {
    let report = evaluate(inputs);
    let requirements = requirements_from_safety(&safety.require);
    let findings = collect_requirement_findings(&report, inputs, &requirements);
    (report, findings)
}

/// Live preflight from already-built [`CapabilityInputs`] (hermetic tests).
///
/// Performs no authority/register writes — pure evaluate + policy check only.
pub fn run_live_preflight_with_inputs(
    config: &ScriptureConfig,
    inputs: &CapabilityInputs,
    sink: &mut dyn Write,
) -> Result<CapabilityReport, Box<dyn Error>> {
    let report = evaluate(inputs);
    let Some(safety) = &config.safety else {
        return Ok(report);
    };
    // Live path never defers: RequiresLivePreflight should not appear when
    // candidates_declared_for_static is true (doctor/live builder sets that).
    enforce_safety_policy_with_options(&report, inputs, safety, sink, false)
}

/// Observe the cluster and enforce `safety.require` against live evidence.
///
/// When `safety` is omitted this is a no-op (doctor may still report separately).
/// Builds inputs via the same mapping as `scripture doctor` (one evidence path).
pub async fn run_live_preflight(
    config: &ScriptureConfig,
    shared: &SharedStore,
) -> Result<CapabilityReport, Box<dyn Error>> {
    run_live_preflight_to(config, shared, &mut std::io::stderr()).await
}

/// Like [`run_live_preflight`], writing warn-mode lines to `sink`.
pub async fn run_live_preflight_to(
    config: &ScriptureConfig,
    shared: &SharedStore,
    sink: &mut dyn Write,
) -> Result<CapabilityReport, Box<dyn Error>> {
    if config.safety.is_none() {
        // No policy to enforce; skip observation I/O.
        return Ok(evaluate(&static_capability_inputs(config)));
    }
    let inputs = doctor::build_live_capability_inputs(config, shared).await?;
    run_live_preflight_with_inputs(config, &inputs, sink)
}

/// CLI: `scripture preflight --config PATH [--live]`.
///
/// Without `--live`, runs the hermetic static gate (same as `validate`'s safety
/// check). With `--live`, connects the store and enforces against observed
/// directory/authority evidence.
pub async fn preflight_command(config: ScriptureConfig, live: bool) -> Result<(), Box<dyn Error>> {
    if live {
        let shared = crate::assemble::connect_shared_store(&config)?;
        run_live_preflight(&config, &shared).await?;
        eprintln!("{}", format_live_preflight_ok(&config));
    } else {
        run_static_preflight(&config)?;
        eprintln!("{}", format_validate_ok(&config));
    }
    Ok(())
}
