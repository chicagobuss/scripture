//! `scripture doctor` — durability/availability capability disclosure.
//!
//! Reports four failure boundaries separately. Capabilities are derived from
//! evidence (config + fleet directory + authority root), never from a replica
//! count or a Kubernetes label (foundation loudness rule 5).
//!
//! The typed satisfaction source is the shared pure
//! [`scripture_runtime::evaluate`]; this module builds live [`CapabilityInputs`]
//! and renders the existing human/JSON disclosure shape.

use std::error::Error;
use std::sync::Arc;

use holylog::virtual_log::{ConditionalRegister, LogletResolver, VirtualLog, VirtualLogError};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::path::Path;
use scripture::OwnerId;
use scripture::serving_authority::{AuthorityState, ServingAuthorityRecord};
use scripture_runtime::ProcessLogletResolver;
use scripture_runtime::directory::{self, DirectoryRecord};
use scripture_runtime::{
    CapabilityInputs, CapabilityReport as TypedCapabilityReport, RecoveryCandidateEvidence,
    ServingNowEvidence, VerseCapabilityInputs, VerseRecoverySatisfaction, evaluate,
};
use serde::Serialize;

use crate::assemble::{self, SharedStore};
use crate::config::{AssignmentConfig, ScriptureConfig};

/// Publication TTL used by the multi-assignment heartbeat. Doctor reports this
/// as the evidence window; it is not re-derived from config because the
/// directory is soft state and the window is a publication choice.
const DIRECTORY_TTL_MS: u64 = 15_000;

/// Output encoding for the capability report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorFormat {
    /// Operator-facing multi-line text (default).
    Human,
    /// Same content as structured JSON.
    Json,
}

/// Runs `scripture doctor` for the configured node.
pub async fn doctor(config: ScriptureConfig, format: DoctorFormat) -> Result<(), Box<dyn Error>> {
    let shared = assemble::connect_shared_store(&config)?;
    let report = build_report(&config, &shared).await?;
    match format {
        DoctorFormat::Human => print!("{}", format_human(&report)),
        DoctorFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityReport {
    pub canon_history: CanonHistoryReport,
    pub producer_continuity: ProducerContinuityReport,
    pub scribe_availability: Vec<ScribeAvailabilityReport>,
    pub failure_domain_durability: FailureDomainReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct CanonHistoryReport {
    pub backend: String,
    pub storage_targets: u32,
    pub summary: String,
    pub evidence: String,
    pub result: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProducerContinuityReport {
    pub durable_local_outbox: String,
    /// Receipt profiles this node can offer (`committed` always; `spooled` only
    /// with a constructed capability that publishes a loss budget).
    pub receipt_profiles: String,
    /// Loss budget text when `spooled` is available; empty when not offered.
    pub spooled_loss_budget: String,
    pub evidence: String,
    pub result: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailureDomainReport {
    pub independent_storage_targets: u32,
    pub evidence: String,
    pub result: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScribeAvailabilityReport {
    pub canon: String,
    pub verse: String,
    /// Fresh heartbeats only — never a guaranteed cluster size.
    pub candidates_observed_heartbeating: u32,
    pub candidates: Vec<CandidateLine>,
    /// Owner named by the authority root, when Serving.
    pub serving_now: Option<ServingNow>,
    /// Why the candidate count is soft evidence.
    pub evidence: String,
    /// Empty vs expired vs fresh — mutually distinct text.
    pub observation: String,
    /// Recovery verdict from the shared evaluator's Satisfaction consequence.
    pub result: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidateLine {
    pub owner_id: String,
    pub posture: String,
    pub disposition: String,
    pub age_ms: u64,
    pub age_display: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServingNow {
    pub owner_id: String,
    pub effective_writer: bool,
    pub state: String,
}

/// Doctor disclosure from injected [`CapabilityInputs`] (no I/O).
///
/// Runs the shared pure evaluator, then renders via the same path as live
/// `scripture doctor`. Acceptance tests use this to assert doctor recovery
/// status derives from evaluator `scribe_recovery` Satisfaction (no drift).
#[cfg(test)]
pub fn disclose_from_inputs(inputs: &CapabilityInputs) -> CapabilityReport {
    let typed = evaluate(inputs);
    render_disclosure(inputs, &typed)
}

async fn build_report(
    config: &ScriptureConfig,
    shared: &SharedStore,
) -> Result<CapabilityReport, Box<dyn Error>> {
    let inputs = build_live_capability_inputs(config, shared).await?;
    let typed = evaluate(&inputs);
    Ok(render_disclosure(&inputs, &typed))
}

/// Assemble live capability evidence (I/O). Pure evaluation is separate.
///
/// Shared by `scripture doctor` and live preflight so evidence mapping cannot
/// drift between disclosure and enforcement.
pub async fn build_live_capability_inputs(
    config: &ScriptureConfig,
    shared: &SharedStore,
) -> Result<CapabilityInputs, Box<dyn Error>> {
    let backend = config.backend()?;
    let records = directory::list_all(&shared.store, &config.store.prefix).await?;
    let now = directory::now_ms();
    let scopes = verse_scopes(config)?;
    let mut verses = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let serving = observe_serving_owner(config, shared, &scope).await?;
        let candidates = candidates_from_directory(&records, &scope.canon, &scope.verse, now);
        verses.push(VerseCapabilityInputs {
            canon: scope.canon,
            verse: scope.verse,
            candidates,
            serving_now: serving.map(|s| ServingNowEvidence {
                owner_id: s.owner_id,
                effective_writer: s.effective_writer,
                state: s.state,
            }),
            // Live path always has observed evidence (even if empty directory).
            candidates_declared_for_static: true,
            fleet_directory_nonempty: !records.is_empty(),
        });
    }
    let durable = config.durable_producer_spool_configured();
    let (producer_spool_loss_budget, producer_spool_scribe_id) =
        match config.validated_producer_spool() {
            Ok(Some(cap)) => (
                format!("{}s", cap.loss_budget.as_secs()),
                cap.scribe_id.clone(),
            ),
            _ => (String::new(), String::new()),
        };
    Ok(CapabilityInputs {
        backend_label: backend.label().to_owned(),
        independent_storage_targets: 1,
        committed_capable_target: true,
        // Config-derived; live observation does not invent a spool.
        durable_producer_spool_configured: durable,
        producer_spool_loss_budget,
        producer_spool_scribe_id,
        verses,
    })
}

fn candidates_from_directory(
    records: &[DirectoryRecord],
    canon: &str,
    verse: &str,
    now_ms: u64,
) -> Vec<RecoveryCandidateEvidence> {
    let mut out = Vec::new();
    for record in records {
        for assignment in &record.assignments {
            if assignment.canon != canon || assignment.verse != verse {
                continue;
            }
            out.push(RecoveryCandidateEvidence {
                owner_id: record.owner_id.clone(),
                canon: assignment.canon.clone(),
                verse: assignment.verse.clone(),
                serving_capable: assignment.admits_committed_acks,
                fresh: record.is_fresh_at(now_ms),
                age_ms: record.age_ms_at(now_ms),
                posture: assignment.posture.clone(),
                disposition: assignment.disposition.clone(),
            });
        }
    }
    out
}

fn render_disclosure(inputs: &CapabilityInputs, typed: &TypedCapabilityReport) -> CapabilityReport {
    debug_assert_eq!(inputs.verses.len(), typed.scribe_recovery.len());
    let scribe_availability = inputs
        .verses
        .iter()
        .zip(typed.scribe_recovery.iter())
        .map(|(verse, recovery)| scribe_availability_for(verse, recovery))
        .collect();

    CapabilityReport {
        canon_history: CanonHistoryReport {
            backend: inputs.backend_label.clone(),
            storage_targets: inputs.independent_storage_targets,
            summary: format!(
                "Canon-committed records: durable on configured object-store target (backend={})",
                inputs.backend_label
            ),
            evidence: "store.backend + single configured target in YAML".to_owned(),
            result: if inputs.committed_capable_target {
                "history survives Scribe process restart after a Canon-committed ACK".to_owned()
            } else {
                typed.canon_history.consequence.clone()
            },
        },
        producer_continuity: ProducerContinuityReport {
            durable_local_outbox: if inputs.durable_producer_spool_configured {
                "CONFIGURED".to_owned()
            } else {
                "NOT CONFIGURED".to_owned()
            },
            receipt_profiles: if inputs.durable_producer_spool_configured {
                "committed (default); spooled available via configured producer_spool + ProducerOutbox"
                    .to_owned()
            } else {
                "committed (default); spooled available only when producer_spool is configured"
                    .to_owned()
            },
            spooled_loss_budget: if inputs.durable_producer_spool_configured {
                inputs.producer_spool_loss_budget.clone()
            } else {
                String::new()
            },
            evidence: if inputs.durable_producer_spool_configured {
                format!(
                    "producer_spool config (scribe_id={}) + ProducerOutbox; scope=one named Scribe local disk",
                    inputs.producer_spool_scribe_id
                )
            } else {
                "producer_spool absent; ProducerOutbox cannot issue spooled receipts without a validated local spool"
                    .to_owned()
            },
            result: if inputs.durable_producer_spool_configured {
                typed.producer_continuity.consequence.clone()
            } else {
                "an ordinary Scribe restart stalls producers for committed-only paths. If spooled were enabled, up to loss_budget of acknowledged data could be lost if this Scribe is destroyed before upload — durability is not availability.".to_owned()
            },
        },
        scribe_availability,
        failure_domain_durability: FailureDomainReport {
            independent_storage_targets: inputs.independent_storage_targets,
            evidence: "store configuration names one backend endpoint/bucket".to_owned(),
            result: typed.failure_domains.consequence.clone(),
        },
    }
}

struct VerseScope {
    canon: String,
    verse: String,
    /// Multi-assignment entry when present; single-assignment uses process root.
    assignment: Option<AssignmentConfig>,
}

fn verse_scopes(config: &ScriptureConfig) -> Result<Vec<VerseScope>, Box<dyn Error>> {
    if config.is_multi_assignment() {
        let scribe = config
            .scribe
            .as_ref()
            .ok_or("doctor: multi-assignment config missing scribe.assignments")?;
        return Ok(scribe
            .assignments
            .iter()
            .map(|assignment| VerseScope {
                canon: assignment.canon.clone(),
                verse: assignment.verse.clone(),
                assignment: Some(assignment.clone()),
            })
            .collect());
    }
    let verse = config
        .verse
        .as_ref()
        .ok_or("doctor requires verse or scribe.assignments")?;
    Ok(vec![VerseScope {
        canon: verse.journal_id.clone(),
        verse: verse.verse_id.clone(),
        assignment: None,
    }])
}

/// Reads the authority root for who may commit — never trusts directory disposition.
async fn observe_serving_owner(
    config: &ScriptureConfig,
    shared: &SharedStore,
    scope: &VerseScope,
) -> Result<Option<ServingNow>, Box<dyn Error>> {
    let store_root = match &scope.assignment {
        Some(assignment) => config.assignment_store_root(assignment)?,
        None => config.store.prefix.trim_end_matches('/').to_owned(),
    };
    let register = Arc::new(ObjectStoreConditionalRegister::new(
        Arc::clone(&shared.store),
        Path::from(store_root).join(register_path("verse").as_ref()),
        shared.backend.register_capabilities(),
    )?) as Arc<dyn ConditionalRegister>;
    let resolver = Arc::new(ProcessLogletResolver::default()) as Arc<dyn LogletResolver>;
    let virtual_log = VirtualLog::new(register, resolver);

    let observed = match virtual_log.observe_membership().await {
        Ok(observed) => observed,
        Err(VirtualLogError::Uninitialized) => return Ok(None),
        Err(error) => return Err(format!("authority root observe failed: {error}").into()),
    };
    if observed.state.application_fence.as_bytes().is_empty() {
        return Ok(None);
    }
    let record =
        match ServingAuthorityRecord::decode_application_fence(&observed.state.application_fence) {
            Ok(record) => record,
            Err(_) => return Ok(None),
        };

    match &record.state {
        AuthorityState::Serving { authority, .. } => {
            let owner = authority.owner_id;
            // Ask whether this root record would grant effective writership to
            // the named owner if that owner holds an unsealed writable. Doctor
            // is an observer and does not hold that writable itself.
            let effective = record.is_effective_writer(&observed.state, owner, true, false);
            Ok(Some(ServingNow {
                owner_id: owner_display(owner),
                effective_writer: effective,
                state: "Serving".to_owned(),
            }))
        }
        AuthorityState::Transitioning { .. } => Ok(Some(ServingNow {
            owner_id: "(transitioning)".to_owned(),
            effective_writer: false,
            state: "Transitioning".to_owned(),
        })),
        AuthorityState::Unassigned => Ok(None),
        AuthorityState::ReconciliationRequired { .. } => Ok(Some(ServingNow {
            owner_id: "(reconciliation-required)".to_owned(),
            effective_writer: false,
            state: "ReconciliationRequired".to_owned(),
        })),
    }
}

fn owner_display(owner: OwnerId) -> String {
    // Owner ids are 16 ASCII bytes in product configs; hex Display is for fences.
    String::from_utf8_lossy(&owner.as_bytes()).into_owned()
}

/// Scribe-availability section: observation from directory evidence; recovery
/// **result** from the shared evaluator's [`VerseRecoverySatisfaction`] only.
///
/// Fresh heartbeats remain soft display evidence. Eligibility (fresh ∧
/// serving_capable) is decided solely by the evaluator — never recomputed here.
fn scribe_availability_for(
    verse_inputs: &VerseCapabilityInputs,
    recovery: &VerseRecoverySatisfaction,
) -> ScribeAvailabilityReport {
    let canon = verse_inputs.canon.as_str();
    let verse = verse_inputs.verse.as_str();
    let fresh: Vec<&RecoveryCandidateEvidence> = verse_inputs
        .candidates
        .iter()
        .filter(|c| c.canon == canon && c.verse == verse && c.fresh)
        .collect();
    let stale_for_verse = verse_inputs
        .candidates
        .iter()
        .any(|c| c.canon == canon && c.verse == verse && !c.fresh);
    let ttl_secs = DIRECTORY_TTL_MS / 1000;

    let observation =
        if !verse_inputs.fleet_directory_nonempty && verse_inputs.candidates.is_empty() {
            "no node has ever published to the fleet directory".to_owned()
        } else if fresh.is_empty() && stale_for_verse {
            "every directory record naming this Verse has expired".to_owned()
        } else if fresh.is_empty() {
            "no directory record names this Verse".to_owned()
        } else {
            format!(
                "candidates observed heartbeating for {canon}/{verse}: {}",
                fresh.len()
            )
        };

    // Recovery verdict: evaluator Satisfaction only (no second computation).
    let result = recovery.satisfaction.consequence.clone();

    let candidates = fresh
        .iter()
        .map(|candidate| CandidateLine {
            owner_id: candidate.owner_id.clone(),
            posture: candidate.posture.clone(),
            disposition: candidate.disposition.clone(),
            age_ms: candidate.age_ms,
            age_display: format_age(candidate.age_ms),
        })
        .collect();

    let serving_now = verse_inputs.serving_now.as_ref().map(|s| ServingNow {
        owner_id: s.owner_id.clone(),
        effective_writer: s.effective_writer,
        state: s.state.clone(),
    });

    ScribeAvailabilityReport {
        canon: canon.to_owned(),
        verse: verse.to_owned(),
        candidates_observed_heartbeating: recovery.candidates_observed_heartbeating,
        candidates,
        serving_now,
        evidence: format!(
            "fleet directory, {ttl_secs}s TTL. A partitioned but healthy Scribe does not appear here, and a dead Scribe can appear for up to one TTL. Recovery eligibility requires fresh ∧ serving_capable (shared evaluator)."
        ),
        observation,
        result,
    }
}

fn format_age(age_ms: u64) -> String {
    if age_ms < 60_000 {
        let secs = age_ms as f64 / 1000.0;
        format!("{secs:.1}s ago")
    } else {
        format!("{}m ago", age_ms / 60_000)
    }
}

fn format_human(report: &CapabilityReport) -> String {
    let mut out = String::new();
    out.push_str("SCRIPTURE CAPABILITY REPORT\n");
    out.push('\n');

    out.push_str("Canon history durability:\n");
    out.push_str(&format!("  {}\n", report.canon_history.summary));
    out.push_str(&format!(
        "  storage targets: {}\n",
        report.canon_history.storage_targets
    ));
    out.push_str(&format!("  Evidence: {}\n", report.canon_history.evidence));
    out.push_str(&format!("  Result: {}\n", report.canon_history.result));
    out.push('\n');

    out.push_str("Producer continuity:\n");
    out.push_str(&format!(
        "  durable local outbox: {}\n",
        report.producer_continuity.durable_local_outbox
    ));
    out.push_str(&format!(
        "  receipt profiles: {}\n",
        report.producer_continuity.receipt_profiles
    ));
    if !report.producer_continuity.spooled_loss_budget.is_empty() {
        out.push_str(&format!(
            "  spooled loss budget: {}\n",
            report.producer_continuity.spooled_loss_budget
        ));
    }
    out.push_str(&format!(
        "  Evidence: {}\n",
        report.producer_continuity.evidence
    ));
    out.push_str(&format!(
        "  Result: {}\n",
        report.producer_continuity.result
    ));
    out.push('\n');

    for availability in &report.scribe_availability {
        out.push_str("Scribe availability:\n");
        out.push_str(&format!("  {}\n", availability.observation));
        for candidate in &availability.candidates {
            out.push_str(&format!(
                "    {}  {}  last heartbeat {}\n",
                candidate.owner_id, candidate.posture, candidate.age_display
            ));
        }
        match &availability.serving_now {
            Some(serving) => out.push_str(&format!(
                "  serving now: {} (effective_writer={})\n",
                serving.owner_id, serving.effective_writer
            )),
            None => out.push_str("  serving now: (none — authority root empty or unassigned)\n"),
        }
        out.push_str(&format!("  Evidence: {}\n", availability.evidence));
        out.push_str(&format!("  Result: {}\n", availability.result));
        out.push('\n');
    }

    out.push_str("Failure-domain durability:\n");
    out.push_str(&format!(
        "  configured independent storage targets: {}\n",
        report.failure_domain_durability.independent_storage_targets
    ));
    out.push_str(&format!(
        "  Evidence: {}\n",
        report.failure_domain_durability.evidence
    ));
    out.push_str(&format!(
        "  Result: {}\n",
        report.failure_domain_durability.result
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use scripture_runtime::SatisfactionKind;

    fn verse_inputs(
        candidates: Vec<RecoveryCandidateEvidence>,
        serving: Option<ServingNowEvidence>,
    ) -> VerseCapabilityInputs {
        let fleet_directory_nonempty = !candidates.is_empty();
        VerseCapabilityInputs {
            canon: "telemetry-cnon!!".to_owned(),
            verse: "telemetry-host-a".to_owned(),
            candidates,
            serving_now: serving,
            candidates_declared_for_static: true,
            fleet_directory_nonempty,
        }
    }

    fn record_candidate(
        owner: &str,
        age_ms: u64,
        fresh: bool,
        posture: &str,
        serving_capable: bool,
    ) -> RecoveryCandidateEvidence {
        RecoveryCandidateEvidence {
            owner_id: owner.to_owned(),
            canon: "telemetry-cnon!!".to_owned(),
            verse: "telemetry-host-a".to_owned(),
            serving_capable,
            fresh,
            age_ms,
            posture: posture.to_owned(),
            disposition: if serving_capable {
                "Serving".to_owned()
            } else {
                "Standby".to_owned()
            },
        }
    }

    fn availability_for(inputs: VerseCapabilityInputs) -> ScribeAvailabilityReport {
        let package = CapabilityInputs {
            backend_label: "rustfs".to_owned(),
            independent_storage_targets: 1,
            committed_capable_target: true,
            durable_producer_spool_configured: false,
            producer_spool_loss_budget: String::new(),
            producer_spool_scribe_id: String::new(),
            verses: vec![inputs],
        };
        disclose_from_inputs(&package)
            .scribe_availability
            .into_iter()
            .next()
            .expect("one verse")
    }

    #[test]
    fn empty_directory_differs_from_stale_and_from_fresh() {
        let empty = availability_for(verse_inputs(vec![], None));
        let stale = availability_for(verse_inputs(
            vec![record_candidate(
                "scripture-own-b!",
                99_000,
                false,
                "standby",
                false,
            )],
            None,
        ));
        // Fresh but NOT serving-capable (standby): heartbeats visible, not recoverable.
        let fresh_standby = availability_for(verse_inputs(
            vec![record_candidate(
                "scripture-own-b!",
                2_400,
                true,
                "standby",
                false,
            )],
            Some(ServingNowEvidence {
                owner_id: "scripture-own-a!".to_owned(),
                effective_writer: true,
                state: "Serving".to_owned(),
            }),
        ));
        // Fresh ∧ serving-capable: recoverable via shared evaluator.
        let fresh_serving = availability_for(verse_inputs(
            vec![record_candidate(
                "scripture-own-b!",
                2_400,
                true,
                "bootstrap-if-empty",
                true,
            )],
            Some(ServingNowEvidence {
                owner_id: "scripture-own-a!".to_owned(),
                effective_writer: true,
                state: "Serving".to_owned(),
            }),
        ));

        let empty_text = format_human(&CapabilityReport {
            canon_history: dummy_canon(),
            producer_continuity: dummy_producer(),
            scribe_availability: vec![empty.clone()],
            failure_domain_durability: dummy_failure(),
        });
        let stale_text = format_human(&CapabilityReport {
            canon_history: dummy_canon(),
            producer_continuity: dummy_producer(),
            scribe_availability: vec![stale.clone()],
            failure_domain_durability: dummy_failure(),
        });
        let fresh_standby_text = format_human(&CapabilityReport {
            canon_history: dummy_canon(),
            producer_continuity: dummy_producer(),
            scribe_availability: vec![fresh_standby.clone()],
            failure_domain_durability: dummy_failure(),
        });
        let fresh_serving_text = format_human(&CapabilityReport {
            canon_history: dummy_canon(),
            producer_continuity: dummy_producer(),
            scribe_availability: vec![fresh_serving.clone()],
            failure_domain_durability: dummy_failure(),
        });

        assert!(empty_text.contains("no node has ever published"));
        assert!(!empty_text.contains("has expired"));
        assert!(!empty_text.contains("candidates observed heartbeating"));
        assert!(empty.result.contains("no automatic Verse recovery"));

        assert!(stale_text.contains("every directory record naming this Verse has expired"));
        assert!(!stale_text.contains("no node has ever published"));
        assert!(!stale_text.contains("candidates observed heartbeating"));
        assert!(stale.result.contains("no automatic Verse recovery"));

        assert!(
            fresh_standby_text.contains(
                "candidates observed heartbeating for telemetry-cnon!!/telemetry-host-a: 1"
            )
        );
        assert!(fresh_standby_text.contains("scripture-own-b!  standby  last heartbeat"));
        assert!(
            fresh_standby_text.contains("serving now: scripture-own-a! (effective_writer=true)")
        );
        // Drift guard: fresh-but-standby must NOT be reported as recoverable.
        assert!(fresh_standby.result.contains("no automatic Verse recovery"));
        assert!(!fresh_standby.result.contains("can recover"));
        assert!(!fresh_standby_text.contains("Post-failure redundancy would be exhausted"));

        assert!(
            fresh_serving_text.contains(
                "candidates observed heartbeating for telemetry-cnon!!/telemetry-host-a: 1"
            )
        );
        assert!(
            fresh_serving
                .result
                .contains("eligible candidate can recover")
        );
        assert!(
            fresh_serving
                .result
                .contains("post-failure redundancy would be exhausted")
        );

        assert_ne!(empty.observation, stale.observation);
        assert_ne!(stale.observation, fresh_standby.observation);
        assert_ne!(empty.observation, fresh_standby.observation);
        assert_eq!(empty.candidates_observed_heartbeating, 0);
        assert_eq!(stale.candidates_observed_heartbeating, 0);
        assert_eq!(fresh_standby.candidates_observed_heartbeating, 1);
        assert_eq!(fresh_serving.candidates_observed_heartbeating, 1);

        // Doctor result bytes equal evaluator consequence (shared source).
        let package = CapabilityInputs {
            backend_label: "rustfs".to_owned(),
            independent_storage_targets: 1,
            committed_capable_target: true,
            durable_producer_spool_configured: false,
            producer_spool_loss_budget: String::new(),
            producer_spool_scribe_id: String::new(),
            verses: vec![verse_inputs(
                vec![record_candidate(
                    "scripture-own-b!",
                    2_400,
                    true,
                    "standby",
                    false,
                )],
                None,
            )],
        };
        let typed = evaluate(&package);
        assert_eq!(
            typed.scribe_recovery[0].satisfaction.kind,
            SatisfactionKind::Unsatisfied
        );
        assert_eq!(
            fresh_standby.result,
            typed.scribe_recovery[0].satisfaction.consequence
        );
    }

    #[test]
    fn producer_continuity_is_explicitly_not_configured() {
        let report = CapabilityReport {
            canon_history: dummy_canon(),
            producer_continuity: ProducerContinuityReport {
                durable_local_outbox: "NOT CONFIGURED".to_owned(),
                receipt_profiles: "committed".to_owned(),
                spooled_loss_budget: String::new(),
                evidence: "no durable producer outbox exists in this codebase yet".to_owned(),
                result: "stalls".to_owned(),
            },
            scribe_availability: vec![],
            failure_domain_durability: dummy_failure(),
        };
        let text = format_human(&report);
        assert!(text.contains("durable local outbox: NOT CONFIGURED"));
    }

    fn dummy_canon() -> CanonHistoryReport {
        CanonHistoryReport {
            backend: "rustfs".to_owned(),
            storage_targets: 1,
            summary: "durable".to_owned(),
            evidence: "store".to_owned(),
            result: "ok".to_owned(),
        }
    }

    fn dummy_producer() -> ProducerContinuityReport {
        ProducerContinuityReport {
            durable_local_outbox: "NOT CONFIGURED".to_owned(),
            receipt_profiles: "committed".to_owned(),
            spooled_loss_budget: String::new(),
            evidence: "none".to_owned(),
            result: "stalls".to_owned(),
        }
    }

    fn dummy_failure() -> FailureDomainReport {
        FailureDomainReport {
            independent_storage_targets: 1,
            evidence: "one".to_owned(),
            result: "one domain".to_owned(),
        }
    }
}
