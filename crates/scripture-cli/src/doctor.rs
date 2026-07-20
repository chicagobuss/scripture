//! `scripture doctor` — durability/availability capability disclosure.
//!
//! Reports four failure boundaries separately. Capabilities are derived from
//! evidence (config + fleet directory + authority root), never from a replica
//! count or a Kubernetes label (foundation loudness rule 5).

use std::error::Error;
use std::sync::Arc;

use holylog::virtual_log::{ConditionalRegister, LogletResolver, VirtualLog, VirtualLogError};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::path::Path;
use scripture::OwnerId;
use scripture::serving_authority::{AuthorityState, ServingAuthorityRecord};
use scripture_runtime::ProcessLogletResolver;
use scripture_runtime::directory::{self, DirectoryRecord, RankedCandidate};
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
struct CapabilityReport {
    canon_history: CanonHistoryReport,
    producer_continuity: ProducerContinuityReport,
    scribe_availability: Vec<ScribeAvailabilityReport>,
    failure_domain_durability: FailureDomainReport,
}

#[derive(Debug, Clone, Serialize)]
struct CanonHistoryReport {
    backend: String,
    storage_targets: u32,
    summary: String,
    evidence: String,
    result: String,
}

#[derive(Debug, Clone, Serialize)]
struct ProducerContinuityReport {
    durable_local_outbox: String,
    /// Receipt profiles this node can offer (`committed` always; `spooled` only
    /// with a constructed capability that publishes a loss budget).
    receipt_profiles: String,
    /// Loss budget text when `spooled` is available; empty when not offered.
    spooled_loss_budget: String,
    evidence: String,
    result: String,
}

#[derive(Debug, Clone, Serialize)]
struct FailureDomainReport {
    independent_storage_targets: u32,
    evidence: String,
    result: String,
}

#[derive(Debug, Clone, Serialize)]
struct ScribeAvailabilityReport {
    canon: String,
    verse: String,
    /// Fresh heartbeats only — never a guaranteed cluster size.
    candidates_observed_heartbeating: u32,
    candidates: Vec<CandidateLine>,
    /// Owner named by the authority root, when Serving.
    serving_now: Option<ServingNow>,
    /// Why the candidate count is soft evidence.
    evidence: String,
    /// Empty vs expired vs fresh — mutually distinct text.
    observation: String,
    result: String,
}

#[derive(Debug, Clone, Serialize)]
struct CandidateLine {
    owner_id: String,
    posture: String,
    disposition: String,
    age_ms: u64,
    age_display: String,
}

#[derive(Debug, Clone, Serialize)]
struct ServingNow {
    owner_id: String,
    effective_writer: bool,
    state: String,
}

async fn build_report(
    config: &ScriptureConfig,
    shared: &SharedStore,
) -> Result<CapabilityReport, Box<dyn Error>> {
    let backend = config.backend()?;
    let records = directory::list_all(&shared.store, &config.store.prefix).await?;
    let now = directory::now_ms();

    let scopes = verse_scopes(config)?;
    let mut scribe_availability = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let serving = observe_serving_owner(config, shared, &scope).await?;
        scribe_availability.push(scribe_availability_for(
            &records,
            &scope.canon,
            &scope.verse,
            now,
            serving,
        ));
    }

    Ok(CapabilityReport {
        canon_history: CanonHistoryReport {
            backend: backend.label().to_owned(),
            storage_targets: 1,
            summary: format!(
                "Canon-committed records: durable on configured object-store target (backend={})",
                backend.label()
            ),
            evidence: "store.backend + single configured target in YAML".to_owned(),
            result: "history survives Scribe process restart after a Canon-committed ACK"
                .to_owned(),
        },
        producer_continuity: ProducerContinuityReport {
            // Honest: no edge outbox exists yet. Do not invent a substrate.
            durable_local_outbox: "NOT CONFIGURED".to_owned(),
            // Pre-commit spool WAL exists in-library; serve-path wiring is not
            // on by default. A `spooled` profile without a published loss budget
            // cannot construct (`ScribeSpoolCapability::validate`).
            receipt_profiles: "committed (default); spooled available only when a ScribeSpoolCapability with a published loss_budget is constructed".to_owned(),
            spooled_loss_budget: String::new(),
            evidence: "receipt vocabulary in scripture::receipt; PreCommitSpool in scripture-runtime; serve path still committed-only unless spool capability is mounted".to_owned(),
            result: "an ordinary Scribe restart stalls producers for committed-only paths. If spooled were enabled, up to loss_budget of acknowledged data could be lost if this Scribe is destroyed before upload — durability is not availability.".to_owned(),
        },
        scribe_availability,
        failure_domain_durability: FailureDomainReport {
            independent_storage_targets: 1,
            evidence: "store configuration names one backend endpoint/bucket".to_owned(),
            result: "acknowledged data and authority share one storage failure domain".to_owned(),
        },
    })
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

/// Pure scribe-availability section so tests can assert empty / stale / fresh
/// without talking to object storage.
fn scribe_availability_for(
    records: &[DirectoryRecord],
    canon: &str,
    verse: &str,
    now_ms: u64,
    serving: Option<ServingNow>,
) -> ScribeAvailabilityReport {
    let ranked = directory::rank_candidates(records, canon, verse, now_ms);
    let fresh: Vec<&RankedCandidate> = ranked.iter().filter(|c| c.fresh).collect();
    let stale_for_verse = ranked.iter().any(|c| !c.fresh);
    let ttl_secs = DIRECTORY_TTL_MS / 1000;

    let (observation, result) = if records.is_empty() {
        (
            "no node has ever published to the fleet directory".to_owned(),
            "no automatic Verse recovery after this Scribe fails; recovery evidence is absent"
                .to_owned(),
        )
    } else if fresh.is_empty() && stale_for_verse {
        (
            "every directory record naming this Verse has expired".to_owned(),
            "no automatic Verse recovery while evidence is stale; a dead Scribe can still appear for up to one TTL, and a partitioned healthy Scribe is invisible".to_owned(),
        )
    } else if fresh.is_empty() {
        (
            "no directory record names this Verse".to_owned(),
            "no automatic Verse recovery after this Scribe fails".to_owned(),
        )
    } else if fresh.len() == 1 {
        (
            format!("candidates observed heartbeating for {canon}/{verse}: 1"),
            "one candidate can recover this Verse. Post-failure redundancy would be exhausted."
                .to_owned(),
        )
    } else {
        (
            format!(
                "candidates observed heartbeating for {canon}/{verse}: {}",
                fresh.len()
            ),
            format!(
                "{} candidates observed heartbeating; one Scribe failure can still leave a recovery path",
                fresh.len()
            ),
        )
    };

    let candidates = fresh
        .iter()
        .map(|candidate| {
            let posture = posture_for(records, &candidate.owner_id, canon, verse)
                .unwrap_or_else(|| candidate.disposition.clone());
            CandidateLine {
                owner_id: candidate.owner_id.clone(),
                posture,
                disposition: candidate.disposition.clone(),
                age_ms: candidate.age_ms,
                age_display: format_age(candidate.age_ms),
            }
        })
        .collect();

    ScribeAvailabilityReport {
        canon: canon.to_owned(),
        verse: verse.to_owned(),
        candidates_observed_heartbeating: u32::try_from(fresh.len()).unwrap_or(u32::MAX),
        candidates,
        serving_now: serving,
        evidence: format!(
            "fleet directory, {ttl_secs}s TTL. A partitioned but healthy Scribe does not appear here, and a dead Scribe can appear for up to one TTL."
        ),
        observation,
        result,
    }
}

fn posture_for(
    records: &[DirectoryRecord],
    owner_id: &str,
    canon: &str,
    verse: &str,
) -> Option<String> {
    records
        .iter()
        .find(|r| r.owner_id == owner_id)
        .and_then(|r| {
            r.assignments
                .iter()
                .find(|a| a.canon == canon && a.verse == verse)
                .map(|a| a.posture.clone())
        })
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
    use scripture_runtime::directory::DirectoryAssignment;

    fn record(owner: &str, published_at_ms: u64, posture: &str) -> DirectoryRecord {
        DirectoryRecord {
            format_version: 1,
            owner_id: owner.to_owned(),
            node_advertise: format!("tcp://{owner}:9000"),
            published_at_ms,
            valid_for_ms: DIRECTORY_TTL_MS,
            assignments: vec![DirectoryAssignment {
                canon: "telemetry-cnon!!".to_owned(),
                verse: "telemetry-host-a".to_owned(),
                advertise: format!("tcp://{owner}:9001"),
                posture: posture.to_owned(),
                disposition: "Standby".to_owned(),
                admits_committed_acks: false,
            }],
        }
    }

    #[test]
    fn empty_directory_differs_from_stale_and_from_fresh() {
        let now = 100_000u64;
        let empty = scribe_availability_for(&[], "telemetry-cnon!!", "telemetry-host-a", now, None);
        let stale = scribe_availability_for(
            &[record("scripture-own-b!", 1_000, "standby")],
            "telemetry-cnon!!",
            "telemetry-host-a",
            now,
            None,
        );
        let fresh = scribe_availability_for(
            &[record("scripture-own-b!", now - 2_400, "standby")],
            "telemetry-cnon!!",
            "telemetry-host-a",
            now,
            Some(ServingNow {
                owner_id: "scripture-own-a!".to_owned(),
                effective_writer: true,
                state: "Serving".to_owned(),
            }),
        );

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
        let fresh_text = format_human(&CapabilityReport {
            canon_history: dummy_canon(),
            producer_continuity: dummy_producer(),
            scribe_availability: vec![fresh.clone()],
            failure_domain_durability: dummy_failure(),
        });

        // Materially different text — not the same number restated three ways.
        assert!(empty_text.contains("no node has ever published"));
        assert!(!empty_text.contains("has expired"));
        assert!(!empty_text.contains("candidates observed heartbeating"));

        assert!(stale_text.contains("every directory record naming this Verse has expired"));
        assert!(!stale_text.contains("no node has ever published"));
        assert!(!stale_text.contains("candidates observed heartbeating"));

        assert!(
            fresh_text.contains(
                "candidates observed heartbeating for telemetry-cnon!!/telemetry-host-a: 1"
            )
        );
        assert!(fresh_text.contains("scripture-own-b!  standby  last heartbeat"));
        assert!(fresh_text.contains("serving now: scripture-own-a! (effective_writer=true)"));
        assert!(fresh_text.contains("Post-failure redundancy would be exhausted"));
        assert!(!fresh_text.contains("no node has ever published"));
        assert!(!fresh_text.contains("has expired"));

        assert_ne!(empty.observation, stale.observation);
        assert_ne!(stale.observation, fresh.observation);
        assert_ne!(empty.observation, fresh.observation);
        assert_eq!(empty.candidates_observed_heartbeating, 0);
        assert_eq!(stale.candidates_observed_heartbeating, 0);
        assert_eq!(fresh.candidates_observed_heartbeating, 1);
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
