//! Typed durability/availability capability evaluation.
//!
//! Pure: [`evaluate`] takes [`CapabilityInputs`] and returns a [`CapabilityReport`].
//! No I/O, no authority/register mutation. Callers (static preflight, live
//! doctor, live preflight gate) differ only in how they build inputs.

use std::fmt;

use serde::Serialize;

/// Stable capability codes for warnings and fail-start errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum CapabilityCode {
    /// Canon history durability boundary.
    CanonHistory,
    /// Producer continuity (durable spool/outbox) boundary.
    ProducerContinuity,
    /// Automatic Scribe recovery / availability boundary.
    ScribeRecovery,
    /// Independent storage failure-domain count.
    StorageFailureDomains,
}

impl CapabilityCode {
    /// Wire / stderr token `SCRIPTURE_CAP_<BOUNDARY>`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CanonHistory => "SCRIPTURE_CAP_CANON_HISTORY",
            Self::ProducerContinuity => "SCRIPTURE_CAP_PRODUCER_CONTINUITY",
            Self::ScribeRecovery => "SCRIPTURE_CAP_SCRIBE_RECOVERY",
            Self::StorageFailureDomains => "SCRIPTURE_CAP_STORAGE_FAILURE_DOMAINS",
        }
    }
}

impl fmt::Display for CapabilityCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Scope attached to a satisfaction / finding.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct CapabilityScope {
    /// Canon id when verse-scoped; empty for deployment-wide findings.
    pub canon: String,
    /// Verse id when verse-scoped; empty for deployment-wide findings.
    pub verse: String,
}

impl CapabilityScope {
    /// Deployment-wide (not Verse-scoped).
    #[must_use]
    pub fn deployment() -> Self {
        Self {
            canon: String::new(),
            verse: String::new(),
        }
    }

    /// Exact `(Canon, Verse)` scope.
    #[must_use]
    pub fn verse(canon: impl Into<String>, verse: impl Into<String>) -> Self {
        Self {
            canon: canon.into(),
            verse: verse.into(),
        }
    }
}

/// Whether a named guarantee is met given injected evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SatisfactionKind {
    /// Evidence satisfies the boundary.
    Satisfied,
    /// Evidence shows the boundary cannot be met.
    Unsatisfied,
    /// Boundary needs live cluster observation; static evidence is insufficient.
    RequiresLivePreflight,
}

/// Per-boundary typed satisfaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Satisfaction {
    /// Satisfied / unsatisfied / needs live preflight.
    pub kind: SatisfactionKind,
    /// Stable code when not satisfied (warnings and fail-start).
    pub code: Option<CapabilityCode>,
    /// Affected scope.
    pub scope: CapabilityScope,
    /// Observed configuration / evidence fact.
    pub observed: String,
    /// Consequence of the observed fact.
    pub consequence: String,
    /// Actionable remediation when not satisfied.
    pub remediation: Option<String>,
}

/// Typed evidence for one recovery candidate (injected; never invented).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecoveryCandidateEvidence {
    /// Publishing owner id.
    pub owner_id: String,
    /// Canon this candidate claims.
    pub canon: String,
    /// Verse this candidate claims.
    pub verse: String,
    /// Serving-capable role (admits committed ACKs / active serving). Not a
    /// dormant or non-ACKing entry. No operator promote/standby concept.
    pub serving_capable: bool,
    /// Fresh within the configured staleness bound.
    pub fresh: bool,
    /// Age since last heartbeat, milliseconds (display / ranking).
    pub age_ms: u64,
    /// Configured posture string from directory (display only).
    pub posture: String,
    /// Disposition hint from directory (display only).
    pub disposition: String,
}

/// Optional authority-root observation for disclosure (not a grant).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServingNowEvidence {
    /// Owner named by the Serving fence, when present.
    pub owner_id: String,
    /// Whether that owner would be an effective writer if holding a writable.
    pub effective_writer: bool,
    /// Authority state label (`Serving`, `Transitioning`, …).
    pub state: String,
}

/// Per-Verse injected evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerseCapabilityInputs {
    /// Canon identity.
    pub canon: String,
    /// Verse identity.
    pub verse: String,
    /// All observed / declared candidates naming any Verse (filter in evaluate).
    pub candidates: Vec<RecoveryCandidateEvidence>,
    /// Authority-root serving observation when available.
    pub serving_now: Option<ServingNowEvidence>,
    /// True when candidates were supplied for static scoring (config-declared).
    /// When false and no live candidates exist, automatic recovery is
    /// [`SatisfactionKind::RequiresLivePreflight`].
    pub candidates_declared_for_static: bool,
    /// True when the fleet directory had any records (live path). Distinguishes
    /// "never published" from "no record names this Verse".
    pub fleet_directory_nonempty: bool,
}

/// Typed evidence package for [`evaluate`]. No I/O.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CapabilityInputs {
    /// Object-store backend label (`rustfs`, `r2`, `s3`, …).
    pub backend_label: String,
    /// Count of configured independent storage targets / failure domains.
    pub independent_storage_targets: u32,
    /// Whether a committed-capable object-store target is configured.
    pub committed_capable_target: bool,
    /// Whether a durable producer/edge spool (outbox) is configured.
    pub durable_producer_spool_configured: bool,
    /// Published spool loss budget disclosure (e.g. `"30s"`); empty when not configured.
    pub producer_spool_loss_budget: String,
    /// Named Scribe identity for the configured spool; empty when not configured.
    pub producer_spool_scribe_id: String,
    /// Per-Verse scopes to evaluate.
    pub verses: Vec<VerseCapabilityInputs>,
}

/// Per-Verse recovery section of the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerseRecoverySatisfaction {
    /// Canon identity.
    pub canon: String,
    /// Verse identity.
    pub verse: String,
    /// Automatic-recovery satisfaction for this Verse.
    pub satisfaction: Satisfaction,
    /// Fresh heartbeats observed (soft evidence; may include non-eligible).
    pub candidates_observed_heartbeating: u32,
    /// Candidates that pass [`is_eligible_recovery_candidate`].
    pub eligible_candidates: u32,
    /// Serving observation when present.
    pub serving_now: Option<ServingNowEvidence>,
}

/// Full typed capability report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CapabilityReport {
    /// Canon history durability.
    pub canon_history: Satisfaction,
    /// Producer continuity.
    pub producer_continuity: Satisfaction,
    /// Per-Verse scribe recovery / availability.
    pub scribe_recovery: Vec<VerseRecoverySatisfaction>,
    /// Independent storage failure-domain durability.
    pub failure_domains: Satisfaction,
}

/// Predicate: eligible recovery candidate for a target Verse.
///
/// A directory/authority entry is eligible iff it (a) targets that exact
/// `(Canon, Verse)`, (b) is a serving-capable role (not dormant / non-ACKing),
/// and (c) is fresh within the staleness bound.
#[must_use]
pub fn is_eligible_recovery_candidate(
    candidate: &RecoveryCandidateEvidence,
    target_canon: &str,
    target_verse: &str,
) -> bool {
    candidate.canon == target_canon
        && candidate.verse == target_verse
        && candidate.serving_capable
        && candidate.fresh
}

/// Pure capability evaluator. No I/O; no authority mutation.
#[must_use]
pub fn evaluate(inputs: &CapabilityInputs) -> CapabilityReport {
    let canon_history = if inputs.committed_capable_target {
        Satisfaction {
            kind: SatisfactionKind::Satisfied,
            code: None,
            scope: CapabilityScope::deployment(),
            observed: format!(
                "committed-capable object-store target configured (backend={})",
                inputs.backend_label
            ),
            consequence:
                "Canon-committed records are durable on the configured object-store target"
                    .to_owned(),
            remediation: None,
        }
    } else {
        Satisfaction {
            kind: SatisfactionKind::Unsatisfied,
            code: Some(CapabilityCode::CanonHistory),
            scope: CapabilityScope::deployment(),
            observed: "no committed-capable object-store target configured".to_owned(),
            consequence: "Canon history durability cannot be claimed".to_owned(),
            remediation: Some(
                "configure a committed-capable object-store backend target (store.backend + endpoint/bucket/prefix)"
                    .to_owned(),
            ),
        }
    };

    let producer_continuity = if inputs.durable_producer_spool_configured {
        Satisfaction {
            kind: SatisfactionKind::Satisfied,
            code: None,
            scope: CapabilityScope::deployment(),
            observed: format!(
                "durable producer/edge spool configured (scribe_id={}, loss_budget={}, scope=one named Scribe local disk)",
                inputs.producer_spool_scribe_id, inputs.producer_spool_loss_budget
            ),
            consequence:
                "producers can retain accepted-but-not-yet-committed records across a Scribe outage"
                    .to_owned(),
            remediation: None,
        }
    } else {
        Satisfaction {
            kind: SatisfactionKind::Unsatisfied,
            code: Some(CapabilityCode::ProducerContinuity),
            scope: CapabilityScope::deployment(),
            observed: "durable producer/edge spool: NOT CONFIGURED".to_owned(),
            consequence: "an ordinary Scribe restart stalls producers; unacknowledged source records depend on the application's own retry/persistence"
                .to_owned(),
            remediation: Some(
                "configure producer_spool (kind: local, path, max_bytes, fsync, on_full, loss_budget, scribe_id) for durable spooled receipts"
                    .to_owned(),
            ),
        }
    };

    let failure_domains = Satisfaction {
        kind: SatisfactionKind::Satisfied,
        code: None,
        scope: CapabilityScope::deployment(),
        observed: format!(
            "configured independent storage targets: {}",
            inputs.independent_storage_targets
        ),
        consequence: if inputs.independent_storage_targets <= 1 {
            "acknowledged data and authority share one storage failure domain".to_owned()
        } else {
            format!(
                "acknowledged data spans {} independent storage failure domains",
                inputs.independent_storage_targets
            )
        },
        remediation: None,
    };

    let scribe_recovery = inputs.verses.iter().map(evaluate_verse_recovery).collect();

    CapabilityReport {
        canon_history,
        producer_continuity,
        scribe_recovery,
        failure_domains,
    }
}

fn evaluate_verse_recovery(verse: &VerseCapabilityInputs) -> VerseRecoverySatisfaction {
    let fresh_for_verse: Vec<&RecoveryCandidateEvidence> = verse
        .candidates
        .iter()
        .filter(|c| c.canon == verse.canon && c.verse == verse.verse && c.fresh)
        .collect();
    let eligible: Vec<&RecoveryCandidateEvidence> = verse
        .candidates
        .iter()
        .filter(|c| is_eligible_recovery_candidate(c, &verse.canon, &verse.verse))
        .collect();
    let eligible_count = u32::try_from(eligible.len()).unwrap_or(u32::MAX);
    let fresh_count = u32::try_from(fresh_for_verse.len()).unwrap_or(u32::MAX);

    let satisfaction = if eligible_count > 0 {
        Satisfaction {
            kind: SatisfactionKind::Satisfied,
            code: None,
            scope: CapabilityScope::verse(&verse.canon, &verse.verse),
            observed: format!(
                "eligible recovery candidates for {}/{}: {eligible_count}",
                verse.canon, verse.verse
            ),
            consequence: if eligible_count == 1 {
                "one eligible candidate can recover this Verse; post-failure redundancy would be exhausted"
                    .to_owned()
            } else {
                format!(
                    "{eligible_count} eligible candidates; one Scribe failure can still leave a recovery path"
                )
            },
            remediation: None,
        }
    } else if !verse.candidates_declared_for_static && verse.candidates.is_empty() {
        Satisfaction {
            kind: SatisfactionKind::RequiresLivePreflight,
            code: Some(CapabilityCode::ScribeRecovery),
            scope: CapabilityScope::verse(&verse.canon, &verse.verse),
            observed: format!(
                "no declared eligible recovery candidates for {}/{} (static path)",
                verse.canon, verse.verse
            ),
            consequence: "cannot verify automatic Scribe recovery statically — require live preflight (`scripture preflight --live` / scripture doctor)"
                .to_owned(),
            remediation: Some(
                "run `scripture preflight --live` (or scripture doctor) against a live cluster, or declare eligible candidates for static scoring"
                    .to_owned(),
            ),
        }
    } else {
        Satisfaction {
            kind: SatisfactionKind::Unsatisfied,
            code: Some(CapabilityCode::ScribeRecovery),
            scope: CapabilityScope::verse(&verse.canon, &verse.verse),
            observed: format!(
                "no eligible recovery candidate for {}/{} (fresh serving-capable entries: 0; fresh heartbeats: {fresh_count})",
                verse.canon, verse.verse
            ),
            consequence: "no automatic Verse recovery after this Scribe fails".to_owned(),
            remediation: Some(
                "add an eligible serving-capable Scribe candidate for this Verse that heartbeats within the staleness bound"
                    .to_owned(),
            ),
        }
    };

    VerseRecoverySatisfaction {
        canon: verse.canon.clone(),
        verse: verse.verse.clone(),
        satisfaction,
        candidates_observed_heartbeating: fresh_count,
        eligible_candidates: eligible_count,
        serving_now: verse.serving_now.clone(),
    }
}

/// A policy requirement checked against a [`CapabilityReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequiredGuarantee {
    /// Require Canon history `committed`.
    CanonHistoryCommitted,
    /// Require producer continuity `spooled`.
    ProducerContinuitySpooled,
    /// Require automatic Scribe recovery eligibility.
    ScribeRecoveryAutomatic,
    /// Require at least this many independent storage failure domains.
    MinStorageFailureDomains(u32),
}

/// One policy finding (warning or fail-start cause).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityFinding {
    /// Stable code.
    pub code: CapabilityCode,
    /// Satisfaction kind that produced this finding.
    pub kind: SatisfactionKind,
    /// Affected scope.
    pub scope: CapabilityScope,
    /// Observed fact.
    pub observed: String,
    /// Consequence.
    pub consequence: String,
    /// Remediation.
    pub remediation: String,
}

impl CapabilityFinding {
    /// Dedup key `(code, canon, verse)`.
    #[must_use]
    pub fn dedup_key(&self) -> (CapabilityCode, &str, &str) {
        (
            self.code,
            self.scope.canon.as_str(),
            self.scope.verse.as_str(),
        )
    }

    /// Single stderr / log line.
    #[must_use]
    pub fn format_line(&self) -> String {
        let scope = if self.scope.canon.is_empty() {
            "deployment".to_owned()
        } else {
            format!("{}/{}", self.scope.canon, self.scope.verse)
        };
        format!(
            "{} scope={scope}: {} — {}. Remediation: {}",
            self.code, self.observed, self.consequence, self.remediation
        )
    }
}

/// Collect findings for required guarantees that are not satisfied.
#[must_use]
pub fn collect_requirement_findings(
    report: &CapabilityReport,
    inputs: &CapabilityInputs,
    requirements: &[RequiredGuarantee],
) -> Vec<CapabilityFinding> {
    let mut findings = Vec::new();
    for requirement in requirements {
        match requirement {
            RequiredGuarantee::CanonHistoryCommitted => {
                if report.canon_history.kind != SatisfactionKind::Satisfied {
                    findings.push(finding_from_satisfaction(&report.canon_history));
                }
            }
            RequiredGuarantee::ProducerContinuitySpooled => {
                if report.producer_continuity.kind != SatisfactionKind::Satisfied {
                    findings.push(finding_from_satisfaction(&report.producer_continuity));
                }
            }
            RequiredGuarantee::ScribeRecoveryAutomatic => {
                for verse in &report.scribe_recovery {
                    if verse.satisfaction.kind != SatisfactionKind::Satisfied {
                        findings.push(finding_from_satisfaction(&verse.satisfaction));
                    }
                }
            }
            RequiredGuarantee::MinStorageFailureDomains(min) => {
                if inputs.independent_storage_targets < *min {
                    findings.push(CapabilityFinding {
                        code: CapabilityCode::StorageFailureDomains,
                        kind: SatisfactionKind::Unsatisfied,
                        scope: CapabilityScope::deployment(),
                        observed: format!(
                            "independent storage targets: {} (required >= {min})",
                            inputs.independent_storage_targets
                        ),
                        consequence: "configured topology cannot meet the required storage failure-domain floor"
                            .to_owned(),
                        remediation: format!(
                            "configure at least {min} independent storage targets, or lower safety.require.min_storage_failure_domains"
                        ),
                    });
                }
            }
        }
    }
    findings.sort_by(|left, right| {
        (
            left.code,
            left.scope.canon.as_str(),
            left.scope.verse.as_str(),
        )
            .cmp(&(
                right.code,
                right.scope.canon.as_str(),
                right.scope.verse.as_str(),
            ))
    });
    findings.dedup_by(|left, right| left.dedup_key() == right.dedup_key());
    findings
}

fn finding_from_satisfaction(satisfaction: &Satisfaction) -> CapabilityFinding {
    CapabilityFinding {
        code: satisfaction.code.unwrap_or(CapabilityCode::ScribeRecovery),
        kind: satisfaction.kind,
        scope: satisfaction.scope.clone(),
        observed: satisfaction.observed.clone(),
        consequence: satisfaction.consequence.clone(),
        remediation: satisfaction
            .remediation
            .clone()
            .unwrap_or_else(|| "see scripture doctor".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_inputs() -> CapabilityInputs {
        CapabilityInputs {
            backend_label: "rustfs".to_owned(),
            independent_storage_targets: 1,
            committed_capable_target: true,
            durable_producer_spool_configured: false,
            producer_spool_loss_budget: String::new(),
            producer_spool_scribe_id: String::new(),
            verses: vec![VerseCapabilityInputs {
                canon: "telemetry-jrnl!!".to_owned(),
                verse: "telemetry-host-a".to_owned(),
                candidates: vec![],
                serving_now: None,
                candidates_declared_for_static: false,
                fleet_directory_nonempty: false,
            }],
        }
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
            age_ms: if fresh { 1_000 } else { 60_000 },
            posture: if serving_capable {
                "bootstrap-if-empty".to_owned()
            } else {
                "dormant".to_owned()
            },
            disposition: if serving_capable {
                "Serving".to_owned()
            } else {
                "Standby".to_owned()
            },
        }
    }

    #[test]
    fn eligible_predicate_active_fresh_same_verse_only() {
        let active = candidate(
            "node-a!!!!!!!!!!",
            "telemetry-jrnl!!",
            "telemetry-host-a",
            true,
            true,
        );
        assert!(is_eligible_recovery_candidate(
            &active,
            "telemetry-jrnl!!",
            "telemetry-host-a"
        ));

        let dormant = candidate(
            "node-a!!!!!!!!!!",
            "telemetry-jrnl!!",
            "telemetry-host-a",
            false,
            true,
        );
        assert!(!is_eligible_recovery_candidate(
            &dormant,
            "telemetry-jrnl!!",
            "telemetry-host-a"
        ));

        let stale = candidate(
            "node-a!!!!!!!!!!",
            "telemetry-jrnl!!",
            "telemetry-host-a",
            true,
            false,
        );
        assert!(!is_eligible_recovery_candidate(
            &stale,
            "telemetry-jrnl!!",
            "telemetry-host-a"
        ));

        let wrong_verse = candidate(
            "node-a!!!!!!!!!!",
            "telemetry-jrnl!!",
            "other-verse!!!!!",
            true,
            true,
        );
        assert!(!is_eligible_recovery_candidate(
            &wrong_verse,
            "telemetry-jrnl!!",
            "telemetry-host-a"
        ));
    }

    #[test]
    fn automatic_recovery_without_declared_candidates_requires_live_preflight() {
        let report = evaluate(&base_inputs());
        assert_eq!(report.scribe_recovery.len(), 1);
        assert_eq!(
            report.scribe_recovery[0].satisfaction.kind,
            SatisfactionKind::RequiresLivePreflight
        );
        assert_eq!(
            report.scribe_recovery[0].satisfaction.code,
            Some(CapabilityCode::ScribeRecovery)
        );
        assert!(
            report.scribe_recovery[0]
                .satisfaction
                .consequence
                .contains("live preflight")
        );
    }

    #[test]
    fn producer_continuity_unsatisfied_without_spool() {
        let report = evaluate(&base_inputs());
        assert_eq!(
            report.producer_continuity.kind,
            SatisfactionKind::Unsatisfied
        );
        assert_eq!(
            report.producer_continuity.code,
            Some(CapabilityCode::ProducerContinuity)
        );
        assert!(
            report
                .producer_continuity
                .remediation
                .as_ref()
                .is_some_and(|r| r.contains("producer_spool"))
        );
    }

    #[test]
    fn producer_continuity_satisfied_with_spool_disclosure() {
        let mut inputs = base_inputs();
        inputs.durable_producer_spool_configured = true;
        inputs.producer_spool_loss_budget = "30s".to_owned();
        inputs.producer_spool_scribe_id = "node-a".to_owned();
        let report = evaluate(&inputs);
        assert_eq!(report.producer_continuity.kind, SatisfactionKind::Satisfied);
        assert!(report.producer_continuity.observed.contains("node-a"));
        assert!(report.producer_continuity.observed.contains("30s"));
        assert!(
            report
                .producer_continuity
                .observed
                .contains("one named Scribe local disk")
        );
    }

    #[test]
    fn evaluate_is_pure_and_deterministic() {
        let inputs = base_inputs();
        let a = evaluate(&inputs);
        let b = evaluate(&inputs);
        assert_eq!(a, b);
    }
}
