//! Pure reconciliation planning vocabulary for Scripture startup.
//!
//! A node inspects durable evidence, plans justified actions, and only then
//! starts an owner. This module is the **planner**: it never talks to storage,
//! never executes CAS/fences, and never starts a [`crate::ChunkJournalService`].
//!
//! Binding model: tracker `scripture/startup-reconciliation-safe-guided-emergency.md`.

use std::fmt;

/// Operator-selected startup mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Read-only: report and suggest plans; never mutate.
    Inspect,
    /// Mechanically idempotent, evidence-preserving actions only.
    SafeRepair,
    /// Propose ranked plans; ask when safety cannot be derived.
    Guided,
    /// Liveness-first among high-confidence fenced options.
    Emergency,
}

/// Confidence attached to a finding or planned action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecoveryConfidence {
    /// Durable evidence uniquely determines the fact/action.
    Certain,
    /// Strong majority / attested quorum agreement.
    High,
    /// Partial evidence; usable only under Guided/Emergency with caveats.
    Limited,
    /// Not enough to act safely in any mode.
    Insufficient,
}

/// Observable lifecycle of a reconciler session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationState {
    /// Gathering facts.
    Inspecting,
    /// Waiting on a named operator choice.
    NeedsGuidance,
    /// Executing a SafeRepair/Emergency plan (executor, not this planner).
    Repairing,
    /// Ready to start the existing owner under SafeRepair/Guided.
    Ready,
    /// Ready under Emergency with accepted incompleteness.
    EmergencyReady,
    /// Cannot proceed without more evidence or a different mode.
    Blocked,
}

/// Facts supplied by an adapter. Values are observed evidence only.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryFacts {
    /// True when an active generation descriptor is uniquely known.
    pub has_active_generation: bool,
    /// True when storage quorum/attestation is available for required reads.
    pub storage_quorum_available: bool,
    /// Competing conditional-register / configuration histories observed.
    pub competing_register_histories: bool,
    /// A missing replica has one attested valid value that can be repaired.
    pub missing_replica_with_attested_value: bool,
    /// Predecessor seal is already determined and incomplete.
    pub known_predecessor_seal_incomplete: bool,
    /// Successor boundary is uniquely derived from sealed predecessor evidence.
    pub successor_boundary_uniquely_derived: bool,
    /// Durable chunk required for reconstruction is corrupt.
    pub required_chunk_corrupt: bool,
    /// No durable evidence exists for this journal prefix (fresh bootstrap case).
    pub fresh_journal_prefix: bool,
    /// Explicit operator request to bootstrap a new journal on an empty prefix.
    pub bootstrap_requested: bool,
}

/// A classified observation from [`RecoveryFacts`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryFinding {
    /// Short machine-oriented code.
    pub code: &'static str,
    /// Human-readable description.
    pub detail: String,
    /// Confidence in the finding.
    pub confidence: RecoveryConfidence,
}

impl fmt::Display for RecoveryFinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({:?}): {}", self.code, self.confidence, self.detail)
    }
}

/// Planned recovery step. Execution is out of scope for this module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Start the already-published active owner after fencing.
    StartExistingOwner,
    /// Propagate one attested valid replica value to restore quorum.
    RepairReplicaValue,
    /// Re-issue an identical immutable write (chunk/seal/register).
    ReissueSameValue,
    /// Complete a seal whose predecessor boundary is already known.
    CompleteKnownSeal,
    /// Propose publishing a specifically derived successor (never SafeRepair).
    PublishDerivedSuccessor,
    /// Explicit fresh-prefix bootstrap — never an implicit fallback.
    BootstrapNewJournal,
    /// Pause pending a named operator choice.
    HoldForOperator,
    /// Refuse to proceed.
    RejectBlocked,
}

/// One step in a [`RecoveryPlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedAction {
    /// What to do.
    pub action: RecoveryAction,
    /// Evidence that justifies the action.
    pub evidence: String,
    /// Confidence.
    pub confidence: RecoveryConfidence,
    /// Whether undoing is possible without data loss.
    pub reversible: bool,
    /// Whether a CAS/fence is required before mutation.
    pub requires_fence: bool,
}

impl fmt::Display for PlannedAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} confidence={:?} fence={} reversible={}: {}",
            self.action, self.confidence, self.requires_fence, self.reversible, self.evidence
        )
    }
}

/// A precise question when Guided mode cannot derive a unique safe plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorQuestion {
    /// Stable question id for operator UIs.
    pub id: &'static str,
    /// What the operator must choose among.
    pub prompt: String,
    /// Ranked alternative action labels.
    pub options: Vec<&'static str>,
}

impl fmt::Display for OperatorQuestion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} options={:?}", self.id, self.prompt, self.options)
    }
}

/// Output of [`plan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPlan {
    /// Mode that produced this plan.
    pub mode: RecoveryMode,
    /// Resulting reconciler state.
    pub state: ReconciliationState,
    /// Findings derived from facts.
    pub findings: Vec<RecoveryFinding>,
    /// Ordered actions (empty when blocked / needs guidance only).
    pub actions: Vec<PlannedAction>,
    /// Present when Guided cannot pick alone.
    pub question: Option<OperatorQuestion>,
}

impl fmt::Display for RecoveryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "mode={:?} state={:?}", self.mode, self.state)?;
        for finding in &self.findings {
            writeln!(f, "finding: {finding}")?;
        }
        for action in &self.actions {
            writeln!(f, "action: {action}")?;
        }
        if let Some(question) = &self.question {
            writeln!(f, "question: {question}")?;
        }
        Ok(())
    }
}

fn finding(
    code: &'static str,
    detail: impl Into<String>,
    confidence: RecoveryConfidence,
) -> RecoveryFinding {
    RecoveryFinding {
        code,
        detail: detail.into(),
        confidence,
    }
}

fn action(
    action: RecoveryAction,
    evidence: impl Into<String>,
    confidence: RecoveryConfidence,
    reversible: bool,
    requires_fence: bool,
) -> PlannedAction {
    PlannedAction {
        action,
        evidence: evidence.into(),
        confidence,
        reversible,
        requires_fence,
    }
}

fn inconsistent_evidence(facts: &RecoveryFacts) -> Option<String> {
    if facts.fresh_journal_prefix {
        let mut conflicts = Vec::new();
        if facts.has_active_generation {
            conflicts.push("has_active_generation");
        }
        if facts.known_predecessor_seal_incomplete {
            conflicts.push("known_predecessor_seal_incomplete");
        }
        if facts.successor_boundary_uniquely_derived {
            conflicts.push("successor_boundary_uniquely_derived");
        }
        if facts.required_chunk_corrupt {
            conflicts.push("required_chunk_corrupt");
        }
        if facts.missing_replica_with_attested_value {
            conflicts.push("missing_replica_with_attested_value");
        }
        if !conflicts.is_empty() {
            return Some(format!(
                "fresh_journal_prefix coexists with existing-journal evidence: {}",
                conflicts.join(", ")
            ));
        }
    }
    if facts.bootstrap_requested && !facts.fresh_journal_prefix {
        return Some(
            "bootstrap_requested requires fresh_journal_prefix; prefix is not fresh".into(),
        );
    }
    None
}

fn blocked_inconsistent(mode: RecoveryMode, detail: String) -> RecoveryPlan {
    RecoveryPlan {
        mode,
        state: ReconciliationState::Blocked,
        findings: vec![finding(
            "inconsistent_evidence",
            detail.clone(),
            RecoveryConfidence::Certain,
        )],
        actions: vec![action(
            RecoveryAction::RejectBlocked,
            detail,
            RecoveryConfidence::Certain,
            true,
            false,
        )],
        question: None,
    }
}

/// Derive findings and a mode-appropriate plan from observed facts.
#[must_use]
pub fn plan(mode: RecoveryMode, facts: &RecoveryFacts) -> RecoveryPlan {
    if let Some(detail) = inconsistent_evidence(facts) {
        return blocked_inconsistent(mode, detail);
    }

    let mut findings = Vec::new();

    if facts.competing_register_histories {
        findings.push(finding(
            "competing_registers",
            "competing conditional-register/configuration histories observed",
            RecoveryConfidence::Insufficient,
        ));
    }
    if !facts.storage_quorum_available {
        findings.push(finding(
            "missing_quorum",
            "storage quorum or metadata attestation unavailable",
            RecoveryConfidence::Insufficient,
        ));
    }
    if facts.required_chunk_corrupt {
        findings.push(finding(
            "corrupt_chunk",
            "a durable chunk required for reconstruction is corrupt",
            RecoveryConfidence::Certain,
        ));
    }
    if facts.missing_replica_with_attested_value {
        findings.push(finding(
            "repairable_replica",
            "a missing replica has one attested valid value",
            RecoveryConfidence::High,
        ));
    }
    if facts.known_predecessor_seal_incomplete {
        findings.push(finding(
            "incomplete_known_seal",
            "predecessor seal is determined but incomplete",
            RecoveryConfidence::Certain,
        ));
    }
    if facts.has_active_generation {
        findings.push(finding(
            "active_generation",
            "an active generation is uniquely known",
            RecoveryConfidence::Certain,
        ));
    }
    if facts.fresh_journal_prefix {
        findings.push(finding(
            "fresh_prefix",
            "no durable evidence for this journal prefix",
            RecoveryConfidence::Certain,
        ));
    }
    if facts.successor_boundary_uniquely_derived {
        findings.push(finding(
            "derived_successor",
            "successor boundary is uniquely derived from sealed predecessor evidence",
            RecoveryConfidence::High,
        ));
    } else if facts.has_active_generation && facts.known_predecessor_seal_incomplete {
        // sealed path without unique successor — finding emitted only when relevant
    }

    // Hard blocks apply in every mode.
    if !facts.storage_quorum_available {
        return RecoveryPlan {
            mode,
            state: ReconciliationState::Blocked,
            findings,
            actions: vec![action(
                RecoveryAction::RejectBlocked,
                "missing storage quorum",
                RecoveryConfidence::Insufficient,
                true,
                false,
            )],
            question: None,
        };
    }
    if facts.competing_register_histories {
        return match mode {
            RecoveryMode::Guided => RecoveryPlan {
                mode,
                state: ReconciliationState::NeedsGuidance,
                findings,
                actions: vec![action(
                    RecoveryAction::HoldForOperator,
                    "competing register histories",
                    RecoveryConfidence::Insufficient,
                    true,
                    false,
                )],
                question: Some(OperatorQuestion {
                    id: "choose_register_history",
                    prompt: "Select which attested register history is authoritative".into(),
                    options: vec!["history_a", "history_b", "hold"],
                }),
            },
            _ => RecoveryPlan {
                mode,
                state: ReconciliationState::Blocked,
                findings,
                actions: vec![action(
                    RecoveryAction::RejectBlocked,
                    "competing register histories",
                    RecoveryConfidence::Insufficient,
                    true,
                    false,
                )],
                question: None,
            },
        };
    }
    if facts.required_chunk_corrupt {
        return match mode {
            RecoveryMode::Guided => RecoveryPlan {
                mode,
                state: ReconciliationState::NeedsGuidance,
                findings,
                actions: vec![action(
                    RecoveryAction::HoldForOperator,
                    "required chunk corrupt",
                    RecoveryConfidence::Certain,
                    true,
                    false,
                )],
                question: Some(OperatorQuestion {
                    id: "corrupt_chunk_disposition",
                    prompt: "Required durable chunk is corrupt; choose forensics hold or emergency incompleteness".into(),
                    options: vec!["hold_forensics", "emergency_mark_unknown"],
                }),
            },
            _ => RecoveryPlan {
                mode,
                state: ReconciliationState::Blocked,
                findings,
                actions: vec![action(
                    RecoveryAction::RejectBlocked,
                    "required chunk corrupt",
                    RecoveryConfidence::Certain,
                    true,
                    false,
                )],
                question: None,
            },
        };
    }

    // Fresh bootstrap: never an implicit fallback.
    if facts.fresh_journal_prefix {
        if facts.bootstrap_requested {
            let state = match mode {
                RecoveryMode::Inspect => ReconciliationState::Inspecting,
                RecoveryMode::Emergency => ReconciliationState::EmergencyReady,
                _ => ReconciliationState::Ready,
            };
            let mut actions = Vec::new();
            if mode != RecoveryMode::Inspect {
                actions.push(action(
                    RecoveryAction::BootstrapNewJournal,
                    "explicit bootstrap on empty prefix",
                    RecoveryConfidence::Certain,
                    false,
                    true,
                ));
            }
            return RecoveryPlan {
                mode,
                state,
                findings,
                actions,
                question: None,
            };
        }
        return match mode {
            RecoveryMode::Guided => RecoveryPlan {
                mode,
                state: ReconciliationState::NeedsGuidance,
                findings,
                actions: vec![action(
                    RecoveryAction::HoldForOperator,
                    "empty prefix without explicit bootstrap",
                    RecoveryConfidence::Certain,
                    true,
                    false,
                )],
                question: Some(OperatorQuestion {
                    id: "confirm_bootstrap",
                    prompt: "Prefix is empty; confirm BootstrapNewJournal or abort".into(),
                    options: vec!["bootstrap", "abort"],
                }),
            },
            _ => RecoveryPlan {
                mode,
                state: ReconciliationState::Blocked,
                findings,
                actions: vec![action(
                    RecoveryAction::RejectBlocked,
                    "empty prefix without explicit bootstrap request",
                    RecoveryConfidence::Certain,
                    true,
                    false,
                )],
                question: None,
            },
        };
    }

    // Ambiguous successor: never automatic SafeRepair.
    if !facts.successor_boundary_uniquely_derived
        && facts.known_predecessor_seal_incomplete
        && !facts.has_active_generation
    {
        return match mode {
            RecoveryMode::Guided | RecoveryMode::Emergency => RecoveryPlan {
                mode,
                state: ReconciliationState::NeedsGuidance,
                findings: {
                    let mut f = findings;
                    f.push(finding(
                        "ambiguous_successor",
                        "successor boundary is not uniquely derivable",
                        RecoveryConfidence::Insufficient,
                    ));
                    f
                },
                actions: vec![action(
                    RecoveryAction::HoldForOperator,
                    "ambiguous successor boundary",
                    RecoveryConfidence::Insufficient,
                    true,
                    false,
                )],
                question: Some(OperatorQuestion {
                    id: "successor_boundary",
                    prompt: "Predecessor seal incomplete and successor boundary not unique".into(),
                    options: vec!["hold", "supply_boundary"],
                }),
            },
            _ => RecoveryPlan {
                mode,
                state: ReconciliationState::Blocked,
                findings: {
                    let mut f = findings;
                    f.push(finding(
                        "ambiguous_successor",
                        "successor boundary is not uniquely derivable",
                        RecoveryConfidence::Insufficient,
                    ));
                    f
                },
                actions: vec![action(
                    RecoveryAction::RejectBlocked,
                    "ambiguous successor boundary",
                    RecoveryConfidence::Insufficient,
                    true,
                    false,
                )],
                question: None,
            },
        };
    }

    let mut actions = Vec::new();

    if facts.missing_replica_with_attested_value {
        match mode {
            RecoveryMode::Inspect => {}
            RecoveryMode::SafeRepair | RecoveryMode::Guided | RecoveryMode::Emergency => {
                actions.push(action(
                    RecoveryAction::RepairReplicaValue,
                    "one attested valid value for a missing replica",
                    RecoveryConfidence::High,
                    true,
                    false,
                ));
            }
        }
    }

    if facts.known_predecessor_seal_incomplete && facts.successor_boundary_uniquely_derived {
        match mode {
            RecoveryMode::Inspect => {}
            RecoveryMode::SafeRepair | RecoveryMode::Guided | RecoveryMode::Emergency => {
                actions.push(action(
                    RecoveryAction::CompleteKnownSeal,
                    "predecessor seal already determined",
                    RecoveryConfidence::Certain,
                    true,
                    true,
                ));
            }
        }
    }

    // PublishDerivedSuccessor is proposal-only here; Guided/Emergency may include
    // it when uniquely derived, SafeRepair never does.
    if facts.successor_boundary_uniquely_derived && !facts.has_active_generation {
        match mode {
            RecoveryMode::Guided | RecoveryMode::Emergency => {
                actions.push(action(
                    RecoveryAction::PublishDerivedSuccessor,
                    "uniquely derived successor boundary (proposal only; executor not in this module)",
                    RecoveryConfidence::High,
                    false,
                    true,
                ));
            }
            RecoveryMode::Inspect | RecoveryMode::SafeRepair => {}
        }
    }

    if facts.has_active_generation {
        match mode {
            RecoveryMode::Inspect => {}
            _ => {
                actions.push(action(
                    RecoveryAction::StartExistingOwner,
                    "active generation uniquely known",
                    RecoveryConfidence::Certain,
                    true,
                    true,
                ));
            }
        }
    }

    let state = match mode {
        RecoveryMode::Inspect => ReconciliationState::Inspecting,
        RecoveryMode::Emergency if !actions.is_empty() => ReconciliationState::EmergencyReady,
        _ if actions.iter().any(|a| {
            matches!(
                a.action,
                RecoveryAction::StartExistingOwner | RecoveryAction::CompleteKnownSeal
            )
        }) =>
        {
            ReconciliationState::Ready
        }
        _ if actions.is_empty() => ReconciliationState::Blocked,
        _ => ReconciliationState::Ready,
    };

    // Inspect never returns mutating actions.
    if mode == RecoveryMode::Inspect {
        return RecoveryPlan {
            mode,
            state: ReconciliationState::Inspecting,
            findings,
            actions: Vec::new(),
            question: None,
        };
    }

    // Emergency may only keep high-confidence fenced or safe repairs.
    let actions = if mode == RecoveryMode::Emergency {
        actions
            .into_iter()
            .filter(|step| {
                step.confidence >= RecoveryConfidence::High
                    && !matches!(step.action, RecoveryAction::HoldForOperator)
            })
            .collect()
    } else {
        actions
    };

    // SafeRepair never publishes a successor.
    let actions = if mode == RecoveryMode::SafeRepair {
        actions
            .into_iter()
            .filter(|step| !matches!(step.action, RecoveryAction::PublishDerivedSuccessor))
            .collect()
    } else {
        actions
    };

    if actions.is_empty() && !facts.has_active_generation {
        return RecoveryPlan {
            mode,
            state: ReconciliationState::Blocked,
            findings,
            actions: vec![action(
                RecoveryAction::RejectBlocked,
                "no safe action under this mode",
                RecoveryConfidence::Insufficient,
                true,
                false,
            )],
            question: None,
        };
    }

    RecoveryPlan {
        mode,
        state,
        findings,
        actions,
        question: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy() -> RecoveryFacts {
        RecoveryFacts {
            has_active_generation: true,
            storage_quorum_available: true,
            ..RecoveryFacts::default()
        }
    }

    #[test]
    fn healthy_active_generation_starts_existing_owner() {
        let plan = plan(RecoveryMode::SafeRepair, &healthy());
        assert_eq!(plan.state, ReconciliationState::Ready);
        assert!(
            plan.actions
                .iter()
                .any(|a| a.action == RecoveryAction::StartExistingOwner)
        );
    }

    #[test]
    fn inspect_never_returns_mutating_actions() {
        let mut facts = healthy();
        facts.missing_replica_with_attested_value = true;
        facts.known_predecessor_seal_incomplete = true;
        facts.successor_boundary_uniquely_derived = true;
        let plan = plan(RecoveryMode::Inspect, &facts);
        assert!(plan.actions.is_empty());
        assert_eq!(plan.state, ReconciliationState::Inspecting);
    }

    #[test]
    fn missing_replica_repairs_in_safe_repair_and_emergency() {
        let mut facts = healthy();
        facts.missing_replica_with_attested_value = true;
        for mode in [RecoveryMode::SafeRepair, RecoveryMode::Emergency] {
            let plan = plan(mode, &facts);
            assert!(
                plan.actions
                    .iter()
                    .any(|a| a.action == RecoveryAction::RepairReplicaValue),
                "{mode:?}"
            );
        }
        let inspect = plan(RecoveryMode::Inspect, &facts);
        assert!(inspect.actions.is_empty());
    }

    #[test]
    fn competing_registers_are_guided_or_blocked() {
        let mut facts = healthy();
        facts.competing_register_histories = true;
        let guided = plan(RecoveryMode::Guided, &facts);
        assert_eq!(guided.state, ReconciliationState::NeedsGuidance);
        assert!(guided.question.is_some());
        for mode in [
            RecoveryMode::Inspect,
            RecoveryMode::SafeRepair,
            RecoveryMode::Emergency,
        ] {
            let blocked = plan(mode, &facts);
            assert_eq!(blocked.state, ReconciliationState::Blocked, "{mode:?}");
        }
    }

    #[test]
    fn missing_quorum_blocks_every_mode() {
        let mut facts = healthy();
        facts.storage_quorum_available = false;
        for mode in [
            RecoveryMode::Inspect,
            RecoveryMode::SafeRepair,
            RecoveryMode::Guided,
            RecoveryMode::Emergency,
        ] {
            let plan = plan(mode, &facts);
            assert_eq!(plan.state, ReconciliationState::Blocked, "{mode:?}");
            assert!(
                plan.actions
                    .iter()
                    .any(|a| a.action == RecoveryAction::RejectBlocked)
            );
        }
    }

    #[test]
    fn known_predecessor_seal_is_safe_and_idempotent() {
        let mut facts = healthy();
        facts.known_predecessor_seal_incomplete = true;
        facts.successor_boundary_uniquely_derived = true;
        let plan = plan(RecoveryMode::SafeRepair, &facts);
        let seal = plan
            .actions
            .iter()
            .find(|a| a.action == RecoveryAction::CompleteKnownSeal)
            .expect("seal");
        assert!(seal.reversible);
        assert!(seal.requires_fence);
        assert_eq!(seal.confidence, RecoveryConfidence::Certain);
    }

    #[test]
    fn ambiguous_successor_never_automatic_safe_repair() {
        let facts = RecoveryFacts {
            storage_quorum_available: true,
            known_predecessor_seal_incomplete: true,
            successor_boundary_uniquely_derived: false,
            has_active_generation: false,
            ..RecoveryFacts::default()
        };
        let safe = plan(RecoveryMode::SafeRepair, &facts);
        assert_eq!(safe.state, ReconciliationState::Blocked);
        assert!(!safe.actions.iter().any(|a| matches!(
            a.action,
            RecoveryAction::PublishDerivedSuccessor | RecoveryAction::StartExistingOwner
        )));
        let guided = plan(RecoveryMode::Guided, &facts);
        assert_eq!(guided.state, ReconciliationState::NeedsGuidance);
    }

    #[test]
    fn emergency_keeps_only_high_confidence_fenced_options() {
        let mut facts = healthy();
        facts.missing_replica_with_attested_value = true;
        let plan = plan(RecoveryMode::Emergency, &facts);
        assert_eq!(plan.state, ReconciliationState::EmergencyReady);
        assert!(
            plan.actions
                .iter()
                .all(|a| a.confidence >= RecoveryConfidence::High)
        );
        assert!(plan.actions.iter().any(|a| {
            matches!(
                a.action,
                RecoveryAction::StartExistingOwner | RecoveryAction::RepairReplicaValue
            )
        }));
    }

    #[test]
    fn fresh_bootstrap_requires_explicit_action() {
        let facts = RecoveryFacts {
            storage_quorum_available: true,
            fresh_journal_prefix: true,
            bootstrap_requested: false,
            ..RecoveryFacts::default()
        };
        let blocked = plan(RecoveryMode::SafeRepair, &facts);
        assert_eq!(blocked.state, ReconciliationState::Blocked);
        assert!(
            !blocked
                .actions
                .iter()
                .any(|a| a.action == RecoveryAction::BootstrapNewJournal)
        );

        let requested = RecoveryFacts {
            bootstrap_requested: true,
            ..facts
        };
        let plan = plan(RecoveryMode::SafeRepair, &requested);
        assert!(
            plan.actions
                .iter()
                .any(|a| a.action == RecoveryAction::BootstrapNewJournal)
        );
    }

    #[test]
    fn safe_repair_never_publishes_successor() {
        let facts = RecoveryFacts {
            storage_quorum_available: true,
            has_active_generation: false,
            successor_boundary_uniquely_derived: true,
            known_predecessor_seal_incomplete: true,
            ..RecoveryFacts::default()
        };
        let safe = plan(RecoveryMode::SafeRepair, &facts);
        assert!(
            !safe
                .actions
                .iter()
                .any(|a| a.action == RecoveryAction::PublishDerivedSuccessor)
        );
        let guided = plan(RecoveryMode::Guided, &facts);
        assert!(
            guided
                .actions
                .iter()
                .any(|a| a.action == RecoveryAction::PublishDerivedSuccessor)
        );
    }

    fn assert_inconsistent_blocks(facts: &RecoveryFacts) {
        const FORBIDDEN: &[RecoveryAction] = &[
            RecoveryAction::StartExistingOwner,
            RecoveryAction::CompleteKnownSeal,
            RecoveryAction::RepairReplicaValue,
            RecoveryAction::PublishDerivedSuccessor,
            RecoveryAction::BootstrapNewJournal,
            RecoveryAction::ReissueSameValue,
            RecoveryAction::HoldForOperator,
        ];
        for mode in [
            RecoveryMode::Inspect,
            RecoveryMode::SafeRepair,
            RecoveryMode::Guided,
            RecoveryMode::Emergency,
        ] {
            let plan = plan(mode, facts);
            assert_eq!(plan.state, ReconciliationState::Blocked, "{mode:?}");
            assert!(
                plan.findings
                    .iter()
                    .any(|f| f.code == "inconsistent_evidence"),
                "{mode:?}"
            );
            assert!(
                plan.actions
                    .iter()
                    .any(|a| a.action == RecoveryAction::RejectBlocked),
                "{mode:?}"
            );
            assert!(
                !plan.actions.iter().any(|a| FORBIDDEN.contains(&a.action)),
                "{mode:?} leaked mutating/hold action: {:?}",
                plan.actions
            );
        }
    }

    #[test]
    fn fresh_plus_active_plus_bootstrap_never_bootstraps() {
        let facts = RecoveryFacts {
            storage_quorum_available: true,
            fresh_journal_prefix: true,
            has_active_generation: true,
            bootstrap_requested: true,
            ..RecoveryFacts::default()
        };
        assert_inconsistent_blocks(&facts);
        let plan = plan(RecoveryMode::Emergency, &facts);
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| a.action == RecoveryAction::BootstrapNewJournal)
        );
    }

    #[test]
    fn non_fresh_bootstrap_request_never_bootstraps() {
        let facts = RecoveryFacts {
            storage_quorum_available: true,
            has_active_generation: true,
            fresh_journal_prefix: false,
            bootstrap_requested: true,
            ..RecoveryFacts::default()
        };
        assert_inconsistent_blocks(&facts);
        assert!(
            !plan(RecoveryMode::SafeRepair, &facts)
                .actions
                .iter()
                .any(|a| a.action == RecoveryAction::BootstrapNewJournal)
        );
    }

    #[test]
    fn each_fresh_conflict_blocks_every_mode() {
        let conflict_flags: &[fn(&mut RecoveryFacts)] = &[
            |f| f.has_active_generation = true,
            |f| f.known_predecessor_seal_incomplete = true,
            |f| f.successor_boundary_uniquely_derived = true,
            |f| f.required_chunk_corrupt = true,
            |f| f.missing_replica_with_attested_value = true,
        ];
        for set_conflict in conflict_flags {
            let mut facts = RecoveryFacts {
                storage_quorum_available: true,
                fresh_journal_prefix: true,
                bootstrap_requested: true,
                ..RecoveryFacts::default()
            };
            set_conflict(&mut facts);
            assert_inconsistent_blocks(&facts);
        }
    }
}
