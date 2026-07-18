//! WP05 22-family coverage matrix catalog.

use serde::Serialize;

use crate::Scenario;

/// Layer of the Holylog/Scripture stack under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageLayer {
    /// AtomicLog / VirtualLog core.
    Core,
    /// Striped / quorum composition.
    Composition,
    /// Process-separated RustFS resilience.
    Resilience,
    /// Cloud / corruption / bounds / release boundary.
    Boundary,
}

/// How a family was (or was not) executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageStatus {
    /// Scenario executed and checker/oracle passed.
    Pass,
    /// Scenario executed and failed.
    Fail,
    /// Evidence incomplete / indeterminate.
    Inconclusive,
    /// Explicitly not run with a capability-backed reason.
    NotRun,
}

/// One row of the WP05 coverage matrix.
#[derive(Debug, Clone, Serialize)]
pub struct CoverageRow {
    /// Stable family id 1..=22.
    pub family: u8,
    /// Short family name.
    pub name: &'static str,
    /// Stack layer.
    pub layer: CoverageLayer,
    /// Backend label (`memory`, `rustfs`, `r2`, …).
    pub backend: &'static str,
    /// Whether distinct OS processes were required/proven.
    pub process_separation: bool,
    /// Schedule / seed identity when executed.
    pub schedule_id: Option<String>,
    /// `development-source` or `kellnr-rc` (or unset for not-run).
    pub release_classification: Option<&'static str>,
    /// Row status.
    pub status: CoverageStatus,
    /// Human reason for not-run / failure detail.
    pub reason: Option<String>,
    /// Relative artifact path when evidence exists.
    pub artifact_path: Option<String>,
    /// Campaign scenario token when wired.
    pub scenario: Option<&'static str>,
}

/// Static catalog of all 22 WP04/WP05 families.
#[must_use]
pub fn family_catalog() -> Vec<CoverageRow> {
    vec![
        row(
            1,
            "normal-committed-ack-dense-offsets",
            CoverageLayer::Core,
            Some(Scenario::BaselineCommittedAck),
        ),
        row(
            2,
            "payload-durable-no-ack",
            CoverageLayer::Core,
            Some(Scenario::WriterDiesAfterPayload),
        ),
        row(
            3,
            "k-window-delayed-completion",
            CoverageLayer::Core,
            Some(Scenario::KWindowDelayedCompletion),
        ),
        // Seal-boundary + VirtualLog successor: `permanent-wedge-seal-successor`.
        row(
            4,
            "permanent-k-window-wedge-seal-successor",
            CoverageLayer::Core,
            Some(Scenario::PermanentWedgeSealSuccessor),
        ),
        row(
            5,
            "seal-tail-race",
            CoverageLayer::Core,
            Some(Scenario::SealTailRace),
        ),
        row(
            6,
            "stale-writer-after-cutover",
            CoverageLayer::Core,
            Some(Scenario::RootCasReplyLost),
        ),
        row(
            7,
            "striped-modulo-mapping",
            CoverageLayer::Composition,
            Some(Scenario::StripedModuloMapping),
        ),
        row(
            8,
            "striped-lagging-scan-reconstruction",
            CoverageLayer::Composition,
            Some(Scenario::StripedLaggingScanReconstruction),
        ),
        row(
            9,
            "quorum-partial-write-not-global",
            CoverageLayer::Composition,
            Some(Scenario::QuorumPartialWriteNotGlobal),
        ),
        row(
            10,
            "quorum-repair-unavailability",
            CoverageLayer::Composition,
            Some(Scenario::QuorumRepairUnavailability),
        ),
        row(
            11,
            "nested-stripe-quorum-schedules",
            CoverageLayer::Composition,
            Some(Scenario::NestedStripeQuorumSchedules),
        ),
        // Families 12/14/16/17: prior orchestration-smoke rows are not semantic
        // pass evidence. Only family 13 is wired to the producer→A→kill→B path.
        row(12, "process-separated-baseline", CoverageLayer::Resilience, None),
        row(
            13,
            "kill-a-explicit-b-promotion",
            CoverageLayer::Resilience,
            Some(Scenario::RawLinesAbCutover),
        ),
        row(
            14,
            "wedged-payload-recovery-process-separated",
            CoverageLayer::Resilience,
            None,
        ),
        row(15, "root-cas-reply-loss-reread", CoverageLayer::Resilience, None),
        row(
            16,
            "directional-backend-loss-recovery",
            CoverageLayer::Resilience,
            None,
        ),
        row(
            17,
            "scoped-credential-invalidation",
            CoverageLayer::Resilience,
            None,
        ),
        row(
            18,
            "cloud-backend-contract-r2-s3-gcs",
            CoverageLayer::Boundary,
            None,
        ),
        row(
            19,
            "malformed-root-fence-object-identity",
            CoverageLayer::Boundary,
            None,
        ),
        row(
            20,
            "reconfiguration-generation-churn",
            CoverageLayer::Boundary,
            None,
        ),
        row(
            21,
            "resource-bounds-cleanup-leaks",
            CoverageLayer::Boundary,
            None,
        ),
        row(
            22,
            "release-version-migration-compatibility",
            CoverageLayer::Boundary,
            None,
        ),
    ]
}

fn row(
    family: u8,
    name: &'static str,
    layer: CoverageLayer,
    scenario: Option<Scenario>,
) -> CoverageRow {
    let (backend, process_separation) = match layer {
        CoverageLayer::Core | CoverageLayer::Composition => ("memory", false),
        CoverageLayer::Resilience => ("rustfs", true),
        CoverageLayer::Boundary => ("external", false),
    };
    CoverageRow {
        family,
        name,
        layer,
        backend,
        process_separation,
        schedule_id: None,
        release_classification: None,
        status: CoverageStatus::NotRun,
        reason: if scenario.is_none() {
            default_not_run_reason(family)
        } else {
            None
        },
        artifact_path: None,
        scenario: scenario.map(Scenario::as_str),
    }
}

fn default_not_run_reason(family: u8) -> Option<String> {
    Some(match family {
        12 => {
            "downgraded: prior row was orchestration smoke (deploy A + in-process campaign); awaiting a dedicated producer→A raw-lines baseline on the actor HA root"
                .into()
        }
        14 => {
            "not-run: force-delete ready A is process-recovery smoke, not family-2 durable-payload/no-ACK wedge across processes; DieAfterPayload seam absent in temporary adapter"
                .into()
        }
        15 => {
            "root-CAS reply-loss fault injection is not available in the temporary bootstrap/promote adapter; family 6 covers in-process semantics"
                .into()
        }
        16 => {
            "downgraded: prior row mutated NetworkPolicy then ran an in-process campaign; not-run until directional loss is proven on the producer→actor raw-lines path"
                .into()
        }
        17 => {
            "downgraded: prior row mutated the run Secret then ran an in-process campaign; not-run until credential denial is proven on the producer→actor raw-lines path"
                .into()
        }
        18 => "R2/S3/GCS require Joshua's explicit approval of the exact command".into(),
        19 => "malformed identity suite not yet wired".into(),
        20 => "reconfiguration storm not yet wired".into(),
        21 => "resource-bounds suite not yet wired".into(),
        22 => "release/version row requires Kellnr RC manifest + locked image attestation".into(),
        _ => "not executed in this run".into(),
    })
}

/// Merges executed scenario reports into the static catalog.
#[must_use]
pub fn merge_executed(
    mut catalog: Vec<CoverageRow>,
    reports: &[crate::CampaignReport],
    release_classification: &'static str,
) -> Vec<CoverageRow> {
    for report in reports {
        for row in &mut catalog {
            if row.scenario != Some(report.scenario) {
                continue;
            }
            row.schedule_id = Some(report.run_id.clone());
            row.release_classification = Some(release_classification);
            row.backend = report.backend;
            row.artifact_path = Some(format!("{}/{}", report.run_id, report.scenario));
            row.status = match report.verdict_label() {
                "pass" => CoverageStatus::Pass,
                "fail" => CoverageStatus::Fail,
                _ => CoverageStatus::Inconclusive,
            };
            row.reason = match &report.verdict {
                holylog_correctness::Verdict::Pass => None,
                holylog_correctness::Verdict::Fail { invariant, .. } => {
                    Some(format!("checker fail: {invariant:?}"))
                }
                holylog_correctness::Verdict::Inconclusive { reason, .. } => Some(reason.clone()),
            };
        }
    }
    catalog
}
