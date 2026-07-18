//! Campaign artifact bundle (WP05 contract).

use std::path::Path;

use crate::CampaignReport;
use crate::coverage::{CoverageRow, family_catalog, merge_executed};
use crate::kellnr::ReleaseAttestation;

/// Matrix entry for one scenario in a suite run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MatrixEntry {
    /// Scenario token.
    pub scenario: String,
    /// Scenario/oracle verdict label.
    pub verdict: String,
    /// Checker attestation status (`evaluated` or `not_applicable`).
    pub checker: String,
    /// Process exit code for the scenario.
    pub exit_code: i32,
}

/// Suite-level artifact bundle.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SuiteArtifacts {
    /// Run identifier.
    pub run_id: String,
    /// Profile label.
    pub profile: String,
    /// Suite label.
    pub suite: String,
    /// Whether this was a dry-run preflight only.
    pub dry_run: bool,
    /// Release attestation classification.
    pub release_classification: String,
    /// Per-scenario matrix rows (executed only).
    pub matrix: Vec<MatrixEntry>,
    /// Full WP05 22-family coverage matrix.
    pub coverage: Vec<CoverageRow>,
}

impl SuiteArtifacts {
    /// Builds suite artifacts including the 22-family coverage matrix.
    #[must_use]
    pub fn build(
        run_id: String,
        profile: String,
        suite: String,
        dry_run: bool,
        matrix: Vec<MatrixEntry>,
        reports: &[CampaignReport],
        attestation: &ReleaseAttestation,
    ) -> Self {
        let coverage = if dry_run {
            family_catalog()
        } else {
            merge_executed(
                family_catalog(),
                reports,
                attestation.classification.as_str(),
            )
        };
        Self {
            run_id,
            profile,
            suite,
            dry_run,
            release_classification: attestation.classification.as_str().to_owned(),
            matrix,
            coverage,
        }
    }

    /// Writes the WP05 artifact bundle under `dir`.
    pub fn write(&self, dir: &Path, reports: &[CampaignReport]) -> Result<(), ArtifactError> {
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join("run.json"), serde_json::to_vec_pretty(self)?)?;
        std::fs::write(
            dir.join("matrix.json"),
            serde_json::to_vec_pretty(&self.matrix)?,
        )?;
        std::fs::write(
            dir.join("coverage-matrix.json"),
            serde_json::to_vec_pretty(&self.coverage)?,
        )?;
        std::fs::write(
            dir.join("verdict.json"),
            serde_json::to_vec_pretty(&self.verdict_json())?,
        )?;
        std::fs::write(dir.join("summary.md"), self.render_summary(reports))?;
        Ok(())
    }

    fn verdict_json(&self) -> serde_json::Value {
        let overall = if self.dry_run {
            "preflight_only"
        } else if self.matrix.iter().all(|row| row.verdict == "pass") && !self.matrix.is_empty() {
            "pass"
        } else if self.matrix.is_empty() {
            "empty"
        } else if self.matrix.iter().any(|row| row.verdict == "fail") {
            "fail"
        } else {
            "inconclusive"
        };
        let coverage_executed_pass = self
            .coverage
            .iter()
            .filter(|row| matches!(row.status, crate::coverage::CoverageStatus::Pass))
            .count();
        let coverage_not_run = self
            .coverage
            .iter()
            .filter(|row| matches!(row.status, crate::coverage::CoverageStatus::NotRun))
            .count();
        serde_json::json!({
            "overall": overall,
            "release_classification": self.release_classification,
            "matrix": self.matrix,
            "coverage_executed_pass": coverage_executed_pass,
            "coverage_not_run": coverage_not_run,
        })
    }

    fn render_summary(&self, reports: &[CampaignReport]) -> String {
        let mut lines = vec![
            "# Correctness campaign summary".into(),
            String::new(),
            format!("- run_id: `{}`", self.run_id),
            format!("- profile: `{}`", self.profile),
            format!("- suite: `{}`", self.suite),
            format!("- dry_run: {}", self.dry_run),
            format!(
                "- release_classification: `{}`",
                self.release_classification
            ),
            String::new(),
            "## Executed matrix".into(),
        ];
        for row in &self.matrix {
            lines.push(format!(
                "- `{}`: **{}** (exit {})",
                row.scenario, row.verdict, row.exit_code
            ));
        }
        lines.push(String::new());
        lines.push("## Coverage (22 families)".into());
        for row in &self.coverage {
            let status = match row.status {
                crate::coverage::CoverageStatus::Pass => "pass",
                crate::coverage::CoverageStatus::Fail => "fail",
                crate::coverage::CoverageStatus::Inconclusive => "inconclusive",
                crate::coverage::CoverageStatus::NotRun => "not-run",
            };
            let reason = row.reason.as_deref().unwrap_or("");
            lines.push(format!(
                "- {:02} `{}`: {} {}",
                row.family, row.name, status, reason
            ));
        }
        if !reports.is_empty() {
            lines.push(String::new());
            lines.push("## Scenarios".into());
            for report in reports {
                let checker = report.checker.label();
                lines.push(format!(
                    "- `{}` backend={} events={} oracle={} checker={} evidence_class={}",
                    report.scenario,
                    report.backend,
                    report.events.len(),
                    report.verdict_label(),
                    checker,
                    report.evidence_class.unwrap_or("checker-trace")
                ));
            }
        }
        lines.push(String::new());
        lines.push(
            "Non-claims: passing memory/core/composition scenarios does not prove multi-node \
             process separation, object-store HA, Kellnr release attestation, or cloud backend equivalence."
                .into(),
        );
        lines.join("\n")
    }

    /// Overall process exit code for the suite (WP05).
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        if self.dry_run {
            return 0;
        }
        if self.matrix.iter().any(|row| row.verdict == "fail") {
            return 3;
        }
        if self
            .matrix
            .iter()
            .any(|row| row.verdict == "inconclusive" || row.exit_code == 4)
        {
            return 4;
        }
        0
    }
}

/// Writes one scenario's evidence bundle under `dir/<scenario>/`.
pub fn write_scenario_artifacts(dir: &Path, report: &CampaignReport) -> Result<(), ArtifactError> {
    std::fs::create_dir_all(dir.join("traces"))?;
    std::fs::create_dir_all(dir.join("observations"))?;
    std::fs::write(dir.join("traces/campaign.ndjson"), report.trace_ndjson()?)?;
    std::fs::write(
        dir.join("observations/final-root.json"),
        serde_json::to_vec_pretty(&report.final_root)?,
    )?;
    std::fs::write(
        dir.join("observations/final-authority.json"),
        serde_json::to_vec_pretty(&report.final_authority)?,
    )?;
    std::fs::write(
        dir.join("environment.redacted.json"),
        serde_json::to_vec_pretty(&report.environment)?,
    )?;
    std::fs::write(
        dir.join("oracle-verdict.json"),
        serde_json::to_vec_pretty(&report.verdict_json()?)?,
    )?;
    std::fs::write(
        dir.join("checker-verdict.json"),
        serde_json::to_vec_pretty(&report.checker_json())?,
    )?;
    Ok(())
}

/// Artifact serialization failures.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    /// Underlying campaign serialization error.
    #[error(transparent)]
    Campaign(#[from] crate::CampaignError),
    /// JSON serialization failure.
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    /// IO failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Builds a matrix row from one scenario report.
#[must_use]
pub fn matrix_from_report(report: &CampaignReport) -> MatrixEntry {
    MatrixEntry {
        scenario: report.scenario.to_owned(),
        verdict: report.verdict_label().to_owned(),
        checker: report.checker.label().to_owned(),
        exit_code: report.exit_code(),
    }
}
