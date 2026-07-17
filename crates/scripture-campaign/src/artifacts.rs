//! Campaign artifact bundle (WP04 contract).

use std::path::Path;

use crate::CampaignReport;

/// Matrix entry for one scenario in a suite run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MatrixEntry {
    /// Scenario token.
    pub scenario: String,
    /// Checker verdict label.
    pub verdict: String,
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
    /// Per-scenario matrix rows.
    pub matrix: Vec<MatrixEntry>,
}

impl SuiteArtifacts {
    /// Writes the WP04 artifact bundle under `dir`.
    pub fn write(&self, dir: &Path, reports: &[CampaignReport]) -> Result<(), ArtifactError> {
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join("run.json"), serde_json::to_vec_pretty(self)?)?;
        std::fs::write(dir.join("matrix.json"), serde_json::to_vec_pretty(&self.matrix)?)?;
        std::fs::write(dir.join("verdict.json"), serde_json::to_vec_pretty(&self.verdict_json())?)?;
        std::fs::write(dir.join("summary.md"), self.render_summary(reports))?;
        Ok(())
    }

    fn verdict_json(&self) -> serde_json::Value {
        let overall = if self.dry_run {
            if self.matrix.is_empty() {
                "preflight_only"
            } else {
                "dry_run"
            }
        } else if self.matrix.iter().all(|row| row.verdict == "pass") {
            "pass"
        } else if self.matrix.iter().any(|row| row.verdict == "fail") {
            "fail"
        } else {
            "inconclusive"
        };
        serde_json::json!({
            "overall": overall,
            "matrix": self.matrix,
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
            String::new(),
            "## Matrix".into(),
        ];
        for row in &self.matrix {
            lines.push(format!(
                "- `{}`: **{}** (exit {})",
                row.scenario, row.verdict, row.exit_code
            ));
        }
        if !reports.is_empty() {
            lines.push(String::new());
            lines.push("## Scenarios".into());
            for report in reports {
                lines.push(format!(
                    "- `{}` backend={} events={} verdict={}",
                    report.scenario,
                    report.backend,
                    report.events.len(),
                    report.verdict_label()
                ));
            }
        }
        lines.push(String::new());
        lines.push(
            "Non-claims: passing memory/core scenarios does not prove multi-node \
             process separation, object-store HA, or cloud backend equivalence."
                .into(),
        );
        lines.join("\n")
    }

    /// Overall process exit code for the suite.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        if self.dry_run {
            return 0;
        }
        if self.matrix.iter().any(|row| row.exit_code == 1) {
            return 4;
        }
        if self.matrix.iter().any(|row| row.verdict == "fail") {
            return 2;
        }
        if self.matrix.iter().any(|row| row.verdict == "inconclusive") {
            return 3;
        }
        0
    }
}

/// Writes one scenario's evidence bundle under `dir/<scenario>/`.
pub fn write_scenario_artifacts(
    dir: &Path,
    report: &CampaignReport,
) -> Result<(), ArtifactError> {
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
        dir.join("checker-verdict.json"),
        serde_json::to_vec_pretty(&report.verdict_json()?)?,
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
        exit_code: report.exit_code(),
    }
}
