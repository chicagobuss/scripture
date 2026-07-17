//! Suite orchestration for autonomous campaigns.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::artifacts::{SuiteArtifacts, matrix_from_report, write_scenario_artifacts};
use crate::preflight::PreflightReport;
use crate::profile::Profile;
use crate::scenario::Suite;
use crate::{CampaignBackend, CampaignError, CampaignReport, run_campaign};

/// Options for one autonomous campaign invocation.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Profile name or resolved profile.
    pub profile: Profile,
    /// Scenario suite.
    pub suite: Suite,
    /// Optional explicit run id; generated when absent.
    pub run_id: Option<String>,
    /// Artifact root directory.
    pub artifact_dir: PathBuf,
    /// When false (default), validate environment only.
    pub execute: bool,
}

impl RunOptions {
    /// Executes preflight and, when `execute` is set, runs the suite scenarios.
    pub async fn run(self) -> Result<RunOutcome, RunError> {
        let run_id = self
            .run_id
            .unwrap_or_else(|| generate_run_id(self.profile.label()));
        let artifact_dir = self.artifact_dir.join(&run_id);
        std::fs::create_dir_all(&artifact_dir)?;

        let preflight = PreflightReport::run(&self.profile, self.execute);
        preflight.write(&artifact_dir)?;

        if !self.execute {
            let suite_artifacts = SuiteArtifacts {
                run_id: run_id.clone(),
                profile: self.profile.label().to_owned(),
                suite: self.suite.schedule_label().to_owned(),
                dry_run: true,
                matrix: Vec::new(),
            };
            suite_artifacts.write(&artifact_dir, &[])?;
            return Ok(RunOutcome {
                run_id,
                artifact_dir,
                preflight_ok: preflight.ok,
                dry_run: true,
                exit_code: if preflight.ok { 0 } else { 4 },
                reports: Vec::new(),
            });
        }

        if !preflight.ok {
            return Err(RunError::PreflightFailed);
        }

        if !self.suite.is_implemented() {
            return Err(RunError::SuiteUnavailable(self.suite.schedule_label()));
        }

        if matches!(self.profile, Profile::RustFsHomeFleet(_)) {
            return Err(RunError::ExecutionUnavailable(
                "rustfs-home-fleet --execute is deferred until Slice 3 owns isolated Kubernetes lifecycle and A/B/C process placement".to_owned(),
            ));
        }

        let backend = build_backend(&self.profile, &run_id)?;
        let mut reports = Vec::new();
        let mut matrix = Vec::new();
        for scenario in self.suite.scenarios() {
            let report = run_campaign(&run_id, scenario, backend.clone()).await?;
            let scenario_dir = artifact_dir.join(scenario.as_str());
            write_scenario_artifacts(&scenario_dir, &report)?;
            matrix.push(matrix_from_report(&report));
            reports.push(report);
        }

        let suite_artifacts = SuiteArtifacts {
            run_id: run_id.clone(),
            profile: self.profile.label().to_owned(),
            suite: self.suite.schedule_label().to_owned(),
            dry_run: false,
            matrix: matrix.clone(),
        };
        suite_artifacts.write(&artifact_dir, &reports)?;

        Ok(RunOutcome {
            run_id,
            artifact_dir,
            preflight_ok: true,
            dry_run: false,
            exit_code: suite_artifacts.exit_code(),
            reports,
        })
    }
}

/// Result of one campaign invocation.
#[derive(Debug)]
pub struct RunOutcome {
    /// Allocated run id.
    pub run_id: String,
    /// Artifact directory for this run.
    pub artifact_dir: PathBuf,
    /// Whether preflight passed.
    pub preflight_ok: bool,
    /// Whether this was dry-run only.
    pub dry_run: bool,
    /// Process exit code.
    pub exit_code: i32,
    /// Per-scenario reports (empty for dry-run).
    pub reports: Vec<CampaignReport>,
}

/// Orchestration failures.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// Preflight failed on execute.
    #[error("preflight failed; refusing --execute")]
    PreflightFailed,
    /// The requested suite has no runnable scenarios in this campaign slice.
    #[error("suite {0} has no executable scenarios in this build")]
    SuiteUnavailable(&'static str),
    /// The profile requires orchestration this campaign slice does not yet own.
    #[error("execution unavailable: {0}")]
    ExecutionUnavailable(String),
    /// Scenario or artifact failure.
    #[error(transparent)]
    Campaign(#[from] CampaignError),
    /// Artifact write failure.
    #[error(transparent)]
    Artifact(#[from] crate::artifacts::ArtifactError),
    /// Preflight write failure.
    #[error(transparent)]
    Preflight(#[from] crate::preflight::PreflightError),
    /// Backend construction failure.
    #[error("backend: {0}")]
    Backend(String),
    /// IO failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

fn generate_run_id(profile: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("{profile}-{millis}")
}

fn build_backend(profile: &Profile, run_id: &str) -> Result<CampaignBackend, RunError> {
    match profile {
        Profile::Memory => Ok(CampaignBackend::InMemory),
        Profile::RustFsHomeFleet(config) => {
            let prefix = format!("scripture/correctness/{run_id}");
            let credentials =
                scripture_runtime::resolve_credentials(scripture_runtime::BackendProfile::RustFs)
                    .map_err(|error| RunError::Backend(error.to_string()))?;
            let store = scripture_runtime::connect_s3_compat(
                &config.rustfs_service_dns,
                &config.rustfs_bucket,
                &config.rustfs_region,
                &credentials.access_key,
                &credentials.secret_key,
            )
            .map_err(|error| RunError::Backend(error.to_string()))?;
            drop(credentials);
            Ok(CampaignBackend::rustfs(store, &prefix))
        }
    }
}

/// Resolves the Scripture repo root from the current working directory.
#[must_use]
pub fn detect_repo_root() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}
