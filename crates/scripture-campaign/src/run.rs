//! Suite orchestration for autonomous campaigns (WP05 v3).

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::artifacts::{SuiteArtifacts, matrix_from_report, write_scenario_artifacts};
use crate::kellnr::ReleaseAttestation;
use crate::lifecycle::RunLifecycle;
use crate::preflight::PreflightReport;
use crate::profile::{Profile, RustFsHomeFleetProfile};
use crate::scenario::Suite;
use crate::{CampaignBackend, CampaignError, CampaignReport, Scenario, run_campaign};

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
    /// Retain failed run namespace (WP05 --keep-failed).
    pub keep_failed: bool,
}

impl RunOptions {
    /// Executes preflight and, when `execute` is set, runs the suite scenarios.
    pub async fn run(self) -> Result<RunOutcome, RunError> {
        let run_id = self
            .run_id
            .clone()
            .unwrap_or_else(|| generate_run_id(self.profile.label()));
        let artifact_dir = self.artifact_dir.join(&run_id);
        std::fs::create_dir_all(&artifact_dir)?;

        let repo_root = detect_repo_root();
        let attestation = ReleaseAttestation::detect(&repo_root);
        std::fs::write(
            artifact_dir.join("release-attestation.json"),
            serde_json::to_vec_pretty(&attestation)?,
        )?;

        let preflight = PreflightReport::run(&self.profile, self.execute);
        preflight.write(&artifact_dir)?;

        if !self.execute {
            let suite_artifacts = SuiteArtifacts::build(
                run_id.clone(),
                self.profile.label().to_owned(),
                self.suite.schedule_label().to_owned(),
                true,
                Vec::new(),
                &[],
                &attestation,
            );
            suite_artifacts.write(&artifact_dir, &[])?;
            return Ok(RunOutcome {
                run_id,
                artifact_dir,
                preflight_ok: preflight.ok,
                dry_run: true,
                exit_code: if preflight.ok { 0 } else { 2 },
                reports: Vec::new(),
            });
        }

        if !preflight.ok {
            return Err(RunError::PreflightFailed);
        }

        if !self.suite.is_implemented() {
            return Err(RunError::SuiteUnavailable(self.suite.schedule_label()));
        }

        match &self.profile {
            Profile::Memory => self.run_memory(&run_id, &artifact_dir, &attestation).await,
            Profile::RustFsHomeFleet(config) => {
                self.run_rustfs_isolated(config, &run_id, &artifact_dir, &attestation)
                    .await
            }
        }
    }

    async fn run_memory(
        &self,
        run_id: &str,
        artifact_dir: &Path,
        attestation: &ReleaseAttestation,
    ) -> Result<RunOutcome, RunError> {
        let backend = CampaignBackend::InMemory;
        let mut reports = Vec::new();
        let mut matrix = Vec::new();
        for scenario in self.suite.scenarios() {
            if scenario.needs_process_lifecycle() {
                return Err(RunError::ExecutionUnavailable(format!(
                    "{} requires rustfs-home-fleet",
                    scenario.as_str()
                )));
            }
            let report = run_campaign(run_id, scenario, backend.clone()).await?;
            let scenario_dir = artifact_dir.join(scenario.as_str());
            write_scenario_artifacts(&scenario_dir, &report)?;
            matrix.push(matrix_from_report(&report));
            reports.push(report);
        }
        finish_suite(
            run_id,
            self.profile.label(),
            self.suite.schedule_label(),
            artifact_dir,
            matrix,
            reports,
            attestation,
        )
    }

    async fn run_rustfs_isolated(
        &self,
        profile: &RustFsHomeFleetProfile,
        run_id: &str,
        artifact_dir: &Path,
        attestation: &ReleaseAttestation,
    ) -> Result<RunOutcome, RunError> {
        let mut lifecycle = RunLifecycle::create(profile, run_id, self.keep_failed)
            .map_err(|error| RunError::Lifecycle(error.to_string()))?;
        lifecycle
            .write_store_identity(artifact_dir)
            .map_err(|error| RunError::Lifecycle(error.to_string()))?;

        let local_port = 19000_u16;
        let mut forward = PortForward::start(
            &lifecycle.kube_context,
            &lifecycle.namespace,
            "svc/rustfs",
            local_port,
            9000,
        )
        .map_err(|error| RunError::Lifecycle(error.to_string()))?;

        let endpoint = format!("http://127.0.0.1:{local_port}");
        let mut reports = Vec::new();
        let mut matrix = Vec::new();
        let mut failed = false;

        for scenario in self.suite.scenarios() {
            if scenario.needs_process_lifecycle() && matches!(self.profile, Profile::Memory) {
                return Err(RunError::ExecutionUnavailable(format!(
                    "{} requires rustfs-home-fleet",
                    scenario.as_str()
                )));
            }
            // Exclusive per-scenario prefix — shared run prefixes leave durable
            // register/loglet state that fails later provision_fresh checks.
            let backend = build_ephemeral_backend(
                &endpoint,
                &lifecycle.store.bucket,
                lifecycle.access_key(),
                lifecycle.secret_key(),
                run_id,
                scenario.as_str(),
            )?;
            let result = if scenario.needs_process_lifecycle() {
                run_process_lifecycle(profile, &lifecycle, run_id, scenario, backend).await
            } else {
                run_campaign(run_id, scenario, backend).await
            };
            match result {
                Ok(report) => {
                    let scenario_dir = artifact_dir.join(scenario.as_str());
                    write_scenario_artifacts(&scenario_dir, &report)?;
                    matrix.push(matrix_from_report(&report));
                    if !report.is_pass() {
                        failed = true;
                    }
                    reports.push(report);
                }
                Err(error) => {
                    if self.keep_failed {
                        lifecycle.retain();
                    }
                    forward.stop();
                    let _ = lifecycle.cleanup();
                    return Err(error.into());
                }
            }
        }

        forward.stop();
        if failed && self.keep_failed {
            lifecycle.retain();
        } else {
            let _ = lifecycle.cleanup();
        }

        finish_suite(
            run_id,
            self.profile.label(),
            self.suite.schedule_label(),
            artifact_dir,
            matrix,
            reports,
            attestation,
        )
    }
}

async fn run_process_lifecycle(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    run_id: &str,
    scenario: Scenario,
    backend: CampaignBackend,
) -> Result<CampaignReport, CampaignError> {
    match scenario {
        Scenario::ProcessSeparatedBaseline => {
            run_process_separated_baseline(profile, lifecycle, run_id, backend).await
        }
        Scenario::KillAExplicitBPromotion => {
            run_kill_a_promote_b(profile, lifecycle, run_id, backend, scenario).await
        }
        Scenario::WedgedPayloadProcessSeparated => {
            run_kill_a_promote_b(profile, lifecycle, run_id, backend, scenario).await
        }
        Scenario::DirectionalBackendLossRecovery => {
            run_directional_backend_loss(profile, lifecycle, run_id, backend).await
        }
        Scenario::ScopedCredentialInvalidation => {
            run_scoped_credential_invalidation(profile, lifecycle, run_id, backend).await
        }
        other => Err(CampaignError::Scenario(format!(
            "{} is not a process-lifecycle scenario",
            other.as_str()
        ))),
    }
}

async fn run_process_separated_baseline(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    run_id: &str,
    backend: CampaignBackend,
) -> Result<CampaignReport, CampaignError> {
    let placement = lifecycle
        .deploy_actor_a(profile, Scenario::ProcessSeparatedBaseline.as_str())
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&placement, &profile.writer_a_node, "A")?;

    let mut report = run_campaign(run_id, Scenario::BaselineCommittedAck, backend).await?;
    report.scenario = Scenario::ProcessSeparatedBaseline.as_str();
    report.environment = process_env(
        run_id,
        Scenario::ProcessSeparatedBaseline,
        lifecycle,
        serde_json::json!({
            "actor_a": placement,
            "claims": [
                "actor A is a distinct OS process/pod on the configured node",
                "object store is the run-owned ephemeral RustFS Service"
            ],
            "non_claims": [
                "Actor A bootstrap process is temporary until stable scripture serve lands",
                "Baseline ACK traffic in this row is campaign-driven against the ephemeral store, not yet a producer-to-actor raw-lines proof",
                "scripture:0.1.0 import for the temporary adapter remains development-source and cannot close family 22"
            ]
        }),
    );
    let _ = lifecycle.kill_actor("scripture-actor-a");
    Ok(report)
}

async fn run_kill_a_promote_b(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    run_id: &str,
    backend: CampaignBackend,
    scenario: Scenario,
) -> Result<CampaignReport, CampaignError> {
    let token = scenario.as_str();
    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("kill actor A: {error}")))?;

    let actor_b = lifecycle
        .deploy_actor_b_promote(profile, token, 2)
        .map_err(|error| CampaignError::Scenario(format!("promote actor B: {error}")))?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;
    if actor_a.uid == actor_b.uid {
        return Err(CampaignError::Scenario(
            "A and B share a pod UID; process separation unproven".into(),
        ));
    }

    // Dense continuation on a distinct campaign prefix (honest non-claim below).
    let mut report = run_campaign(run_id, Scenario::BaselineCommittedAck, backend).await?;
    report.scenario = scenario.as_str();
    let wedge_note = if scenario == Scenario::WedgedPayloadProcessSeparated {
        "Writer death is approximated by force-deleting ready actor A; exact DieAfterPayload injection inside the temporary adapter is not claimed"
    } else {
        "Kill A is an operator-forced pod delete, not an automatic failover"
    };
    report.environment = process_env(
        run_id,
        scenario,
        lifecycle,
        serde_json::json!({
            "actor_a": actor_a,
            "actor_b": actor_b,
            "actions": ["bootstrap-a", "kill-a", "promote-b-term-2"],
            "claims": [
                "A and B are distinct OS processes/pods on configured nodes",
                "B was promoted after A was deleted",
                "object store is the run-owned ephemeral RustFS Service"
            ],
            "non_claims": [
                "temporary-bootstrap-promote adapter until stable scripture serve lands",
                wedge_note,
                "Dense continuation ACK traffic is campaign-driven against the ephemeral store, not producer-to-B raw-lines",
                "scripture:0.1.0 remains development-source; cannot close family 22"
            ]
        }),
    );
    let _ = lifecycle.kill_actor("scripture-actor-b");
    Ok(report)
}

async fn run_directional_backend_loss(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    run_id: &str,
    backend: CampaignBackend,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::DirectionalBackendLossRecovery.as_str();
    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    lifecycle
        .deny_rustfs_egress()
        .map_err(|error| CampaignError::Scenario(format!("deny rustfs egress: {error}")))?;
    // Give NetworkPolicy a moment to apply; readiness may flap but we do not
    // require a crash — loss of store path is the directional fault.
    std::thread::sleep(std::time::Duration::from_secs(5));

    lifecycle
        .restore_rustfs_egress()
        .map_err(|error| CampaignError::Scenario(format!("restore rustfs egress: {error}")))?;

    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("kill actor A after loss: {error}")))?;
    let actor_b = lifecycle
        .deploy_actor_b_promote(profile, token, 2)
        .map_err(|error| CampaignError::Scenario(format!("promote actor B after loss: {error}")))?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;

    let mut report = run_campaign(run_id, Scenario::BaselineCommittedAck, backend).await?;
    report.scenario = Scenario::DirectionalBackendLossRecovery.as_str();
    report.environment = process_env(
        run_id,
        Scenario::DirectionalBackendLossRecovery,
        lifecycle,
        serde_json::json!({
            "actor_a": actor_a,
            "actor_b": actor_b,
            "actions": [
                "bootstrap-a",
                "deny-rustfs-egress",
                "restore-rustfs-egress",
                "kill-a",
                "promote-b-term-2"
            ],
            "claims": [
                "Directional loss targeted only the run-owned RustFS Service path via NetworkPolicy",
                "Recovery used explicit promote of B after restoring egress",
                "A and B are distinct processes/pods"
            ],
            "non_claims": [
                "Does not prove automatic failover or store HA",
                "Does not prove mid-flight request semantics during the deny window",
                "Campaign ACK path remains campaign-driven; temporary adapter",
                "development-source only"
            ]
        }),
    );
    let _ = lifecycle.kill_actor("scripture-actor-b");
    Ok(report)
}

async fn run_scoped_credential_invalidation(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    run_id: &str,
    backend: CampaignBackend,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::ScopedCredentialInvalidation.as_str();
    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    lifecycle
        .invalidate_store_credentials()
        .map_err(|error| CampaignError::Scenario(format!("invalidate credentials: {error}")))?;
    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("kill A for cred restart: {error}")))?;

    // Redeploy A with invalidated Secret — must fail Ready within timeout.
    let invalid_a = lifecycle.deploy_actor_a(profile, token);
    let invalid_redeploy_error = match &invalid_a {
        Ok(_) => {
            let _ = lifecycle.kill_actor("scripture-actor-a");
            return Err(CampaignError::Scenario(
                "actor A became Ready with invalidated credentials; scoped invalidation unproven"
                    .into(),
            ));
        }
        Err(error) => error.to_string(),
    };
    let _ = lifecycle.kill_actor("scripture-actor-a");

    lifecycle
        .restore_store_credentials()
        .map_err(|error| CampaignError::Scenario(format!("restore credentials: {error}")))?;
    let actor_b = lifecycle
        .deploy_actor_b_promote(profile, token, 2)
        .map_err(|error| {
            CampaignError::Scenario(format!("promote B after credential restore: {error}"))
        })?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;

    let mut report = run_campaign(run_id, Scenario::BaselineCommittedAck, backend).await?;
    report.scenario = Scenario::ScopedCredentialInvalidation.as_str();
    report.environment = process_env(
        run_id,
        Scenario::ScopedCredentialInvalidation,
        lifecycle,
        serde_json::json!({
            "actor_a_before": actor_a,
            "actor_b": actor_b,
            "invalid_redeploy_error": invalid_redeploy_error,
            "actions": [
                "bootstrap-a",
                "invalidate-run-secret",
                "kill-a",
                "redeploy-a-expect-fail",
                "restore-run-secret",
                "promote-b-term-2"
            ],
            "claims": [
                "Credential invalidation was scoped to the run-owned Secret",
                "Actor restart with invalid credentials failed closed (not Ready)",
                "Recovery used restored credentials and explicit B promote"
            ],
            "non_claims": [
                "Not a proxy/middleware invalidation proof",
                "temporary adapter; development-source",
                "Campaign ACK path is campaign-driven"
            ]
        }),
    );
    let _ = lifecycle.kill_actor("scripture-actor-b");
    Ok(report)
}

fn assert_actor_placement(
    placement: &crate::lifecycle::ActorPlacement,
    expected_node: &str,
    label: &str,
) -> Result<(), CampaignError> {
    if placement.node != expected_node {
        return Err(CampaignError::Scenario(format!(
            "actor {label} placed on {} want {expected_node}",
            placement.node
        )));
    }
    if placement.uid.is_empty() {
        return Err(CampaignError::Scenario(format!(
            "actor {label} missing pod UID (process separation unproven)"
        )));
    }
    Ok(())
}

fn process_env(
    run_id: &str,
    scenario: Scenario,
    lifecycle: &RunLifecycle,
    process_separation: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "run_id": run_id,
        "scenario": scenario.as_str(),
        "adapter": "temporary-bootstrap-promote",
        "release_classification": "development-source",
        "process_separation": process_separation,
        "isolated_store": {
            "namespace": lifecycle.store.namespace,
            "service": lifecycle.store.service,
            "service_uid": lifecycle.store.service_uid,
            "bucket": lifecycle.store.bucket,
            "rustfs_node": lifecycle.store.rustfs_node
        }
    })
}

fn finish_suite(
    run_id: &str,
    profile: &str,
    suite: &str,
    artifact_dir: &Path,
    matrix: Vec<crate::artifacts::MatrixEntry>,
    reports: Vec<CampaignReport>,
    attestation: &ReleaseAttestation,
) -> Result<RunOutcome, RunError> {
    let suite_artifacts = SuiteArtifacts::build(
        run_id.to_owned(),
        profile.to_owned(),
        suite.to_owned(),
        false,
        matrix,
        &reports,
        attestation,
    );
    suite_artifacts.write(artifact_dir, &reports)?;
    Ok(RunOutcome {
        run_id: run_id.to_owned(),
        artifact_dir: artifact_dir.to_path_buf(),
        preflight_ok: true,
        dry_run: false,
        exit_code: suite_artifacts.exit_code(),
        reports,
    })
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
    /// Isolated lifecycle failure.
    #[error("lifecycle: {0}")]
    Lifecycle(String),
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
    /// JSON serialization failure.
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    /// IO failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl RunError {
    /// WP05 exit code for orchestration errors.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::PreflightFailed
            | Self::SuiteUnavailable(_)
            | Self::ExecutionUnavailable(_)
            | Self::Lifecycle(_) => 2,
            Self::Campaign(_) => 3,
            _ => 4,
        }
    }
}

fn generate_run_id(profile: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("{profile}-{millis}")
}

fn build_ephemeral_backend(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    run_id: &str,
    scenario: &str,
) -> Result<CampaignBackend, RunError> {
    let prefix = format!("scripture/correctness/{run_id}/{scenario}");
    let store =
        scripture_runtime::connect_s3_compat(endpoint, bucket, "us-east-1", access_key, secret_key)
            .map_err(|error| RunError::Backend(error.to_string()))?;
    Ok(CampaignBackend::rustfs(store, &prefix))
}

struct PortForward {
    child: Child,
}

impl PortForward {
    fn start(
        context: &str,
        namespace: &str,
        target: &str,
        local: u16,
        remote: u16,
    ) -> Result<Self, String> {
        let child = Command::new("kubectl")
            .arg("--context")
            .arg(context)
            .args([
                "-n",
                namespace,
                "port-forward",
                target,
                &format!("{local}:{remote}"),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|error| format!("port-forward spawn: {error}"))?;
        // Give the forward a moment to bind.
        std::thread::sleep(std::time::Duration::from_millis(800));
        Ok(Self { child })
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for PortForward {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Resolves the Scripture repo root from the current working directory.
#[must_use]
pub fn detect_repo_root() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}
