//! Suite orchestration for autonomous campaigns (WP05 v3).

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::artifacts::{SuiteArtifacts, matrix_from_report, write_scenario_artifacts};
use crate::cutover_oracle::{self, ExpectedAuthority};
use crate::kellnr::ReleaseAttestation;
use crate::lifecycle::RunLifecycle;
use crate::preflight::PreflightReport;
use crate::profile::{Profile, RustFsHomeFleetProfile};
use crate::raw_lines_client::{self, RawLinesAck};
use crate::scenario::Suite;
use crate::{
    CampaignBackend, CampaignError, CampaignReport, CheckerAttestation, Scenario, run_campaign,
};
use holylog_correctness::Verdict;

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
                run_process_lifecycle(
                    profile,
                    &lifecycle,
                    &endpoint,
                    run_id,
                    scenario,
                    backend,
                )
                .await
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
                    } else {
                        let _ = lifecycle.cleanup();
                    }
                    forward.stop();
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
    rustfs_endpoint: &str,
    run_id: &str,
    scenario: Scenario,
    _backend: CampaignBackend,
) -> Result<CampaignReport, CampaignError> {
    match scenario {
        Scenario::RawLinesAbCutover => {
            run_raw_lines_ab_cutover(profile, lifecycle, rustfs_endpoint, run_id).await
        }
        other => Err(CampaignError::Scenario(format!(
            "{} is not a process-lifecycle scenario",
            other.as_str()
        ))),
    }
}

async fn run_raw_lines_ab_cutover(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    rustfs_endpoint: &str,
    run_id: &str,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::RawLinesAbCutover.as_str();
    let ha_prefix = format!("scripture/correctness/{run_id}/{token}/ha");

    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    let mut forward_a = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        19_001,
        9000,
    )
    .map_err(|error| CampaignError::Scenario(format!("port-forward A: {error}")))?;
    wait_tcp_ready("127.0.0.1:19001").await?;

    let phase_a: [&str; 3] = ["cutover-a-0", "cutover-a-1", "cutover-a-2"];
    let acks_a = raw_lines_client::exchange_committed("127.0.0.1:19001", &phase_a).await?;
    assert_dense_continuation(&acks_a, None)?;

    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("kill actor A: {error}")))?;
    // Force-delete can remove the pod from the API before the process exits.
    // Drop the forward and wait until A is unreachable so a later OK cannot be
    // mistaken for a lawful stale-writer ACK.
    forward_a.stop();
    wait_actor_unreachable(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        19_011,
    )
    .await?;

    let actor_b = lifecycle
        .deploy_actor_b_promote(profile, token, 2)
        .map_err(|error| CampaignError::Scenario(format!("promote actor B: {error}")))?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;
    if actor_a.uid == actor_b.uid {
        return Err(CampaignError::Scenario(
            "A and B share a pod UID; process separation unproven".into(),
        ));
    }

    let mut forward_b = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-b",
        19_002,
        9000,
    )
    .map_err(|error| CampaignError::Scenario(format!("port-forward B: {error}")))?;
    wait_tcp_ready("127.0.0.1:19002").await?;

    let phase_b: [&str; 3] = ["cutover-b-0", "cutover-b-1", "cutover-b-2"];
    let acks_b = raw_lines_client::exchange_committed("127.0.0.1:19002", &phase_b).await?;
    // Per-epoch denseness on the temporary raw-lines OK surface (loglet-local).
    assert_dense_continuation(&acks_b, None)?;
    forward_b.stop();

    let expected_payloads: Vec<&str> = phase_a
        .iter()
        .chain(phase_b.iter())
        .copied()
        .collect();
    let cutover = cutover_oracle::prove_raw_lines_cutover(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &expected_payloads,
        ExpectedAuthority {
            owner: cutover_oracle::actor_b_owner(),
            term: 2,
        },
    )
    .await?;

    // A-death / unreachability only — not a live stale-writer fence proof (family 6).
    assert_a_unreachable_for_producer(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        19_012,
    )
    .await?;

    let _ = lifecycle.kill_actor("scripture-actor-b");

    let ack_summary = |acks: &[RawLinesAck]| {
        acks.iter()
            .map(|ack| {
                serde_json::json!({
                    "first_offset": ack.first_offset,
                    "next_offset": ack.next_offset,
                    "payload_len": ack.payload.len(),
                })
            })
            .collect::<Vec<_>>()
    };

    Ok(CampaignReport {
        run_id: run_id.to_owned(),
        scenario: token,
        backend: "rustfs",
        environment: process_env(
            run_id,
            Scenario::RawLinesAbCutover,
            lifecycle,
            serde_json::json!({
                "evidence_class": "producer-ack + Holylog-state/readback oracle",
                "ha_prefix": ha_prefix,
                "actor_a": actor_a,
                "actor_b": actor_b,
                "cutover_oracle": cutover.observation,
                "actions": [
                    "bootstrap-a-and-serve",
                    "producer-raw-lines-to-a",
                    "kill-a",
                    "wait-a-unreachable",
                    "promote-b-term-2-same-ha-prefix",
                    "producer-raw-lines-to-b",
                    "assert-dense-per-epoch-ok-acks",
                    "holylog-exact-boundary-and-readback-oracle",
                    "assert-a-service-unreachable"
                ],
                "producer_acks_a": ack_summary(&acks_a),
                "producer_acks_b": ack_summary(&acks_b),
                "claims": [
                    "Producer committed OK ACKs came from actor A's raw-lines listener",
                    "A was force-deleted and became unreachable before B promote",
                    "B was promoted on the identical HA object prefix and returned dense per-epoch OK ACKs",
                    "Holylog membership: predecessor sealed.tail + start equals successor.start exactly",
                    "Serving authority fence decodes to owner scripture-own-b! at writer_term 2 on the successor generation",
                    "VirtualLog readback across the generation chain returns A then B payload identities exactly once and in order",
                    "A's Service has no live producer ACK path after cutover (death/unreachability, not live stale-writer fencing)",
                    "A and B are distinct OS processes/pods on configured nodes",
                    "object store is the run-owned ephemeral RustFS Service"
                ],
                "non_claims": [
                    "temporary-bootstrap-promote adapter until stable scripture serve lands",
                    "Raw-lines OK offsets are loglet-local; cross-cutover denseness is Holylog sealed-tail == successor.start plus chunk readback",
                    "checker not applied: temporary actor has no TraceEvent export",
                    "not automatic failover",
                    "not a live stale-writer fence (family 6); not family 14 DieAfterPayload; not family 15 reply-loss",
                    "scripture:0.1.0 remains development-source; cannot close family 22"
                ]
            }),
        ),
        events: Vec::new(),
        final_root: cutover.observation.clone(),
        final_authority: cutover.observation.get("serving_authority").cloned().unwrap_or(serde_json::Value::Null),
        verdict: Verdict::Pass,
        checker: CheckerAttestation::NotApplicable {
            reason: "temporary actor has no TraceEvent export".into(),
        },
        evidence_class: Some("producer-ack + Holylog-state/readback oracle"),
    })
}


fn assert_dense_continuation(
    acks: &[RawLinesAck],
    expected_first: Option<u64>,
) -> Result<(), CampaignError> {
    if acks.is_empty() {
        return Err(CampaignError::Scenario("no committed ACKs".into()));
    }
    if let Some(expected) = expected_first {
        if acks[0].first_offset != expected {
            return Err(CampaignError::Scenario(format!(
                "dense continuation broke: want first_offset {expected}, got {}",
                acks[0].first_offset
            )));
        }
    }
    for window in acks.windows(2) {
        if window[1].first_offset != window[0].next_offset {
            return Err(CampaignError::Scenario(format!(
                "non-dense ACK offsets: {} then {} (next was {})",
                window[0].first_offset, window[1].first_offset, window[0].next_offset
            )));
        }
    }
    for ack in acks {
        if ack.next_offset <= ack.first_offset {
            return Err(CampaignError::Scenario(format!(
                "OK next_offset {} not after first_offset {}",
                ack.next_offset, ack.first_offset
            )));
        }
    }
    Ok(())
}

async fn wait_tcp_ready(endpoint: &str) -> Result<(), CampaignError> {
    use tokio::net::TcpStream;
    use tokio::time::{Duration, timeout};
    for _ in 0..40 {
        match timeout(Duration::from_secs(1), TcpStream::connect(endpoint)).await {
            Ok(Ok(_stream)) => return Ok(()),
            Ok(Err(_)) | Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    Err(CampaignError::Scenario(format!(
        "raw-lines listener not ready at {endpoint}"
    )))
}

/// Waits until a Service has no reachable raw-lines TCP endpoint.
async fn wait_actor_unreachable(
    context: &str,
    namespace: &str,
    target: &str,
    local_port: u16,
) -> Result<(), CampaignError> {
    use tokio::net::TcpStream;
    use tokio::time::{Duration, timeout};
    for _ in 0..40 {
        let mut forward = match PortForward::start(context, namespace, target, local_port, 9000) {
            Ok(forward) => forward,
            Err(_) => return Ok(()),
        };
        let reachable = matches!(
            timeout(
                Duration::from_secs(1),
                TcpStream::connect(format!("127.0.0.1:{local_port}"))
            )
            .await,
            Ok(Ok(_))
        );
        forward.stop();
        if !reachable {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(CampaignError::Scenario(format!(
        "actor target {target} still accepts TCP connections after kill"
    )))
}

/// Asserts a producer exchange cannot obtain a committed OK from a dead actor.
async fn assert_a_unreachable_for_producer(
    context: &str,
    namespace: &str,
    target: &str,
    local_port: u16,
) -> Result<(), CampaignError> {
    use tokio::time::{Duration, timeout};
    let mut forward = match PortForward::start(context, namespace, target, local_port, 9000) {
        Ok(forward) => forward,
        Err(_) => return Ok(()),
    };
    let result = timeout(
        Duration::from_secs(3),
        raw_lines_client::exchange_committed(
            &format!("127.0.0.1:{local_port}"),
            &["stale-a-probe"],
        ),
    )
    .await;
    forward.stop();
    match result {
        Ok(Ok(_)) => Err(CampaignError::Scenario(format!(
            "killed actor target {target} still returned a committed OK ACK"
        ))),
        Ok(Err(_)) | Err(_) => Ok(()),
    }
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
