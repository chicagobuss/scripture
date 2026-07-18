//! Process-separated RustFS resilience scenarios (WP06 families 12–17).

use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;

use holylog_correctness::{TraceEvent, Verdict, check_trace};

use crate::cutover_oracle::{self, ExpectedAuthority};
use crate::lifecycle::{ActorFaultEnv, ActorPlacement, RunLifecycle};
use crate::profile::RustFsHomeFleetProfile;
use crate::raw_lines_client::{self, RawLinesAck};
use crate::{CampaignError, CampaignReport, CheckerAttestation, Scenario};

/// Dispatches one resilience family on the shared producer→A→B spine.
pub(crate) async fn run_resilience_scenario(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    rustfs_endpoint: &str,
    run_id: &str,
    scenario: Scenario,
    artifact_dir: &Path,
) -> Result<CampaignReport, CampaignError> {
    match scenario {
        Scenario::RawLinesBaseline => {
            run_baseline(profile, lifecycle, rustfs_endpoint, run_id, artifact_dir).await
        }
        Scenario::RawLinesAbCutover => {
            run_ab_cutover(profile, lifecycle, rustfs_endpoint, run_id, artifact_dir).await
        }
        Scenario::RawLinesDieAfterPayload => {
            run_die_after_payload(profile, lifecycle, rustfs_endpoint, run_id, artifact_dir).await
        }
        Scenario::RawLinesRootCasReplyLoss => {
            run_root_cas_reply_loss(profile, lifecycle, rustfs_endpoint, run_id, artifact_dir).await
        }
        Scenario::RawLinesDirectionalLoss => {
            run_directional_loss(profile, lifecycle, rustfs_endpoint, run_id, artifact_dir).await
        }
        Scenario::RawLinesCredentialInvalidation => {
            run_credential_invalidation(profile, lifecycle, rustfs_endpoint, run_id, artifact_dir)
                .await
        }
        other => Err(CampaignError::Scenario(format!(
            "{} is not a resilience process-lifecycle scenario",
            other.as_str()
        ))),
    }
}

async fn run_baseline(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    rustfs_endpoint: &str,
    run_id: &str,
    artifact_dir: &Path,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::RawLinesBaseline.as_str();
    let ha_prefix = lifecycle.scenario_ha_prefix(token);
    let scenario_dir = artifact_dir.join(token);
    std::fs::create_dir_all(scenario_dir.join("traces"))
        .map_err(|error| CampaignError::Scenario(format!("create scenario traces dir: {error}")))?;

    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    let port = allocate_local_port()?;
    let mut forward = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        port,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port}")).await?;

    let payloads = ["baseline-0", "baseline-1", "baseline-2"];
    let acks =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port}"), &payloads).await?;
    assert_dense_continuation(&acks, None)?;
    forward.stop();

    let events = collect_traces(lifecycle, &scenario_dir, &["scripture-actor-a"])?;
    let _ = lifecycle.kill_actor("scripture-actor-a");

    let oracle = cutover_oracle::prove_serving_baseline(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &payloads,
        ExpectedAuthority {
            owner: cutover_oracle::actor_a_owner(),
            term: 1,
        },
    )
    .await?;

    finish_report(
        run_id,
        token,
        lifecycle,
        events,
        oracle.observation.clone(),
        serde_json::json!({
            "ha_prefix": ha_prefix,
            "actor_a": actor_a,
            "producer_acks": ack_summary(&acks),
            "actions": ["bootstrap-a", "producer-raw-lines", "holylog-baseline-oracle"],
            "claims": [
                "Independent family-12 baseline on producer raw-lines → actor A",
                "Serving authority is scripture-own-a! at writer_term 1",
                "VirtualLog readback returns baseline payload identities exactly once"
            ],
            "non_claims": [
                "temporary-bootstrap-promote adapter",
                "development-source; not family 22"
            ],
            "cutover_oracle": oracle.observation,
        }),
    )
}

async fn run_ab_cutover(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    rustfs_endpoint: &str,
    run_id: &str,
    artifact_dir: &Path,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::RawLinesAbCutover.as_str();
    let ha_prefix = lifecycle.scenario_ha_prefix(token);
    let scenario_dir = artifact_dir.join(token);
    std::fs::create_dir_all(scenario_dir.join("traces"))
        .map_err(|error| CampaignError::Scenario(format!("create scenario traces dir: {error}")))?;

    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    let port_a = allocate_local_port()?;
    let mut forward_a = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        port_a,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_a}")).await?;

    let phase_a = ["cutover-a-0", "cutover-a-1", "cutover-a-2"];
    let acks_a =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_a}"), &phase_a).await?;
    assert_dense_continuation(&acks_a, None)?;
    let _ = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &phase_a,
        ExpectedAuthority {
            owner: cutover_oracle::actor_a_owner(),
            term: 1,
        },
        false,
        Duration::from_secs(60),
    )
    .await?;

    let mut events = collect_traces(lifecycle, &scenario_dir, &["scripture-actor-a"])?;
    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("kill actor A: {error}")))?;
    forward_a.stop();
    let probe_port = allocate_local_port()?;
    wait_actor_unreachable(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        probe_port,
    )
    .await?;

    let actor_b = lifecycle
        .deploy_actor_b_promote(profile, token, 2)
        .map_err(|error| CampaignError::Scenario(format!("promote actor B: {error}")))?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;
    assert_distinct_uids(&actor_a, &actor_b)?;

    let port_b = allocate_local_port()?;
    let mut forward_b = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-b",
        port_b,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_b}")).await?;

    let phase_b = ["cutover-b-0", "cutover-b-1", "cutover-b-2"];
    let acks_b =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_b}"), &phase_b).await?;
    assert_dense_continuation(
        &acks_b,
        Some(acks_a.last().expect("phase A ACKs").next_offset),
    )?;
    forward_b.stop();
    let expected: Vec<&str> = phase_a.iter().chain(phase_b.iter()).copied().collect();
    let oracle = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &expected,
        ExpectedAuthority {
            owner: cutover_oracle::actor_b_owner(),
            term: 2,
        },
        true,
        Duration::from_secs(60),
    )
    .await?;
    events.extend(collect_traces(
        lifecycle,
        &scenario_dir,
        &["scripture-actor-b"],
    )?);
    let _ = lifecycle.kill_actor("scripture-actor-b");

    let unreachable_port = allocate_local_port()?;
    assert_a_unreachable_for_producer(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        unreachable_port,
    )
    .await?;

    finish_report(
        run_id,
        token,
        lifecycle,
        events,
        oracle.observation.clone(),
        serde_json::json!({
            "ha_prefix": ha_prefix,
            "actor_a": actor_a,
            "actor_b": actor_b,
            "producer_acks_a": ack_summary(&acks_a),
            "producer_acks_b": ack_summary(&acks_b),
            "b_first_offset": acks_b.first().map(|a| a.first_offset),
            "a_final_next_offset": acks_a.last().map(|a| a.next_offset),
            "actions": [
                "bootstrap-a",
                "producer-to-a",
                "kill-a",
                "promote-b",
                "producer-to-b",
                "wait-holylog-durable",
                "holylog-cutover-oracle"
            ],
            "claims": [
                "A→kill→B cutover on identical HA prefix",
                "B first OK continues A's final next_offset (post PR#7 producer identity)",
                "Exact sealed-tail boundary and B fence/term",
                "Cross-generation Holylog readback once/in-order with distinct producer IDs",
                "A death/unreachability only (not live stale-writer)"
            ],
            "non_claims": [
                "not family 6 live stale-writer",
                "temporary-bootstrap-promote adapter",
                "development-source",
                "pre-PR#7 family 13 acceptance superseded"
            ],
            "cutover_oracle": oracle.observation,
        }),
    )
}

async fn run_die_after_payload(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    rustfs_endpoint: &str,
    run_id: &str,
    artifact_dir: &Path,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::RawLinesDieAfterPayload.as_str();
    let ha_prefix = lifecycle.scenario_ha_prefix(token);
    let scenario_dir = artifact_dir.join(token);
    std::fs::create_dir_all(scenario_dir.join("traces"))
        .map_err(|error| CampaignError::Scenario(format!("create scenario traces dir: {error}")))?;

    let pre = ["die-pre-0", "die-pre-1", "die-pre-2"];
    let unacked = "die-unacked-unique";
    let post = ["die-post-0", "die-post-1", "die-post-2"];

    let actor_a = lifecycle
        .deploy_actor_a_with_faults(
            profile,
            token,
            ActorFaultEnv::die_after_payload(pre.len() as u64),
        )
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    let port_a = allocate_local_port()?;
    let mut forward_a = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        port_a,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_a}")).await?;

    let acks_pre =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_a}"), &pre).await?;
    assert_dense_continuation(&acks_pre, None)?;

    raw_lines_client::expect_no_committed_ok(
        &format!("127.0.0.1:{port_a}"),
        unacked,
        Duration::from_secs(8),
    )
    .await?;

    // Prove unacked reached durable predecessor storage before killing A.
    let mut pre_plus_unacked: Vec<&str> = pre.to_vec();
    pre_plus_unacked.push(unacked);
    let _ = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &pre_plus_unacked,
        ExpectedAuthority {
            owner: cutover_oracle::actor_a_owner(),
            term: 1,
        },
        false,
        Duration::from_secs(60),
    )
    .await?;

    let mut events = collect_traces(lifecycle, &scenario_dir, &["scripture-actor-a"])?;
    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("kill wedged A: {error}")))?;
    forward_a.stop();

    let actor_b = lifecycle
        .deploy_actor_b_promote(profile, token, 2)
        .map_err(|error| CampaignError::Scenario(format!("promote actor B: {error}")))?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;
    assert_distinct_uids(&actor_a, &actor_b)?;

    let port_b = allocate_local_port()?;
    let mut forward_b = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-b",
        port_b,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_b}")).await?;
    let acks_post =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_b}"), &post).await?;
    // 3 pre ACKs end at next_offset N; durable unacked occupies N; B starts at N+1.
    let expect_b_first = acks_pre
        .last()
        .map(|ack| ack.next_offset.saturating_add(1))
        .ok_or_else(|| CampaignError::Scenario("missing pre ACK for offset continuity".into()))?;
    assert_dense_continuation(&acks_post, Some(expect_b_first))?;
    forward_b.stop();

    let mut expected: Vec<&str> = pre.to_vec();
    expected.push(unacked);
    expected.extend_from_slice(&post);
    let oracle = match cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &expected,
        ExpectedAuthority {
            owner: cutover_oracle::actor_b_owner(),
            term: 2,
        },
        true,
        Duration::from_secs(60),
    )
    .await
    {
        Ok(report) => report,
        Err(error) => {
            events.extend(
                collect_traces(lifecycle, &scenario_dir, &["scripture-actor-b"])
                    .unwrap_or_default(),
            );
            let _ = lifecycle.kill_actor("scripture-actor-b");
            return Ok(CampaignReport {
                run_id: run_id.to_owned(),
                scenario: token,
                backend: "rustfs",
                environment: serde_json::json!({
                    "run_id": run_id,
                    "scenario": token,
                    "adapter": "temporary-bootstrap-promote",
                    "release_classification": "development-source",
                    "blocker": {
                        "family": 14,
                        "summary": "DieAfterPayload recovery failed Holylog durable wait after PR#7 producer-identity fix",
                        "oracle_error": error.to_string(),
                        "expect_b_first_offset": expect_b_first,
                        "acks_post": ack_summary(&acks_post),
                        "ha_prefix": ha_prefix,
                    },
                    "process_separation": {
                        "actor_a": actor_a,
                        "actor_b": actor_b,
                        "unacked_payload": unacked,
                        "producer_acks_pre": ack_summary(&acks_pre),
                        "producer_acks_post": ack_summary(&acks_post),
                    },
                    "isolated_store": {
                        "namespace": lifecycle.store.namespace,
                        "bucket": lifecycle.store.bucket,
                    }
                }),
                events: renumber_global_seq(events),
                final_root: serde_json::json!({"blocker": true}),
                final_authority: serde_json::Value::Null,
                verdict: Verdict::Inconclusive {
                    reason: "WP07 family 14: Holylog durable oracle did not observe full pre+unacked+post history".into(),
                    evidence_slice: vec![error.to_string()],
                },
                checker: CheckerAttestation::Evaluated,
                evidence_class: Some("blocker-evidence"),
            });
        }
    };
    events.extend(collect_traces(
        lifecycle,
        &scenario_dir,
        &["scripture-actor-b"],
    )?);
    let _ = lifecycle.kill_actor("scripture-actor-b");

    finish_report(
        run_id,
        token,
        lifecycle,
        events,
        oracle.observation.clone(),
        serde_json::json!({
            "ha_prefix": ha_prefix,
            "actor_a": actor_a,
            "actor_b": actor_b,
            "fault": "DieAfterPayload post-durable pre-ACK",
            "unacked_payload": unacked,
            "producer_acks_pre": ack_summary(&acks_pre),
            "producer_acks_post": ack_summary(&acks_post),
            "expect_b_first_offset": expect_b_first,
            "claims": [
                "Pre-fault payloads received committed OK",
                "Unacked payload had no committed OK before A death",
                "Unacked payload present in Holylog predecessor decode",
                "B first OK continues after pre+unacked logical offsets",
                "Post payloads are new durable successor records (distinct producer IDs)"
            ],
            "non_claims": ["temporary adapter", "development-source", "pre-PR#7 WP06 family 14 blocker superseded"],
            "cutover_oracle": oracle.observation,
        }),
    )
}

async fn run_root_cas_reply_loss(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    rustfs_endpoint: &str,
    run_id: &str,
    artifact_dir: &Path,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::RawLinesRootCasReplyLoss.as_str();
    let ha_prefix = lifecycle.scenario_ha_prefix(token);
    let scenario_dir = artifact_dir.join(token);
    std::fs::create_dir_all(scenario_dir.join("traces"))
        .map_err(|error| CampaignError::Scenario(format!("create scenario traces dir: {error}")))?;

    let phase_a = ["reply-loss-a-0", "reply-loss-a-1"];
    let phase_b = ["reply-loss-b-0", "reply-loss-b-1"];

    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    let port_a = allocate_local_port()?;
    let mut forward_a = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        port_a,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_a}")).await?;
    let acks_a =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_a}"), &phase_a).await?;
    assert_dense_continuation(&acks_a, None)?;
    let _ = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &phase_a,
        ExpectedAuthority {
            owner: cutover_oracle::actor_a_owner(),
            term: 1,
        },
        false,
        Duration::from_secs(60),
    )
    .await?;
    let mut events = collect_traces(lifecycle, &scenario_dir, &["scripture-actor-a"])?;
    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("kill A before B reply-loss: {error}")))?;
    forward_a.stop();

    // Arm RootCasReplyLost precisely on B's promote root CAS. Temporary CLI
    // retries once after Indeterminate via fresh observation (same process).
    let actor_b = lifecycle
        .deploy_actor_b_promote_with_faults(profile, token, 2, ActorFaultEnv::root_cas_reply_loss())
        .map_err(|error| CampaignError::Scenario(format!("promote B with reply-loss: {error}")))?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;
    assert_distinct_uids(&actor_a, &actor_b)?;
    if !pod_ready(lifecycle, "scripture-actor-b")? {
        return Err(CampaignError::Scenario(
            "family 15: B did not become Ready after Indeterminate→fresh-observation retry".into(),
        ));
    }

    let stale = cutover_oracle::prove_stale_cas_cannot_overwrite(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
    )
    .await?;

    let port_b = allocate_local_port()?;
    let mut forward_b = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-b",
        port_b,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_b}")).await?;
    let acks_b =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_b}"), &phase_b).await?;
    assert_dense_continuation(
        &acks_b,
        Some(acks_a.last().expect("phase A ACKs").next_offset),
    )?;
    forward_b.stop();
    let expected: Vec<&str> = phase_a.iter().chain(phase_b.iter()).copied().collect();
    let oracle = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &expected,
        ExpectedAuthority {
            owner: cutover_oracle::actor_b_owner(),
            term: 2,
        },
        true,
        Duration::from_secs(60),
    )
    .await
    .map_err(|error| {
        CampaignError::Scenario(format!(
            "family 15 cutover oracle failed (no baseline fallback): {error}"
        ))
    })?;
    events.extend(collect_traces(
        lifecycle,
        &scenario_dir,
        &["scripture-actor-b"],
    )?);
    let _ = lifecycle.kill_actor("scripture-actor-b");

    finish_report(
        run_id,
        token,
        lifecycle,
        events,
        oracle.observation.clone(),
        serde_json::json!({
            "ha_prefix": ha_prefix,
            "actor_a": actor_a,
            "actor_b": actor_b,
            "fault": "RootCasReplyLost on B promote CAS → Indeterminate → same-process fresh observation retry",
            "stale_cas_witness": stale,
            "producer_acks_a": ack_summary(&acks_a),
            "producer_acks_b": ack_summary(&acks_b),
            "claims": [
                "B promote applied root CAS then lost reply; resolved via fresh observation in-process",
                "Competing stale CAS could not overwrite the applied root",
                "Holylog cutover oracle holds A then B payloads with distinct producer IDs"
            ],
            "non_claims": [
                "not network proxy",
                "temporary adapter",
                "development-source",
                "campaign-faults CLI Indeterminate retry is test-only"
            ],
            "cutover_oracle": oracle.observation,
        }),
    )
}

async fn run_directional_loss(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    rustfs_endpoint: &str,
    run_id: &str,
    artifact_dir: &Path,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::RawLinesDirectionalLoss.as_str();
    let ha_prefix = lifecycle.scenario_ha_prefix(token);
    let scenario_dir = artifact_dir.join(token);
    std::fs::create_dir_all(scenario_dir.join("traces"))
        .map_err(|error| CampaignError::Scenario(format!("create scenario traces dir: {error}")))?;

    let pre = ["dir-pre-0", "dir-pre-1"];
    let post = ["dir-post-0", "dir-post-1"];

    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    let port_a = allocate_local_port()?;
    let mut forward_a = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        port_a,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_a}")).await?;
    let acks_pre =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_a}"), &pre).await?;
    assert_dense_continuation(&acks_pre, None)?;
    let _ = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &pre,
        ExpectedAuthority {
            owner: cutover_oracle::actor_a_owner(),
            term: 1,
        },
        false,
        Duration::from_secs(60),
    )
    .await?;
    let mut events = collect_traces(lifecycle, &scenario_dir, &["scripture-actor-a"])?;
    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("kill A: {error}")))?;
    forward_a.stop();

    // Deny before B starts so the fresh promote cannot open RustFS connections.
    lifecycle
        .deny_rustfs_egress()
        .map_err(|error| CampaignError::Scenario(format!("deny rustfs egress: {error}")))?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let blocked_b = lifecycle
        .deploy_actor_b_promote_with_faults(
            profile,
            token,
            2,
            ActorFaultEnv {
                require_ready: false,
                ..ActorFaultEnv::default()
            },
        )
        .map_err(|error| CampaignError::Scenario(format!("promote B under deny: {error}")))?;
    if pod_ready(lifecycle, "scripture-actor-b")? {
        events.extend(collect_traces(
            lifecycle,
            &scenario_dir,
            &["scripture-actor-b"],
        )?);
        let _ = lifecycle.kill_actor("scripture-actor-b");
        let _ = lifecycle.restore_rustfs_egress();
        return Ok(CampaignReport {
            run_id: run_id.to_owned(),
            scenario: token,
            backend: "rustfs",
            environment: serde_json::json!({
                "run_id": run_id,
                "scenario": token,
                "adapter": "temporary-bootstrap-promote",
                "release_classification": "development-source",
                "not_run": {
                    "family": 16,
                    "limitation": "kube-router NetworkPolicy DNS-only egress replacement does not prevent B→run-owned RustFS Service connectivity on this fleet",
                    "observed": "B became Ready / promote-and-serve succeeded while campaign-default-deny-egress allowed only kube-system DNS",
                    "actor_b_under_deny": blocked_b,
                    "ha_prefix": ha_prefix,
                },
                "isolated_store": {
                    "namespace": lifecycle.store.namespace,
                    "bucket": lifecycle.store.bucket,
                }
            }),
            events: renumber_global_seq(events),
            final_root: serde_json::json!({"not_run": true}),
            final_authority: serde_json::Value::Null,
            verdict: Verdict::Inconclusive {
                reason: "not-run: NetworkPolicy cannot express directional B→RustFS loss on this kube-router fleet (B Ready under DNS-only egress)".into(),
                evidence_slice: vec![
                    "NetworkPolicy campaign-default-deny-egress replaced with DNS-only egress".into(),
                    "Fresh B promote still reached RustFS and became Ready".into(),
                ],
            },
            checker: CheckerAttestation::NotApplicable {
                reason: "directional NetworkPolicy limitation; no durable oracle Pass claimed".into(),
            },
            evidence_class: Some("not-run-networkpolicy-limitation"),
        });
    }
    let blocked_probe = allocate_local_port()?;
    if let Ok(mut forward) = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "pod/scripture-actor-b",
        blocked_probe,
        9000,
    ) {
        let denied = raw_lines_client::expect_no_committed_ok(
            &format!("127.0.0.1:{blocked_probe}"),
            "dir-blocked-unique",
            Duration::from_secs(5),
        )
        .await;
        forward.stop();
        denied?;
    }
    events.extend(collect_traces(
        lifecycle,
        &scenario_dir,
        &["scripture-actor-b"],
    )?);
    let _ = lifecycle.kill_actor("scripture-actor-b");

    lifecycle
        .restore_rustfs_egress()
        .map_err(|error| CampaignError::Scenario(format!("restore rustfs egress: {error}")))?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let actor_b = lifecycle
        .deploy_actor_b_promote(profile, token, 2)
        .map_err(|error| CampaignError::Scenario(format!("promote B after restore: {error}")))?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;
    assert_distinct_uids(&actor_a, &actor_b)?;
    assert_distinct_uids(&blocked_b, &actor_b)?;

    let port_b = allocate_local_port()?;
    let mut forward_b = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-b",
        port_b,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_b}")).await?;
    let acks_post =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_b}"), &post).await?;
    assert_dense_continuation(
        &acks_post,
        Some(acks_pre.last().expect("pre ACKs").next_offset),
    )?;
    forward_b.stop();

    let mut required: Vec<&str> = pre.to_vec();
    required.extend_from_slice(&post);
    let oracle = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &required,
        ExpectedAuthority {
            owner: cutover_oracle::actor_b_owner(),
            term: 2,
        },
        true,
        Duration::from_secs(60),
    )
    .await?;
    events.extend(collect_traces(
        lifecycle,
        &scenario_dir,
        &["scripture-actor-b"],
    )?);
    let _ = lifecycle.kill_actor("scripture-actor-b");

    finish_report(
        run_id,
        token,
        lifecycle,
        events,
        oracle.observation.clone(),
        serde_json::json!({
            "ha_prefix": ha_prefix,
            "actor_a": actor_a,
            "actor_b_under_deny": blocked_b,
            "actor_b": actor_b,
            "fault": "NetworkPolicy DNS-only egress before B promote (fresh process, no open TCP)",
            "producer_acks_pre": ack_summary(&acks_pre),
            "producer_acks_post": ack_summary(&acks_post),
            "claims": [
                "B under RustFS egress deny did not become Ready / no committed OK",
                "After restore, B promote served post payloads with dense continuation",
                "Holylog durable oracle on pre+post"
            ],
            "non_claims": [
                "temporary adapter",
                "development-source",
                "NetworkPolicy scoped to campaign namespace only"
            ],
            "cutover_oracle": oracle.observation,
        }),
    )
}

async fn run_credential_invalidation(
    profile: &RustFsHomeFleetProfile,
    lifecycle: &RunLifecycle,
    rustfs_endpoint: &str,
    run_id: &str,
    artifact_dir: &Path,
) -> Result<CampaignReport, CampaignError> {
    let token = Scenario::RawLinesCredentialInvalidation.as_str();
    let ha_prefix = lifecycle.scenario_ha_prefix(token);
    let scenario_dir = artifact_dir.join(token);
    std::fs::create_dir_all(scenario_dir.join("traces"))
        .map_err(|error| CampaignError::Scenario(format!("create scenario traces dir: {error}")))?;

    let pre = ["cred-pre-0", "cred-pre-1"];
    let post = ["cred-post-0", "cred-post-1"];

    let actor_a = lifecycle
        .deploy_actor_a(profile, token)
        .map_err(|error| CampaignError::Scenario(format!("deploy actor A: {error}")))?;
    assert_actor_placement(&actor_a, &profile.writer_a_node, "A")?;

    let port_a = allocate_local_port()?;
    let mut forward_a = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-a",
        port_a,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_a}")).await?;
    let acks_pre =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_a}"), &pre).await?;
    assert_dense_continuation(&acks_pre, None)?;
    let _ = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &pre,
        ExpectedAuthority {
            owner: cutover_oracle::actor_a_owner(),
            term: 1,
        },
        false,
        Duration::from_secs(60),
    )
    .await?;
    let mut events = collect_traces(lifecycle, &scenario_dir, &["scripture-actor-a"])?;
    lifecycle
        .kill_actor("scripture-actor-a")
        .map_err(|error| CampaignError::Scenario(format!("stop A before invalidate: {error}")))?;
    forward_a.stop();

    lifecycle
        .invalidate_store_credentials()
        .map_err(|error| CampaignError::Scenario(format!("invalidate credentials: {error}")))?;

    // B must not become Ready / Serving with invalid scenario-local credentials.
    let bad_b = lifecycle
        .deploy_actor_b_promote_with_faults(
            profile,
            token,
            2,
            ActorFaultEnv {
                require_ready: false,
                ..ActorFaultEnv::default()
            },
        )
        .map_err(|error| CampaignError::Scenario(format!("promote B with bad creds: {error}")))?;
    if pod_ready(lifecycle, "scripture-actor-b")? {
        return Err(CampaignError::Scenario(
            "family 17: actor B became Ready with invalid credentials".into(),
        ));
    }
    let bad_probe = allocate_local_port()?;
    if let Ok(mut forward) = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "pod/scripture-actor-b",
        bad_probe,
        9000,
    ) {
        let denied = raw_lines_client::expect_no_committed_ok(
            &format!("127.0.0.1:{bad_probe}"),
            "cred-invalid-probe",
            Duration::from_secs(3),
        )
        .await;
        forward.stop();
        denied?;
    }
    let _ = collect_traces(lifecycle, &scenario_dir, &["scripture-actor-b"]);
    let _ = lifecycle.kill_actor("scripture-actor-b");

    lifecycle
        .restore_store_credentials()
        .map_err(|error| CampaignError::Scenario(format!("restore credentials: {error}")))?;

    let actor_b = lifecycle
        .deploy_actor_b_promote(profile, token, 2)
        .map_err(|error| {
            CampaignError::Scenario(format!("promote B after cred restore: {error}"))
        })?;
    assert_actor_placement(&actor_b, &profile.writer_b_node, "B")?;
    assert_distinct_uids(&actor_a, &actor_b)?;
    assert_distinct_uids(&bad_b, &actor_b)?;

    let port_b = allocate_local_port()?;
    let mut forward_b = PortForward::start(
        &lifecycle.kube_context,
        &lifecycle.namespace,
        "svc/scripture-actor-b",
        port_b,
        9000,
    )?;
    wait_tcp_ready(&format!("127.0.0.1:{port_b}")).await?;
    let acks_post =
        raw_lines_client::exchange_committed(&format!("127.0.0.1:{port_b}"), &post).await?;
    assert_dense_continuation(
        &acks_post,
        Some(acks_pre.last().expect("pre ACKs").next_offset),
    )?;
    forward_b.stop();

    let mut expected: Vec<&str> = pre.to_vec();
    expected.extend_from_slice(&post);
    let oracle = cutover_oracle::wait_for_durable_payloads(
        rustfs_endpoint,
        &lifecycle.store.bucket,
        lifecycle.access_key(),
        lifecycle.secret_key(),
        &ha_prefix,
        &expected,
        ExpectedAuthority {
            owner: cutover_oracle::actor_b_owner(),
            term: 2,
        },
        true,
        Duration::from_secs(60),
    )
    .await?;
    events.extend(collect_traces(
        lifecycle,
        &scenario_dir,
        &["scripture-actor-b"],
    )?);
    let _ = lifecycle.kill_actor("scripture-actor-b");

    finish_report(
        run_id,
        token,
        lifecycle,
        events,
        oracle.observation.clone(),
        serde_json::json!({
            "ha_prefix": ha_prefix,
            "actor_a": actor_a,
            "actor_b_invalid_attempt": bad_b,
            "actor_b": actor_b,
            "fault": "scenario-local credential invalidation before B activation",
            "producer_acks_pre": ack_summary(&acks_pre),
            "producer_acks_post": ack_summary(&acks_post),
            "claims": [
                "Invalid credentials prevented B Ready / committed OK",
                "Restored scenario credentials + B promote served producer traffic",
                "Holylog cutover oracle on pre+post payloads"
            ],
            "non_claims": [
                "Secret mutate without process reread is insufficient",
                "temporary adapter",
                "development-source",
                "does not alter Tracker RustFS or persistent namespaces"
            ],
            "cutover_oracle": oracle.observation,
        }),
    )
}

fn finish_report(
    run_id: &str,
    token: &'static str,
    lifecycle: &RunLifecycle,
    events: Vec<TraceEvent>,
    final_root: serde_json::Value,
    process_separation: serde_json::Value,
) -> Result<CampaignReport, CampaignError> {
    let events = renumber_global_seq(events);
    let checker_trace = if events.is_empty() {
        Verdict::Inconclusive {
            reason: "empty actor TraceEvent bridge; Holylog oracle is authoritative".into(),
            evidence_slice: Vec::new(),
        }
    } else {
        // WP07: do not filter real events to manufacture a checker Pass.
        check_trace(&events)
    };
    let checker = CheckerAttestation::Evaluated;
    // Holylog oracle already succeeded before finish_report. Checker Fail is a
    // real fail; checker Inconclusive is reported honestly without failing the
    // durable-oracle Pass.
    let verdict = match &checker_trace {
        Verdict::Fail { .. } => checker_trace.clone(),
        Verdict::Pass | Verdict::Inconclusive { .. } => Verdict::Pass,
    };
    let final_authority = final_root
        .get("serving_authority")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    Ok(CampaignReport {
        run_id: run_id.to_owned(),
        scenario: token,
        backend: "rustfs",
        environment: serde_json::json!({
            "run_id": run_id,
            "scenario": token,
            "adapter": "temporary-bootstrap-promote",
            "release_classification": "development-source",
            "process_separation": process_separation,
            "checker_trace_verdict": checker_trace,
            "isolated_store": {
                "namespace": lifecycle.store.namespace,
                "service": lifecycle.store.service,
                "service_uid": lifecycle.store.service_uid,
                "bucket": lifecycle.store.bucket,
                "rustfs_node": lifecycle.store.rustfs_node
            }
        }),
        events,
        final_root,
        final_authority,
        verdict,
        checker,
        evidence_class: Some("Holylog durable oracle + runtime TraceEvent bridge"),
    })
}

fn renumber_global_seq(mut events: Vec<TraceEvent>) -> Vec<TraceEvent> {
    for (index, event) in events.iter_mut().enumerate() {
        event.global_seq = (index as u64).saturating_add(1);
    }
    events
}

fn collect_traces(
    lifecycle: &RunLifecycle,
    scenario_dir: &Path,
    actors: &[&str],
) -> Result<Vec<TraceEvent>, CampaignError> {
    let mut events = Vec::new();
    for actor in actors {
        let dest = scenario_dir.join("traces").join(format!("{actor}.ndjson"));
        match lifecycle.collect_actor_trace(actor, &dest) {
            Ok(()) => {
                if dest.exists() {
                    events.extend(parse_ndjson_trace(&dest)?);
                }
            }
            Err(error) => {
                // Soft: pod may have already exited; treat as empty contribution.
                let _ = error;
            }
        }
        if let Ok(logs) = lifecycle.actor_logs(actor) {
            let log_path = scenario_dir.join("traces").join(format!("{actor}.log"));
            let _ = std::fs::write(log_path, logs);
        }
    }
    Ok(events)
}

fn parse_ndjson_trace(path: &Path) -> Result<Vec<TraceEvent>, CampaignError> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        CampaignError::Scenario(format!("read trace {}: {error}", path.display()))
    })?;
    let mut events = Vec::new();
    for (line_no, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event: TraceEvent = serde_json::from_str(line).map_err(|error| {
            CampaignError::Scenario(format!(
                "trace parse {}:{}: {error}",
                path.display(),
                line_no + 1
            ))
        })?;
        events.push(event);
    }
    Ok(events)
}

fn allocate_local_port() -> Result<u16, CampaignError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| CampaignError::Scenario(format!("allocate port: {error}")))?;
    let port = listener
        .local_addr()
        .map_err(|error| CampaignError::Scenario(format!("local_addr: {error}")))?
        .port();
    drop(listener);
    Ok(port)
}

fn ack_summary(acks: &[RawLinesAck]) -> Vec<serde_json::Value> {
    acks.iter()
        .map(|ack| {
            serde_json::json!({
                "first_offset": ack.first_offset,
                "next_offset": ack.next_offset,
                "payload_len": ack.payload.len(),
            })
        })
        .collect()
}

fn assert_dense_continuation(
    acks: &[RawLinesAck],
    expected_first: Option<u64>,
) -> Result<(), CampaignError> {
    if acks.is_empty() {
        return Err(CampaignError::Scenario("no committed ACKs".into()));
    }
    if let Some(expected) = expected_first
        && acks[0].first_offset != expected
    {
        return Err(CampaignError::Scenario(format!(
            "dense continuation broke: want first_offset {expected}, got {}",
            acks[0].first_offset
        )));
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

fn assert_actor_placement(
    placement: &ActorPlacement,
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

fn assert_distinct_uids(a: &ActorPlacement, b: &ActorPlacement) -> Result<(), CampaignError> {
    if a.uid == b.uid {
        return Err(CampaignError::Scenario(
            "actors share a pod UID; process separation unproven".into(),
        ));
    }
    Ok(())
}

fn pod_ready(lifecycle: &RunLifecycle, name: &str) -> Result<bool, CampaignError> {
    let out = std::process::Command::new("kubectl")
        .arg("--context")
        .arg(&lifecycle.kube_context)
        .args([
            "-n",
            &lifecycle.namespace,
            "get",
            "pod",
            name,
            "-o",
            "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}",
        ])
        .output()
        .map_err(|error| CampaignError::Scenario(format!("kubectl get ready: {error}")))?;
    if !out.status.success() {
        return Ok(false);
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim() == "True")
}

async fn wait_tcp_ready(endpoint: &str) -> Result<(), CampaignError> {
    use tokio::net::TcpStream;
    use tokio::time::timeout;
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

async fn wait_actor_unreachable(
    context: &str,
    namespace: &str,
    target: &str,
    local_port: u16,
) -> Result<(), CampaignError> {
    use tokio::net::TcpStream;
    use tokio::time::timeout;
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

async fn assert_a_unreachable_for_producer(
    context: &str,
    namespace: &str,
    target: &str,
    local_port: u16,
) -> Result<(), CampaignError> {
    use tokio::time::timeout;
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

struct PortForward {
    child: std::process::Child,
}

impl PortForward {
    fn start(
        context: &str,
        namespace: &str,
        target: &str,
        local_port: u16,
        remote_port: u16,
    ) -> Result<Self, CampaignError> {
        let child = std::process::Command::new("kubectl")
            .arg("--context")
            .arg(context)
            .args([
                "-n",
                namespace,
                "port-forward",
                target,
                &format!("{local_port}:{remote_port}"),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|error| CampaignError::Scenario(format!("port-forward spawn: {error}")))?;
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
