//! Multi-assignment Scribe activation for Serving-Authority configs.
//!
//! Starts one independent runtime/session per `scribe.assignments[]` entry.
//! Authority is never process-global: each assignment uses its own VirtualLog
//! register root under `{store.prefix}/cv/{hex(canon)}/{hex(verse)}`.
//!
//! Standby is a dormant candidate: no Serving authority, no warm recovery, no
//! committed ACKs until a targeted `promote --assignment`.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use holylog::virtual_log::LogletResolver;
use object_store::ObjectStore;
use scripture::serving_authority::{AuthorityKey, JournalGenerationRef, RouteHint, WriterTerm};
use scripture_runtime::{
    AssignmentResourceBudget, AssignmentResourceLimits, AssignmentRuntime, HaServingSession,
    HolylogJournalFoundation, IngressBudgets, NodeIdentity, NodeResourceBudget, RawLinesConfig,
    ScribeResourceLimits, ScribeSupervisor, bootstrap_and_serve, promote_and_serve,
    serve_ha_raw_lines_connection_with_budgets, system_clocks,
};
use scripture_service::{
    AuthorityCoordinator, JournalFoundationTransition, SecureTransitionIdGenerator,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::assemble::{self, AssembledNode, SharedStore};
use crate::config::{AssignmentConfig, AssignmentPosture, ScriptureConfig};

fn authority_key(
    config: &ScriptureConfig,
    assignment: &AssignmentConfig,
) -> Result<AuthorityKey, Box<dyn Error>> {
    let verse = config.assignment_runtime_config(assignment)?;
    Ok(AuthorityKey {
        journal_id: verse.journal_id,
        verse_id: verse.verse_id,
    })
}

fn build_foundation(assembled: &AssembledNode, key: AuthorityKey) -> HolylogJournalFoundation {
    HolylogJournalFoundation::with_default_loglet_ids(
        key,
        NodeIdentity {
            owner_id: assembled.node.identity().owner_id,
            endpoint: assembled.advertise.clone(),
        },
        Arc::clone(&assembled.register),
        Arc::clone(&assembled.resolver),
        Arc::clone(&assembled.parts),
        Arc::clone(&assembled.claims),
        2,
    )
}

/// Equal share of node pending ceilings, floored at [`RawLinesConfig`] defaults.
fn assignment_budget_for_node(
    limits: &ScribeResourceLimits,
    assignment_count: usize,
) -> Arc<AssignmentResourceBudget> {
    let n = assignment_count.max(1);
    let defaults = RawLinesConfig::default();
    Arc::new(AssignmentResourceBudget::new(AssignmentResourceLimits {
        max_pending_bytes: (limits.max_pending_bytes / n).max(defaults.max_pending_bytes),
        max_pending_records: (limits.max_pending_records / n).max(defaults.max_pending_records),
        min_pending_bytes_floor: 0,
    }))
}

fn raw_lines_config_from_budget(budget: &AssignmentResourceBudget) -> RawLinesConfig {
    let defaults = RawLinesConfig::default();
    let limits = budget.limits();
    RawLinesConfig {
        max_line_bytes: defaults.max_line_bytes,
        max_pending_records: limits.max_pending_records,
        max_pending_bytes: limits.max_pending_bytes.max(defaults.max_line_bytes),
        idle_flush: defaults.idle_flush,
        attributes: defaults.attributes,
    }
}

fn scribe_limits(config: &ScriptureConfig) -> Result<ScribeResourceLimits, Box<dyn Error>> {
    let scribe = config
        .scribe
        .as_ref()
        .ok_or("multi-assignment path requires scribe.assignments")?;
    Ok(ScribeResourceLimits {
        max_assignments: scribe.limits.max_assignments,
        max_pending_bytes: scribe.limits.max_pending_bytes,
        max_pending_records: scribe.limits.max_pending_records,
        max_concurrent_tasks: scribe.limits.max_concurrent_tasks,
    })
}

async fn observe_expected_generation(
    assembled: &AssembledNode,
) -> Result<JournalGenerationRef, Box<dyn Error>> {
    let virtual_log = assembled.node.virtual_log();
    let versioned = virtual_log.observe_membership().await.map_err(|error| {
        format!("cannot observe Journal Foundation for promote expected generation: {error}")
    })?;
    JournalGenerationRef::from_virtual_log_state(&versioned.state)
        .map_err(|error| format!("cannot derive JournalGenerationRef: {error}").into())
}

async fn activate_one(
    config: &ScriptureConfig,
    shared: &SharedStore,
    assignment: &AssignmentConfig,
    initial_term: u64,
    budget: Arc<AssignmentResourceBudget>,
) -> Result<AssignmentRuntime, Box<dyn Error>> {
    let key = authority_key(config, assignment)?;
    let store_root = config.assignment_store_root(assignment)?;
    let advertise = assignment.advertise.clone();
    let assembled = assemble::assemble_assignment(config, shared, assignment)?;

    match assignment.posture {
        AssignmentPosture::Standby => {
            eprintln!(
                "scripture: assignment id={} disposition=Standby standby_kind=dormant-candidate canon={} verse={} store_root={} advertise={} bind={}",
                assignment.id,
                assignment.canon,
                assignment.verse,
                store_root,
                advertise,
                assignment.ingress.bind,
            );
            Ok(AssignmentRuntime::standby(
                assignment.id.clone(),
                key,
                store_root,
                advertise,
                budget,
            ))
        }
        AssignmentPosture::BootstrapIfEmpty => {
            let foundation = Arc::new(build_foundation(&assembled, key));
            let coordinator = AuthorityCoordinator::new(
                Arc::clone(&assembled.register),
                Arc::clone(&assembled.resolver) as Arc<dyn LogletResolver>,
                Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
                Arc::new(SecureTransitionIdGenerator::new()),
                assembled.node.identity().owner_id,
                RouteHint::new(assembled.advertise.as_str())?,
            );
            let term = WriterTerm::new(initial_term)?;
            let (clock, timer) = system_clocks();
            match bootstrap_and_serve(
                &coordinator,
                foundation.as_ref(),
                key,
                term,
                config.assignment_runtime_config(assignment)?,
                Arc::clone(&assembled.register),
                Arc::clone(&assembled.resolver),
                clock,
                timer,
            )
            .await
            {
                Ok(session) => {
                    eprintln!(
                        "scripture: assignment id={} disposition=Serving canon={} verse={} store_root={} advertise={} bind={} action=bootstrap-and-serve",
                        assignment.id,
                        assignment.canon,
                        assignment.verse,
                        store_root,
                        advertise,
                        assignment.ingress.bind,
                    );
                    Ok(AssignmentRuntime::serving(
                        assignment.id.clone(),
                        key,
                        session,
                        store_root,
                        advertise,
                        budget,
                    ))
                }
                Err(error) => {
                    let reason = error.to_string();
                    eprintln!(
                        "scripture: assignment id={} disposition=FailClosed canon={} verse={} store_root={} advertise={} reason={reason}",
                        assignment.id, assignment.canon, assignment.verse, store_root, advertise,
                    );
                    Ok(AssignmentRuntime::fail_closed(
                        assignment.id.clone(),
                        key,
                        store_root,
                        advertise,
                        budget,
                        reason,
                    ))
                }
            }
        }
    }
}

async fn promote_one(
    config: &ScriptureConfig,
    shared: &SharedStore,
    assignment: &AssignmentConfig,
    candidate_term: u64,
    budget: Arc<AssignmentResourceBudget>,
) -> Result<AssignmentRuntime, Box<dyn Error>> {
    let key = authority_key(config, assignment)?;
    let store_root = config.assignment_store_root(assignment)?;
    let advertise = assignment.advertise.clone();
    let assembled = assemble::assemble_assignment(config, shared, assignment)?;
    let expected = observe_expected_generation(&assembled).await?;
    let foundation = Arc::new(build_foundation(&assembled, key));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&assembled.register),
        Arc::clone(&assembled.resolver) as Arc<dyn LogletResolver>,
        Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
        Arc::new(SecureTransitionIdGenerator::new()),
        assembled.node.identity().owner_id,
        RouteHint::new(assembled.advertise.as_str())?,
    );
    let term = WriterTerm::new(candidate_term)?;
    let (clock, timer) = system_clocks();
    match promote_and_serve(
        &coordinator,
        foundation.as_ref(),
        key,
        term,
        expected,
        config.assignment_runtime_config(assignment)?,
        Arc::clone(&assembled.register),
        Arc::clone(&assembled.resolver),
        clock,
        timer,
    )
    .await
    {
        Ok(session) => {
            eprintln!(
                "scripture: assignment id={} disposition=Serving canon={} verse={} store_root={} advertise={} bind={} action=promote-and-serve candidate_term={candidate_term}",
                assignment.id,
                assignment.canon,
                assignment.verse,
                store_root,
                advertise,
                assignment.ingress.bind,
            );
            Ok(AssignmentRuntime::serving(
                assignment.id.clone(),
                key,
                session,
                store_root,
                advertise,
                budget,
            ))
        }
        Err(error) => {
            let reason = error.to_string();
            eprintln!(
                "scripture: assignment id={} disposition=FailClosed canon={} verse={} store_root={} advertise={} action=promote-and-serve reason={reason}",
                assignment.id, assignment.canon, assignment.verse, store_root, advertise,
            );
            Ok(AssignmentRuntime::fail_closed(
                assignment.id.clone(),
                key,
                store_root,
                advertise,
                budget,
                reason,
            ))
        }
    }
}

/// Long-lived multi-assignment bootstrap-and-serve for `scribe.assignments`.
pub async fn bootstrap_multi_assignment(
    config: ScriptureConfig,
    initial_term: u64,
) -> Result<(), Box<dyn Error>> {
    let limits = scribe_limits(&config)?;
    let shared = assemble::connect_shared_store(&config)?;
    let assignments = config
        .scribe
        .as_ref()
        .ok_or("multi-assignment bootstrap requires scribe.assignments")?
        .assignments
        .clone();
    let count = assignments.len();
    let mut runtimes = Vec::with_capacity(count);
    for assignment in &assignments {
        let budget = assignment_budget_for_node(&limits, count);
        runtimes.push(activate_one(&config, &shared, assignment, initial_term, budget).await?);
    }
    let supervisor = ScribeSupervisor::from_assignments(limits, runtimes)?;
    eprintln!(
        "scripture: ha_mode=serving-authority action=scribe-multi-assignment owner={} advertise={} backend={} assignments={}",
        config.node.owner_id,
        shared.advertise.as_str(),
        shared.backend.label(),
        supervisor.assignments().len(),
    );
    run_multi_ingress(config, supervisor, Arc::clone(&shared.store)).await
}

/// Targeted promote for one assignment inside a multi-assignment Scribe.
///
/// Only `assignment_id` receives `promote_and_serve`. Sibling assignments activate
/// by configured posture (standby stays dormant; bootstrap-if-empty may Serve or
/// FailClosed). Authority remains per-assignment — never process-global.
pub async fn promote_multi_assignment(
    config: ScriptureConfig,
    assignment_id: &str,
    candidate_term: u64,
) -> Result<(), Box<dyn Error>> {
    let limits = scribe_limits(&config)?;
    let shared = assemble::connect_shared_store(&config)?;
    let assignments = config
        .scribe
        .as_ref()
        .ok_or("multi-assignment promote requires scribe.assignments")?
        .assignments
        .clone();
    if !assignments
        .iter()
        .any(|assignment| assignment.id == assignment_id)
    {
        return Err(format!(
            "promote --assignment {assignment_id:?} not found in scribe.assignments"
        )
        .into());
    }
    let count = assignments.len();
    let mut runtimes = Vec::with_capacity(count);
    for assignment in &assignments {
        let budget = assignment_budget_for_node(&limits, count);
        if assignment.id == assignment_id {
            runtimes.push(promote_one(&config, &shared, assignment, candidate_term, budget).await?);
        } else {
            // Sibling activation uses posture only — never promote.
            runtimes.push(activate_one(&config, &shared, assignment, 1, budget).await?);
        }
    }
    let supervisor = ScribeSupervisor::from_assignments(limits, runtimes)?;
    eprintln!(
        "scripture: ha_mode=serving-authority action=scribe-targeted-promote owner={} advertise={} backend={} target_assignment={} candidate_term={candidate_term} assignments={}",
        config.node.owner_id,
        shared.advertise.as_str(),
        shared.backend.label(),
        assignment_id,
        supervisor.assignments().len(),
    );
    run_multi_ingress(config, supervisor, Arc::clone(&shared.store)).await
}

/// Publishes this node's fleet-directory record on a heartbeat (decision 0014).
///
/// Soft state only: a failure degrades discovery and can never affect
/// authority, so errors are reported and the loop continues.
fn spawn_directory_heartbeat(
    config: &ScriptureConfig,
    supervisor: Arc<ScribeSupervisor>,
    store: Arc<dyn ObjectStore>,
) {
    let Some(scribe) = config.scribe.as_ref() else {
        return;
    };
    let assignments = scribe.assignments.clone();
    let owner_id = config.node.owner_id.clone();
    let node_advertise = config.node.advertise.clone();
    let prefix = config.store.prefix.clone();

    // Beat well inside the validity window so ordinary scheduling jitter
    // cannot expire a live node.
    let valid_for_ms = 15_000u64;
    let beat = std::time::Duration::from_millis(3_000);

    tokio::spawn(async move {
        loop {
            let mut entries: Vec<scripture_runtime::directory::DirectoryAssignment> = Vec::new();
            for assignment in &assignments {
                let runtime = supervisor.assignment(&assignment.id);
                // Evidence, not cached activation state. `admits_committed_acks`
                // reports the disposition this assignment started with and stays
                // true on a deposed-but-alive node, which would advertise a
                // Scribe that can no longer commit. Re-observe the root instead.
                let (disposition, admits) = match runtime {
                    Some(runtime) => match runtime.session.as_ref() {
                        Some(session) => {
                            let effective = session.is_effective_writer().await;
                            (
                                if effective {
                                    "Serving"
                                } else {
                                    "NotEffectiveWriter"
                                },
                                effective,
                            )
                        }
                        None => (runtime.disposition.label(), false),
                    },
                    None => ("Unknown", false),
                };
                entries.push(scripture_runtime::directory::DirectoryAssignment {
                    canon: assignment.canon.clone(),
                    verse: assignment.verse.clone(),
                    advertise: assignment.advertise.clone(),
                    posture: match assignment.posture {
                        AssignmentPosture::BootstrapIfEmpty => "bootstrap-if-empty".to_owned(),
                        AssignmentPosture::Standby => "standby".to_owned(),
                    },
                    disposition: disposition.to_owned(),
                    admits_committed_acks: admits,
                });
            }

            let record = scripture_runtime::directory::DirectoryRecord {
                format_version: 1,
                owner_id: owner_id.clone(),
                node_advertise: node_advertise.clone(),
                published_at_ms: scripture_runtime::directory::now_ms(),
                valid_for_ms,
                assignments: entries,
            };

            if let Err(error) =
                scripture_runtime::directory::publish(&store, &prefix, &record).await
            {
                eprintln!("scripture: directory heartbeat failed (discovery only): {error}");
            }
            tokio::time::sleep(beat).await;
        }
    });
}

async fn run_multi_ingress(
    config: ScriptureConfig,
    supervisor: ScribeSupervisor,
    store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn Error>> {
    let supervisor = Arc::new(supervisor);
    let alive = Arc::new(AtomicBool::new(true));
    let node_budget = supervisor.budget();

    if let Some(status_bind) = &config.metrics.status_bind {
        let listener = TcpListener::bind(status_bind).await?;
        eprintln!(
            "scripture: status/liveness/readiness on {} (/livez /readyz /status)",
            listener.local_addr()?
        );
        let supervisor = Arc::clone(&supervisor);
        let alive = Arc::clone(&alive);
        tokio::spawn(async move {
            serve_probe_loop(listener, supervisor, alive).await;
        });
    }

    let assignments = config
        .scribe
        .as_ref()
        .map(|scribe| scribe.assignments.clone())
        .unwrap_or_default();

    for assignment in assignments {
        let runtime = supervisor
            .assignment(&assignment.id)
            .ok_or_else(|| format!("missing activated assignment {}", assignment.id))?;
        if !runtime.admits_committed_acks() {
            eprintln!(
                "scripture: assignment id={} skips producer ingress disposition={} standby_kind={}",
                assignment.id,
                runtime.disposition.label(),
                if matches!(
                    runtime.disposition,
                    scripture_runtime::AssignmentDisposition::Standby
                ) {
                    "dormant-candidate"
                } else {
                    "n/a"
                }
            );
            continue;
        }
        let session = runtime
            .session
            .as_ref()
            .cloned()
            .ok_or_else(|| format!("serving assignment {} missing session", assignment.id))?;
        let ingress_budgets = runtime.ingress_budgets(Arc::clone(&node_budget));
        let raw_config = raw_lines_config_from_budget(&runtime.budget);
        let listener = TcpListener::bind(&assignment.ingress.bind).await?;
        eprintln!(
            "scripture: assignment id={} listening on {} (authority scope canon={} verse={}; advertise={}; not a public producer protocol)",
            assignment.id,
            listener.local_addr()?,
            assignment.canon,
            assignment.verse,
            assignment.advertise,
        );
        let assignment_id = assignment.id.clone();
        let node_budget = Arc::clone(&node_budget);
        tokio::spawn(async move {
            serve_assignment_ingress(
                listener,
                session,
                node_budget,
                ingress_budgets,
                raw_config,
                assignment_id,
            )
            .await;
        });
    }

    spawn_directory_heartbeat(&config, Arc::clone(&supervisor), store);

    // Keep the process alive; dispositions remain as activated (no auto-promote).
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

async fn serve_assignment_ingress(
    listener: TcpListener,
    session: Arc<HaServingSession>,
    node_budget: Arc<NodeResourceBudget>,
    ingress_budgets: IngressBudgets,
    raw_config: RawLinesConfig,
    assignment_id: String,
) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        if !node_budget.try_acquire_task() {
            eprintln!(
                "scripture: assignment id={assignment_id} rejecting connection from {peer}: node task budget exhausted"
            );
            continue;
        }
        let session = Arc::clone(&session);
        let node_budget = Arc::clone(&node_budget);
        let ingress_budgets = ingress_budgets.clone();
        let raw_config = raw_config.clone();
        let assignment_id = assignment_id.clone();
        tokio::spawn(async move {
            let result = serve_ha_raw_lines_connection_with_budgets(
                stream,
                session,
                raw_config,
                Some(ingress_budgets),
            )
            .await;
            node_budget.release_task();
            if let Err(error) = result {
                eprintln!(
                    "scripture: assignment id={assignment_id} HA connection from {peer} closed: {error}"
                );
            }
        });
    }
}

async fn serve_probe_loop(
    listener: TcpListener,
    supervisor: Arc<ScribeSupervisor>,
    alive: Arc<AtomicBool>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let supervisor = Arc::clone(&supervisor);
        let alive = Arc::clone(&alive);
        tokio::spawn(async move {
            serve_probe_connection(stream, supervisor, alive).await;
        });
    }
}

async fn serve_probe_connection(
    mut stream: tokio::net::TcpStream,
    supervisor: Arc<ScribeSupervisor>,
    alive: Arc<AtomicBool>,
) {
    let mut buf = [0_u8; 1024];
    let _ = stream.read(&mut buf).await;
    let request = String::from_utf8_lossy(&buf);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/status");

    let any_serving = supervisor.any_serving();
    let (code, body) = match path {
        "/livez" | "/healthz" => {
            if alive.load(Ordering::Relaxed) {
                (200, "alive\n".to_owned())
            } else {
                (503, "not-alive\n".to_owned())
            }
        }
        "/readyz" => {
            if any_serving {
                (200, "ready\n".to_owned())
            } else {
                (503, "not-ready disposition=multi-assignment\n".to_owned())
            }
        }
        _ => (200, supervisor.status_body()),
    };
    let response = format!(
        "HTTP/1.1 {code} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        if code == 200 {
            "OK"
        } else {
            "Service Unavailable"
        },
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}
