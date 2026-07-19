//! Multi-assignment Scribe activation for Serving-Authority configs.
//!
//! Starts one independent runtime/session per `scribe.assignments[]` entry.
//! Authority is never process-global: each assignment uses its own VirtualLog
//! register root under `{store.prefix}/cv/{hex(canon)}/{hex(verse)}`.
//!
//! Standby is a dormant candidate: no Serving authority, no warm recovery, no
//! committed ACKs until a targeted `promote --assignment`.

use std::collections::HashMap;
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

/// Bind ingress for every assignment that will attempt Serving publication.
///
/// Must run before any root CAS for those assignments. A candidate that cannot
/// open ingress must not depose a Scribe that can — bind failure exits without
/// touching the root, so the predecessor keeps authority.
async fn bind_ingress_before_authority(
    assignments: &[AssignmentConfig],
    will_attempt_serve: impl Fn(&AssignmentConfig) -> bool,
) -> Result<HashMap<String, TcpListener>, Box<dyn Error>> {
    let mut listeners = HashMap::new();
    for assignment in assignments {
        if !will_attempt_serve(assignment) {
            continue;
        }
        let listener = TcpListener::bind(&assignment.ingress.bind)
            .await
            .map_err(|error| {
                format!(
                    "assignment id={} cannot bind ingress {}: {error}",
                    assignment.id, assignment.ingress.bind
                )
            })?;
        listeners.insert(assignment.id.clone(), listener);
    }
    Ok(listeners)
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
    // Hold ingress before any Empty→Serving CAS. Publishing authority and then
    // failing to bind leaves the Verse with no reachable writer.
    let listeners = bind_ingress_before_authority(&assignments, |assignment| {
        matches!(assignment.posture, AssignmentPosture::BootstrapIfEmpty)
    })
    .await?;
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
    run_multi_ingress(config, supervisor, Arc::clone(&shared.store), listeners).await
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
    // Bind before any root CAS. Target always attempts promote; BootstrapIfEmpty
    // siblings may publish too. Standby siblings stay dormant and need no port.
    let listeners = bind_ingress_before_authority(&assignments, |assignment| {
        assignment.id == assignment_id
            || matches!(assignment.posture, AssignmentPosture::BootstrapIfEmpty)
    })
    .await?;
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
    run_multi_ingress(config, supervisor, Arc::clone(&shared.store), listeners).await
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
    mut prebound: HashMap<String, TcpListener>,
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
            // FailClosed / Standby: drop any listener held before a failed CAS.
            let _ = prebound.remove(&assignment.id);
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
        // Serve on the listener bound before the root CAS — never bind here.
        let listener = prebound.remove(&assignment.id).ok_or_else(|| {
            format!(
                "serving assignment {} missing pre-bound ingress (bind must precede authority CAS)",
                assignment.id
            )
        })?;
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

    let (code, body) = match path {
        "/livez" | "/healthz" => {
            if alive.load(Ordering::Relaxed) {
                (200, "alive\n".to_owned())
            } else {
                (503, "not-alive\n".to_owned())
            }
        }
        // Readiness re-observes the root rather than trusting the disposition
        // this process started with. A deposed-but-alive Scribe answering 200
        // keeps a load balancer sending it producer traffic it can only refuse.
        "/readyz" => {
            if supervisor.any_effective_writer().await {
                (200, "ready\n".to_owned())
            } else {
                (503, "not-ready reason=no-effective-writer\n".to_owned())
            }
        }
        _ => {
            let mut body = supervisor.status_body();
            body.push_str("live authority (re-observed from the root):\n");
            for (id, effective) in supervisor.effective_writers().await {
                body.push_str(&format!(
                    "assignment_id={id} effective_writer={effective}\n"
                ));
            }
            (200, body)
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
    use holylog::virtual_log::{
        ConditionalRegister, InMemoryConditionalRegister, LogletResolver, VirtualLog,
    };
    use scripture::serving_authority::{
        AuthorityKey, AuthorityState, RouteHint, ServingAuthorityRecord, WriterTerm,
    };
    use scripture::{
        ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, SystemClock,
        SystemTimer, VerseId, WriterId,
    };
    use scripture_runtime::{
        HolylogJournalFoundation, NodeIdentity, ProcessLogletResolver, SharedMemoryPartsFactory,
        bootstrap_and_serve, promote_and_serve,
    };
    use scripture_service::{
        AuthorityCoordinator, DeterministicTransitionIdGenerator, JournalFoundationTransition,
        VerseRuntimeConfig,
    };
    use tokio::net::TcpListener;

    use super::bind_ingress_before_authority;
    use crate::config::{AssignmentConfig, AssignmentPosture, ListenerConfig};

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"bind-ord-owner-a")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"bind-ord-owner-b")
    }

    fn key() -> AuthorityKey {
        AuthorityKey {
            journal_id: JournalId::from_bytes(*b"bind-ord-jrnl!!!"),
            verse_id: VerseId::from_bytes(*b"bind-ord-verse!!"),
        }
    }

    fn runtime_config(owner_id: OwnerId, writer: [u8; 16]) -> VerseRuntimeConfig {
        VerseRuntimeConfig {
            journal_id: JournalId::from_bytes(*b"bind-ord-jrnl!!!"),
            verse_id: VerseId::from_bytes(*b"bind-ord-verse!!"),
            owner_id,
            cohort_id: CohortId::from_bytes(*b"bind-ord-cohort!"),
            writer_id: WriterId::from_bytes(writer),
            policy: ChunkPolicy {
                max_chunk_bytes: 64 * 1024,
                max_record_bytes: 16 * 1024,
                max_chunk_records: 8,
                max_chunk_age: Duration::from_secs(60),
                max_buffered_bytes: 64 * 1024,
                max_inflight_chunks: 1,
                max_uncommitted_age: Duration::from_secs(60),
                recovery_scan: RecoveryBound::new(8).expect("bound"),
            },
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 16,
        }
    }

    fn serving_owner(record: &ServingAuthorityRecord) -> OwnerId {
        match &record.state {
            AuthorityState::Serving { authority, .. } => authority.owner_id,
            other => panic!("expected Serving authority, got {other:?}"),
        }
    }

    async fn observe_root(
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<dyn LogletResolver>,
    ) -> (u64, OwnerId) {
        let virtual_log = VirtualLog::new(register, resolver);
        let observed = virtual_log
            .observe_membership()
            .await
            .expect("observe root");
        let record =
            ServingAuthorityRecord::decode_application_fence(&observed.state.application_fence)
                .expect("decode Serving Authority fence");
        (observed.state.revision, serving_owner(&record))
    }

    /// Occupied ingress must abort the promote path before the root CAS, so the
    /// predecessor remains the effective writer (availability: no silent outage).
    #[tokio::test]
    async fn promote_bind_failure_leaves_predecessor_root_unchanged() {
        let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
        let auth = key();

        let a_resolver = Arc::new(ProcessLogletResolver::default());
        let a_foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
            auth,
            NodeIdentity {
                owner_id: owner_a(),
                endpoint: OwnerEndpoint::new("tcp://owner-a:9000").expect("ep"),
            },
            Arc::clone(&register),
            Arc::clone(&a_resolver),
            Arc::clone(&parts) as Arc<dyn scripture_runtime::PartsFactory>,
            Arc::clone(&claims),
            2,
        ));
        let a_coordinator = AuthorityCoordinator::new(
            Arc::clone(&register),
            Arc::clone(&a_resolver) as Arc<dyn LogletResolver>,
            Arc::clone(&a_foundation) as Arc<dyn JournalFoundationTransition>,
            Arc::new(DeterministicTransitionIdGenerator::new()),
            owner_a(),
            RouteHint::new("tcp://owner-a:9000").expect("route"),
        );
        let a_session = bootstrap_and_serve(
            &a_coordinator,
            a_foundation.as_ref(),
            auth,
            WriterTerm::new(1).expect("t1"),
            runtime_config(owner_a(), *b"bind-ord-wrtr-a!"),
            Arc::clone(&register),
            Arc::clone(&a_resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .expect("predecessor Serving");
        let expected = a_session.generation().clone();
        let (revision_before, owner_before) = observe_root(
            Arc::clone(&register),
            Arc::clone(&a_resolver) as Arc<dyn LogletResolver>,
        )
        .await;
        assert_eq!(owner_before, owner_a());
        assert!(a_session.is_effective_writer().await);

        // Stale holder on the candidate's ingress — the live failure mode.
        let occupied = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("occupy ingress");
        let bind = occupied.local_addr().expect("addr").to_string();
        let assignment = AssignmentConfig {
            id: "telemetry-host-a".into(),
            canon: "bind-ord-jrnl!!!".into(),
            verse: "bind-ord-verse!!".into(),
            cohort_id: "bind-ord-cohort!".into(),
            writer_id: "bind-ord-wrtr-b!".into(),
            posture: AssignmentPosture::Standby,
            ingress: ListenerConfig { bind: bind.clone() },
            advertise: "tcp://owner-b:9000".into(),
        };

        let b_resolver = Arc::new(ProcessLogletResolver::default());
        let b_foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
            auth,
            NodeIdentity {
                owner_id: owner_b(),
                endpoint: OwnerEndpoint::new("tcp://owner-b:9000").expect("ep"),
            },
            Arc::clone(&register),
            Arc::clone(&b_resolver),
            Arc::clone(&parts) as Arc<dyn scripture_runtime::PartsFactory>,
            Arc::clone(&claims),
            2,
        ));
        let b_coordinator = AuthorityCoordinator::new(
            Arc::clone(&register),
            Arc::clone(&b_resolver) as Arc<dyn LogletResolver>,
            Arc::clone(&b_foundation) as Arc<dyn JournalFoundationTransition>,
            Arc::new(DeterministicTransitionIdGenerator::new()),
            owner_b(),
            RouteHint::new("tcp://owner-b:9000").expect("route"),
        );

        // Same ordering as promote_multi_assignment: bind all authority attempts,
        // then seal/root-CAS. Bind failure must never reach promote_and_serve.
        let promote_path = async {
            let _listeners =
                bind_ingress_before_authority(std::slice::from_ref(&assignment), |candidate| {
                    candidate.id == assignment.id
                })
                .await?;
            promote_and_serve(
                &b_coordinator,
                b_foundation.as_ref(),
                auth,
                WriterTerm::new(2).expect("t2"),
                expected.clone(),
                runtime_config(owner_b(), *b"bind-ord-wrtr-b!"),
                Arc::clone(&register),
                Arc::clone(&b_resolver),
                SystemClock::new(),
                SystemTimer::new(),
            )
            .await
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
            Ok::<(), Box<dyn std::error::Error>>(())
        };

        let error = promote_path
            .await
            .expect_err("occupied ingress must fail promote before root CAS");
        let message = error.to_string();
        assert!(
            message.contains("cannot bind ingress") || message.contains("Address already in use"),
            "unexpected error: {message}"
        );

        let (revision_after, owner_after) = observe_root(
            Arc::clone(&register),
            Arc::clone(&a_resolver) as Arc<dyn LogletResolver>,
        )
        .await;
        assert_eq!(
            revision_after, revision_before,
            "root revision must be unchanged when bind fails before CAS"
        );
        assert_eq!(
            owner_after, owner_before,
            "predecessor must remain the root owner when bind fails before CAS"
        );
        assert!(
            a_session.is_effective_writer().await,
            "predecessor must remain the effective writer"
        );
        // Keep `occupied` in scope so the port stays held through the promote attempt.
        let _ = occupied.local_addr();
    }
}
