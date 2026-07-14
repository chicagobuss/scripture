//! Fleet-exercise Scripture node against a shared object-store root.
//!
//! Lab only: proves Serving/Standby over RustFS or Cloudflare R2. Does not claim
//! automatic failover, restart-safe remote sequencing, or Decision 0012 recovery.
//!
//! Secrets come from the process environment or `--env-file`. `--access-key` /
//! `--secret-key` are rejected.

use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use holylog::virtual_log::{ConditionalRegister, LogletId};
use holylog_object_store::{ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::path::Path;
use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, SystemClock, VerseId,
    WriterId,
};
use scripture_service::{CanonRoute, VerseRuntime, VerseRuntimeConfig};
use scriptured::{
    BackendProfile, FleetLabResolver, NodeIdentity, ObjectStorePartsFactory, RawLinesConfig,
    StoreEndpointConfig, VerseControlOutcome, VerseNodeSupervisor, connect_s3_compat,
    load_env_file, resolve_credentials, resolve_endpoint_config, serve_canon_raw_lines_connection,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = try_main().await {
        eprintln!("fleet-lab-node: {error}");
        process::exit(1);
    }
}

#[derive(Debug)]
struct Args {
    bind: String,
    status_bind: Option<String>,
    owner: OwnerId,
    endpoint: OwnerEndpoint,
    run_id: String,
    bootstrap: bool,
    loglet_id: Option<String>,
    profile: BackendProfile,
    endpoint_url: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    env_file: Option<PathBuf>,
    summary_dir: PathBuf,
}

#[derive(Debug)]
struct IngressCounters {
    accepted: AtomicU64,
    closed_ok: AtomicU64,
    closed_err: AtomicU64,
}

impl IngressCounters {
    fn new() -> Self {
        Self {
            accepted: AtomicU64::new(0),
            closed_ok: AtomicU64::new(0),
            closed_err: AtomicU64::new(0),
        }
    }
}

async fn try_main() -> Result<(), Box<dyn Error>> {
    let args = parse_args(env::args().skip(1))?;
    let overlay = if let Some(path) = &args.env_file {
        let overlay = load_env_file(path)?;
        eprintln!(
            "fleet-lab-node: loaded {} keys from env-file {} (overlay only; process env unchanged)",
            overlay.len(),
            path.display()
        );
        overlay
    } else {
        std::collections::BTreeMap::new()
    };

    let store_cfg = resolve_endpoint_config(
        args.profile,
        &args.run_id,
        args.endpoint_url.clone(),
        args.bucket.clone(),
        args.region.clone(),
        &overlay,
    )?;
    let credentials = resolve_credentials(args.profile, &overlay)?;
    let store = connect_s3_compat(
        &store_cfg.endpoint,
        &store_cfg.bucket,
        &store_cfg.region,
        &credentials.access_key,
        &credentials.secret_key,
    )?;
    // Credentials leave scope; never log them.
    drop(credentials);

    let register = Arc::new(ObjectStoreConditionalRegister::new(
        Arc::clone(&store),
        Path::from(store_cfg.root.clone()).join(register_path("verse").as_ref()),
        args.profile.register_capabilities(),
    )?) as Arc<dyn ConditionalRegister>;
    let metrics = Arc::new(ObjectStoreMetrics::default());
    let parts = Arc::new(ObjectStorePartsFactory::new(
        store,
        store_cfg.root.clone(),
        args.profile.drive_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::clone(&metrics),
    ));
    let resolver = Arc::new(FleetLabResolver::default());
    let config = verse_config(args.owner);
    let node = VerseNodeSupervisor::with_parts_factory(
        NodeIdentity {
            owner_id: args.owner,
            endpoint: args.endpoint.clone(),
        },
        register,
        resolver,
        parts,
        config,
    );

    let outcome = if args.bootstrap {
        let loglet = LogletId::new(
            args.loglet_id
                .as_deref()
                .ok_or("--bootstrap requires --loglet-id")?,
        )?;
        node.bootstrap_verse(loglet, SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await?
    } else {
        node.start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await?
    };
    if matches!(outcome, VerseControlOutcome::RecoveryRequired { .. }) {
        return Err(
            "RecoveryRequired: open generation needs explicit seal-and-replace (not serving)"
                .into(),
        );
    }

    let disposition = disposition_label(&outcome);
    print_startup_banner(&args, &store_cfg, disposition);

    let runtime = node.runtime().await.ok_or("runtime missing after start")?;
    let ingress = Arc::new(IngressCounters::new());

    if let Some(status_bind) = &args.status_bind {
        let status_listener = TcpListener::bind(status_bind).await?;
        eprintln!(
            "fleet-lab-node: lab-only status on {} (read-only; no ownership routes)",
            status_listener.local_addr()?
        );
        let runtime = Arc::clone(&runtime);
        let metrics = Arc::clone(&metrics);
        let ingress = Arc::clone(&ingress);
        let disposition = disposition.to_owned();
        let profile = args.profile.label().to_owned();
        let run_id = args.run_id.clone();
        tokio::spawn(async move {
            serve_status_loop(
                status_listener,
                runtime,
                metrics,
                ingress,
                disposition,
                profile,
                run_id,
            )
            .await;
        });
    }

    let listener = TcpListener::bind(&args.bind).await?;
    let bind_addr = listener.local_addr()?;
    eprintln!("fleet-lab-node: raw-lines listening on {bind_addr}");

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    loop {
        let accepted = {
            #[cfg(unix)]
            {
                tokio::select! {
                    result = &mut ctrl_c => {
                        let _ = result;
                        None
                    }
                    _ = sigterm.recv() => None,
                    accepted = listener.accept() => Some(accepted),
                }
            }
            #[cfg(not(unix))]
            {
                tokio::select! {
                    result = &mut ctrl_c => {
                        let _ = result;
                        None
                    }
                    accepted = listener.accept() => Some(accepted),
                }
            }
        };
        let Some(accepted) = accepted else {
            break;
        };
        let Ok((stream, peer)) = accepted else {
            break;
        };
        ingress.accepted.fetch_add(1, Ordering::Relaxed);
        let runtime = Arc::clone(&runtime);
        let ingress = Arc::clone(&ingress);
        tokio::spawn(async move {
            let result =
                serve_canon_raw_lines_connection(stream, runtime, RawLinesConfig::default()).await;
            match result {
                Ok(()) => {
                    ingress.closed_ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    ingress.closed_err.fetch_add(1, Ordering::Relaxed);
                    eprintln!("fleet-lab-node: connection {peer} failed: {error}");
                }
            }
        });
    }
    // Best-effort: flush the open chunk if Serving. This is not drain_seal_publish,
    // does not wait for in-flight producers, and does not claim graceful replacement.
    let flush_result = runtime.flush().await;
    let exit_reason = match &flush_result {
        Ok(()) => "signal:flush-ok".to_owned(),
        Err(_) if runtime.is_standby() || runtime.is_terminal() => {
            "signal:standby-or-terminal".to_owned()
        }
        Err(error) => format!("signal:flush-failed:{error}"),
    };
    write_summary(
        &args,
        &store_cfg,
        disposition,
        &exit_reason,
        &runtime,
        &metrics,
        &ingress,
        bind_addr.to_string(),
    )?;
    eprintln!("fleet-lab-node: exit reason={exit_reason}");
    Ok(())
}

fn disposition_label(outcome: &VerseControlOutcome) -> &'static str {
    match outcome {
        VerseControlOutcome::Serving => "Serving",
        VerseControlOutcome::Standby => "Standby",
        VerseControlOutcome::RecoveryRequired { .. } => "RecoveryRequired",
        VerseControlOutcome::ConflictNeedsInspect => "ConflictNeedsInspect",
        VerseControlOutcome::StartFailed(_) => "StartFailed",
    }
}

fn print_startup_banner(args: &Args, store_cfg: &StoreEndpointConfig, disposition: &str) {
    eprintln!(
        "fleet-exercise: backend={} run_id={} root={} owner={} advertise={} disposition={} safety=lab-no-ha-no-auto-failover bucket={}",
        store_cfg.profile.label(),
        args.run_id,
        store_cfg.root,
        owner_display(args.owner),
        args.endpoint.as_str(),
        disposition,
        store_cfg.bucket
    );
}

fn owner_display(owner: OwnerId) -> String {
    String::from_utf8_lossy(&owner.as_bytes()).into_owned()
}

async fn serve_status_loop(
    listener: TcpListener,
    runtime: Arc<VerseRuntime>,
    metrics: Arc<ObjectStoreMetrics>,
    ingress: Arc<IngressCounters>,
    disposition: String,
    profile: String,
    run_id: String,
) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            break;
        };
        let body = status_json(
            &runtime,
            &metrics,
            &ingress,
            &disposition,
            &profile,
            &run_id,
        )
        .await;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

async fn status_json(
    runtime: &VerseRuntime,
    metrics: &ObjectStoreMetrics,
    ingress: &IngressCounters,
    disposition: &str,
    profile: &str,
    run_id: &str,
) -> String {
    let role = if runtime.is_serving() {
        "Serving"
    } else if runtime.is_standby() {
        "Standby"
    } else if runtime.is_terminal() {
        "Terminal"
    } else {
        "Unknown"
    };
    let (canon_revision, route_label) = match runtime.resolve_route().await {
        Ok(CanonRoute::Serve { canon_revision, .. }) => (Some(canon_revision), "Serve"),
        Ok(CanonRoute::NotOwner { canon_revision, .. }) => (Some(canon_revision), "NotOwner"),
        Ok(CanonRoute::Fenced { canon_revision, .. }) => (Some(canon_revision), "Fenced"),
        Ok(CanonRoute::Recovering { canon_revision }) => (Some(canon_revision), "Recovering"),
        Err(_) => (None, "ObserveFailed"),
    };
    let snap = metrics.snapshot();
    let driver = runtime.driver_metrics();
    let health = runtime.health();
    let mut out = String::new();
    out.push_str("{\n");
    out.push_str("  \"lab\": true,\n");
    out.push_str("  \"ha_claim\": false,\n");
    push_str_field(&mut out, "backend", profile);
    push_str_field(&mut out, "run_id", run_id);
    push_str_field(&mut out, "startup_disposition", disposition);
    push_str_field(&mut out, "role", role);
    push_str_field(&mut out, "canon_route", route_label);
    if let Some(revision) = canon_revision {
        out.push_str(&format!("  \"canon_revision\": {revision},\n"));
    } else {
        out.push_str("  \"canon_revision\": null,\n");
    }
    out.push_str("  \"object_store\": {\n");
    out.push_str(&format!("    \"puts\": {},\n", snap.puts));
    out.push_str(&format!("    \"gets\": {},\n", snap.gets));
    out.push_str(&format!("    \"lists\": {},\n", snap.lists));
    out.push_str(&format!(
        "    \"objects_listed\": {},\n",
        snap.objects_listed
    ));
    out.push_str(&format!(
        "    \"uploaded_bytes\": {},\n",
        snap.uploaded_bytes
    ));
    out.push_str(&format!(
        "    \"downloaded_bytes\": {}\n",
        snap.downloaded_bytes
    ));
    out.push_str("  },\n");
    out.push_str("  \"ingress\": {\n");
    out.push_str(&format!(
        "    \"accepted\": {},\n",
        ingress.accepted.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "    \"closed_ok\": {},\n",
        ingress.closed_ok.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "    \"closed_err\": {}\n",
        ingress.closed_err.load(Ordering::Relaxed)
    ));
    out.push_str("  },\n");
    if let Some(metrics) = driver {
        out.push_str("  \"driver\": {\n");
        out.push_str(&format!(
            "    \"bytes_at_risk\": {},\n",
            metrics.bytes_at_risk
        ));
        out.push_str(&format!(
            "    \"reserved_bytes\": {},\n",
            metrics.reserved_bytes
        ));
        out.push_str(&format!(
            "    \"inflight_chunks\": {},\n",
            metrics.inflight_chunks
        ));
        out.push_str(&format!("    \"dedup_hits\": {},\n", metrics.dedup_hits));
        out.push_str(&format!("    \"admitted\": {},\n", metrics.admitted));
        out.push_str(&format!("    \"rejected\": {},\n", metrics.rejected));
        out.push_str(&format!("    \"poisoned\": {}\n", metrics.poisoned));
        out.push_str("  },\n");
    } else {
        out.push_str("  \"driver\": null,\n");
    }
    if let Some(health) = health {
        out.push_str(&format!("  \"owner_status\": \"{:?}\",\n", health.status));
    } else {
        out.push_str("  \"owner_status\": null,\n");
    }
    out.push_str("  \"ownership_routes\": false\n");
    out.push('}');
    out
}

fn push_str_field(out: &mut String, key: &str, value: &str) {
    out.push_str(&format!("  \"{key}\": \"{}\",\n", json_escape(value)));
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[allow(clippy::too_many_arguments)]
fn write_summary(
    args: &Args,
    store_cfg: &StoreEndpointConfig,
    disposition: &str,
    exit_reason: &str,
    runtime: &VerseRuntime,
    metrics: &ObjectStoreMetrics,
    ingress: &IngressCounters,
    bind_addr: String,
) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(&args.summary_dir)?;
    let path = args
        .summary_dir
        .join(format!("fleet-lab-node-{}-summary.json", args.run_id));
    let snap = metrics.snapshot();
    let role = if runtime.is_serving() {
        "Serving"
    } else if runtime.is_standby() {
        "Standby"
    } else {
        "TerminalOrOther"
    };
    let driver = runtime.driver_metrics();
    let mut body = String::new();
    body.push_str("{\n");
    body.push_str("  \"lab\": true,\n");
    body.push_str("  \"ha_claim\": false,\n");
    push_str_field(&mut body, "backend", store_cfg.profile.label());
    push_str_field(&mut body, "run_id", &args.run_id);
    push_str_field(&mut body, "root", &store_cfg.root);
    push_str_field(&mut body, "bucket", &store_cfg.bucket);
    push_str_field(&mut body, "bind", &bind_addr);
    push_str_field(&mut body, "advertise", args.endpoint.as_str());
    push_str_field(&mut body, "startup_disposition", disposition);
    push_str_field(&mut body, "role_at_exit", role);
    push_str_field(&mut body, "exit_reason", exit_reason);
    body.push_str(&format!(
        "  \"unix_secs\": {},\n",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    ));
    body.push_str("  \"object_store\": {\n");
    body.push_str(&format!("    \"puts\": {},\n", snap.puts));
    body.push_str(&format!("    \"gets\": {},\n", snap.gets));
    body.push_str(&format!("    \"lists\": {},\n", snap.lists));
    body.push_str(&format!(
        "    \"objects_listed\": {},\n",
        snap.objects_listed
    ));
    body.push_str(&format!(
        "    \"uploaded_bytes\": {},\n",
        snap.uploaded_bytes
    ));
    body.push_str(&format!(
        "    \"downloaded_bytes\": {}\n",
        snap.downloaded_bytes
    ));
    body.push_str("  },\n");
    body.push_str("  \"ingress\": {\n");
    body.push_str(&format!(
        "    \"accepted\": {},\n",
        ingress.accepted.load(Ordering::Relaxed)
    ));
    body.push_str(&format!(
        "    \"closed_ok\": {},\n",
        ingress.closed_ok.load(Ordering::Relaxed)
    ));
    body.push_str(&format!(
        "    \"closed_err\": {}\n",
        ingress.closed_err.load(Ordering::Relaxed)
    ));
    body.push_str("  },\n");
    if let Some(metrics) = driver {
        body.push_str("  \"driver\": {\n");
        body.push_str(&format!("    \"admitted\": {},\n", metrics.admitted));
        body.push_str(&format!("    \"rejected\": {},\n", metrics.rejected));
        body.push_str(&format!(
            "    \"reserved_bytes\": {},\n",
            metrics.reserved_bytes
        ));
        body.push_str(&format!(
            "    \"inflight_chunks\": {},\n",
            metrics.inflight_chunks
        ));
        body.push_str(&format!("    \"poisoned\": {}\n", metrics.poisoned));
        body.push_str("  },\n");
    } else {
        body.push_str("  \"driver\": null,\n");
    }
    body.push_str(
        "  \"graceful_note\": \"signal stops accept and flushes open chunk when Serving; does not drain_seal_publish or wait for producers\"\n",
    );
    body.push('}');
    std::fs::write(&path, body)?;
    eprintln!("fleet-lab-node: wrote summary {}", path.display());
    Ok(())
}

fn verse_config(owner: OwnerId) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: JournalId::from_bytes(*b"fleet-lab-jrnl!!"),
        verse_id: VerseId::from_bytes(*b"fleet-lab-verse!"),
        owner_id: owner,
        cohort_id: CohortId::from_bytes(*b"fleet-lab-cohrt!"),
        writer_id: WriterId::from_bytes(*b"fleet-lab-wrtr!!"),
        policy: ChunkPolicy {
            max_chunk_bytes: 64 * 1024,
            max_record_bytes: 16 * 1024,
            max_chunk_records: 256,
            max_chunk_age: Duration::from_secs(60),
            max_buffered_bytes: 256 * 1024,
            max_inflight_chunks: 1,
            max_uncommitted_age: Duration::from_secs(60),
            recovery_scan: RecoveryBound::new(8).expect("bound"),
        },
        recovery_bound: RecoveryBound::new(8).expect("bound"),
        queue_capacity: 256,
    }
}

fn parse_args(arguments: impl Iterator<Item = String>) -> Result<Args, Box<dyn Error>> {
    let mut bind = "127.0.0.1:9000".to_owned();
    let mut status_bind = None;
    let mut owner = OwnerId::from_bytes(*b"fleet-lab-own-a!");
    let mut endpoint = OwnerEndpoint::new("tcp://127.0.0.1:9000")?;
    let mut run_id = String::new();
    let mut bootstrap = false;
    let mut loglet_id = None;
    let mut profile = BackendProfile::RustFs;
    let mut endpoint_url = None;
    let mut bucket = None;
    let mut region = None;
    let mut env_file = None;
    let mut summary_dir = PathBuf::from(".");
    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--bind" => bind = required(&mut arguments, "--bind")?,
            "--status-bind" => status_bind = Some(required(&mut arguments, "--status-bind")?),
            "--owner" => {
                let raw = required(&mut arguments, "--owner")?;
                let bytes = parse_owner_bytes(&raw)?;
                owner = OwnerId::from_bytes(bytes);
            }
            "--advertise" => {
                endpoint = OwnerEndpoint::new(required(&mut arguments, "--advertise")?)?;
            }
            "--run-id" => run_id = required(&mut arguments, "--run-id")?,
            "--bootstrap" => bootstrap = true,
            "--loglet-id" => loglet_id = Some(required(&mut arguments, "--loglet-id")?),
            "--backend" => {
                profile = BackendProfile::parse(&required(&mut arguments, "--backend")?)?;
            }
            "--endpoint" | "--s3-endpoint" => {
                endpoint_url = Some(required(&mut arguments, "--endpoint")?);
            }
            "--bucket" => bucket = Some(required(&mut arguments, "--bucket")?),
            "--region" => region = Some(required(&mut arguments, "--region")?),
            "--env-file" => env_file = Some(PathBuf::from(required(&mut arguments, "--env-file")?)),
            "--summary-dir" => {
                summary_dir = PathBuf::from(required(&mut arguments, "--summary-dir")?);
            }
            "--access-key" | "--secret-key" => {
                return Err(
                    "secrets must not be passed on argv; use --env-file or process environment (RUSTFS_* / R2_*)"
                        .into(),
                );
            }
            "--help" | "-h" => {
                print_help();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    if run_id.is_empty() || run_id.contains('/') {
        return Err("--run-id is required and must not contain '/'".into());
    }
    Ok(Args {
        bind,
        status_bind,
        owner,
        endpoint,
        run_id,
        bootstrap,
        loglet_id,
        profile,
        endpoint_url,
        bucket,
        region,
        env_file,
        summary_dir,
    })
}

fn parse_owner_bytes(raw: &str) -> Result<[u8; 16], Box<dyn Error>> {
    let bytes = raw.as_bytes();
    if bytes.len() != 16 {
        return Err("--owner must be exactly 16 bytes (ASCII)".into());
    }
    let mut out = [0_u8; 16];
    out.copy_from_slice(bytes);
    Ok(out)
}

fn required(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, Box<dyn Error>> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a value").into())
}

fn print_help() {
    println!(
        "usage: fleet-lab-node --run-id ID --backend rustfs|r2 [--bootstrap --loglet-id ID] [options]\n\
         secrets: --env-file PATH or RUSTFS_*/R2_* environment variables (never argv)\n\
         optional: --status-bind 127.0.0.1:PORT  --summary-dir DIR\n\
         See docs/fleet-lab-two-process-drill.md and deploy/fleet-exercise/"
    );
}
