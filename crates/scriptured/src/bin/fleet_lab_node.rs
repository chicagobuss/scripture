//! Two-process fleet-lab Scripture node against a shared object-store root.
//!
//! Requires `--features fleet-lab` and a local RustFS (or other attested S3)
//! endpoint. Ordinary startup never bootstraps or replaces: pass `--bootstrap`
//! or `--replace-after-crash` explicitly.

use std::env;
use std::error::Error;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use holylog::virtual_log::{ConditionalRegister, LogletId};
use holylog_object_store::{ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{
    ObjectStoreConditionalRegister, RegisterCapabilities, register_path,
};
use object_store::path::Path;
use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, SystemClock, VerseId,
    WriterId,
};
use scripture_service::{VerseKey, VerseRuntimeConfig};
use scriptured::{
    FleetLabResolver, NodeIdentity, ObjectStorePartsFactory, RawLinesConfig, VerseControlOutcome,
    VerseNodeSupervisor, connect_rustfs, serve_canon_raw_lines_connection,
};
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
    owner: OwnerId,
    endpoint: OwnerEndpoint,
    run_id: String,
    bootstrap: bool,
    loglet_id: Option<String>,
    s3_endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
}

async fn try_main() -> Result<(), Box<dyn Error>> {
    let args = parse_args(env::args().skip(1))?;
    let store = connect_rustfs(
        &args.s3_endpoint,
        &args.bucket,
        &args.region,
        &args.access_key,
        &args.secret_key,
    )?;
    let root = format!("fleet-lab/{}", args.run_id);
    let register = Arc::new(ObjectStoreConditionalRegister::new(
        Arc::clone(&store),
        Path::from(root.clone()).join(register_path("verse").as_ref()),
        RegisterCapabilities::amazon_s3(),
    )?) as Arc<dyn ConditionalRegister>;
    let metrics = Arc::new(ObjectStoreMetrics::default());
    let parts = Arc::new(ObjectStorePartsFactory::new(
        store,
        root,
        ObjectStorePartsFactory::rustfs_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::clone(&metrics),
    ));
    let resolver = Arc::new(FleetLabResolver::default());
    let config = verse_config(args.owner);
    let key = VerseKey::from_config(&config);
    let node = VerseNodeSupervisor::with_parts_factory(
        NodeIdentity {
            owner_id: args.owner,
            endpoint: args.endpoint.clone(),
        },
        register,
        resolver,
        parts,
        vec![config],
    )?;

    if args.bootstrap {
        let loglet = LogletId::new(
            args.loglet_id
                .as_deref()
                .ok_or("--bootstrap requires --loglet-id")?,
        )?;
        let outcome = node
            .bootstrap_verse(
                key,
                loglet,
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await?;
        eprintln!("bootstrap: {outcome:?}");
        if !matches!(outcome, VerseControlOutcome::Serving) {
            return Err("bootstrap did not yield Serving".into());
        }
    } else {
        let outcomes = node
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await?;
        eprintln!("start_configured: {outcomes:?}");
    }

    let runtime = node
        .runtime(key)
        .await
        .ok_or("runtime missing after start")?;
    let listener = TcpListener::bind(&args.bind).await?;
    eprintln!(
        "fleet-lab-node listening on {}; run_id={}; owner={}",
        listener.local_addr()?,
        args.run_id,
        args.owner
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            if let Err(error) =
                serve_canon_raw_lines_connection(stream, runtime, RawLinesConfig::default()).await
            {
                eprintln!("connection {peer} failed: {error}");
            }
        });
    }
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
    let mut owner = OwnerId::from_bytes(*b"fleet-lab-own-a!");
    let mut endpoint = OwnerEndpoint::new("tcp://127.0.0.1:9000")?;
    let mut run_id = String::new();
    let mut bootstrap = false;
    let mut loglet_id = None;
    let mut s3_endpoint = "http://127.0.0.1:9100".to_owned();
    let mut bucket = "holylog-rustfs".to_owned();
    let mut region = "us-east-1".to_owned();
    let mut access_key = "holylog-rustfs".to_owned();
    let mut secret_key = "holylog-rustfs-local-secret".to_owned();
    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--bind" => bind = required(&mut arguments, "--bind")?,
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
            "--s3-endpoint" => s3_endpoint = required(&mut arguments, "--s3-endpoint")?,
            "--bucket" => bucket = required(&mut arguments, "--bucket")?,
            "--region" => region = required(&mut arguments, "--region")?,
            "--access-key" => access_key = required(&mut arguments, "--access-key")?,
            "--secret-key" => secret_key = required(&mut arguments, "--secret-key")?,
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
        owner,
        endpoint,
        run_id,
        bootstrap,
        loglet_id,
        s3_endpoint,
        bucket,
        region,
        access_key,
        secret_key,
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
        "usage: fleet-lab-node --run-id ID [--bootstrap --loglet-id ID] [options]\n\
         See docs/fleet-lab-two-process-drill.md"
    );
}
