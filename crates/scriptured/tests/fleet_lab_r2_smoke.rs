#![cfg(feature = "fleet-lab-r2-smoke")]

//! Opt-in Cloudflare R2 fleet-exercise smoke.
//!
//! Requires `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`.
//! Never run as part of the normal locked gate.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use holylog::virtual_log::{ConditionalRegister, LogletId};
use holylog_object_store::{ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, SystemClock, VerseId,
    WriterId,
};
use scripture_service::VerseRuntimeConfig;
use scriptured::{
    BackendProfile, FleetLabResolver, NodeIdentity, ObjectStorePartsFactory, RawLinesConfig,
    VerseControlOutcome, VerseNodeSupervisor, connect_s3_compat, fleet_exercise_root,
    resolve_credentials, resolve_endpoint_config, serve_canon_raw_lines_connection,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn required_env(name: &str) -> TestResult<String> {
    std::env::var(name).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("required environment variable {name} is not set"),
        )
        .into()
    })
}

fn unique_run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    format!("r2-smoke-{nanos}")
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

async fn clear_prefix(store: &Arc<dyn ObjectStore>, prefix: &Path) -> TestResult {
    let mut objects = store.list(Some(prefix));
    let mut paths = Vec::new();
    while let Some(meta) = objects.try_next().await? {
        paths.push(meta.location);
    }
    for path in paths {
        store.delete(&path).await?;
    }
    Ok(())
}

async fn send_raw_lines(addr: SocketAddr, lines: &[&str]) -> TestResult<Vec<String>> {
    let stream = TcpStream::connect(addr).await?;
    let (reader, mut writer) = stream.into_split();
    for line in lines {
        writer.write_all(line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    writer.shutdown().await?;
    let mut lines_out = Vec::new();
    let mut buf = BufReader::new(reader);
    let mut line = String::new();
    while buf.read_line(&mut line).await? > 0 {
        lines_out.push(line.trim_end().to_owned());
        line.clear();
    }
    Ok(lines_out)
}

#[tokio::test]
#[ignore = "requires live R2 credentials; never part of the locked gate"]
async fn fleet_exercise_owner_standby_and_raw_lines_over_r2() -> TestResult {
    let _ = (
        required_env("R2_ENDPOINT")?,
        required_env("R2_BUCKET")?,
        required_env("R2_ACCESS_KEY_ID")?,
        required_env("R2_SECRET_ACCESS_KEY")?,
    );
    let retain = std::env::var("FLEET_EXERCISE_RETAIN_ON_FAILURE")
        .ok()
        .as_deref()
        == Some("1");
    let run_id = unique_run_id();
    let overlay = std::collections::BTreeMap::new();
    let profile = BackendProfile::CloudflareR2;
    let store_cfg = resolve_endpoint_config(profile, &run_id, None, None, None, &overlay)?;
    let credentials = resolve_credentials(profile, &overlay)?;
    let store = connect_s3_compat(
        &store_cfg.endpoint,
        &store_cfg.bucket,
        &store_cfg.region,
        &credentials.access_key,
        &credentials.secret_key,
    )?;
    drop(credentials);

    let root_path = Path::from(store_cfg.root.clone());
    let cleanup = async {
        if !retain {
            let _ = clear_prefix(&store, &root_path).await;
        }
    };

    let result = run_smoke(store.clone(), store_cfg.root.clone(), profile).await;
    if result.is_err() && retain {
        eprintln!("retained prefix {root_path} because FLEET_EXERCISE_RETAIN_ON_FAILURE=1");
    } else {
        cleanup.await;
    }
    result
}

async fn run_smoke(
    store: Arc<dyn ObjectStore>,
    root: String,
    profile: BackendProfile,
) -> TestResult {
    let metrics = Arc::new(ObjectStoreMetrics::default());
    let register = Arc::new(ObjectStoreConditionalRegister::new(
        Arc::clone(&store),
        Path::from(root.clone()).join(register_path("verse").as_ref()),
        profile.register_capabilities(),
    )?) as Arc<dyn ConditionalRegister>;
    let parts: Arc<dyn scriptured::PartsFactory> = Arc::new(ObjectStorePartsFactory::new(
        Arc::clone(&store),
        root,
        profile.drive_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::clone(&metrics),
    ));
    let resolver = Arc::new(FleetLabResolver::default());

    let owner_a = OwnerId::from_bytes(*b"fleet-lab-own-a!");
    let owner_b = OwnerId::from_bytes(*b"fleet-lab-own-b!");
    let node_a = VerseNodeSupervisor::with_parts_factory(
        NodeIdentity {
            owner_id: owner_a,
            endpoint: OwnerEndpoint::new("tcp://127.0.0.1:19000")?,
        },
        Arc::clone(&register),
        Arc::clone(&resolver),
        Arc::clone(&parts),
        verse_config(owner_a),
    );
    let outcome = node_a
        .bootstrap_verse(
            LogletId::new("gen-r2-0")?,
            SystemClock::new(),
            scripture::SystemTimer::new(),
            2,
        )
        .await?;
    assert!(matches!(outcome, VerseControlOutcome::Serving));

    let node_b = VerseNodeSupervisor::with_parts_factory(
        NodeIdentity {
            owner_id: owner_b,
            endpoint: OwnerEndpoint::new("tcp://127.0.0.1:19001")?,
        },
        register,
        resolver,
        parts,
        verse_config(owner_b),
    );
    let standby = node_b
        .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
        .await?;
    assert!(matches!(standby, VerseControlOutcome::Standby));
    assert!(node_b.runtime().await.expect("runtime").is_standby());

    let runtime = node_a.runtime().await.expect("owner runtime");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break; };
                    let runtime = Arc::clone(&runtime);
                    tokio::spawn(async move {
                        let _ = serve_canon_raw_lines_connection(
                            stream,
                            runtime,
                            RawLinesConfig::default(),
                        )
                        .await;
                    });
                }
            }
        }
    });

    let acks = send_raw_lines(addr, &["alpha", "beta", "gamma"]).await?;
    assert!(
        acks.iter().any(|line| line.starts_with("OK")),
        "expected committed OK ACKs, got {acks:?}"
    );

    let _ = stop_tx.send(());
    let _ = serve.await;

    let snap = metrics.snapshot();
    eprintln!(
        "r2 fleet-exercise smoke metrics: puts={} gets={} lists={} uploaded_bytes={} downloaded_bytes={}",
        snap.puts, snap.gets, snap.lists, snap.uploaded_bytes, snap.downloaded_bytes
    );
    assert!(snap.puts > 0, "owner must have written objects");
    assert!(
        fleet_exercise_root("dummy").is_ok(),
        "root helper remains valid"
    );
    Ok(())
}
