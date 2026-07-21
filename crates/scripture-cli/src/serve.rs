//! `scripture serve` assembly over [`VerseNodeSupervisor`].
//!
//! Does not claim HA, automatic failover, or Decision 0012 recovery. Kubernetes
//! must never grant write ownership from scheduling or restarts — this path
//! always consults Canon evidence. Greenfield bootstrap is a separate command.
//!
//! After a one-shot bootstrap process exits, an open generation's soft sequencer
//! is gone. Ordinary `serve` then observes Canon and may report
//! `RecoveryRequired` for the named owner — it does not seal-and-replace.
//! Post-bootstrap first Serving remains an open product decision.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use scripture::{FileSpoolStorage, SpoolCell, SpoolCellHandle, SpoolConfig, SystemClock};
use scripture_runtime::{
    RawLinesConfig, VerseControlOutcome, disposition_label, is_ready_to_serve,
    serve_canon_raw_lines_connection, serve_canon_raw_lines_connection_with_spool, status_body,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::assemble;
use crate::config::{HaMode, ScriptureConfig};
use crate::preflight;

pub async fn serve(config: ScriptureConfig) -> Result<(), Box<dyn Error>> {
    // Static safety.require gate before any assemble / listen / lifecycle.
    preflight::run_static_preflight(&config)?;

    if config.is_multi_assignment() {
        return Err(
            "refusing plain `scripture serve` for scribe.assignments — use \
             `scripture bootstrap --config …` under ha.mode: serving-authority"
                .into(),
        );
    }
    if config.ha.mode == HaMode::ServingAuthority {
        return Err(
            "refusing plain `scripture serve` under ha.mode: serving-authority — \
             Holylog open writables cannot cross process exit. Use long-lived \
             `scripture bootstrap --config …` (Empty→Serving) or \
             `scripture promote --config … --candidate-term N` (single-assignment) or \
             `scripture promote --config … --assignment ID --candidate-term N` (multi). \
             Authority is the VirtualLog root fence (no separate authority store)."
                .into(),
        );
    }

    let shared = assemble::connect_shared_store(&config)?;
    let store_root = config.store.prefix.trim_end_matches('/').to_owned();
    let verse_config = config.verse_runtime_config()?;
    let assembled = assemble::assemble_assignment_seams(
        &shared,
        &store_root,
        verse_config,
        shared.advertise.clone(),
    )?;
    let advertise = assembled.advertise;
    let backend = assembled.backend;
    let store_root = assembled.store_root;
    let node = assembled.node;
    let directory_store = Arc::clone(&shared.store);

    let outcome = match node.virtual_log().observe_membership().await {
        Err(holylog::virtual_log::VirtualLogError::Uninitialized) => {
            // Greenfield / cargo try-it: empty store bootstraps in-process so
            // plain `serve` can open ingress without a separate bootstrap process.
            // Holylog soft sequencers still cannot cross process exit — restarting
            // against a non-empty root remains RecoveryRequired (delete store.path
            // to reset the try-it directory).
            let loglet = holylog::virtual_log::LogletId::new("scripture-gen-01")
                .map_err(|error| format!("bootstrap loglet id: {error}"))?;
            eprintln!(
                "scripture: empty Canon — bootstrapping in-process (loglet_id=scripture-gen-01)"
            );
            node.bootstrap_verse(loglet, SystemClock::new(), scripture::SystemTimer::new(), 2)
                .await?
        }
        Ok(_) => {
            node.start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
                .await?
        }
        Err(error) => return Err(error.into()),
    };

    match &outcome {
        VerseControlOutcome::ConflictNeedsInspect { .. } | VerseControlOutcome::StartFailed(_) => {
            return Err(format!(
                "refusing to listen: disposition={}",
                disposition_label(&outcome)
            )
            .into());
        }
        _ => {}
    }

    let disposition = disposition_label(&outcome);
    let ready = is_ready_to_serve(&outcome);
    let recovery_required = matches!(outcome, VerseControlOutcome::RecoveryRequired { .. });

    eprintln!(
        "scripture: ha_mode=legacy disposition={disposition} ready={ready} owner={} advertise={} bind={} backend={} prefix={}",
        config.node.owner_id,
        advertise.as_str(),
        config.listener_bind()?,
        backend.label(),
        store_root,
    );
    if recovery_required {
        eprintln!(
            "scripture: RecoveryRequired — probes only; open generation needs an accepted seal-and-replace decision (no ownership invented)"
        );
    }

    let spool: Option<Arc<SpoolCellHandle<FileSpoolStorage>>> = if recovery_required {
        None
    } else if let Some(path) = &config.paths.spool_dir {
        let storage = FileSpoolStorage::open(path)?;
        let (handle, completer) = SpoolCell::open(
            config.verse_runtime_config()?.journal_id,
            SpoolConfig::default(),
            storage,
        )?;
        if !handle.is_serving() {
            return Err(
                "spool opened non-serving after clean inspect (race/corruption); refusing serve"
                    .into(),
            );
        }
        eprintln!(
            "scripture: spool Serving dir={} (local WAL; not a Journaled quorum)",
            path.display()
        );
        tokio::spawn(async move {
            completer.run().await;
        });
        Some(Arc::new(handle))
    } else {
        None
    };

    let runtime = if recovery_required {
        None
    } else {
        Some(node.runtime().await.ok_or("runtime missing after start")?)
    };
    let alive = Arc::new(AtomicBool::new(true));

    if let Some(status_bind) = &config.metrics.status_bind {
        let listener = TcpListener::bind(status_bind).await?;
        eprintln!(
            "scripture: status/liveness/readiness on {} (/livez /readyz /status)",
            listener.local_addr()?
        );
        let runtime = runtime.clone();
        let disposition = disposition.to_owned();
        let alive = Arc::clone(&alive);
        tokio::spawn(async move {
            serve_probe_loop(listener, runtime, disposition, ready, alive).await;
        });
    } else if recovery_required {
        return Err(
            "RecoveryRequired with no metrics.status_bind: refusing silent wait without probes"
                .into(),
        );
    }

    if recovery_required {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }

    let runtime = runtime.ok_or("runtime missing after start")?;
    let listener = TcpListener::bind(config.listener_bind()?).await?;
    eprintln!(
        "scripture: listening on {} (temporary ingress; not a public producer protocol)",
        listener.local_addr()?
    );

    // Discovery only: produce-lab resolves routes from the fleet directory.
    spawn_single_assignment_directory_heartbeat(directory_store, &config, ready);

    loop {
        let (stream, peer) = listener.accept().await?;
        let runtime = Arc::clone(&runtime);
        let spool = spool.clone();
        tokio::spawn(async move {
            let result = if let Some(spool) = spool {
                serve_canon_raw_lines_connection_with_spool(
                    stream,
                    runtime,
                    spool,
                    RawLinesConfig::default(),
                )
                .await
            } else {
                serve_canon_raw_lines_connection(stream, runtime, RawLinesConfig::default()).await
            };
            if let Err(error) = result {
                eprintln!("scripture: connection from {peer} closed: {error}");
            }
        });
    }
}

/// Publishes a soft directory route so `produce-lab` can find this serve.
fn spawn_single_assignment_directory_heartbeat(
    store: Arc<dyn object_store::ObjectStore>,
    config: &ScriptureConfig,
    ready: bool,
) {
    let Some(verse) = config.verse.clone() else {
        return;
    };
    let owner_id = config.node.owner_id.clone();
    let node_advertise = config.node.advertise.clone();
    let prefix = config.store.prefix.clone();
    let valid_for_ms = 15_000u64;
    let beat = std::time::Duration::from_millis(3_000);

    tokio::spawn(async move {
        loop {
            let record = scripture_runtime::directory::DirectoryRecord {
                format_version: 1,
                owner_id: owner_id.clone(),
                node_advertise: node_advertise.clone(),
                published_at_ms: scripture_runtime::directory::now_ms(),
                valid_for_ms,
                assignments: vec![scripture_runtime::directory::DirectoryAssignment {
                    canon: verse.journal_id.clone(),
                    verse: verse.verse_id.clone(),
                    advertise: node_advertise.clone(),
                    posture: "legacy-serve".to_owned(),
                    disposition: if ready {
                        "Serving".to_owned()
                    } else {
                        "NotReady".to_owned()
                    },
                    admits_committed_acks: ready,
                }],
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

async fn serve_probe_loop(
    listener: TcpListener,
    runtime: Option<Arc<scripture_service::VerseRuntime>>,
    disposition: String,
    ready: bool,
    alive: Arc<AtomicBool>,
) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let mut buf = [0_u8; 1024];
        let _ = stream.read(&mut buf).await;
        let request = String::from_utf8_lossy(&buf);
        let path = request
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/status");

        let serving = runtime.as_ref().is_some_and(|r| r.is_serving());
        let standby = runtime.as_ref().is_some_and(|r| r.is_standby());
        let (code, body) = match path {
            "/livez" | "/healthz" => {
                if alive.load(Ordering::Relaxed) {
                    (200, "alive\n".to_owned())
                } else {
                    (503, "not-alive\n".to_owned())
                }
            }
            "/readyz" => {
                if ready && serving {
                    (200, "ready\n".to_owned())
                } else {
                    (503, format!("not-ready disposition={disposition}\n"))
                }
            }
            _ => {
                let body = status_body(&disposition, serving, standby, ready && serving);
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
}

#[cfg(test)]
mod tests {
    use scripture_runtime::{VerseControlOutcome, is_ready_to_serve};

    #[test]
    fn standby_and_recovery_are_not_ready() {
        assert!(!is_ready_to_serve(&VerseControlOutcome::Standby));
        assert!(is_ready_to_serve(&VerseControlOutcome::Serving));
    }

    #[test]
    fn kubernetes_owner_label_does_not_imply_readiness() {
        // Ownership comes from Canon disposition, never from a Deployment label.
        assert!(!is_ready_to_serve(&VerseControlOutcome::Standby));
    }
}
