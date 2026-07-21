//! `scripture scribe run` — normal same-Verse fleet lifecycle.
//!
//! Every fleet member starts with this command. It observes the durable root,
//! bootstraps when empty, joins as a healthy non-writer when another owner
//! lawfully Serves, and attempts a lawful successor CAS only after the peer
//! advertise endpoint looks unreachable for a grace period.
//!
//! Compatibility: prefer this over `bootstrap` / `promote` / standby posture.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use holylog::virtual_log::LogletResolver;
use scripture::serving_authority::{AuthorityKey, RouteHint, WriterTerm};
use scripture_runtime::{
    HolylogJournalFoundation, NodeIdentity, PeerProbe, RawLinesConfig, ScribeLifecycle,
    ScribeRunOptions, TcpAdvertiseProbe, serve_ha_raw_lines_connection, system_clocks,
};
use scripture_service::{
    AuthorityCoordinator, JournalFoundationTransition, SecureTransitionIdGenerator,
};
use tokio::net::TcpListener;

use crate::assemble;
use crate::config::{HaMode, ScriptureConfig};

/// Runs the automatic Scribe lifecycle until this process is the lawful writer,
/// then serves HA ingress on the pre-bound listener.
pub async fn scribe_run(
    config: ScriptureConfig,
    peer_grace_ms: u64,
    initial_term: u64,
) -> Result<(), Box<dyn Error>> {
    if config.is_multi_assignment() {
        return Err(
            "scribe run currently targets one Canon/Verse per process; use a single-assignment Serving-Authority config (shared store root across fleet members)"
                .into(),
        );
    }
    if config.ha.mode != HaMode::ServingAuthority {
        return Err("scribe run requires ha.mode: serving-authority".into());
    }

    // Bind ingress before any root CAS so a candidate that cannot open the
    // socket never deposes a reachable writer.
    let listener = TcpListener::bind(config.listener_bind()?).await?;
    let assembled = assemble::assemble_supervisor(&config)?;
    let verse = config.verse_runtime_config()?;
    let key = AuthorityKey {
        journal_id: verse.journal_id,
        verse_id: verse.verse_id,
    };
    let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
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
    ));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&assembled.register),
        Arc::clone(&assembled.resolver) as Arc<dyn LogletResolver>,
        Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
        Arc::new(SecureTransitionIdGenerator::new()),
        assembled.node.identity().owner_id,
        RouteHint::new(assembled.advertise.as_str())?,
    );
    let (clock, timer) = system_clocks();
    let peer: Arc<dyn PeerProbe> = Arc::new(TcpAdvertiseProbe {
        timeout: Duration::from_millis(500),
    });
    let lifecycle = ScribeLifecycle {
        coordinator: &coordinator,
        foundation: foundation.as_ref(),
        key,
        owner_id: assembled.node.identity().owner_id,
        runtime_config: assembled.verse_config.clone(),
        register: Arc::clone(&assembled.register),
        resolver: Arc::clone(&assembled.resolver),
        parts: Arc::clone(&assembled.parts),
        clock,
        timer,
        options: ScribeRunOptions {
            peer_grace: Duration::from_millis(peer_grace_ms.max(1)),
            initial_term,
        },
        peer,
    };

    eprintln!(
        "scripture: scribe run owner={} advertise={} bind={} peer_grace_ms={peer_grace_ms} (read-only join until root authorizes Serving)",
        config.node.owner_id,
        assembled.advertise.as_str(),
        config.listener_bind()?,
    );

    let session = lifecycle.await_lawful_writer().await?;
    let _ = WriterTerm::new(initial_term);
    eprintln!(
        "scripture: ha_mode=serving-authority action=scribe-run ready=true owner={} advertise={} bind={} backend={} prefix={}",
        config.node.owner_id,
        assembled.advertise.as_str(),
        config.listener_bind()?,
        assembled.backend.label(),
        assembled.store_root,
    );

    // Reuse the HA ingress loop from ha_activate (same probes / raw-lines).
    run_bound_ingress(config, session, listener).await
}

async fn run_bound_ingress(
    config: ScriptureConfig,
    session: scripture_runtime::HaServingSession,
    listener: TcpListener,
) -> Result<(), Box<dyn Error>> {
    // Delegate body by duplicating the stable ingress path: keep ha_activate
    // private helpers untouched by exporting a thin shared entry later if needed.
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let session = Arc::new(session);
    let alive = Arc::new(AtomicBool::new(true));

    if let Some(status_bind) = &config.metrics.status_bind {
        let status_listener = TcpListener::bind(status_bind).await?;
        eprintln!(
            "scripture: status/liveness/readiness on {} (/livez /readyz /status)",
            status_listener.local_addr()?
        );
        let session = Arc::clone(&session);
        let alive = Arc::clone(&alive);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = status_listener.accept().await else {
                    continue;
                };
                let session = Arc::clone(&session);
                let alive = Arc::clone(&alive);
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    let _ = stream.read(&mut buf).await;
                    let request = String::from_utf8_lossy(&buf);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/status");
                    let serving = if matches!(path, "/livez" | "/healthz") {
                        false
                    } else {
                        session.is_effective_writer().await
                    };
                    let (code, body) = match path {
                        "/livez" | "/healthz" => {
                            if alive.load(Ordering::Relaxed) {
                                (200, "alive\n".to_owned())
                            } else {
                                (503, "not-alive\n".to_owned())
                            }
                        }
                        "/readyz" => {
                            if serving {
                                (200, "ready\n".to_owned())
                            } else {
                                (503, "not-ready disposition=scribe-run\n".to_owned())
                            }
                        }
                        _ => (
                            200,
                            scripture_runtime::status_body("scribe-run", serving, false, serving),
                        ),
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
                });
            }
        });
    }

    eprintln!(
        "scripture: listening on {} (authority-gated; scribe run lifecycle)",
        listener.local_addr()?
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            if let Err(error) =
                serve_ha_raw_lines_connection(stream, session, RawLinesConfig::default()).await
            {
                eprintln!("scripture: HA connection from {peer} closed: {error}");
            }
        });
    }
}

/// Compatibility notice for deprecated operator paths.
pub fn print_compat_notice(old: &str) {
    eprintln!(
        "scripture: note: `{old}` remains for compatibility; prefer `scripture scribe run --config …` for the normal fleet lifecycle"
    );
}

/// Wraps legacy bootstrap with a compatibility notice under Serving Authority.
pub async fn bootstrap_compat(
    config: ScriptureConfig,
    loglet_id: Option<String>,
    initial_term: u64,
) -> Result<(), Box<dyn Error>> {
    if config.ha.mode == HaMode::ServingAuthority {
        print_compat_notice("bootstrap");
    }
    crate::bootstrap::bootstrap(config, loglet_id, initial_term).await
}

/// Wraps legacy promote with a compatibility notice.
pub async fn promote_compat(
    config: ScriptureConfig,
    candidate_term: u64,
    assignment_id: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    print_compat_notice("promote");
    crate::promote::promote(config, candidate_term, assignment_id).await
}
