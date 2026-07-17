//! Long-lived Serving Authority activation for CLI composition.
//!
//! Holylog soft-sequencer writables cannot cross process exit, so bootstrap /
//! promote under `ha.mode: serving-authority` remain in-process and then open
//! ingress on the same process. Authority is the VirtualLog root fence only.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use holylog::virtual_log::LogletResolver;
use scripture::serving_authority::{AuthorityKey, JournalGenerationRef, RouteHint, WriterTerm};
use scripture_runtime::{
    HaServingSession, HolylogJournalFoundation, NodeIdentity, RawLinesConfig, bootstrap_and_serve,
    promote_and_serve, serve_ha_raw_lines_connection, status_body, system_clocks,
};
use scripture_service::{
    AuthorityCoordinator, JournalFoundationTransition, SecureTransitionIdGenerator,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::assemble::{self, AssembledNode};
use crate::config::ScriptureConfig;

fn authority_key(config: &ScriptureConfig) -> Result<AuthorityKey, Box<dyn Error>> {
    let verse = config.verse_runtime_config()?;
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

/// Operator-directed Empty → Serving bootstrap that remains the serving process.
pub async fn bootstrap_and_serve_cli(
    config: ScriptureConfig,
    initial_term: u64,
) -> Result<(), Box<dyn Error>> {
    let assembled = assemble::assemble_supervisor(&config)?;
    let key = authority_key(&config)?;
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
    let session = bootstrap_and_serve(
        &coordinator,
        foundation.as_ref(),
        key,
        term,
        config.verse_runtime_config()?,
        Arc::clone(&assembled.register),
        Arc::clone(&assembled.resolver),
        clock,
        timer,
    )
    .await?;
    eprintln!(
        "scripture: ha_mode=serving-authority action=bootstrap-and-serve ready=true owner={} advertise={} bind={} backend={} prefix={}",
        config.node.owner_id,
        assembled.advertise.as_str(),
        config.listener.bind,
        assembled.backend.label(),
        assembled.store_root,
    );
    run_ha_ingress(config, session).await
}

/// Operator-directed promote that remains the serving process.
pub async fn promote_and_serve_cli(
    config: ScriptureConfig,
    candidate_term: u64,
) -> Result<(), Box<dyn Error>> {
    let assembled = assemble::assemble_supervisor(&config)?;
    let key = authority_key(&config)?;
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
    let session = promote_and_serve(
        &coordinator,
        foundation.as_ref(),
        key,
        term,
        expected,
        config.verse_runtime_config()?,
        Arc::clone(&assembled.register),
        Arc::clone(&assembled.resolver),
        clock,
        timer,
    )
    .await?;
    eprintln!(
        "scripture: ha_mode=serving-authority action=promote-and-serve ready=true owner={} advertise={} bind={} backend={} prefix={} candidate_term={candidate_term}",
        config.node.owner_id,
        assembled.advertise.as_str(),
        config.listener.bind,
        assembled.backend.label(),
        assembled.store_root,
    );
    run_ha_ingress(config, session).await
}

async fn run_ha_ingress(
    config: ScriptureConfig,
    session: HaServingSession,
) -> Result<(), Box<dyn Error>> {
    let session = Arc::new(session);
    let alive = Arc::new(AtomicBool::new(true));

    if let Some(status_bind) = &config.metrics.status_bind {
        let listener = TcpListener::bind(status_bind).await?;
        eprintln!(
            "scripture: status/liveness/readiness on {} (/livez /readyz /status)",
            listener.local_addr()?
        );
        let session = Arc::clone(&session);
        let alive = Arc::clone(&alive);
        tokio::spawn(async move {
            serve_probe_loop(listener, session, alive).await;
        });
    }

    let listener = TcpListener::bind(&config.listener.bind).await?;
    eprintln!(
        "scripture: listening on {} (temporary ingress; authority-gated; not a public producer protocol)",
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

async fn serve_probe_loop(
    listener: TcpListener,
    session: Arc<HaServingSession>,
    alive: Arc<AtomicBool>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let session = Arc::clone(&session);
        let alive = Arc::clone(&alive);
        tokio::spawn(async move {
            serve_probe_connection(stream, session, alive).await;
        });
    }
}

async fn serve_probe_connection(
    mut stream: tokio::net::TcpStream,
    session: Arc<HaServingSession>,
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
                (503, "not-ready disposition=serving-authority\n".to_owned())
            }
        }
        _ => {
            let body = status_body("serving-authority", serving, false, serving);
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
