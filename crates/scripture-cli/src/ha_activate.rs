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
    HaActivationError, HaServingSession, HolylogJournalFoundation, NodeIdentity, RawLinesConfig,
    bootstrap_and_serve, promote_and_serve, serve_ha_raw_lines_connection, status_body,
    system_clocks,
};
use scripture_service::{
    AuthorityCoordinator, JournalFoundationTransition, SecureTransitionIdGenerator,
};
#[cfg(feature = "campaign-faults")]
use scripture_service::{CoordinatorError, FoundationTransitionError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::assemble::{self, AssembledNode};
use crate::config::ScriptureConfig;

/// Process environment name for the admin promotion bearer token.
pub const ADMIN_TOKEN_ENV: &str = "SCRIPTURE_ADMIN_TOKEN";

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
    #[cfg(feature = "campaign-faults")]
    let (assembled, campaign) = {
        let mut assembled = assemble::assemble_supervisor(&config)?;
        let campaign = crate::campaign_faults::install_into_assembled(&mut assembled)?;
        (assembled, campaign)
    };
    #[cfg(not(feature = "campaign-faults"))]
    let assembled = assemble::assemble_supervisor(&config)?;
    #[cfg(not(feature = "campaign-faults"))]
    let campaign = Option::<()>::None;
    let _ = &campaign;
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
    #[cfg(feature = "campaign-faults")]
    let session = match &campaign {
        Some(ctx) => crate::campaign_faults::observe_session(session, ctx, &config.node.owner_id),
        None => session,
    };
    eprintln!(
        "scripture: ha_mode=serving-authority action=bootstrap-and-serve ready=true owner={} advertise={} bind={} backend={} prefix={}",
        config.node.owner_id,
        assembled.advertise.as_str(),
        config.listener.bind,
        assembled.backend.label(),
        assembled.store_root,
    );
    run_ha_ingress(config, session, None).await
}

/// Operator-directed promote that remains the serving process.
pub async fn promote_and_serve_cli(
    config: ScriptureConfig,
    candidate_term: u64,
) -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "campaign-faults")]
    let (assembled, campaign) = {
        let mut assembled = assemble::assemble_supervisor(&config)?;
        let campaign = crate::campaign_faults::install_into_assembled(&mut assembled)?;
        (assembled, campaign)
    };
    #[cfg(not(feature = "campaign-faults"))]
    let assembled = assemble::assemble_supervisor(&config)?;
    #[cfg(not(feature = "campaign-faults"))]
    let campaign = Option::<()>::None;
    let _ = &campaign;
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
    let session = match promote_and_serve(
        &coordinator,
        foundation.as_ref(),
        key,
        term,
        expected.clone(),
        config.verse_runtime_config()?,
        Arc::clone(&assembled.register),
        Arc::clone(&assembled.resolver),
        clock.clone(),
        timer.clone(),
    )
    .await
    {
        Ok(session) => session,
        Err(error) if should_retry_promote_after_reply_loss(&campaign, &error) => {
            // RootCasReplyLost (campaign seam): CAS applied, reply lost — either on the
            // Transitioning intent fence (CoordinatorError::Root) or the Serving/membership
            // CAS (Foundation Indeterminate). Resolve with one in-process promote retry
            // using the *same* Expected precondition so durable Transitioning intent matches
            // (fault is one-shot; resume uses complete_after_intent).
            eprintln!(
                "scripture: promote reply-loss after applied RootCasReplyLost ({error}); retrying once with same Expected precondition"
            );
            promote_and_serve(
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
            .await?
        }
        Err(error) => return Err(error.into()),
    };
    #[cfg(feature = "campaign-faults")]
    let session = match &campaign {
        Some(ctx) => crate::campaign_faults::observe_session(session, ctx, &config.node.owner_id),
        None => session,
    };
    eprintln!(
        "scripture: ha_mode=serving-authority action=promote-and-serve ready=true owner={} advertise={} bind={} backend={} prefix={} candidate_term={candidate_term}",
        config.node.owner_id,
        assembled.advertise.as_str(),
        config.listener.bind,
        assembled.backend.label(),
        assembled.store_root,
    );
    run_ha_ingress(config, session, None).await
}

/// Live standby under Serving Authority: probes up, producer refuse until admin promote.
pub async fn standby_and_serve_cli(config: ScriptureConfig) -> Result<(), Box<dyn Error>> {
    let assembled = assemble::assemble_supervisor(&config)?;
    let admin_bind = config
        .admin
        .bind
        .clone()
        .ok_or("admin.bind is required for standby")?;
    let admin_token = std::env::var(ADMIN_TOKEN_ENV).map_err(|_| {
        format!("{ADMIN_TOKEN_ENV} must be set in the process environment for admin.bind")
    })?;
    if admin_token.trim().is_empty() {
        return Err(format!("{ADMIN_TOKEN_ENV} must be non-empty").into());
    }

    let session_slot: Arc<RwLock<Option<Arc<HaServingSession>>>> = Arc::new(RwLock::new(None));
    let alive = Arc::new(AtomicBool::new(true));
    let promote_inflight = Arc::new(AtomicBool::new(false));

    if let Some(status_bind) = &config.metrics.status_bind {
        let listener = TcpListener::bind(status_bind).await?;
        eprintln!(
            "scripture: status/liveness/readiness on {} (/livez /readyz /status)",
            listener.local_addr()?
        );
        let session_slot = Arc::clone(&session_slot);
        let alive = Arc::clone(&alive);
        tokio::spawn(async move {
            standby_probe_loop(listener, session_slot, alive).await;
        });
    }

    let admin_listener = TcpListener::bind(&admin_bind).await?;
    eprintln!(
        "scripture: admin promote on {} (POST /v1/promote; bearer {ADMIN_TOKEN_ENV})",
        admin_listener.local_addr()?
    );
    {
        let session_slot = Arc::clone(&session_slot);
        let promote_inflight = Arc::clone(&promote_inflight);
        let config = config.clone();
        let token = admin_token.clone();
        tokio::spawn(async move {
            admin_promote_loop(
                admin_listener,
                config,
                session_slot,
                promote_inflight,
                token,
            )
            .await;
        });
    }

    eprintln!(
        "scripture: ha_mode=serving-authority action=standby ready=false owner={} advertise={} bind={} backend={} prefix={}",
        config.node.owner_id,
        assembled.advertise.as_str(),
        config.listener.bind,
        assembled.backend.label(),
        assembled.store_root,
    );

    let producer = TcpListener::bind(&config.listener.bind).await?;
    eprintln!(
        "scripture: listening on {} (standby; committed ACK denied until admin promote)",
        producer.local_addr()?
    );

    loop {
        let (stream, peer) = producer.accept().await?;
        let session_slot = Arc::clone(&session_slot);
        tokio::spawn(async move {
            let session = session_slot.read().await.clone();
            match session {
                Some(session) => {
                    if let Err(error) =
                        serve_ha_raw_lines_connection(stream, session, RawLinesConfig::default())
                            .await
                    {
                        eprintln!("scripture: HA connection from {peer} closed: {error}");
                    }
                }
                None => {
                    // Fail closed: no committed ACK surface while standby.
                    let _ = stream;
                    eprintln!(
                        "scripture: refused producer from {peer}: standby (not effective writer)"
                    );
                }
            }
        });
    }
}

async fn admin_promote_loop(
    listener: TcpListener,
    config: ScriptureConfig,
    session_slot: Arc<RwLock<Option<Arc<HaServingSession>>>>,
    promote_inflight: Arc<AtomicBool>,
    token: String,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let session_slot = Arc::clone(&session_slot);
        let promote_inflight = Arc::clone(&promote_inflight);
        let config = config.clone();
        let token = token.clone();
        tokio::spawn(async move {
            handle_admin_connection(stream, config, session_slot, promote_inflight, token).await;
        });
    }
}

async fn handle_admin_connection(
    mut stream: tokio::net::TcpStream,
    config: ScriptureConfig,
    session_slot: Arc<RwLock<Option<Arc<HaServingSession>>>>,
    promote_inflight: Arc<AtomicBool>,
    expected_token: String,
) {
    use crate::admin_http::{MAX_ADMIN_REQUEST_BYTES, parse_promote_request};
    use tokio::time::{Duration, timeout};

    let raw = match timeout(
        Duration::from_secs(2),
        read_bounded_http_request(&mut stream, MAX_ADMIN_REQUEST_BYTES),
    )
    .await
    {
        Ok(Ok(raw)) => raw,
        Ok(Err(error)) => {
            let (code, body) = admin_error_response(&error);
            let _ = write_http(stream, code, body).await;
            return;
        }
        Err(_) => {
            let _ = write_http(stream, 408, "request timeout\n").await;
            return;
        }
    };

    // Authenticate + strict decode before assemble / promote work.
    let promote = match parse_promote_request(&raw, &expected_token) {
        Ok(req) => req,
        Err(error) => {
            let (code, body) = admin_error_response(&error);
            let _ = write_http(stream, code, body).await;
            return;
        }
    };

    if session_slot.read().await.is_some() {
        let _ = write_http(stream, 409, "already serving\n").await;
        return;
    }
    if promote_inflight
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        let _ = write_http(stream, 409, "promote in flight\n").await;
        return;
    }

    let assemble_result = assemble::assemble_supervisor(&config).map_err(|error| error.to_string());
    let assembled = match assemble_result {
        Ok(assembled) => assembled,
        Err(message) => {
            promote_inflight.store(false, Ordering::SeqCst);
            let _ = write_http(stream, 500, &format!("assemble failed: {message}\n")).await;
            return;
        }
    };

    let result = activate_promote_session(&config, &assembled, promote.candidate_term)
        .await
        .map_err(|error| error.to_string());
    promote_inflight.store(false, Ordering::SeqCst);
    match result {
        Ok(session) => {
            *session_slot.write().await = Some(Arc::new(session));
            eprintln!(
                "scripture: admin promote ok owner={} candidate_term={}",
                config.node.owner_id, promote.candidate_term
            );
            let _ = write_http(stream, 200, "promoted\n").await;
        }
        Err(message) => {
            eprintln!("scripture: admin promote refused: {message}");
            let _ = write_http(stream, 409, &format!("promote refused: {message}\n")).await;
        }
    }
}

async fn read_bounded_http_request(
    stream: &mut tokio::net::TcpStream,
    max_bytes: usize,
) -> Result<Vec<u8>, crate::admin_http::AdminParseError> {
    use crate::admin_http::AdminParseError;

    let mut buf = Vec::with_capacity(512);
    let mut tmp = [0_u8; 512];
    loop {
        let n = stream
            .read(&mut tmp)
            .await
            .map_err(|_| AdminParseError::Incomplete)?;
        if n == 0 {
            break;
        }
        if buf.len() + n > max_bytes {
            return Err(AdminParseError::BadRequest("request too large"));
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(header_end) = find_header_body_split(&buf)
            && let Some(content_length) = content_length_from_headers(&buf[..header_end])?
        {
            let total = header_end + 4 + content_length;
            if total > max_bytes {
                return Err(AdminParseError::BadRequest("request too large"));
            }
            if buf.len() > total {
                return Err(AdminParseError::BadRequest("trailing bytes after body"));
            }
            if buf.len() == total {
                return Ok(buf);
            }
        }
    }
    Ok(buf)
}

fn find_header_body_split(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn content_length_from_headers(
    headers: &[u8],
) -> Result<Option<usize>, crate::admin_http::AdminParseError> {
    use crate::admin_http::AdminParseError;
    let text =
        std::str::from_utf8(headers).map_err(|_| AdminParseError::BadRequest("not utf-8"))?;
    for line in text.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("content-length") {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| AdminParseError::BadRequest("bad content-length"))?;
            return Ok(Some(parsed));
        }
    }
    Ok(None)
}

fn admin_error_response(error: &crate::admin_http::AdminParseError) -> (u16, &'static str) {
    use crate::admin_http::AdminParseError;
    match error {
        AdminParseError::Incomplete => (400, "incomplete request\n"),
        AdminParseError::NotFound => (404, "not found\n"),
        AdminParseError::Unauthorized => (401, "unauthorized\n"),
        AdminParseError::BadRequest(message) => match *message {
            "request too large" => (413, "request too large\n"),
            "candidate_term must be >= 1" => (400, "candidate_term must be >= 1\n"),
            "content-length required" => (400, "content-length required\n"),
            "trailing bytes after body" => (400, "trailing bytes after body\n"),
            "body must be exact JSON object" => (400, "body must be exact JSON object\n"),
            "not utf-8" => (400, "not utf-8\n"),
            "malformed header" => (400, "malformed header\n"),
            "bad content-length" => (400, "bad content-length\n"),
            "duplicate content-length" => (400, "duplicate content-length\n"),
            "duplicate authorization" => (400, "duplicate authorization\n"),
            _ => (400, "bad request\n"),
        },
    }
}

async fn activate_promote_session(
    config: &ScriptureConfig,
    assembled: &AssembledNode,
    candidate_term: u64,
) -> Result<HaServingSession, Box<dyn Error>> {
    let key = authority_key(config)?;
    let foundation = Arc::new(build_foundation(assembled, key));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&assembled.register),
        Arc::clone(&assembled.resolver) as Arc<dyn LogletResolver>,
        Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
        Arc::new(SecureTransitionIdGenerator::new()),
        assembled.node.identity().owner_id,
        RouteHint::new(assembled.advertise.as_str())?,
    );
    let expected = observe_expected_generation(assembled).await?;
    let term = WriterTerm::new(candidate_term)?;
    let (clock, timer) = system_clocks();
    Ok(promote_and_serve(
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
    .await?)
}

async fn write_http(
    mut stream: tokio::net::TcpStream,
    code: u16,
    body: &str,
) -> Result<(), std::io::Error> {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Payload Too Large",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await
}

async fn standby_probe_loop(
    listener: TcpListener,
    session_slot: Arc<RwLock<Option<Arc<HaServingSession>>>>,
    alive: Arc<AtomicBool>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let session_slot = Arc::clone(&session_slot);
        let alive = Arc::clone(&alive);
        tokio::spawn(async move {
            standby_probe_connection(stream, session_slot, alive).await;
        });
    }
}

async fn standby_probe_connection(
    mut stream: tokio::net::TcpStream,
    session_slot: Arc<RwLock<Option<Arc<HaServingSession>>>>,
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

    let session = session_slot.read().await.clone();
    let serving = match &session {
        Some(session) => session.is_effective_writer().await,
        None => false,
    };
    let standby = session.is_none();
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
                (503, "not-ready disposition=standby\n".to_owned())
            }
        }
        _ => {
            let body = status_body(
                if standby {
                    "standby"
                } else {
                    "serving-authority"
                },
                serving,
                standby,
                serving,
            );
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

async fn run_ha_ingress(
    config: ScriptureConfig,
    session: HaServingSession,
    _admin_bind: Option<String>,
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

#[cfg(feature = "campaign-faults")]
fn should_retry_promote_after_reply_loss(
    campaign: &Option<crate::campaign_faults::CampaignFaultContext>,
    error: &HaActivationError,
) -> bool {
    let Some(ctx) = campaign.as_ref() else {
        return false;
    };
    // Retry only for an explicitly armed RootCasReplyLost that the harness
    // evidenced as applied — never for an unrelated root/Indeterminate error
    // merely because campaign tracing is enabled.
    if !ctx.root_cas_reply_loss_armed() || !ctx.root_cas_reply_loss_applied() {
        return false;
    }
    matches!(
        error,
        HaActivationError::Coordinator(CoordinatorError::FoundationFailed(
            FoundationTransitionError::Indeterminate(_)
        )) | HaActivationError::Coordinator(CoordinatorError::Root(_))
    )
}

#[cfg(not(feature = "campaign-faults"))]
fn should_retry_promote_after_reply_loss(
    _campaign: &Option<()>,
    _error: &HaActivationError,
) -> bool {
    false
}

#[cfg(test)]
mod admin_reader_tests {
    use super::{read_bounded_http_request, write_http};
    use crate::admin_http::{AdminParseError, MAX_ADMIN_REQUEST_BYTES};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn bounded_reader_rejects_trailing_bytes_in_one_tcp_read() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            read_bounded_http_request(&mut stream, MAX_ADMIN_REQUEST_BYTES).await
        });

        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        let body = r#"{"candidate_term":2}"#;
        let raw = format!(
            "POST /v1/promote HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}junk",
            body.len()
        );
        client.write_all(raw.as_bytes()).await.expect("write");

        assert_eq!(
            server.await.expect("server task"),
            Err(AdminParseError::BadRequest("trailing bytes after body"))
        );
    }

    #[tokio::test]
    async fn error_statuses_have_standard_reason_phrases() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            write_http(stream, 413, "too large\n")
                .await
                .expect("response");
        });
        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        let mut response = String::new();
        client.read_to_string(&mut response).await.expect("read");
        server.await.expect("server task");
        assert!(response.starts_with("HTTP/1.1 413 Payload Too Large\r\n"));
    }
}
