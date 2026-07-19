//! Continuous scrape → normalize → send loop (Phase 2–3).
//!
//! Scrape and send are decoupled: one send task per Verse drains its buffer
//! independently so a slow or Denied lane cannot stall other Verses' sends or
//! couple retry latency to the scrape interval. Phase 3 adds exponential
//! backoff, ordered failover connects (A→B), a bounded shutdown drain, and
//! per-Verse authority ledger rows.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, watch};

use crate::buffer::DropOldestBuffer;
use crate::client::{AckStatus, ClientError, RawLinesClient};
use crate::config::{ProducerConfig, ScrapeSource};
use crate::envelope::SeqAllocator;
use crate::ledger::{LedgerAuthorityRow, LedgerSendRow, SendLedger};
use crate::pipeline::{enqueue, prepare_scrape};
use crate::scrape::scrape_url;

/// Runtime counters for one producer process.
#[derive(Debug, Default, Clone)]
pub struct RunnerCounters {
    /// Successful scrapes per verse.
    pub scrapes_ok: HashMap<String, u64>,
    /// Scrape transport/parse failures per verse.
    pub scrape_errors: HashMap<String, u64>,
    /// Dropped buffer records per verse.
    pub dropped_records: HashMap<String, u64>,
    /// Dropped series (cardinality) per verse.
    pub dropped_series: HashMap<String, u64>,
    /// Illegal payloads skipped (should be unreachable).
    pub skipped_illegal_payload: u64,
    /// Committed ACKs.
    pub committed: u64,
    /// Unacked / denied attempts (may later commit on resend).
    pub unacked_attempts: u64,
    /// Per-Verse A→B (or further) promotions recorded.
    pub promotions: u64,
    /// Pending records abandoned after drain deadline.
    pub abandoned_on_drain_deadline: u64,
}

/// Optional run bounds (CLI / drills).
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Stop after this many scrape iterations (`None` = forever).
    pub max_iterations: Option<u64>,
    /// Per-send ACK wait.
    pub ack_timeout: Duration,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            max_iterations: None,
            ack_timeout: Duration::from_secs(5),
        }
    }
}

/// Runs until `max_iterations` scrapes complete (or forever if `None`).
pub async fn run_producer(
    config: ProducerConfig,
    ledger_path: &Path,
    options: RunOptions,
) -> Result<RunnerCounters, RunError> {
    config.validate().map_err(RunError::Config)?;
    let ledger = Arc::new(Mutex::new(SendLedger::open(ledger_path).await?));
    let counters = Arc::new(Mutex::new(RunnerCounters::default()));

    // Single construction site for SeqAllocators — one incarnation per verse
    // for the life of this process (never lazily re-minted).
    let mut seqs: HashMap<String, SeqAllocator> = HashMap::new();
    let mut buffers: HashMap<String, Arc<Mutex<DropOldestBuffer>>> = HashMap::new();
    let mut wake: HashMap<String, Arc<Notify>> = HashMap::new();

    for endpoint in &config.endpoints {
        buffers.insert(
            endpoint.verse.clone(),
            Arc::new(Mutex::new(DropOldestBuffer::new(
                endpoint.verse.clone(),
                config.buffer.max_records_per_verse,
                config.buffer.max_bytes_per_verse,
            ))),
        );
        wake.insert(endpoint.verse.clone(), Arc::new(Notify::new()));
        seqs.insert(endpoint.verse.clone(), SeqAllocator::new());
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut send_tasks = Vec::new();
    for endpoint in &config.endpoints {
        let verse = endpoint.verse.clone();
        let Some(buffer) = buffers.get(&verse).cloned() else {
            continue;
        };
        let Some(notify) = wake.get(&verse).cloned() else {
            continue;
        };
        let connect_chain = endpoint.connect_chain();
        let Some(primary) = connect_chain.first().cloned() else {
            continue;
        };
        let ledger = Arc::clone(&ledger);
        let counters = Arc::clone(&counters);
        let producer_id = config.producer_id.clone();
        let resend = config.resend_unacked_on_reconnect;
        let drain_deadline = config.drain_deadline;
        let retry_initial = config.retry_initial_backoff;
        let retry_max = config.retry_max_backoff;
        let mut client = RawLinesClient::new(primary, config.connect_timeout, options.ack_timeout);
        let mut shutdown_rx = shutdown_rx.clone();
        send_tasks.push(tokio::spawn(async move {
            verse_send_loop(
                VerseSendParams {
                    verse,
                    producer_id,
                    resend_unacked_on_reconnect: resend,
                    connect_chain,
                    drain_deadline,
                    retry_initial_backoff: retry_initial,
                    retry_max_backoff: retry_max,
                    buffer,
                    notify,
                    ledger,
                    counters,
                },
                &mut client,
                &mut shutdown_rx,
            )
            .await
        }));
    }

    let mut iterations = 0_u64;
    loop {
        for source in &config.scrape.sources {
            scrape_once(&config, source, &mut seqs, &buffers, &wake, &counters).await;
        }

        iterations += 1;
        if options.max_iterations.is_some_and(|max| iterations >= max) {
            break;
        }
        tokio::time::sleep(config.scrape.interval).await;
    }

    // Stop accepting new work: signal send tasks, wake them, wait for drain.
    let _ = shutdown_tx.send(true);
    for notify in wake.values() {
        notify.notify_waiters();
    }
    for task in send_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(RunError::Join(error.to_string())),
        }
    }

    let final_counters = counters.lock().await.clone();
    Ok(final_counters)
}

async fn scrape_once(
    config: &ProducerConfig,
    source: &ScrapeSource,
    seqs: &mut HashMap<String, SeqAllocator>,
    buffers: &HashMap<String, Arc<Mutex<DropOldestBuffer>>>,
    wake: &HashMap<String, Arc<Notify>>,
    counters: &Arc<Mutex<RunnerCounters>>,
) {
    let body = match scrape_url(
        &source.url,
        config.scrape.max_response_bytes,
        config.scrape.timeout,
    )
    .await
    {
        Ok(body) => body,
        Err(error) => {
            eprintln!(
                "scripture-telemetry-producer: scrape_error verse={} err={error}",
                source.verse
            );
            let mut guard = counters.lock().await;
            *guard.scrape_errors.entry(source.verse.clone()).or_default() += 1;
            return;
        }
    };
    // Structurally impossible after validate + setup — never mint a new allocator.
    let Some(allocator) = seqs.get_mut(&source.verse) else {
        eprintln!(
            "scripture-telemetry-producer: missing allocator for verse={} (skipped)",
            source.verse
        );
        let mut guard = counters.lock().await;
        *guard.scrape_errors.entry(source.verse.clone()).or_default() += 1;
        return;
    };
    match prepare_scrape(config, &source.verse, source.kind, &body, allocator) {
        Ok((prepared, prepare_counters)) => {
            {
                let mut guard = counters.lock().await;
                *guard
                    .dropped_series
                    .entry(source.verse.clone())
                    .or_default() += prepare_counters.dropped_series;
            }
            if let Some(buffer) = buffers.get(&source.verse) {
                let dropped = {
                    let mut buffer = buffer.lock().await;
                    enqueue(&mut buffer, &prepared)
                };
                {
                    let mut guard = counters.lock().await;
                    *guard
                        .dropped_records
                        .entry(source.verse.clone())
                        .or_default() += dropped as u64;
                    *guard.scrapes_ok.entry(source.verse.clone()).or_default() += 1;
                }
                if let Some(notify) = wake.get(&source.verse) {
                    notify.notify_one();
                }
            }
        }
        Err(error) => {
            eprintln!(
                "scripture-telemetry-producer: prepare_error verse={} err={error}",
                source.verse
            );
            let mut guard = counters.lock().await;
            *guard.scrape_errors.entry(source.verse.clone()).or_default() += 1;
        }
    }
}

struct VerseSendParams {
    verse: String,
    producer_id: String,
    resend_unacked_on_reconnect: bool,
    connect_chain: Vec<String>,
    drain_deadline: Duration,
    retry_initial_backoff: Duration,
    retry_max_backoff: Duration,
    buffer: Arc<Mutex<DropOldestBuffer>>,
    notify: Arc<Notify>,
    ledger: Arc<Mutex<SendLedger>>,
    counters: Arc<Mutex<RunnerCounters>>,
}

async fn verse_send_loop(
    params: VerseSendParams,
    client: &mut RawLinesClient,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), RunError> {
    let VerseSendParams {
        verse,
        producer_id,
        resend_unacked_on_reconnect,
        connect_chain,
        drain_deadline,
        retry_initial_backoff,
        retry_max_backoff,
        buffer,
        notify,
        ledger,
        counters,
    } = params;

    let mut endpoint_index = 0_usize;
    let mut backoff = retry_initial_backoff;
    let mut consecutive_unacked = 0_u32;
    let mut empty_idle = Duration::from_millis(25);
    let mut drain_deadline_at: Option<Instant> = None;

    loop {
        if *shutdown_rx.borrow() {
            if drain_deadline_at.is_none() {
                drain_deadline_at = Some(Instant::now() + drain_deadline);
            }
            if drain_deadline_at.is_some_and(|deadline| Instant::now() >= deadline) {
                let pending = buffer.lock().await.len() as u64;
                if pending > 0 {
                    eprintln!(
                        "scripture-telemetry-producer: drain_deadline verse={verse} abandoned={pending}"
                    );
                    counters.lock().await.abandoned_on_drain_deadline += pending;
                }
                break;
            }
        }

        let front = {
            let guard = buffer.lock().await;
            guard.front().cloned()
        };
        let Some(line) = front else {
            if *shutdown_rx.borrow() {
                let empty = buffer.lock().await.is_empty();
                if empty {
                    break;
                }
                continue;
            }
            tokio::select! {
                _ = notify.notified() => {}
                _ = shutdown_rx.changed() => {}
                () = tokio::time::sleep(empty_idle) => {
                    empty_idle = (empty_idle * 2).min(Duration::from_millis(250));
                }
            }
            continue;
        };
        empty_idle = Duration::from_millis(25);

        let status = match client.send_await_ack(&line).await {
            Ok(status) => status,
            Err(ClientError::NewlineInPayload) => {
                eprintln!(
                    "scripture-telemetry-producer: skip illegal newline payload verse={verse} seq={}",
                    line.seq
                );
                let _ = buffer.lock().await.pop_front();
                let mut guard = counters.lock().await;
                guard.skipped_illegal_payload += 1;
                continue;
            }
            Err(error) => return Err(RunError::Client(error)),
        };

        {
            let row = LedgerSendRow::from_ack(
                &producer_id,
                &verse,
                &line.incarnation,
                line.seq,
                &line.payload_digest,
                status,
                client.endpoint(),
            );
            ledger.lock().await.append(&row).await?;
        }

        match status {
            AckStatus::Committed { .. } => {
                let _ = buffer.lock().await.pop_front();
                counters.lock().await.committed += 1;
                backoff = retry_initial_backoff;
                consecutive_unacked = 0;
            }
            AckStatus::Denied | AckStatus::Unacked => {
                counters.lock().await.unacked_attempts += 1;
                client.disconnect();
                if !resend_unacked_on_reconnect {
                    let _ = buffer.lock().await.pop_front();
                    backoff = retry_initial_backoff;
                    consecutive_unacked = 0;
                    continue;
                }

                let promote = match status {
                    AckStatus::Denied => true,
                    AckStatus::Unacked => {
                        consecutive_unacked = consecutive_unacked.saturating_add(1);
                        consecutive_unacked >= 3
                    }
                    AckStatus::Committed { .. } => false,
                };

                if promote && endpoint_index + 1 < connect_chain.len() {
                    let from = connect_chain[endpoint_index].clone();
                    endpoint_index += 1;
                    let to = connect_chain[endpoint_index].clone();
                    let authority = LedgerAuthorityRow::verse_promoted(
                        &verse,
                        &from,
                        &to,
                        match status {
                            AckStatus::Denied => "denied",
                            _ => "unacked_exhausted",
                        },
                    );
                    eprintln!("scripture-telemetry-producer: {}", authority.message);
                    ledger.lock().await.append_authority(&authority).await?;
                    counters.lock().await.promotions += 1;
                    client.retarget(to);
                    consecutive_unacked = 0;
                    backoff = retry_initial_backoff;
                } else {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff.saturating_mul(2)).min(retry_max_backoff);
                }
            }
        }
    }
    Ok(())
}

/// Runner failures.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// Invalid config.
    #[error(transparent)]
    Config(#[from] crate::config::ValidateError),
    /// Ledger / IO.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Raw-lines client.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// Send-task join failure.
    #[error("send task join: {0}")]
    Join(String),
}
