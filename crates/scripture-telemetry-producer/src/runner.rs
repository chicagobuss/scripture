//! Continuous scrape → normalize → send loop (Phase 2+).
//!
//! Scrape and send are decoupled: one send task per Verse drains its buffer
//! independently so a slow or Denied lane cannot stall other Verses' sends or
//! couple retry latency to the scrape interval.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Notify, watch};

use crate::buffer::DropOldestBuffer;
use crate::client::{AckStatus, ClientError, RawLinesClient};
use crate::config::{ProducerConfig, ScrapeSource};
use crate::envelope::SeqAllocator;
use crate::ledger::{LedgerSendRow, SendLedger};
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
}

/// Runs until `max_iterations` scrapes complete (or forever if `None`).
pub async fn run_producer(
    config: ProducerConfig,
    ledger_path: &Path,
    max_iterations: Option<u64>,
    ack_timeout: Duration,
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
        let ledger = Arc::clone(&ledger);
        let counters = Arc::clone(&counters);
        let producer_id = config.producer_id.clone();
        let resend = config.resend_unacked_on_reconnect;
        let mut client = RawLinesClient::new(
            endpoint.connect.clone(),
            config.connect_timeout,
            ack_timeout,
        );
        let mut shutdown_rx = shutdown_rx.clone();
        send_tasks.push(tokio::spawn(async move {
            verse_send_loop(
                VerseSendParams {
                    verse,
                    producer_id,
                    resend_unacked_on_reconnect: resend,
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
        if max_iterations.is_some_and(|max| iterations >= max) {
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
        buffer,
        notify,
        ledger,
        counters,
    } = params;
    let mut empty_idle = Duration::from_millis(25);
    loop {
        let front = {
            let guard = buffer.lock().await;
            guard.front().cloned()
        };
        let Some(line) = front else {
            if *shutdown_rx.borrow() {
                // Final drain check under lock.
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
            );
            ledger.lock().await.append(&row).await?;
        }

        match status {
            AckStatus::Committed { .. } => {
                let _ = buffer.lock().await.pop_front();
                counters.lock().await.committed += 1;
            }
            AckStatus::Denied | AckStatus::Unacked => {
                counters.lock().await.unacked_attempts += 1;
                client.disconnect();
                if !resend_unacked_on_reconnect {
                    let _ = buffer.lock().await.pop_front();
                }
                // Retry this Verse immediately (do not wait for scrape interval).
                tokio::time::sleep(Duration::from_millis(50)).await;
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
