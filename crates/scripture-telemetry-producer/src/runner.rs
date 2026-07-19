//! Continuous scrape → normalize → send loop (Phase 2).

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use crate::buffer::DropOldestBuffer;
use crate::client::{AckStatus, RawLinesClient};
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
    let mut ledger = SendLedger::open(ledger_path).await?;
    let mut counters = RunnerCounters::default();
    let mut seqs: HashMap<String, SeqAllocator> = HashMap::new();
    let mut buffers: HashMap<String, DropOldestBuffer> = HashMap::new();
    let mut clients: HashMap<String, RawLinesClient> = HashMap::new();

    for endpoint in &config.endpoints {
        buffers.insert(
            endpoint.verse.clone(),
            DropOldestBuffer::new(
                endpoint.verse.clone(),
                config.buffer.max_records_per_verse,
                config.buffer.max_bytes_per_verse,
            ),
        );
        clients.insert(
            endpoint.verse.clone(),
            RawLinesClient::new(
                endpoint.connect.clone(),
                config.connect_timeout,
                ack_timeout,
            ),
        );
        seqs.insert(endpoint.verse.clone(), SeqAllocator::new());
    }

    let mut iterations = 0_u64;
    loop {
        for source in &config.scrape.sources {
            scrape_once(&config, source, &mut seqs, &mut buffers, &mut counters).await;
        }
        drain_buffers(
            &config,
            &mut buffers,
            &mut clients,
            &mut ledger,
            &mut counters,
        )
        .await?;

        iterations += 1;
        if max_iterations.is_some_and(|max| iterations >= max) {
            break;
        }
        tokio::time::sleep(config.scrape.interval).await;
    }
    Ok(counters)
}

async fn scrape_once(
    config: &ProducerConfig,
    source: &ScrapeSource,
    seqs: &mut HashMap<String, SeqAllocator>,
    buffers: &mut HashMap<String, DropOldestBuffer>,
    counters: &mut RunnerCounters,
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
            *counters
                .scrape_errors
                .entry(source.verse.clone())
                .or_default() += 1;
            return;
        }
    };
    let allocator = seqs.entry(source.verse.clone()).or_default();
    match prepare_scrape(config, &source.verse, source.kind, &body, allocator) {
        Ok((prepared, prepare_counters)) => {
            *counters
                .dropped_series
                .entry(source.verse.clone())
                .or_default() += prepare_counters.dropped_series;
            if let Some(buffer) = buffers.get_mut(&source.verse) {
                let dropped = enqueue(buffer, &prepared);
                *counters
                    .dropped_records
                    .entry(source.verse.clone())
                    .or_default() += dropped as u64;
            }
            *counters.scrapes_ok.entry(source.verse.clone()).or_default() += 1;
        }
        Err(error) => {
            eprintln!(
                "scripture-telemetry-producer: prepare_error verse={} err={error}",
                source.verse
            );
            *counters
                .scrape_errors
                .entry(source.verse.clone())
                .or_default() += 1;
        }
    }
}

async fn drain_buffers(
    config: &ProducerConfig,
    buffers: &mut HashMap<String, DropOldestBuffer>,
    clients: &mut HashMap<String, RawLinesClient>,
    ledger: &mut SendLedger,
    counters: &mut RunnerCounters,
) -> Result<(), RunError> {
    for (verse, buffer) in buffers.iter_mut() {
        let Some(client) = clients.get_mut(verse) else {
            continue;
        };
        while let Some(line) = buffer.front().cloned() {
            // Phase 2 single-endpoint policy: Denied and Unacked both reconnect
            // and resend the same line (at-least-once). No multi-endpoint redirect yet.
            let status = client.send_await_ack(&line).await?;
            let row = LedgerSendRow::from_ack(
                &config.producer_id,
                verse,
                line.incarnation,
                line.seq,
                &line.payload_digest,
                status,
            );
            ledger.append(&row).await?;
            match status {
                AckStatus::Committed { .. } => {
                    counters.committed += 1;
                    let _ = buffer.pop_front();
                }
                AckStatus::Denied | AckStatus::Unacked => {
                    counters.unacked_attempts += 1;
                    client.disconnect();
                    if !config.resend_unacked_on_reconnect {
                        let _ = buffer.pop_front();
                    }
                    // Leave at front for resend on next drain / after reconnect.
                    break;
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
    Client(#[from] crate::client::ClientError),
}
