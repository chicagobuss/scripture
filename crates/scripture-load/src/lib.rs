//! Bounded concurrent raw-lines producer for the Scripture fleet lab.
//!
//! Streams deterministic newline-terminated records and waits for `OK` /
//! `ERR` acknowledgements. Does not claim throughput targets; it produces a
//! measured baseline against a named endpoint and run ID.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Fixed chunk-policy description reported with load results (server-side).
///
/// The producer does not enforce chunking; naming the policy used by the
/// serving node is required for comparable baselines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedChunkPolicy {
    /// Human-readable label (for example `fleet-lab-64kib`).
    pub name: String,
    /// `max_chunk_bytes` on the serving Verse.
    pub max_chunk_bytes: u64,
    /// `max_chunk_records` on the serving Verse.
    pub max_chunk_records: u64,
    /// `max_inflight_chunks` on the serving Verse.
    pub max_inflight_chunks: u64,
}

impl NamedChunkPolicy {
    /// Default load-facing policy used by the fleet-lab drill docs.
    #[must_use]
    pub fn fleet_lab_default() -> Self {
        Self {
            name: "fleet-lab-64kib-phase-one".to_owned(),
            max_chunk_bytes: 64 * 1024,
            max_chunk_records: 256,
            // Phase-one ChunkDriver requires inflight=1.
            max_inflight_chunks: 1,
        }
    }
}

/// Configuration for one bounded load run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadConfig {
    /// TCP endpoint (`host:port`).
    pub endpoint: String,
    /// Concurrent raw-lines connections.
    pub connections: usize,
    /// Payload body size including metadata prefix (bytes).
    pub record_bytes: usize,
    /// Soft wall-clock duration; the run also stops on [`Self::max_bytes`].
    pub duration: Duration,
    /// Hard ceiling on accepted payload bytes (excluding newlines).
    pub max_bytes: u64,
    /// Optional target rate across all connections (records/sec). `None` = unbounded.
    pub target_records_per_sec: Option<u64>,
    /// Deterministic run identifier embedded in every payload.
    pub run_id: String,
    /// Per-record ACK wait timeout.
    pub ack_timeout: Duration,
    /// Server chunk policy name for the report (not enforced here).
    pub chunk_policy: NamedChunkPolicy,
    /// Backend label for the report (for example `in-memory` or `rustfs`).
    pub backend: String,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            endpoint: "127.0.0.1:9000".to_owned(),
            connections: 4,
            record_bytes: 256,
            duration: Duration::from_secs(5),
            max_bytes: 8 * 1024 * 1024,
            target_records_per_sec: None,
            run_id: "run-local".to_owned(),
            ack_timeout: Duration::from_secs(5),
            chunk_policy: NamedChunkPolicy::fleet_lab_default(),
            backend: "unspecified".to_owned(),
        }
    }
}

/// Aggregate counters and latency samples from one run.
#[derive(Debug, Clone)]
pub struct LoadReport {
    /// Configured run ID.
    pub run_id: String,
    /// Endpoint targeted.
    pub endpoint: String,
    /// Backend label.
    pub backend: String,
    /// Named chunk policy on the server.
    pub chunk_policy: String,
    /// Records that received `OK`.
    pub accepted_records: u64,
    /// Payload bytes for accepted records (excluding newlines).
    pub accepted_bytes: u64,
    /// `ERR` responses.
    pub errors: u64,
    /// Connect / IO / timeout failures.
    pub transport_failures: u64,
    /// ACK latency percentiles in microseconds.
    pub ack_latency_p50_us: u64,
    /// ACK latency p95.
    pub ack_latency_p95_us: u64,
    /// ACK latency p99.
    pub ack_latency_p99_us: u64,
    /// Wall time of the producer run.
    pub elapsed: Duration,
}

impl LoadReport {
    /// Formats a single-line summary suitable for CI logs.
    #[must_use]
    pub fn summary_line(&self) -> String {
        format!(
            "scripture-load run_id={} endpoint={} backend={} chunk_policy={} \
             accepted_records={} accepted_bytes={} errors={} transport_failures={} \
             ack_p50_us={} ack_p95_us={} ack_p99_us={} elapsed_ms={}",
            self.run_id,
            self.endpoint,
            self.backend,
            self.chunk_policy,
            self.accepted_records,
            self.accepted_bytes,
            self.errors,
            self.transport_failures,
            self.ack_latency_p50_us,
            self.ack_latency_p95_us,
            self.ack_latency_p99_us,
            self.elapsed.as_millis()
        )
    }
}

struct SharedBudget {
    max_bytes: u64,
    accepted_bytes: AtomicU64,
    accepted_records: AtomicU64,
    errors: AtomicU64,
    transport_failures: AtomicU64,
    rate: Option<TokenBucket>,
}

struct TokenBucket {
    interval: Duration,
    next: Mutex<Instant>,
}

impl TokenBucket {
    fn new(records_per_sec: u64) -> Self {
        let nanos = 1_000_000_000u64 / records_per_sec.max(1);
        Self {
            interval: Duration::from_nanos(nanos),
            next: Mutex::new(Instant::now()),
        }
    }

    async fn wait(&self) {
        let sleep_for = {
            let mut next = self.next.lock().await;
            let now = Instant::now();
            let wait = if now < *next {
                *next - now
            } else {
                Duration::ZERO
            };
            *next = now.max(*next) + self.interval;
            wait
        };
        if !sleep_for.is_zero() {
            tokio::time::sleep(sleep_for).await;
        }
    }
}

/// Runs the load producer to completion and returns a metrics report.
pub async fn run_load(config: LoadConfig) -> io::Result<LoadReport> {
    if config.connections == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "connections must be >= 1",
        ));
    }
    if config.record_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "record_bytes must be >= 1",
        ));
    }
    if config.run_id.is_empty() || config.run_id.contains('|') || config.run_id.contains('\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "run_id must be non-empty and must not contain '|' or newlines",
        ));
    }

    let started = Instant::now();
    let deadline = started + config.duration;
    let shared = Arc::new(SharedBudget {
        max_bytes: config.max_bytes,
        accepted_bytes: AtomicU64::new(0),
        accepted_records: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        transport_failures: AtomicU64::new(0),
        rate: config.target_records_per_sec.map(TokenBucket::new),
    });
    let latencies = Arc::new(Mutex::new(Vec::<u64>::new()));
    let mut joins = Vec::with_capacity(config.connections);

    for connection_id in 0..config.connections {
        let config = config.clone();
        let shared = Arc::clone(&shared);
        let latencies = Arc::clone(&latencies);
        joins.push(tokio::spawn(async move {
            run_connection(connection_id, config, shared, latencies, deadline).await
        }));
    }

    for join in joins {
        match join.await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => {
                shared.transport_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let mut samples = latencies.lock().await;
    samples.sort_unstable();
    Ok(LoadReport {
        run_id: config.run_id,
        endpoint: config.endpoint,
        backend: config.backend,
        chunk_policy: format!(
            "{}(max_chunk_bytes={},max_chunk_records={},max_inflight_chunks={})",
            config.chunk_policy.name,
            config.chunk_policy.max_chunk_bytes,
            config.chunk_policy.max_chunk_records,
            config.chunk_policy.max_inflight_chunks
        ),
        accepted_records: shared.accepted_records.load(Ordering::Relaxed),
        accepted_bytes: shared.accepted_bytes.load(Ordering::Relaxed),
        errors: shared.errors.load(Ordering::Relaxed),
        transport_failures: shared.transport_failures.load(Ordering::Relaxed),
        ack_latency_p50_us: percentile_us(&samples, 50),
        ack_latency_p95_us: percentile_us(&samples, 95),
        ack_latency_p99_us: percentile_us(&samples, 99),
        elapsed: started.elapsed(),
    })
}

async fn run_connection(
    connection_id: usize,
    config: LoadConfig,
    shared: Arc<SharedBudget>,
    latencies: Arc<Mutex<Vec<u64>>>,
    deadline: Instant,
) -> io::Result<()> {
    let stream = TcpStream::connect(&config.endpoint).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut ack_line = String::new();
    let mut sequence = 0_u64;

    loop {
        if Instant::now() >= deadline {
            break;
        }
        let record = encode_record(&config.run_id, connection_id, sequence, config.record_bytes);
        let record_len = record.len() as u64;
        let prior = shared.accepted_bytes.load(Ordering::Relaxed);
        if prior.saturating_add(record_len) > shared.max_bytes {
            break;
        }
        if let Some(rate) = &shared.rate {
            rate.wait().await;
        }

        let send_started = Instant::now();
        writer.write_all(&record).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        ack_line.clear();
        match timeout(config.ack_timeout, reader.read_line(&mut ack_line)).await {
            Ok(Ok(0)) => {
                shared.transport_failures.fetch_add(1, Ordering::Relaxed);
                break;
            }
            Ok(Ok(_)) => {
                let latency = send_started.elapsed().as_micros() as u64;
                if ack_line.starts_with("OK ") {
                    shared.accepted_records.fetch_add(1, Ordering::Relaxed);
                    shared
                        .accepted_bytes
                        .fetch_add(record_len, Ordering::Relaxed);
                    latencies.lock().await.push(latency);
                    sequence = sequence.saturating_add(1);
                } else if ack_line.starts_with("ERR ") {
                    shared.errors.fetch_add(1, Ordering::Relaxed);
                    break;
                } else {
                    shared.transport_failures.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                shared.transport_failures.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }
    Ok(())
}

fn percentile_us(sorted: &[u64], pct: u8) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((pct as usize) * (sorted.len() - 1)) / 100;
    sorted[rank]
}

/// Builds one deterministic record payload (without trailing newline).
#[must_use]
pub fn encode_record(
    run_id: &str,
    connection_id: usize,
    sequence: u64,
    record_bytes: usize,
) -> Vec<u8> {
    let meta = format!("{run_id}|{connection_id}|{sequence}|");
    let mut out = meta.into_bytes();
    if out.len() >= record_bytes {
        out.truncate(record_bytes);
        return out;
    }
    out.resize(record_bytes, b'x');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_record_is_deterministic_and_sized() {
        let a = encode_record("run-a", 2, 9, 32);
        let b = encode_record("run-a", 2, 9, 32);
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
        assert!(a.starts_with(b"run-a|2|9|"));
    }

    #[test]
    fn percentile_handles_empty() {
        assert_eq!(percentile_us(&[], 99), 0);
        assert_eq!(percentile_us(&[10, 20, 30, 40], 50), 20);
    }
}
