//! `scripture produce-lab` — a load/failover driver for [`RoutingProducer`].
//!
//! A lab instrument, not a product surface. It exists to exercise the routing
//! producer against real Scribes and a real object store, because the routing
//! client's own tests drive a scripted `TcpListener` and therefore cannot
//! observe what latency does to a failover: identical code produced a
//! contiguous offset run on local rustfs and a two-offset acknowledgement gap
//! on Cloudflare R2, purely because slower round trips left more records in
//! flight when the writer died.
//!
//! It reports throughput and latency alongside the correctness evidence, so a
//! run answers "did anything get lost" and "how fast was it" from the same
//! records. Nothing here is optimised, and the numbers should be read as a
//! baseline to improve against rather than as a performance claim.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use scripture_runtime::{OutboundRecord, RetryPolicy, RoutingProducer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::assemble;
use crate::config::ScriptureConfig;

/// One committed record, kept so the run can be audited after the fact.
#[derive(Debug, Clone)]
struct AckSample {
    worker: usize,
    seq: u64,
    first_offset: u64,
    endpoint: String,
    /// Milliseconds from send to committed ACK.
    latency_ms: f64,
    /// Seconds since run start, for windowed throughput.
    at_s: f64,
}

/// Options for one lab run.
pub struct LabOptions {
    pub canon: String,
    pub verse: String,
    pub workers: usize,
    pub per_worker: u64,
    pub payload_bytes: usize,
}

/// Object-store request counts scraped from a Scribe's `/status`.
///
/// Read before and after a run so the report states requests *per record*
/// rather than leaving an operator to diff two snapshots by hand. Throughput
/// without the request count is only half the cost picture: on this path the
/// authority reads dominate and do not amortise with batching, so a run that
/// reports only records/second hides the term that actually bills.
#[derive(Debug, Clone, Copy, Default)]
struct StoreCounters {
    data_puts: u64,
    data_gets: u64,
    authority_puts: u64,
    authority_gets: u64,
}

impl StoreCounters {
    fn total(&self) -> u64 {
        self.data_puts + self.data_gets + self.authority_puts + self.authority_gets
    }

    fn delta(self, before: Self) -> Self {
        Self {
            data_puts: self.data_puts.saturating_sub(before.data_puts),
            data_gets: self.data_gets.saturating_sub(before.data_gets),
            authority_puts: self.authority_puts.saturating_sub(before.authority_puts),
            authority_gets: self.authority_gets.saturating_sub(before.authority_gets),
        }
    }
}

/// Scrapes `/status`; returns `None` when no status bind is configured or the
/// Scribe is unreachable, so a lab run still reports throughput without it.
async fn scrape_counters(status_bind: Option<&String>) -> Option<StoreCounters> {
    let bind = status_bind?;
    let mut stream = tokio::net::TcpStream::connect(bind).await.ok()?;
    stream
        .write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).await.ok()?;

    let field = |key: &str| -> u64 {
        body.split_whitespace()
            .find_map(|token| token.strip_prefix(key)?.parse().ok())
            .unwrap_or(0)
    };
    Some(StoreCounters {
        data_puts: field("store_puts="),
        data_gets: field("store_gets="),
        authority_puts: field("authority_puts="),
        authority_gets: field("authority_gets="),
    })
}

/// Runs concurrent producers and reports throughput plus correctness evidence.
pub async fn produce_lab(
    config: ScriptureConfig,
    options: LabOptions,
) -> Result<(), Box<dyn Error>> {
    let shared = assemble::connect_shared_store(&config)?;
    let prefix = config.store.prefix.clone();

    println!(
        "scripture produce-lab: canon={} verse={} workers={} per_worker={} payload_bytes={}",
        options.canon, options.verse, options.workers, options.per_worker, options.payload_bytes
    );
    println!("scripture produce-lab: this is a lab instrument; nothing here is optimised");

    let before = scrape_counters(config.metrics.status_bind.as_ref()).await;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(options.workers);

    for worker in 0..options.workers {
        let store = Arc::clone(&shared.store);
        let prefix = prefix.clone();
        let canon = options.canon.clone();
        let verse = options.verse.clone();
        let per_worker = options.per_worker;
        let payload_bytes = options.payload_bytes;

        handles.push(tokio::spawn(async move {
            let mut acks: Vec<AckSample> = Vec::with_capacity(per_worker as usize);
            let mut failures: Vec<String> = Vec::new();

            // A generous attempt budget: a failover drill deliberately spends
            // attempts, and giving up early would hide recovery rather than
            // measure it.
            let policy = RetryPolicy {
                max_attempts: 60,
                connect_timeout: Duration::from_secs(3),
                ack_timeout: Duration::from_secs(20),
                transitioning_backoff: Duration::from_millis(20),
                max_transitioning_backoff: Duration::from_millis(500),
                transport_backoff: Duration::from_millis(500),
            };

            let mut producer =
                match RoutingProducer::open(store, prefix, canon, verse, policy).await {
                    Ok(producer) => producer,
                    Err(error) => {
                        failures.push(format!("open: {error}"));
                        return (acks, failures);
                    }
                };

            for seq in 0..per_worker {
                // Payload carries worker/seq so a readback can attribute every
                // record, and is padded to the requested size.
                let mut payload = format!("w{worker:03}-s{seq:08}-").into_bytes();
                payload.resize(payload_bytes.max(payload.len()), b'x');
                let record = OutboundRecord::new(payload);

                let sent = Instant::now();
                match producer.send(&record).await {
                    Ok(ack) => acks.push(AckSample {
                        worker,
                        seq,
                        first_offset: ack.first_offset,
                        endpoint: ack.endpoint.clone(),
                        latency_ms: sent.elapsed().as_secs_f64() * 1000.0,
                        at_s: start.elapsed().as_secs_f64(),
                    }),
                    Err(error) => failures.push(format!("w{worker} seq={seq}: {error}")),
                }
            }
            (acks, failures)
        }));
    }

    let mut acks: Vec<AckSample> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for handle in handles {
        let (worker_acks, worker_failures) = handle.await?;
        acks.extend(worker_acks);
        failures.extend(worker_failures);
    }
    let wall = start.elapsed().as_secs_f64();
    let after = scrape_counters(config.metrics.status_bind.as_ref()).await;

    report(&acks, &failures, &options, wall);
    report_cost(before, after, acks.len());
    Ok(())
}

fn report(acks: &[AckSample], failures: &[String], options: &LabOptions, wall: f64) {
    let requested = options.workers as u64 * options.per_worker;
    println!("\n=== throughput ===");
    println!("wall_seconds={wall:.2}");
    println!(
        "requested={requested} committed={} failed={}",
        acks.len(),
        failures.len()
    );
    if wall > 0.0 {
        let rps = acks.len() as f64 / wall;
        println!(
            "records_per_second={rps:.1}  bytes_per_second={:.0}",
            rps * options.payload_bytes as f64
        );
        println!(
            "per_worker_records_per_second={:.1}",
            rps / options.workers.max(1) as f64
        );
    }

    if !acks.is_empty() {
        let mut lat: Vec<f64> = acks.iter().map(|a| a.latency_ms).collect();
        lat.sort_by(|a, b| a.partial_cmp(b).expect("no NaN latencies"));
        let pct = |p: f64| lat[((lat.len() - 1) as f64 * p).round() as usize];
        println!("\n=== committed-ACK latency (ms) ===");
        println!(
            "min={:.0} p50={:.0} p90={:.0} p99={:.0} max={:.0}",
            lat[0],
            pct(0.50),
            pct(0.90),
            pct(0.99),
            lat[lat.len() - 1]
        );
    }

    // Correctness evidence. Offsets are the log's own opinion of what it
    // accepted, so duplicates or gaps here are stronger than any client count.
    println!("\n=== correctness evidence ===");
    let mut offsets: Vec<u64> = acks.iter().map(|a| a.first_offset).collect();
    offsets.sort_unstable();
    let dupes = offsets.windows(2).filter(|w| w[0] == w[1]).count();
    let gaps: Vec<(u64, u64)> = offsets
        .windows(2)
        .filter(|w| w[1] != w[0] + 1 && w[1] != w[0])
        .map(|w| (w[0], w[1]))
        .collect();
    println!("duplicate_offsets={dupes}");
    if let (Some(lo), Some(hi)) = (offsets.first(), offsets.last()) {
        println!(
            "offset_range={lo}..{hi} contiguous={} gap_count={}",
            gaps.is_empty(),
            gaps.len()
        );
        for gap in gaps.iter().take(5) {
            println!("  gap after {} -> {}", gap.0, gap.1);
        }
        if !gaps.is_empty() {
            println!(
                "  note: a gap in ACKED offsets is not necessarily loss. A record the writer\n\
                 \x20 committed but could not acknowledge before dying leaves its offset\n\
                 \x20 allocated and unacknowledged; the client retries and the payload lands\n\
                 \x20 twice. Confirm against the sealed tail before calling it loss."
            );
        }
    }

    // Endpoint attribution shows whether a failover actually happened.
    let mut endpoints: Vec<(&str, usize)> = Vec::new();
    for ack in acks {
        match endpoints.iter_mut().find(|(e, _)| *e == ack.endpoint) {
            Some((_, count)) => *count += 1,
            None => endpoints.push((&ack.endpoint, 1)),
        }
    }
    println!("\n=== endpoint attribution ===");
    for (endpoint, count) in &endpoints {
        println!("  {endpoint}: {count} committed");
    }
    if endpoints.len() > 1 {
        println!("  (more than one endpoint served this run — a failover occurred)");
    }

    // A write outage is the interesting number in a failover run.
    let mut times: Vec<f64> = acks.iter().map(|a| a.at_s).collect();
    times.sort_by(|a, b| a.partial_cmp(b).expect("no NaN timestamps"));
    let outages: Vec<(f64, f64)> = times
        .windows(2)
        .filter(|w| w[1] - w[0] > 1.0)
        .map(|w| (w[0], w[1] - w[0]))
        .collect();
    if !outages.is_empty() {
        println!("\n=== write outages (>1s with no commit) ===");
        for (at, gap) in outages.iter().take(5) {
            println!("  {gap:.1}s starting at t+{at:.1}s");
        }
    }

    if !failures.is_empty() {
        println!("\n=== failures ({}) ===", failures.len());
        for failure in failures.iter().take(10) {
            println!("  {failure}");
        }
    }

    // Per-worker completeness catches a worker that silently stopped early.
    println!("\n=== per-worker ===");
    for worker in 0..options.workers {
        let count = acks.iter().filter(|a| a.worker == worker).count();
        let max_seq = acks
            .iter()
            .filter(|a| a.worker == worker)
            .map(|a| a.seq)
            .max();
        println!(
            "  worker {worker}: committed={count}/{} highest_seq={max_seq:?}",
            options.per_worker
        );
    }
}

/// Reports object-store requests per committed record, split by path.
fn report_cost(before: Option<StoreCounters>, after: Option<StoreCounters>, committed: usize) {
    let (Some(before), Some(after)) = (before, after) else {
        println!("\n=== object-store cost ===");
        println!("unavailable: no metrics.status_bind configured, or /status unreachable");
        return;
    };
    if committed == 0 {
        return;
    }
    let d = after.delta(before);
    let n = committed as f64;
    println!("\n=== object-store cost (requests per committed record) ===");
    println!(
        "data_put={:.3} data_get={:.3}",
        d.data_puts as f64 / n,
        d.data_gets as f64 / n
    );
    println!(
        "authority_put={:.3} authority_get={:.3}",
        d.authority_puts as f64 / n,
        d.authority_gets as f64 / n
    );
    println!("total_requests_per_record={:.3}", d.total() as f64 / n);
    let authority = (d.authority_puts + d.authority_gets) as f64;
    if d.total() > 0 {
        println!(
            "authority_share={:.0}%  (does not amortise with batching)",
            100.0 * authority / d.total() as f64
        );
    }
    if d.data_puts > 0 {
        println!(
            "records_per_batch={:.2}  (data PUTs are the batch count)",
            n / d.data_puts as f64
        );
    }
}
