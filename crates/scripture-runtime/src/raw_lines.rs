//! Bounded committed raw-lines FIFO pipeline.
//!
//! Admits newline records into the chunk driver without flushing per line, then
//! emits ordered `OK`/`ERR` acknowledgements after committed receipts resolve.
//! Pending work is capped; EOF requests one flush and drains the FIFO.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::scribe::IngressBudgets;
use bytes::Bytes;
use scripture::{
    AttributeValue, DriverMetrics, ProducerId, Receipt, ReceiptFuture, Record, Submission,
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

/// Fixed configuration for one raw-lines listener.
///
/// This is product-internal temporary ingress for HA testing — not a public
/// producer protocol. Listener configuration is immutable for the connection.
#[derive(Debug, Clone, PartialEq)]
pub struct RawLinesConfig {
    /// Largest accepted line in bytes, excluding the terminating newline.
    pub max_line_bytes: usize,
    /// Max admitted-but-unacked records held for ordered emission.
    pub max_pending_records: usize,
    /// Max payload bytes across pending FIFO entries.
    pub max_pending_bytes: usize,
    /// When pending receipts exist and the peer is idle, flush after this delay.
    pub idle_flush: Option<Duration>,
    /// Static attributes attached to each accepted line.
    pub attributes: BTreeMap<String, AttributeValue>,
}

impl Default for RawLinesConfig {
    fn default() -> Self {
        Self {
            max_line_bytes: 8 * 1024,
            max_pending_records: 32,
            max_pending_bytes: 256 * 1024,
            idle_flush: Some(Duration::from_millis(5)),
            attributes: BTreeMap::new(),
        }
    }
}

impl RawLinesConfig {
    /// Validates pending caps against the line limit.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_line_bytes == 0 {
            return Err("max_line_bytes must be >= 1");
        }
        if self.max_pending_records == 0 {
            return Err("max_pending_records must be >= 1");
        }
        if self.max_pending_bytes == 0 {
            return Err("max_pending_bytes must be >= 1");
        }
        if self.max_pending_bytes < self.max_line_bytes {
            return Err("max_pending_bytes must be >= max_line_bytes");
        }
        Ok(())
    }
}

/// Allocates a fresh producer identity for one accepted connection.
///
/// The temporary raw-lines transport has no client-supplied producer identity.
/// It must therefore never derive an identity from a process-local counter:
/// that counter resets after an HA cutover, which would turn a new connection
/// to the promoted process into an apparent retry of a connection to the dead
/// process. A random identity deliberately gives this temporary transport
/// at-most-once admission per connection rather than inventing retry semantics
/// it cannot faithfully provide.
pub(crate) fn allocate_connection_producer() -> io::Result<ProducerId> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        io::Error::other(format!(
            "cannot allocate raw-lines producer identity: {error}"
        ))
    })?;
    Ok(ProducerId::from_bytes(bytes))
}

/// Advances a producer sequence after it has been used in a submission.
#[must_use = "callers must fail closed when the sequence space is exhausted"]
pub(crate) fn next_producer_sequence(used: u64) -> Option<u64> {
    used.checked_add(1)
}

/// Observable counters for one raw-lines connection (connection diagnostics).
#[derive(Debug, Default)]
pub struct RawLinesConnectionMetrics {
    /// Successfully admitted submissions.
    pub admitted: AtomicU64,
    /// Ordered committed `OK` lines written.
    pub committed_ok: AtomicU64,
    /// Terminal `ERR` lines written.
    pub errors: AtomicU64,
    /// High-water mark of pending FIFO depth.
    pub peak_pending: AtomicU64,
}

impl RawLinesConnectionMetrics {
    /// Snapshot numbers for tests/status.
    #[must_use]
    pub fn snapshot(&self) -> RawLinesConnectionSnapshot {
        RawLinesConnectionSnapshot {
            admitted: self.admitted.load(Ordering::Relaxed),
            committed_ok: self.committed_ok.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            peak_pending: self.peak_pending.load(Ordering::Relaxed),
        }
    }
}

/// Frozen connection metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RawLinesConnectionSnapshot {
    /// Admitted submissions.
    pub admitted: u64,
    /// Ordered committed OKs.
    pub committed_ok: u64,
    /// Terminal ERRs.
    pub errors: u64,
    /// Peak pending FIFO depth.
    pub peak_pending: u64,
}

/// Derived batching view combining driver + connection counters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BatchingSnapshot {
    /// Driver-admitted submissions.
    pub admitted_records: u64,
    /// Driver committed chunk appends.
    pub committed_chunks: u64,
    /// `admitted_records / committed_chunks` when chunks > 0.
    pub records_per_chunk: f64,
    /// Raw-lines connection peak pending depth.
    pub peak_pending: u64,
}

impl BatchingSnapshot {
    /// Combines driver and connection snapshots.
    #[must_use]
    pub fn from_parts(driver: DriverMetrics, connection: RawLinesConnectionSnapshot) -> Self {
        let records_per_chunk = if driver.committed_chunks == 0 {
            0.0
        } else {
            driver.admitted as f64 / driver.committed_chunks as f64
        };
        Self {
            admitted_records: driver.admitted,
            committed_chunks: driver.committed_chunks,
            records_per_chunk,
            peak_pending: connection.peak_pending,
        }
    }
}

struct Pending {
    receipt: ReceiptFuture,
    payload_bytes: usize,
    budget_reserved: bool,
}

/// Admit + flush surface shared by chunk-service and VerseRuntime paths.
pub(crate) trait RawLinesSink {
    async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, String>;
    async fn flush(&self) -> Result<(), String>;
}

/// Runs the bounded FIFO until EOF or a terminal error.
pub(crate) async fn serve_raw_lines_pipeline<R, W, S>(
    reader: R,
    mut writer: W,
    sink: S,
    config: RawLinesConfig,
    mut sequence: u64,
    metrics: Option<Arc<RawLinesConnectionMetrics>>,
    budgets: Option<IngressBudgets>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    S: RawLinesSink,
{
    let producer_id = allocate_connection_producer()?;
    let mut reader = BufReader::new(reader);
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut pending_bytes = 0_usize;
    let mut exhausted = false;

    loop {
        // Reserve space for one maximum-sized line before accepting another.
        // A line is read before its exact payload size is known, so merely
        // checking `pending_bytes < max_pending_bytes` could overshoot the
        // advertised byte cap by one complete line.
        let under_local_cap = pending.len() < config.max_pending_records
            && pending_bytes <= config.max_pending_bytes - config.max_line_bytes;
        let under_budget = budgets
            .as_ref()
            .is_none_or(|budget| budget.can_admit(1, config.max_line_bytes));
        let under_cap = under_local_cap && under_budget;

        if pending.is_empty() {
            match read_capped_line(&mut reader, config.max_line_bytes).await {
                Ok(None) => {
                    if let Err(reason) = sink.flush().await {
                        return fail_closed(&mut writer, &metrics, &reason).await;
                    }
                    return Ok(());
                }
                Ok(Some(payload)) => {
                    if let Err(reason) = admit_line(
                        &sink,
                        &mut pending,
                        &mut pending_bytes,
                        &mut sequence,
                        &mut exhausted,
                        producer_id,
                        &config.attributes,
                        payload,
                        &metrics,
                        &budgets,
                    )
                    .await
                    {
                        return fail_closed(&mut writer, &metrics, &reason).await;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    return fail_closed(&mut writer, &metrics, &error.to_string()).await;
                }
                Err(error) => return Err(error),
            }
            continue;
        }

        if under_cap && !exhausted {
            let head = pending.front_mut().expect("non-empty");
            let idle = config.idle_flush;
            tokio::select! {
                biased;
                result = &mut head.receipt => {
                    let payload_bytes = head.payload_bytes;
                    let budget_reserved = head.budget_reserved;
                    let _ = pending.pop_front();
                    pending_bytes = pending_bytes.saturating_sub(payload_bytes);
                    release_budget(&budgets, budget_reserved, payload_bytes);
                    match result {
                        Ok(receipt) => {
                            write_ok(&mut writer, &receipt).await?;
                            if let Some(metrics) = &metrics {
                                metrics.committed_ok.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(error) => {
                            return fail_closed(&mut writer, &metrics, &error.to_string()).await;
                        }
                    }
                }
                read = read_capped_line(&mut reader, config.max_line_bytes) => {
                    match read {
                        Ok(None) => {
                            if let Err(reason) = sink.flush().await {
                                return fail_closed(&mut writer, &metrics, &reason).await;
                            }
                            return drain_all(
                                &mut writer,
                                &mut pending,
                                &mut pending_bytes,
                                &metrics,
                                &budgets,
                            )
                            .await;
                        }
                        Ok(Some(payload)) => {
                            if let Err(reason) = admit_line(
                                &sink,
                                &mut pending,
                                &mut pending_bytes,
                                &mut sequence,
                                &mut exhausted,
                                producer_id,
                                &config.attributes,
                                payload,
                                &metrics,
                                &budgets,
                            )
                            .await
                            {
                                drain_all(
                                    &mut writer,
                                    &mut pending,
                                    &mut pending_bytes,
                                    &metrics,
                                    &budgets,
                                )
                                .await?;
                                return fail_closed(&mut writer, &metrics, &reason).await;
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                            if let Err(reason) = sink.flush().await {
                                return fail_closed(&mut writer, &metrics, &reason).await;
                            }
                            drain_all(
                                &mut writer,
                                &mut pending,
                                &mut pending_bytes,
                                &metrics,
                                &budgets,
                            )
                            .await?;
                            return fail_closed(&mut writer, &metrics, &error.to_string()).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
                _ = sleep_idle(idle), if idle.is_some() => {
                    if let Err(reason) = sink.flush().await {
                        return fail_closed(&mut writer, &metrics, &reason).await;
                    }
                }
            }
        } else {
            // Cap reached (or exhausted): ask the driver to seal so head receipts can resolve.
            // This is not a per-line flush; it fires once the FIFO refuses more admissions.
            if let Err(reason) = sink.flush().await {
                return fail_closed(&mut writer, &metrics, &reason).await;
            }
            let head = pending.front_mut().expect("non-empty");
            let result = (&mut head.receipt).await;
            let payload_bytes = head.payload_bytes;
            let budget_reserved = head.budget_reserved;
            let _ = pending.pop_front();
            pending_bytes = pending_bytes.saturating_sub(payload_bytes);
            release_budget(&budgets, budget_reserved, payload_bytes);
            match result {
                Ok(receipt) => {
                    write_ok(&mut writer, &receipt).await?;
                    if let Some(metrics) = &metrics {
                        metrics.committed_ok.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(error) => {
                    return fail_closed(&mut writer, &metrics, &error.to_string()).await;
                }
            }
            if exhausted && pending.is_empty() {
                return fail_closed(&mut writer, &metrics, "producer sequence space exhausted")
                    .await;
            }
        }
    }
}

fn release_budget(budgets: &Option<IngressBudgets>, reserved: bool, payload_bytes: usize) {
    if reserved && let Some(budgets) = budgets {
        budgets.release_pending(1, payload_bytes);
    }
}

async fn read_capped_line<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    max_line_bytes: usize,
) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "line exceeds configured byte limit",
                ))
            };
        }
        let byte = buf[0];
        reader.consume(1);
        if byte == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(Some(line));
        }
        if line.len() == max_line_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "line exceeds configured byte limit",
            ));
        }
        line.push(byte);
    }
}

#[allow(clippy::too_many_arguments)]
async fn admit_line<S: RawLinesSink>(
    sink: &S,
    pending: &mut VecDeque<Pending>,
    pending_bytes: &mut usize,
    sequence: &mut u64,
    exhausted: &mut bool,
    producer_id: ProducerId,
    attributes: &BTreeMap<String, AttributeValue>,
    payload: Vec<u8>,
    metrics: &Option<Arc<RawLinesConnectionMetrics>>,
    budgets: &Option<IngressBudgets>,
) -> Result<(), String> {
    if *exhausted {
        return Err("producer sequence space exhausted".into());
    }
    let payload_bytes = payload.len();
    let budget_reserved = if let Some(budgets) = budgets {
        if !budgets.try_reserve_pending(1, payload_bytes) {
            return Err("node/assignment pending budget exhausted".into());
        }
        true
    } else {
        false
    };
    let submission = Submission {
        producer_id,
        producer_epoch: 0,
        sequence: *sequence,
        records: vec![Record {
            attributes: attributes.clone(),
            payload: Bytes::from(payload),
        }],
    };
    match next_producer_sequence(*sequence) {
        Some(next) => *sequence = next,
        None => *exhausted = true,
    }
    let receipt = match sink.submit(submission).await {
        Ok(receipt) => receipt,
        Err(error) => {
            release_budget(budgets, budget_reserved, payload_bytes);
            return Err(error);
        }
    };
    pending.push_back(Pending {
        receipt,
        payload_bytes,
        budget_reserved,
    });
    *pending_bytes = pending_bytes.saturating_add(payload_bytes);
    if let Some(metrics) = metrics {
        metrics.admitted.fetch_add(1, Ordering::Relaxed);
        let depth = pending.len() as u64;
        metrics.peak_pending.fetch_max(depth, Ordering::Relaxed);
    }
    Ok(())
}

async fn drain_all<W: AsyncWrite + Unpin>(
    writer: &mut W,
    pending: &mut VecDeque<Pending>,
    pending_bytes: &mut usize,
    metrics: &Option<Arc<RawLinesConnectionMetrics>>,
    budgets: &Option<IngressBudgets>,
) -> io::Result<()> {
    while let Some(mut front) = pending.pop_front() {
        *pending_bytes = pending_bytes.saturating_sub(front.payload_bytes);
        release_budget(budgets, front.budget_reserved, front.payload_bytes);
        match (&mut front.receipt).await {
            Ok(receipt) => {
                write_ok(writer, &receipt).await?;
                if let Some(metrics) = metrics {
                    metrics.committed_ok.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(error) => return fail_closed(writer, metrics, &error.to_string()).await,
        }
    }
    Ok(())
}

async fn write_ok<W: AsyncWrite + Unpin>(writer: &mut W, receipt: &Receipt) -> io::Result<()> {
    writer
        .write_all(
            format!(
                "OK {} {}\n",
                receipt.first_offset.get(),
                receipt.next_offset.get()
            )
            .as_bytes(),
        )
        .await?;
    writer.flush().await
}

async fn fail_closed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    metrics: &Option<Arc<RawLinesConnectionMetrics>>,
    reason: &str,
) -> io::Result<()> {
    if let Some(metrics) = metrics {
        metrics.errors.fetch_add(1, Ordering::Relaxed);
    }
    writer
        .write_all(format!("ERR {reason}\n").as_bytes())
        .await?;
    writer.flush().await?;
    Ok(())
}

async fn sleep_idle(idle: Option<Duration>) {
    if let Some(duration) = idle {
        tokio::time::sleep(duration).await;
    } else {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use futures::channel::oneshot;
    use scripture::{AckLevel, ChunkId, JournalId, Receipt, ReceiptFuture, RecordOffset};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    use super::{
        RawLinesConfig, RawLinesConnectionMetrics, RawLinesSink, allocate_connection_producer,
        serve_raw_lines_pipeline,
    };

    #[test]
    fn connection_producers_are_fresh() {
        let first = allocate_connection_producer().expect("allocate first producer");
        let second = allocate_connection_producer().expect("allocate second producer");
        assert_ne!(
            first, second,
            "distinct connections must not share dedup identity"
        );
    }

    /// Sink that holds every receipt until `release_all` — models wedged receipts.
    struct WedgedSink {
        held: Mutex<Vec<oneshot::Sender<Result<Receipt, scripture::DriverError>>>>,
        admitted: Mutex<u64>,
        flushes: Mutex<u64>,
    }

    impl WedgedSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                held: Mutex::new(Vec::new()),
                admitted: Mutex::new(0),
                flushes: Mutex::new(0),
            })
        }

        fn admitted(&self) -> u64 {
            *self.admitted.lock().expect("admitted")
        }

        fn held_count(&self) -> usize {
            self.held.lock().expect("held").len()
        }

        fn release_all(&self) {
            let senders = std::mem::take(&mut *self.held.lock().expect("held"));
            for (index, sender) in senders.into_iter().enumerate() {
                let offset = RecordOffset::new(index as u64);
                let next = RecordOffset::new(index as u64 + 1);
                let _ = sender.send(Ok(Receipt {
                    level: AckLevel::Committed,
                    journal_id: JournalId::from_bytes(*b"rawlines-bounds!"),
                    first_offset: offset,
                    next_offset: next,
                    chunk_id: ChunkId::from_bytes(*b"rawlines-chunk!!"),
                    slot: index as u64,
                    canon_revision: 1,
                    deduplicated: false,
                }));
            }
        }
    }

    impl RawLinesSink for Arc<WedgedSink> {
        async fn submit(
            &self,
            _submission: scripture::Submission,
        ) -> Result<ReceiptFuture, String> {
            let (tx, rx) = oneshot::channel();
            self.held.lock().expect("held").push(tx);
            *self.admitted.lock().expect("admitted") += 1;
            Ok(ReceiptFuture::from_receiver(rx))
        }

        async fn flush(&self) -> Result<(), String> {
            *self.flushes.lock().expect("flushes") += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn pending_record_cap_under_wedged_receipts_bounds_admission_and_oks() {
        let sink = WedgedSink::new();
        let metrics = Arc::new(RawLinesConnectionMetrics::default());
        let config = RawLinesConfig {
            max_line_bytes: 64,
            max_pending_records: 2,
            max_pending_bytes: 256,
            idle_flush: None,
            attributes: Default::default(),
        };

        let (mut client, server) = duplex(4096);
        let (reader, writer) = tokio::io::split(server);
        let pipeline = {
            let sink = Arc::clone(&sink);
            let metrics = Arc::clone(&metrics);
            tokio::spawn(async move {
                serve_raw_lines_pipeline(reader, writer, sink, config, 0, Some(metrics), None).await
            })
        };

        // Three lines: two fill the pending cap; the third waits for a head receipt.
        client.write_all(b"one\ntwo\nthree\n").await.expect("write");
        // Give the pipeline time to admit up to the cap while receipts stay wedged.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            sink.admitted(),
            2,
            "must stop admitting at max_pending_records"
        );
        assert_eq!(sink.held_count(), 2);
        let snap = metrics.snapshot();
        assert_eq!(snap.committed_ok, 0, "no OK while receipts are wedged");
        assert!(snap.peak_pending <= 2);

        sink.release_all();
        // After release, the third line can admit; send EOF to drain.
        client.shutdown().await.expect("shutdown write");
        // Unblock any remaining held receipts created after release for line three.
        for _ in 0..10 {
            if sink.held_count() == 0 && pipeline.is_finished() {
                break;
            }
            sink.release_all();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let mut response = String::new();
        let mut reader = tokio::io::BufReader::new(client);
        let _ = tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut response).await;
        let ok_count = response
            .lines()
            .filter(|line| line.starts_with("OK "))
            .count();
        assert!(
            ok_count <= sink.admitted() as usize,
            "committed OK count ({ok_count}) must not exceed admitted ({})",
            sink.admitted()
        );
        assert!(
            ok_count >= 2,
            "at least the capped admissions should commit after release; got {ok_count} from {response:?}"
        );
        let _ = pipeline.await;
    }

    #[tokio::test]
    async fn oversized_line_is_rejected_without_committed_ok() {
        let sink = WedgedSink::new();
        let metrics = Arc::new(RawLinesConnectionMetrics::default());
        let config = RawLinesConfig {
            max_line_bytes: 4,
            max_pending_records: 8,
            max_pending_bytes: 64,
            idle_flush: None,
            attributes: Default::default(),
        };
        let (mut client, server) = duplex(4096);
        let (reader, writer) = tokio::io::split(server);
        let pipeline = {
            let sink = Arc::clone(&sink);
            let metrics = Arc::clone(&metrics);
            tokio::spawn(async move {
                serve_raw_lines_pipeline(reader, writer, sink, config, 0, Some(metrics), None).await
            })
        };
        client.write_all(b"toolong\n").await.expect("write");
        client.shutdown().await.expect("shutdown");
        let mut response = Vec::new();
        let _ = client.read_to_end(&mut response).await;
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.contains("ERR"),
            "oversized line must fail-closed with ERR; got {text:?}"
        );
        assert_eq!(sink.admitted(), 0);
        assert_eq!(metrics.snapshot().committed_ok, 0);
        let _ = pipeline.await;
    }

    #[tokio::test]
    async fn pending_byte_cap_reserves_one_max_line() {
        let sink = WedgedSink::new();
        let metrics = Arc::new(RawLinesConnectionMetrics::default());
        // max_pending_bytes=16, max_line_bytes=8 → admit while pending_bytes <= 8.
        // First 6-byte line leaves room; second fills past the reserve.
        let config = RawLinesConfig {
            max_line_bytes: 8,
            max_pending_records: 32,
            max_pending_bytes: 16,
            idle_flush: None,
            attributes: Default::default(),
        };
        let (mut client, server) = duplex(4096);
        let (reader, writer) = tokio::io::split(server);
        let pipeline = {
            let sink = Arc::clone(&sink);
            let metrics = Arc::clone(&metrics);
            tokio::spawn(async move {
                serve_raw_lines_pipeline(reader, writer, sink, config, 0, Some(metrics), None).await
            })
        };
        client
            .write_all(b"abcdef\nghijkl\nmnopqr\n")
            .await
            .expect("write");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            sink.admitted(),
            2,
            "byte cap with max-line reserve must stop after the second line"
        );
        assert_eq!(metrics.snapshot().committed_ok, 0);
        sink.release_all();
        client.shutdown().await.expect("shutdown");
        for _ in 0..10 {
            if pipeline.is_finished() {
                break;
            }
            sink.release_all();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let _ = pipeline.await;
        assert!(sink.admitted() <= 3);
        assert!(metrics.snapshot().committed_ok <= sink.admitted());
    }
}
