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

use bytes::Bytes;
use scripture::{
    AttributeValue, DriverMetrics, ProducerId, Receipt, ReceiptFuture, Record, Submission,
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::{RawLinesConfig, allocate_connection_producer, next_producer_sequence};

/// Observable counters for one raw-lines connection (lab diagnostics).
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
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    S: RawLinesSink,
{
    let producer_id = allocate_connection_producer();
    let mut reader = BufReader::new(reader);
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut pending_bytes = 0_usize;
    let mut exhausted = false;

    loop {
        // Reserve space for one maximum-sized line before accepting another.
        // A line is read before its exact payload size is known, so merely
        // checking `pending_bytes < max_pending_bytes` could overshoot the
        // advertised byte cap by one complete line.
        let under_cap = pending.len() < config.max_pending_records
            && pending_bytes <= config.max_pending_bytes - config.max_line_bytes;

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
                    let _ = pending.pop_front();
                    pending_bytes = pending_bytes.saturating_sub(payload_bytes);
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
                            )
                            .await
                            {
                                drain_all(
                                    &mut writer,
                                    &mut pending,
                                    &mut pending_bytes,
                                    &metrics,
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
            let _ = pending.pop_front();
            pending_bytes = pending_bytes.saturating_sub(payload_bytes);
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
) -> Result<(), String> {
    if *exhausted {
        return Err("producer sequence space exhausted".into());
    }
    let payload_bytes = payload.len();
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
    let receipt = sink.submit(submission).await?;
    pending.push_back(Pending {
        receipt,
        payload_bytes,
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
) -> io::Result<()> {
    while let Some(mut front) = pending.pop_front() {
        *pending_bytes = pending_bytes.saturating_sub(front.payload_bytes);
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
