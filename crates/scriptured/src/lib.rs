//! Lab-grade network adapters for Scripture.
//!
//! Three raw-lines paths exist during migration:
//! - [`serve_raw_lines_connection`] — legacy v0 [`JournalHandle`]
//! - [`serve_chunk_raw_lines_connection`] — Phase 1 lab [`ChunkJournalService`]
//! - [`serve_canon_raw_lines_connection`] — Canon-gated admission over [`VerseRuntime`]
//!
//! Fleet-lab composition lives in [`VerseNodeSupervisor`].
//!
//! New durable work targets the Canon-gated path. Lab helpers remain for local
//! composition tests until a separate removal review.

#[cfg(feature = "fleet-lab")]
mod fleet_exercise_config;
mod fleet_lab;
#[cfg(feature = "fleet-lab")]
mod object_store_lab;
mod raw_lines;

pub use fleet_lab::{
    DurableLogletParts, FleetLabResolver, InMemoryPartsFactory, NodeIdentity, PartsFactory,
    PartsFactoryError, SharedMemoryPartsFactory, SupervisorError, VerseControlOutcome,
    VerseNodeSupervisor,
};
pub use raw_lines::{BatchingSnapshot, RawLinesConnectionMetrics, RawLinesConnectionSnapshot};

#[cfg(feature = "fleet-lab")]
pub use fleet_exercise_config::{
    FleetConfigError, StoreCredentials, StoreEndpointConfig, credential_presence, env_file_exists,
    load_env_file, resolve_credentials, resolve_endpoint_config,
};
#[cfg(feature = "fleet-lab")]
pub use object_store_lab::{
    BackendProfile, FLEET_EXERCISE_ROOT_PREFIX, ObjectStoreLabError, ObjectStorePartsFactory,
    connect_rustfs, connect_s3_compat, fleet_exercise_root,
};

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use scripture::{AttributeValue, ProducerId, Record, Submission};
use scripture_service::{CanonRoute, ChunkJournalService, JournalHandle, VerseRuntime};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use raw_lines::{RawLinesSink, serve_raw_lines_pipeline};

/// Fixed configuration for one raw-lines listener.
///
/// Listener configuration is intentionally immutable for the connection. A
/// future schema registry selects a versioned parser before a connection is
/// accepted; it must not change parsing halfway through a byte stream.
#[derive(Debug, Clone, PartialEq)]
pub struct RawLinesConfig {
    /// Largest accepted line in bytes, excluding the terminating newline.
    pub max_line_bytes: usize,
    /// Max admitted-but-unacked records held for ordered emission.
    pub max_pending_records: usize,
    /// Max payload bytes across pending FIFO entries.
    pub max_pending_bytes: usize,
    /// When pending receipts exist and the peer is idle, flush after this delay so
    /// open-chunk N=1 request/ack still progresses without waiting for chunk age.
    /// Bursts faster than this remain co-packed. `None` disables idle flush (cap/EOF/age only).
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

/// Allocates a fresh lab producer identity for one accepted connection.
///
/// Sequences are monotonic within that connection starting at zero. This does
/// not claim reconnect deduplication across connections.
#[must_use]
pub fn allocate_connection_producer() -> ProducerId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(b"rawline\0");
    bytes[8..16].copy_from_slice(&n.to_be_bytes());
    ProducerId::from_bytes(bytes)
}

/// Advances a producer sequence after it has been used in a submission.
///
/// Returns [`None`] when `used == u64::MAX` so the caller can refuse further
/// payloads under the same identity instead of silently replaying.
#[must_use = "callers must fail closed when the sequence space is exhausted"]
pub fn next_producer_sequence(used: u64) -> Option<u64> {
    used.checked_add(1)
}

/// Serves one newline-delimited connection over the legacy v0 journal handle.
pub async fn serve_raw_lines_connection(
    stream: TcpStream,
    journal: JournalHandle,
    config: RawLinesConfig,
) -> io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(reader);
    loop {
        match read_line(&mut reader, config.max_line_bytes).await {
            Ok(Some(line)) => {
                let record = Record {
                    attributes: config.attributes.clone(),
                    payload: Bytes::from(line),
                };
                let acknowledgement = match journal.submit(vec![record]).await {
                    Ok(acknowledgement) => acknowledgement,
                    Err(error) => {
                        write_error(&mut writer, &error.to_string()).await?;
                        return Ok(());
                    }
                };
                match acknowledgement.await {
                    Ok(acknowledgement) => {
                        writer
                            .write_all(
                                format!(
                                    "OK {} {}\n",
                                    acknowledgement.first_offset.get(),
                                    acknowledgement.next_offset.get()
                                )
                                .as_bytes(),
                            )
                            .await?;
                        writer.flush().await?;
                    }
                    Err(error) => {
                        write_error(&mut writer, &error.to_string()).await?;
                        return Ok(());
                    }
                }
            }
            Ok(None) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                write_error(&mut writer, &error.to_string()).await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }
}

/// Serves one newline-delimited connection over [`ChunkJournalService`].
///
/// Each line becomes a one-record [`Submission`]. The connection owns a freshly
/// allocated producer identity at epoch 0. `OK` is written only after a
/// committed receipt, in input order, without flushing once per line.
///
/// This is the **lab** path: it does not observe Canon before accepting work.
/// Prefer [`serve_canon_raw_lines_connection`] for a Scripture node that must
/// refuse non-owners.
pub async fn serve_chunk_raw_lines_connection(
    stream: TcpStream,
    service: Arc<ChunkJournalService>,
    journal_id: scripture::JournalId,
    config: RawLinesConfig,
) -> io::Result<()> {
    serve_chunk_raw_lines_from(stream, service, journal_id, config, 0).await
}

/// Canon-gated raw-lines admission over a started [`VerseRuntime`].
///
/// Performs one fresh [`VerseRuntime::resolve_route`] before accepting any payload:
/// - [`CanonRoute::Serve`] admits through [`VerseRuntime::submit`];
/// - [`CanonRoute::NotOwner`] writes a compact provisional `ERR not-owner …` line;
/// - [`CanonRoute::Fenced`] writes `ERR fenced …` with the newer route when known;
/// - [`CanonRoute::Recovering`] writes `ERR recovering …`;
/// - resolver failure writes `ERR unavailable`.
///
/// These `ERR` forms are an explicitly provisional raw-lines adapter convention,
/// not the future generic text-protocol schema. Compare tokens and owner IDs are
/// never written. The gate is per connection; Holylog's seal fence remains the
/// append guard if Canon changes mid-connection. Standby runtimes (no actor)
/// answer NotOwner/Recovering without constructing an owner.
pub async fn serve_canon_raw_lines_connection(
    stream: TcpStream,
    runtime: Arc<VerseRuntime>,
    config: RawLinesConfig,
) -> io::Result<()> {
    serve_canon_raw_lines_connection_with_metrics(stream, runtime, config, None).await
}

/// Like [`serve_canon_raw_lines_connection`], with optional connection metrics.
pub async fn serve_canon_raw_lines_connection_with_metrics(
    stream: TcpStream,
    runtime: Arc<VerseRuntime>,
    config: RawLinesConfig,
    metrics: Option<Arc<RawLinesConnectionMetrics>>,
) -> io::Result<()> {
    if let Err(reason) = config.validate() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, reason));
    }
    let (reader, mut writer) = stream.into_split();
    match runtime.resolve_route().await {
        Ok(CanonRoute::Serve { .. }) => {
            serve_canon_raw_lines_from_split(reader, writer, runtime, config, 0, metrics).await
        }
        Ok(CanonRoute::NotOwner {
            canon_revision,
            endpoint,
            ..
        }) => {
            write_error(
                &mut writer,
                &format!(
                    "not-owner canon={canon_revision} endpoint={}",
                    endpoint.as_str()
                ),
            )
            .await?;
            Ok(())
        }
        Ok(CanonRoute::Fenced {
            canon_revision,
            endpoint,
            sequencer_epoch,
            ..
        }) => {
            write_error(
                &mut writer,
                &format!(
                    "fenced canon={canon_revision} endpoint={} epoch={sequencer_epoch}",
                    endpoint.as_str()
                ),
            )
            .await?;
            Ok(())
        }
        Ok(CanonRoute::Recovering { canon_revision }) => {
            write_error(&mut writer, &format!("recovering canon={canon_revision}")).await?;
            Ok(())
        }
        Err(_) => {
            write_error(&mut writer, "unavailable").await?;
            Ok(())
        }
    }
}

/// Like [`serve_chunk_raw_lines_connection`], but starts at `initial_sequence`.
///
/// Used by tests to exercise sequence exhaustion without 2^64 requests.
pub async fn serve_chunk_raw_lines_from(
    stream: TcpStream,
    service: Arc<ChunkJournalService>,
    journal_id: scripture::JournalId,
    config: RawLinesConfig,
    sequence: u64,
) -> io::Result<()> {
    if let Err(reason) = config.validate() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, reason));
    }
    let (reader, writer) = stream.into_split();
    serve_chunk_raw_lines_from_split(reader, writer, service, journal_id, config, sequence).await
}

struct ChunkSink {
    service: Arc<ChunkJournalService>,
    journal_id: scripture::JournalId,
}

impl RawLinesSink for ChunkSink {
    async fn submit(&self, submission: Submission) -> Result<scripture::ReceiptFuture, String> {
        self.service
            .submit(self.journal_id, submission)
            .await
            .map_err(|error| error.to_string())
    }

    async fn flush(&self) -> Result<(), String> {
        self.service
            .flush(self.journal_id)
            .await
            .map_err(|error| error.to_string())
    }
}

struct CanonSink {
    runtime: Arc<VerseRuntime>,
}

impl RawLinesSink for CanonSink {
    async fn submit(&self, submission: Submission) -> Result<scripture::ReceiptFuture, String> {
        self.runtime
            .submit(submission)
            .await
            .map_err(|error| error.to_string())
    }

    async fn flush(&self) -> Result<(), String> {
        self.runtime
            .flush()
            .await
            .map_err(|error| error.to_string())
    }
}

async fn serve_chunk_raw_lines_from_split<R, W>(
    reader: R,
    writer: W,
    service: Arc<ChunkJournalService>,
    journal_id: scripture::JournalId,
    config: RawLinesConfig,
    sequence: u64,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_raw_lines_pipeline(
        reader,
        writer,
        ChunkSink {
            service,
            journal_id,
        },
        config,
        sequence,
        None,
    )
    .await
}

async fn serve_canon_raw_lines_from_split<R, W>(
    reader: R,
    writer: W,
    runtime: Arc<VerseRuntime>,
    config: RawLinesConfig,
    sequence: u64,
    metrics: Option<Arc<RawLinesConnectionMetrics>>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_raw_lines_pipeline(
        reader,
        writer,
        CanonSink { runtime },
        config,
        sequence,
        metrics,
    )
    .await
}

async fn write_error<W: AsyncWrite + Unpin>(writer: &mut W, reason: &str) -> io::Result<()> {
    writer
        .write_all(format!("ERR {reason}\n").as_bytes())
        .await?;
    writer.flush().await
}

async fn read_line<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_line_bytes: usize,
) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        match reader.read_exact(&mut byte).await {
            Ok(_) if byte[0] == b'\n' => {
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(Some(line));
            }
            Ok(_) if line.len() == max_line_bytes => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "line exceeds configured byte limit",
                ));
            }
            Ok(_) => line.push(byte[0]),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof && line.is_empty() => {
                return Ok(None);
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "connection ended before newline-delimited record completed",
                ));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use holylog::atomic::AtomicLog;
    use holylog::drive::{DriveError, DriveFuture, LogDrive};
    use holylog::logdrive::{Address, ReferenceLogDrive, TailDescription};
    use holylog::memory::InMemoryLogDrive;
    use scripture::{
        ChunkDriverActor, ChunkLogWriter, ChunkPolicy, CohortId, JournalId, JournalReader,
        JournalWriter, ReadEvent, RecordOffset, RecoveryBound, SystemClock, WriterId,
    };
    use scripture_service::{ChunkJournalService, JournalActor};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::{
        RawLinesConfig, next_producer_sequence, serve_chunk_raw_lines_connection,
        serve_raw_lines_connection,
    };

    fn policy() -> ChunkPolicy {
        ChunkPolicy {
            max_chunk_bytes: 64 * 1024,
            max_record_bytes: 16 * 1024,
            max_chunk_records: 8,
            max_chunk_age: Duration::from_secs(60),
            max_buffered_bytes: 64 * 1024,
            max_inflight_chunks: 1,
            max_uncommitted_age: Duration::from_secs(60),
            recovery_scan: RecoveryBound::new(16).expect("bound"),
        }
    }

    fn cohort() -> CohortId {
        CohortId::from_bytes(*b"raw-cohort!!!!!!")
    }

    fn writer_id() -> WriterId {
        WriterId::from_bytes(*b"raw-writer!!!!!!")
    }

    async fn chunk_service(
        journal_id: JournalId,
        drive: Arc<dyn LogDrive>,
    ) -> Arc<ChunkJournalService> {
        let log = AtomicLog::builder(drive, 0).build().expect("log");
        let writer = ChunkLogWriter::new(journal_id, cohort(), 1, log, RecordOffset::new(0));
        let (clock, timer) = SystemClock::pair();
        let (handle, actor) = ChunkDriverActor::new(
            journal_id,
            cohort(),
            writer_id(),
            1,
            writer,
            &[],
            policy(),
            clock,
            timer,
            16,
        )
        .expect("actor");
        let mut service = ChunkJournalService::new();
        service
            .register_owner(journal_id, 1, handle, actor)
            .expect("register");
        Arc::new(service)
    }

    #[tokio::test]
    async fn raw_lines_is_a_thin_durable_fifo_adapter() {
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let log = AtomicLog::builder(drive, 4).build().expect("build log");
        let journal_id = JournalId::from_bytes(*b"raw-lines-test!!");
        let writer = JournalWriter::new(journal_id, log.clone(), RecordOffset::new(0));
        let (handle, actor) = JournalActor::new(writer, 4);
        let actor = tokio::spawn(actor.run());

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server_handle = handle.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            serve_raw_lines_connection(stream, server_handle, RawLinesConfig::default())
                .await
                .expect("serve")
        });

        let mut client = TcpStream::connect(address).await.expect("connect");
        client.write_all(b"first\r\nsecond\n").await.expect("write");
        client.shutdown().await.expect("finish input");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("read acks");
        assert_eq!(response, b"OK 0 1\nOK 1 2\n");
        server.await.expect("server joins");

        let mut reader = JournalReader::from_start(journal_id, log);
        reader.refresh_tail().await.expect("tail");
        let ReadEvent::Record(first) = reader.read_next().await.expect("first record") else {
            panic!("expected first record");
        };
        let ReadEvent::Record(second) = reader.read_next().await.expect("second record") else {
            panic!("expected second record");
        };
        assert_eq!(first.payload.as_ref(), b"first");
        assert_eq!(second.payload.as_ref(), b"second");

        drop(handle);
        actor.await.expect("actor joins");
    }

    #[tokio::test]
    async fn raw_lines_buffering_handles_split_writes() {
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let log = AtomicLog::builder(drive, 4).build().expect("build log");
        let journal_id = JournalId::from_bytes(*b"split-writes-t!!");
        let writer = JournalWriter::new(journal_id, log.clone(), RecordOffset::new(0));
        let (handle, actor) = JournalActor::new(writer, 4);
        let actor = tokio::spawn(actor.run());

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server_handle = handle.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            serve_raw_lines_connection(stream, server_handle, RawLinesConfig::default())
                .await
                .expect("serve")
        });

        let mut client = TcpStream::connect(address).await.expect("connect");

        for byte in b"hello split world\n" {
            client.write_all(&[*byte]).await.expect("write byte");
            tokio::task::yield_now().await;
        }
        client.shutdown().await.expect("finish input");

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("read acks");
        assert_eq!(response, b"OK 0 1\n");
        server.await.expect("server joins");

        let mut reader = JournalReader::from_start(journal_id, log);
        reader.refresh_tail().await.expect("tail");
        let ReadEvent::Record(first) = reader.read_next().await.expect("first record") else {
            panic!("expected record");
        };
        assert_eq!(first.payload.as_ref(), b"hello split world");

        drop(handle);
        actor.await.expect("actor joins");
    }

    #[tokio::test]
    async fn chunk_raw_lines_is_a_thin_durable_fifo_adapter() {
        let journal_id = JournalId::from_bytes(*b"chunk-raw-lines!");
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let service = chunk_service(journal_id, Arc::clone(&drive)).await;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server_service = Arc::clone(&service);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            serve_chunk_raw_lines_connection(
                stream,
                server_service,
                journal_id,
                RawLinesConfig::default(),
            )
            .await
            .expect("serve")
        });

        let mut client = TcpStream::connect(address).await.expect("connect");
        client.write_all(b"first\r\nsecond\n").await.expect("write");
        client.shutdown().await.expect("finish input");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("read acks");
        assert_eq!(response, b"OK 0 1\nOK 1 2\n");
        server.await.expect("server joins");
    }

    #[tokio::test]
    async fn chunk_raw_lines_buffering_handles_split_writes() {
        let journal_id = JournalId::from_bytes(*b"chunk-split-wr!!");
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let service = chunk_service(journal_id, drive).await;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server_service = Arc::clone(&service);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            serve_chunk_raw_lines_connection(
                stream,
                server_service,
                journal_id,
                RawLinesConfig::default(),
            )
            .await
            .expect("serve")
        });

        let mut client = TcpStream::connect(address).await.expect("connect");
        for byte in b"hello split world\n" {
            client.write_all(&[*byte]).await.expect("write byte");
            tokio::task::yield_now().await;
        }
        client.shutdown().await.expect("finish input");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("read acks");
        assert_eq!(response, b"OK 0 1\n");
        server.await.expect("server joins");
    }

    #[derive(Debug, thiserror::Error)]
    #[error("injected durable-then-error")]
    struct InjectedFailure;

    #[derive(Debug, Default)]
    struct FailAfterWriteDrive {
        model: std::sync::Mutex<ReferenceLogDrive>,
        armed: AtomicBool,
    }

    impl FailAfterWriteDrive {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn arm(&self) {
            self.armed.store(true, Ordering::Release);
        }
    }

    impl LogDrive for FailAfterWriteDrive {
        fn write(&self, address: Address, value: Bytes) -> DriveFuture<'_, ()> {
            Box::pin(async move {
                self.model
                    .lock()
                    .map_err(|_| DriveError::backend(InjectedFailure))?
                    .write(address, value)?;
                if self.armed.load(Ordering::Acquire) {
                    return Err(DriveError::backend(InjectedFailure));
                }
                Ok(())
            })
        }

        fn read(&self, address: Address) -> DriveFuture<'_, Option<Bytes>> {
            Box::pin(async move {
                Ok(self
                    .model
                    .lock()
                    .map_err(|_| DriveError::backend(InjectedFailure))?
                    .read(address)
                    .cloned())
            })
        }

        fn weak_tail(&self, k: u64) -> DriveFuture<'_, TailDescription> {
            Box::pin(async move {
                Ok(self
                    .model
                    .lock()
                    .map_err(|_| DriveError::backend(InjectedFailure))?
                    .weak_tail(k))
            })
        }
    }

    #[tokio::test]
    async fn chunk_raw_lines_poison_returns_err_and_closes() {
        let journal_id = JournalId::from_bytes(*b"chunk-poison-rl!");
        let drive = FailAfterWriteDrive::new();
        drive.arm();
        let service = chunk_service(journal_id, Arc::clone(&drive) as Arc<dyn LogDrive>).await;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server_service = Arc::clone(&service);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            serve_chunk_raw_lines_connection(
                stream,
                server_service,
                journal_id,
                RawLinesConfig::default(),
            )
            .await
            .expect("serve")
        });

        let mut client = TcpStream::connect(address).await.expect("connect");
        client.write_all(b"boom\n").await.expect("write");
        client.shutdown().await.expect("shutdown");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("read");
        let text = String::from_utf8(response).expect("utf8");
        assert!(
            text.starts_with("ERR "),
            "expected ERR response, got {text}"
        );
        server.await.expect("server joins");
    }

    #[test]
    fn next_producer_sequence_fails_closed_at_max() {
        assert_eq!(next_producer_sequence(0), Some(1));
        assert_eq!(next_producer_sequence(u64::MAX - 1), Some(u64::MAX));
        assert_eq!(next_producer_sequence(u64::MAX), None);
    }

    #[test]
    fn exhausted_flag_blocks_further_payloads_after_max() {
        // Models the connection loop: after using u64::MAX, the next payload
        // must fail closed before another submit under the same identity.
        let mut sequence = u64::MAX - 1;
        let mut exhausted = false;
        for _ in 0..2 {
            assert!(!exhausted, "must still accept until MAX is used");
            match next_producer_sequence(sequence) {
                Some(next) => sequence = next,
                None => exhausted = true,
            }
        }
        assert!(exhausted);
        assert!(next_producer_sequence(sequence).is_none());
    }

    mod canon_gated {
        use std::collections::BTreeMap;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        use holylog::atomic::AtomicLog;
        use holylog::memory::InMemoryLogDrive;
        use holylog::virtual_log::{
            ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver,
            ResolveFuture, VirtualLog,
        };
        use scripture::{
            CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, OwnedSequencerBinding,
            OwnerEndpoint, OwnerId, RecoveryBound, SequencerEpoch, SystemClock, VerseId, WriterId,
        };
        use scripture_service::{VerseHandoffRequest, VerseRuntime, VerseRuntimeConfig};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        use super::super::{
            BatchingSnapshot, RawLinesConfig, RawLinesConnectionMetrics,
            serve_canon_raw_lines_connection, serve_canon_raw_lines_connection_with_metrics,
        };

        fn journal() -> JournalId {
            JournalId::from_bytes(*b"canon-raw-jrnl!!")
        }

        fn verse() -> VerseId {
            VerseId::from_bytes(*b"canon-raw-line!!")
        }

        fn owner_a() -> OwnerId {
            OwnerId::from_bytes(*b"canon-raw-own-a!")
        }

        fn owner_b() -> OwnerId {
            OwnerId::from_bytes(*b"canon-raw-own-b!")
        }

        fn config(owner: OwnerId) -> VerseRuntimeConfig {
            VerseRuntimeConfig {
                journal_id: journal(),
                verse_id: verse(),
                owner_id: owner,
                cohort_id: CohortId::from_bytes(*b"canon-raw-cohrt!"),
                writer_id: WriterId::from_bytes(*b"canon-raw-writer"),
                policy: ChunkPolicy {
                    max_chunk_bytes: 64 * 1024,
                    max_record_bytes: 16 * 1024,
                    max_chunk_records: 8,
                    max_chunk_age: Duration::from_secs(60),
                    max_buffered_bytes: 64 * 1024,
                    max_inflight_chunks: 1,
                    max_uncommitted_age: Duration::from_secs(60),
                    recovery_scan: RecoveryBound::new(8).expect("bound"),
                },
                recovery_bound: RecoveryBound::new(8).expect("bound"),
                queue_capacity: 16,
            }
        }

        fn fence(revision: u64, owner: OwnerId) -> CanonFence {
            let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
            CanonFence::new(
                revision,
                journal(),
                verse(),
                CanonOwner::Owned {
                    owner_id: owner,
                    endpoint,
                    sequencer: None,
                },
            )
        }

        #[derive(Default)]
        struct Resolver {
            loglets: Mutex<BTreeMap<LogletId, Arc<AtomicLog>>>,
        }

        impl Resolver {
            fn insert(&self, id: LogletId, log: Arc<AtomicLog>) {
                self.loglets.lock().expect("lock").insert(id, log);
            }
        }

        impl LogletResolver for Resolver {
            fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<Arc<AtomicLog>>> {
                let id = id.clone();
                Box::pin(async move { Ok(self.loglets.lock().expect("lock").get(&id).cloned()) })
            }
        }

        struct Harness {
            register: Arc<dyn ConditionalRegister>,
            resolver: Arc<Resolver>,
            first: LogletId,
        }

        impl Harness {
            fn memory() -> Self {
                let resolver = Arc::new(Resolver::default());
                let first = LogletId::new("canon-raw-first").expect("id");
                resolver.insert(
                    first.clone(),
                    Arc::new(
                        AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                            .build()
                            .expect("log"),
                    ),
                );
                Self {
                    register: Arc::new(InMemoryConditionalRegister::new()),
                    resolver,
                    first,
                }
            }

            fn virtual_log(&self) -> VirtualLog {
                VirtualLog::new(
                    Arc::clone(&self.register),
                    Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
                )
            }
        }

        async fn exchange(runtime: Arc<VerseRuntime>, payload: &[u8]) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let address = listener.local_addr().expect("address");
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                serve_canon_raw_lines_connection(stream, runtime, RawLinesConfig::default())
                    .await
                    .expect("serve")
            });
            let mut client = TcpStream::connect(address).await.expect("connect");
            if !payload.is_empty() {
                client.write_all(payload).await.expect("write");
            }
            client.shutdown().await.expect("shutdown");
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.expect("read");
            server.await.expect("join");
            String::from_utf8(response).expect("utf8")
        }

        #[tokio::test]
        async fn serving_node_writes_committed_ok() {
            let harness = Harness::memory();
            harness
                .virtual_log()
                .bootstrap_with_application_fence(
                    harness.first.clone(),
                    fence(0, owner_a()).encode(),
                )
                .await
                .expect("bootstrap");
            let runtime = VerseRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .expect("start");
            assert!(runtime.is_serving());
            let response = exchange(Arc::new(runtime), b"hello\n").await;
            assert_eq!(response, "OK 0 1\n");
        }

        #[tokio::test]
        async fn pipelined_small_lines_share_one_committed_chunk() {
            let harness = Harness::memory();
            harness
                .virtual_log()
                .bootstrap_with_application_fence(
                    harness.first.clone(),
                    fence(0, owner_a()).encode(),
                )
                .await
                .expect("bootstrap");
            let mut cfg = config(owner_a());
            cfg.policy.max_chunk_records = 8;
            cfg.policy.max_chunk_bytes = 64 * 1024;
            let runtime = Arc::new(
                VerseRuntime::start(
                    cfg,
                    harness.virtual_log(),
                    SystemClock::new(),
                    scripture::SystemTimer::new(),
                )
                .await
                .expect("start"),
            );
            let metrics = Arc::new(RawLinesConnectionMetrics::default());
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let address = listener.local_addr().expect("address");
            let serve_runtime = Arc::clone(&runtime);
            let serve_metrics = Arc::clone(&metrics);
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                let cfg = RawLinesConfig {
                    idle_flush: None,
                    max_pending_records: 16,
                    ..RawLinesConfig::default()
                };
                serve_canon_raw_lines_connection_with_metrics(
                    stream,
                    serve_runtime,
                    cfg,
                    Some(serve_metrics),
                )
                .await
                .expect("serve")
            });

            let mut client = TcpStream::connect(address).await.expect("connect");
            for i in 0..8 {
                client
                    .write_all(format!("line-{i}\n").as_bytes())
                    .await
                    .expect("write");
            }
            client.shutdown().await.expect("shutdown");
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.expect("read");
            server.await.expect("join");
            let text = String::from_utf8(response).expect("utf8");
            let oks: Vec<_> = text
                .lines()
                .filter(|line| line.starts_with("OK "))
                .collect();
            assert_eq!(oks.len(), 8, "expected 8 ordered OKs, got {text}");

            let driver = runtime.driver_metrics().expect("serving metrics");
            let batching = BatchingSnapshot::from_parts(driver, metrics.snapshot());
            assert_eq!(
                batching.committed_chunks, 1,
                "eight small lines must co-pack into one data chunk; got {batching:?}"
            );
            assert_eq!(batching.admitted_records, 8);
            assert!(batching.records_per_chunk >= 7.9);
        }

        #[tokio::test]
        async fn other_owner_returns_exact_not_owner_without_append() {
            let harness = Harness::memory();
            harness
                .virtual_log()
                .bootstrap_with_application_fence(
                    harness.first.clone(),
                    fence(0, owner_a()).encode(),
                )
                .await
                .expect("bootstrap");
            let runtime = VerseRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .expect("start a");
            assert!(runtime.is_serving());
            let second = LogletId::new("canon-raw-second").expect("id");
            harness.resolver.insert(
                second.clone(),
                Arc::new(
                    AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                        .build()
                        .expect("log"),
                ),
            );
            {
                let log = harness.virtual_log();
                let observed = log.observe_membership().await.expect("observe");
                log.reconfigure_from_observation(
                    &observed,
                    second.clone(),
                    fence(1, owner_b()).encode(),
                )
                .await
                .expect("cutover to B");
            }
            let response = exchange(Arc::new(runtime), b"should-not-append\n").await;
            assert_eq!(
                response,
                "ERR not-owner canon=1 endpoint=tcp://owner.local:9000\n"
            );
            let tail = harness.virtual_log().check_tail().await.expect("tail");
            assert_eq!(tail.tail, 0);
            assert_eq!(tail.loglet_id, second);
        }

        #[tokio::test]
        async fn unowned_returns_recovering_without_append() {
            let harness = Harness::memory();
            harness
                .virtual_log()
                .bootstrap_with_application_fence(
                    harness.first.clone(),
                    fence(0, owner_a()).encode(),
                )
                .await
                .expect("bootstrap");
            let runtime = VerseRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .expect("start");
            assert!(runtime.is_serving());
            let second = LogletId::new("canon-raw-unowned").expect("id");
            harness.resolver.insert(
                second.clone(),
                Arc::new(
                    AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                        .build()
                        .expect("log"),
                ),
            );
            {
                let log = harness.virtual_log();
                let observed = log.observe_membership().await.expect("observe");
                log.reconfigure_from_observation(
                    &observed,
                    second.clone(),
                    CanonFence::new(1, journal(), verse(), CanonOwner::Unowned).encode(),
                )
                .await
                .expect("unowned");
            }
            let response = exchange(Arc::new(runtime), b"nope\n").await;
            assert_eq!(response, "ERR recovering canon=1\n");
            let tail = harness.virtual_log().check_tail().await.expect("tail");
            assert_eq!(tail.tail, 0);
            assert_eq!(tail.loglet_id, second);
        }

        #[tokio::test]
        async fn resolver_failure_returns_unavailable() {
            use holylog::virtual_log::{
                RegisterError, RegisterFuture, VersionedState, VirtualLogState,
            };
            use std::sync::atomic::{AtomicBool, Ordering};

            struct FailAfterArm {
                inner: InMemoryConditionalRegister,
                armed: AtomicBool,
            }

            impl ConditionalRegister for FailAfterArm {
                fn read(&self) -> RegisterFuture<'_, Option<VersionedState>> {
                    Box::pin(async {
                        if self.armed.load(Ordering::Acquire) {
                            return Err(RegisterError::backend(std::io::Error::other(
                                "register unavailable",
                            )));
                        }
                        self.inner.read().await
                    })
                }

                fn compare_and_swap(
                    &self,
                    expected: Option<&VersionedState>,
                    new_state: VirtualLogState,
                ) -> RegisterFuture<'_, bool> {
                    self.inner.compare_and_swap(expected, new_state)
                }
            }

            let register = Arc::new(FailAfterArm {
                inner: InMemoryConditionalRegister::new(),
                armed: AtomicBool::new(false),
            });
            let resolver = Arc::new(Resolver::default());
            let first = LogletId::new("canon-raw-fail").expect("id");
            resolver.insert(
                first.clone(),
                Arc::new(
                    AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                        .build()
                        .expect("log"),
                ),
            );
            let log = VirtualLog::new(
                Arc::clone(&register) as Arc<dyn ConditionalRegister>,
                Arc::clone(&resolver) as Arc<dyn LogletResolver>,
            );
            log.bootstrap_with_application_fence(first, fence(0, owner_a()).encode())
                .await
                .expect("bootstrap");
            let runtime = VerseRuntime::start(
                config(owner_a()),
                log,
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .expect("start");
            assert!(runtime.is_serving());
            register.armed.store(true, Ordering::Release);
            let response = exchange(Arc::new(runtime), b"nope\n").await;
            assert_eq!(response, "ERR unavailable\n");
        }

        #[tokio::test]
        async fn standby_runtime_listener_returns_not_owner_without_append() {
            let harness = Harness::memory();
            harness
                .virtual_log()
                .bootstrap_with_application_fence(
                    harness.first.clone(),
                    fence(0, owner_b()).encode(),
                )
                .await
                .expect("bootstrap");
            let runtime = VerseRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .expect("standby");
            assert!(runtime.is_standby());
            let response = exchange(Arc::new(runtime), b"nope\n").await;
            assert_eq!(
                response,
                "ERR not-owner canon=0 endpoint=tcp://owner.local:9000\n"
            );
            assert_eq!(
                harness.virtual_log().check_tail().await.expect("tail").tail,
                0
            );
        }

        #[tokio::test]
        async fn after_handoff_old_runtime_never_accepts_payload() {
            let harness = Harness::memory();
            harness
                .virtual_log()
                .bootstrap_with_application_fence(
                    harness.first.clone(),
                    fence(0, owner_a()).encode(),
                )
                .await
                .expect("bootstrap");
            let runtime = VerseRuntime::start(
                config(owner_a()),
                harness.virtual_log(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
            )
            .await
            .expect("start");
            let second = LogletId::new("canon-raw-handoff").expect("id");
            harness.resolver.insert(
                second.clone(),
                Arc::new(
                    AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                        .build()
                        .expect("log"),
                ),
            );
            let (runtime, outcome) = runtime
                .drain_seal_publish(VerseHandoffRequest {
                    successor: second,
                    next_owner: {
                        let endpoint =
                            OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
                        CanonOwner::Owned {
                            owner_id: owner_b(),
                            endpoint: endpoint.clone(),
                            sequencer: Some(OwnedSequencerBinding {
                                epoch: SequencerEpoch::test(1),
                                sequencer_endpoint: endpoint,
                            }),
                        }
                    },
                    journal_id: journal(),
                    verse_id: verse(),
                })
                .await
                .expect("handoff");
            assert!(matches!(
                outcome,
                scripture_service::CanonTransitionOutcome::Published(_)
            ));
            assert!(runtime.is_terminal());
            let response = exchange(Arc::new(runtime), b"should-fail\n").await;
            assert_eq!(
                response,
                "ERR not-owner canon=1 endpoint=tcp://owner.local:9000\n"
            );
        }
    }
}
