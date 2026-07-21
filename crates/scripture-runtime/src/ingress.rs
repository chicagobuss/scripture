//! Product-internal temporary ingress used by HA tests.
//!
//! This is **not** a public producer protocol. Transport-adapter configuration
//! remains deferred; callers may use these helpers only behind the product
//! process surface.

use std::io;
use std::sync::Arc;

use scripture::{DriverError, ProducerWireErrorCode, ReceiptFuture, Submission};
use scripture_service::{CanonRoute, ChunkServiceError, VerseAdmitError, VerseRuntime};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::ha_session::HaServingSession;
use crate::producer_wire::{
    ProducerWireRejection, ProducerWireSink, serve_producer_wire_connection,
};
use crate::raw_lines::{
    RawLinesConfig, RawLinesConnectionMetrics, RawLinesSink, serve_raw_lines_pipeline,
};
use crate::scribe::IngressBudgets;

struct CanonSink {
    runtime: Arc<VerseRuntime>,
}

impl RawLinesSink for CanonSink {
    async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, String> {
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

struct SpoolCanonSink {
    runtime: Arc<VerseRuntime>,
    spool: Arc<scripture::SpoolCellHandle<scripture::FileSpoolStorage>>,
}

impl RawLinesSink for SpoolCanonSink {
    async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, String> {
        let runtime = Arc::clone(&self.runtime);
        let spool_fut = self
            .spool
            .submit_forwarded(submission, move |submission| {
                let runtime = Arc::clone(&runtime);
                async move {
                    runtime
                        .submit(submission)
                        .await
                        .map_err(|error| scripture::SpoolError::Forward(error.to_string()))
                }
            })
            .await
            .map_err(|error| error.to_string())?;
        let (tx, rx) = futures::channel::oneshot::channel();
        tokio::spawn(async move {
            let mapped = match spool_fut.await {
                Ok(receipt) => Ok(receipt),
                Err(scripture::SpoolError::ProgressFailed) => {
                    Err(scripture::DriverError::NotWritten)
                }
                Err(scripture::SpoolError::Poisoned { .. })
                | Err(scripture::SpoolError::RecoveryRequired) => {
                    Err(scripture::DriverError::Poisoned)
                }
                Err(_) => Err(scripture::DriverError::Unavailable),
            };
            let _ = tx.send(mapped);
        });
        Ok(ReceiptFuture::from_receiver(rx))
    }

    async fn flush(&self) -> Result<(), String> {
        self.runtime
            .flush()
            .await
            .map_err(|error| error.to_string())
    }
}

struct HaAuthoritySink {
    session: Arc<HaServingSession>,
}

/// Producer Wire preserves structured refusal classes instead of reducing them
/// to the legacy raw-lines text surface.
struct HaProducerWireSink {
    session: Arc<HaServingSession>,
}

fn wire_rejection_from_admission(error: impl std::fmt::Display) -> ProducerWireRejection {
    ProducerWireRejection::new(ProducerWireErrorCode::NotServing, error.to_string())
}

fn wire_rejection_from_driver(error: DriverError) -> ProducerWireRejection {
    let code = match error {
        DriverError::OutOfSequence { .. }
        | DriverError::FencedProducer { .. }
        | DriverError::IdentityConflict { .. } => ProducerWireErrorCode::IdentityConflict,
        DriverError::Indeterminate { .. }
        | DriverError::Uncertain { .. }
        | DriverError::Poisoned
        | DriverError::Policy(_)
        | DriverError::Codec(_)
        | DriverError::Log(_)
        | DriverError::Invariant(_) => ProducerWireErrorCode::Ambiguous,
        DriverError::RecordTooLarge { .. }
        | DriverError::SubmissionTooLarge { .. }
        | DriverError::EmptySubmission
        | DriverError::BlobSinkBufferFull => ProducerWireErrorCode::Backpressure,
        DriverError::NotWritten | DriverError::Unavailable => ProducerWireErrorCode::NotServing,
    };
    ProducerWireRejection::new(code, error.to_string())
}

fn wire_rejection_from_verse(error: VerseAdmitError) -> ProducerWireRejection {
    match error {
        VerseAdmitError::Unavailable(error) => wire_rejection_from_admission(error),
        VerseAdmitError::Service(ChunkServiceError::Driver(error)) => {
            wire_rejection_from_driver(error)
        }
        VerseAdmitError::Service(error) => wire_rejection_from_admission(error),
    }
}

impl ProducerWireSink for HaProducerWireSink {
    async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, ProducerWireRejection> {
        self.session
            .submit(submission)
            .await
            .map_err(|error| match error {
                crate::HaAdmissionError::GateDenied { .. } => wire_rejection_from_admission(error),
                crate::HaAdmissionError::Runtime(error) => wire_rejection_from_verse(error),
            })
    }

    async fn flush(&self) -> Result<(), ProducerWireRejection> {
        self.session
            .flush()
            .await
            .map_err(wire_rejection_from_admission)
    }
}

impl RawLinesSink for HaAuthoritySink {
    async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, String> {
        self.session
            .submit(submission)
            .await
            .map_err(|error| error.to_string())
    }

    async fn flush(&self) -> Result<(), String> {
        self.session
            .flush()
            .await
            .map_err(|error| error.to_string())
    }
}

/// Serving-Authority-gated raw-lines admission over a live [`HaServingSession`].
///
/// Admission and acknowledgement both re-observe authority through the session;
/// this is not ordinary Canon-route ingress.
pub async fn serve_ha_raw_lines_connection(
    stream: TcpStream,
    session: Arc<HaServingSession>,
    config: RawLinesConfig,
) -> io::Result<()> {
    serve_ha_raw_lines_connection_with_budgets(stream, session, config, None).await
}

/// Like [`serve_ha_raw_lines_connection`], with node/assignment pending budgets.
pub async fn serve_ha_raw_lines_connection_with_budgets(
    stream: TcpStream,
    session: Arc<HaServingSession>,
    config: RawLinesConfig,
    budgets: Option<IngressBudgets>,
) -> io::Result<()> {
    if let Err(reason) = config.validate() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, reason));
    }
    let (reader, writer) = stream.into_split();
    serve_raw_lines_pipeline(
        reader,
        writer,
        HaAuthoritySink { session },
        config,
        0,
        None,
        budgets,
    )
    .await
}

/// Serving-Authority-gated experimental Producer Wire v1 admission.
///
/// Producer Wire retains the producer identity, epoch, and sequence supplied
/// by the client. Unlike the legacy raw-lines listener, it can therefore make
/// an exact retry after a connection loss without inventing a new producer
/// identity. The caller must expose it on a distinct listener: protocol
/// selection is deliberately not inferred from arbitrary producer bytes.
pub async fn serve_ha_producer_wire_connection(
    stream: TcpStream,
    session: Arc<HaServingSession>,
) -> io::Result<()> {
    let (reader, writer) = stream.into_split();
    serve_ha_producer_wire_io(reader, writer, session).await
}

/// Transport-generic form of [`serve_ha_producer_wire_connection`].
///
/// Keeping authority admission above the transport lets hermetic tests exercise
/// the real active-generation serving path without opening host sockets.
pub async fn serve_ha_producer_wire_io<R, W>(
    reader: R,
    writer: W,
    session: Arc<HaServingSession>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_producer_wire_connection(reader, writer, HaProducerWireSink { session }).await
}

/// Canon-gated raw-lines admission over a started [`VerseRuntime`].
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
            serve_raw_lines_pipeline(
                reader,
                writer,
                CanonSink { runtime },
                config,
                0,
                metrics,
                None,
            )
            .await
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
            .await
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
            .await
        }
        Ok(CanonRoute::Recovering { canon_revision }) => {
            write_error(&mut writer, &format!("recovering canon={canon_revision}")).await
        }
        Err(_) => write_error(&mut writer, "unavailable").await,
    }
}

/// Canon raw-lines with an optional local spool cell.
pub async fn serve_canon_raw_lines_connection_with_spool(
    stream: TcpStream,
    runtime: Arc<VerseRuntime>,
    spool: Arc<scripture::SpoolCellHandle<scripture::FileSpoolStorage>>,
    config: RawLinesConfig,
) -> io::Result<()> {
    if let Err(reason) = config.validate() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, reason));
    }
    let (reader, mut writer) = stream.into_split();
    match runtime.resolve_route().await {
        Ok(CanonRoute::Serve { .. }) => {
            serve_raw_lines_pipeline(
                reader,
                writer,
                SpoolCanonSink { runtime, spool },
                config,
                0,
                None,
                None,
            )
            .await
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
            .await
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
            .await
        }
        Ok(CanonRoute::Recovering { canon_revision }) => {
            write_error(&mut writer, &format!("recovering canon={canon_revision}")).await
        }
        Err(_) => write_error(&mut writer, "unavailable").await,
    }
}

async fn write_error<W: AsyncWrite + Unpin>(writer: &mut W, reason: &str) -> io::Result<()> {
    writer
        .write_all(format!("ERR {reason}\n").as_bytes())
        .await?;
    writer.flush().await
}
