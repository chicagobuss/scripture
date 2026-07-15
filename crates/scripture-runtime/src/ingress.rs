//! Product-internal temporary ingress used by HA tests.
//!
//! This is **not** a public producer protocol. Transport-adapter configuration
//! remains deferred; callers may use these helpers only behind the product
//! process surface.

use std::io;
use std::sync::Arc;

use scripture::{ReceiptFuture, Submission};
use scripture_service::{CanonRoute, VerseRuntime};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::raw_lines::{
    RawLinesConfig, RawLinesConnectionMetrics, RawLinesSink, serve_raw_lines_pipeline,
};

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
            serve_raw_lines_pipeline(reader, writer, CanonSink { runtime }, config, 0, metrics)
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
