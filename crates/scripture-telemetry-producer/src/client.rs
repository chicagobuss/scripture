//! Raw-lines TCP client with commit-ACK and reconnect resend.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::buffer::BufferedLine;

/// Outcome of one send attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    /// Received `OK first next`.
    Committed {
        /// First logical offset.
        first_offset: u64,
        /// Next logical offset.
        next_offset: u64,
    },
    /// Received `ERR …` or protocol failure.
    Denied,
    /// Timeout / disconnect before ACK.
    Unacked,
}

/// Evidence log row for one send attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendLogEntry {
    /// Config-pinned producer id.
    pub producer_id: String,
    /// Verse lane.
    pub verse: String,
    /// Restart incarnation (128-bit hex).
    pub incarnation: String,
    /// Envelope seq.
    pub seq: u64,
    /// Payload digest.
    pub payload_digest: String,
    /// ACK outcome.
    pub ack_status: AckStatus,
}

/// Errors from the raw-lines client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// TCP / IO failure.
    #[error("raw-lines io: {0}")]
    Io(#[from] std::io::Error),
    /// Connect timed out.
    #[error("connect timeout")]
    ConnectTimeout,
    /// Payload contains a newline (illegal for raw-lines).
    #[error("payload must not contain newlines")]
    NewlineInPayload,
}

/// One TCP session to a raw-lines ingress endpoint.
pub struct RawLinesClient {
    endpoint: String,
    connect_timeout: Duration,
    ack_timeout: Duration,
    stream: Option<Connected>,
}

struct Connected {
    writer: tokio::net::tcp::OwnedWriteHalf,
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
}

impl RawLinesClient {
    /// Creates a disconnected client for `endpoint` (`host:port`).
    #[must_use]
    pub fn new(
        endpoint: impl Into<String>,
        connect_timeout: Duration,
        ack_timeout: Duration,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            connect_timeout,
            ack_timeout,
            stream: None,
        }
    }

    /// Ensures a live TCP connection.
    pub async fn ensure_connected(&mut self) -> Result<(), ClientError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let stream = timeout(self.connect_timeout, TcpStream::connect(&self.endpoint))
            .await
            .map_err(|_| ClientError::ConnectTimeout)?
            .map_err(ClientError::Io)?;
        let (reader, writer) = stream.into_split();
        self.stream = Some(Connected {
            writer,
            reader: BufReader::new(reader),
        });
        Ok(())
    }

    /// Drops the current connection (forces reconnect on next send).
    pub fn disconnect(&mut self) {
        self.stream = None;
    }

    /// Retargets to a new `host:port` and drops any live session.
    pub fn retarget(&mut self, endpoint: impl Into<String>) {
        self.endpoint = endpoint.into();
        self.stream = None;
    }

    /// Current target endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Sends one buffered line and awaits a commit ACK.
    ///
    /// On disconnect/timeout before `OK`, returns [`AckStatus::Unacked`] and
    /// clears the session so the caller can reconnect and resend.
    pub async fn send_await_ack(&mut self, line: &BufferedLine) -> Result<AckStatus, ClientError> {
        if line.line.contains('\n') {
            return Err(ClientError::NewlineInPayload);
        }
        if let Err(error) = self.ensure_connected().await {
            return match error {
                ClientError::ConnectTimeout | ClientError::Io(_) => Ok(AckStatus::Unacked),
                other => Err(other),
            };
        }
        let Some(conn) = self.stream.as_mut() else {
            return Ok(AckStatus::Unacked);
        };
        let payload = format!("{}\n", line.line);
        if conn.writer.write_all(payload.as_bytes()).await.is_err() {
            self.stream = None;
            return Ok(AckStatus::Unacked);
        }
        let mut response = String::new();
        match timeout(self.ack_timeout, conn.reader.read_line(&mut response)).await {
            Ok(Ok(0)) => {
                self.stream = None;
                Ok(AckStatus::Unacked)
            }
            Ok(Ok(_)) => {
                let trimmed = response.trim_end();
                if let Some(ack) = parse_ok(trimmed) {
                    Ok(AckStatus::Committed {
                        first_offset: ack.0,
                        next_offset: ack.1,
                    })
                } else if trimmed.starts_with("ERR ") {
                    self.stream = None;
                    Ok(AckStatus::Denied)
                } else {
                    self.stream = None;
                    Ok(AckStatus::Unacked)
                }
            }
            Ok(Err(_)) | Err(_) => {
                self.stream = None;
                Ok(AckStatus::Unacked)
            }
        }
    }
}

fn parse_ok(line: &str) -> Option<(u64, u64)> {
    let rest = line.strip_prefix("OK ")?;
    let mut parts = rest.split_whitespace();
    let first = parts.next()?.parse().ok()?;
    let next = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((first, next))
}

/// Builds a send-log entry for evidence ledgers.
#[must_use]
pub fn send_log_entry(
    producer_id: &str,
    line: &BufferedLine,
    ack_status: AckStatus,
) -> SendLogEntry {
    SendLogEntry {
        producer_id: producer_id.to_owned(),
        verse: line.verse.clone(),
        incarnation: line.incarnation.clone(),
        seq: line.seq,
        payload_digest: line.payload_digest.clone(),
        ack_status,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ok_line() {
        assert_eq!(parse_ok("OK 0 1"), Some((0, 1)));
        assert_eq!(parse_ok("ERR not-owner"), None);
    }
}
