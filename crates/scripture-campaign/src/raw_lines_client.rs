//! Temporary raw-lines producer client for campaign HA ingress.
//!
//! Speaks the product-internal newline/`OK`/`ERR` temporary ingress used by
//! `serve_ha_raw_lines_connection`. This is not a public producer protocol.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::CampaignError;

/// One committed raw-lines acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawLinesAck {
    /// `first_offset` from the temporary `OK first_offset next_offset` line.
    pub first_offset: u64,
    /// `next_offset` from the temporary `OK` line.
    pub next_offset: u64,
    /// Payload bytes that were acknowledged (excluding newline).
    pub payload: String,
}

/// Sends newline-terminated payloads and collects ordered `OK` acknowledgements.
pub(crate) async fn exchange_committed(
    endpoint: &str,
    payloads: &[&str],
) -> Result<Vec<RawLinesAck>, CampaignError> {
    let stream = timeout(Duration::from_secs(10), TcpStream::connect(endpoint))
        .await
        .map_err(|_| CampaignError::Scenario(format!("connect timeout to {endpoint}")))?
        .map_err(|error| CampaignError::Scenario(format!("connect {endpoint}: {error}")))?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    for payload in payloads {
        if payload.contains('\n') {
            return Err(CampaignError::Scenario(
                "raw-lines payload must not contain newlines".into(),
            ));
        }
        let line = format!("{payload}\n");
        writer
            .write_all(line.as_bytes())
            .await
            .map_err(|error| CampaignError::Scenario(format!("write: {error}")))?;
    }
    writer
        .shutdown()
        .await
        .map_err(|error| CampaignError::Scenario(format!("shutdown: {error}")))?;

    let mut acks = Vec::with_capacity(payloads.len());
    let mut line = String::new();
    for payload in payloads {
        line.clear();
        let read = timeout(Duration::from_secs(30), reader.read_line(&mut line))
            .await
            .map_err(|_| CampaignError::Scenario("ACK read timeout".into()))?
            .map_err(|error| CampaignError::Scenario(format!("ACK read: {error}")))?;
        if read == 0 {
            return Err(CampaignError::Scenario(
                "connection closed before committed ACK".into(),
            ));
        }
        let trimmed = line.trim_end();
        let ack = parse_ok(trimmed)?;
        acks.push(RawLinesAck {
            first_offset: ack.0,
            next_offset: ack.1,
            payload: (*payload).to_owned(),
        });
    }
    Ok(acks)
}

/// Attempts a single line exchange; used to prove stale A cannot ACK.
#[allow(dead_code)]
pub(crate) async fn expect_connect_failure(endpoint: &str) -> Result<(), CampaignError> {
    match timeout(Duration::from_secs(3), TcpStream::connect(endpoint)).await {
        Ok(Ok(_stream)) => Err(CampaignError::Scenario(format!(
            "stale endpoint {endpoint} still accepted a connection"
        ))),
        Ok(Err(_)) | Err(_) => Ok(()),
    }
}

/// Sends one payload and asserts no committed `OK` arrives within `limit`.
///
/// Connection refusal / timeout counts as success: the peer is not serving
/// committed acknowledgements.
pub(crate) async fn expect_no_committed_ok(
    endpoint: &str,
    payload: &str,
    limit: Duration,
) -> Result<(), CampaignError> {
    if payload.contains('\n') {
        return Err(CampaignError::Scenario(
            "raw-lines payload must not contain newlines".into(),
        ));
    }
    let stream = match timeout(Duration::from_secs(10), TcpStream::connect(endpoint)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(_)) | Err(_) => return Ok(()),
    };
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let line = format!("{payload}\n");
    if writer.write_all(line.as_bytes()).await.is_err() {
        return Ok(());
    }
    let mut response = String::new();
    match timeout(limit, reader.read_line(&mut response)).await {
        Ok(Ok(0)) => Ok(()),
        Ok(Ok(_)) => {
            let trimmed = response.trim_end();
            if trimmed.starts_with("OK ") {
                Err(CampaignError::Scenario(format!(
                    "unexpected committed OK for ambiguity payload {payload:?}: {trimmed}"
                )))
            } else {
                // ERR / protocol noise is acceptable as long as it is not OK.
                Ok(())
            }
        }
        Ok(Err(_)) | Err(_) => Ok(()),
    }
}

fn parse_ok(line: &str) -> Result<(u64, u64), CampaignError> {
    let mut parts = line.split_whitespace();
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("OK"), Some(offset), Some(size), None) => {
            let offset = offset
                .parse::<u64>()
                .map_err(|error| CampaignError::Scenario(format!("OK offset: {error}")))?;
            let size = size
                .parse::<u64>()
                .map_err(|error| CampaignError::Scenario(format!("OK size: {error}")))?;
            Ok((offset, size))
        }
        (Some("ERR"), ..) => Err(CampaignError::Scenario(format!(
            "raw-lines denial (not a committed ACK): {line}"
        ))),
        _ => Err(CampaignError::Scenario(format!(
            "unexpected raw-lines response: {line}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_ok;

    #[test]
    fn parses_ok_line() {
        assert_eq!(parse_ok("OK 2 5").expect("parse OK"), (2, 5));
    }

    #[test]
    fn rejects_err_line() {
        assert!(parse_ok("ERR denied").is_err());
    }
}
