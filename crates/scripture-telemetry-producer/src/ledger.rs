//! Evidence ledger (producer → Canon leg, Phase 2).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::client::AckStatus;

/// Builds a greppable per-Verse promotion message (WP §13).
#[must_use]
pub fn promotion_message(verse: &str, from_endpoint: &str, to_endpoint: &str) -> String {
    format!("Verse `{verse}` promoted {from_endpoint}→{to_endpoint}")
}

/// One send-attempt row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerSendRow {
    /// Discriminator for mixed JSONL ledgers.
    #[serde(default = "send_row_type")]
    pub row_type: String,
    /// Config-pinned producer id.
    pub producer_id: String,
    /// Verse lane.
    pub verse: String,
    /// Restart incarnation (128-bit hex).
    pub incarnation: String,
    /// Sequence within the incarnation.
    pub seq: u64,
    /// Payload digest.
    pub payload_digest: String,
    /// ACK outcome label.
    pub ack_status: String,
    /// Whether this row is still awaiting a successful commit ACK.
    pub unacked: bool,
    /// Endpoint that handled this attempt (`host:port`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

fn send_row_type() -> String {
    "send".to_owned()
}

impl LedgerSendRow {
    /// Builds a row from an ack outcome.
    #[must_use]
    pub fn from_ack(
        producer_id: &str,
        verse: &str,
        incarnation: &str,
        seq: u64,
        payload_digest: &str,
        ack: AckStatus,
        endpoint: &str,
    ) -> Self {
        let (ack_status, unacked) = match ack {
            AckStatus::Committed {
                first_offset,
                next_offset,
            } => (format!("committed:{first_offset}:{next_offset}"), false),
            AckStatus::Denied => ("denied".to_owned(), true),
            AckStatus::Unacked => ("unacked".to_owned(), true),
        };
        Self {
            row_type: send_row_type(),
            producer_id: producer_id.to_owned(),
            verse: verse.to_owned(),
            incarnation: incarnation.to_owned(),
            seq,
            payload_digest: payload_digest.to_owned(),
            ack_status,
            unacked,
            endpoint: Some(endpoint.to_owned()),
        }
    }
}

/// Per-Verse authority / promotion event (WP §13 scope).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerAuthorityRow {
    /// Discriminator (`authority`).
    pub row_type: String,
    /// Verse whose serving endpoint changed.
    pub verse: String,
    /// Previous serving endpoint.
    pub from_endpoint: String,
    /// New serving endpoint.
    pub to_endpoint: String,
    /// Why the producer advanced (`denied`, `unacked_exhausted`, …).
    pub reason: String,
    /// Greppable message: `Verse \`name\` promoted A→B`.
    pub message: String,
}

impl LedgerAuthorityRow {
    /// Builds a promotion row with the required per-Verse message shape.
    #[must_use]
    pub fn verse_promoted(
        verse: &str,
        from_endpoint: &str,
        to_endpoint: &str,
        reason: &str,
    ) -> Self {
        Self {
            row_type: "authority".to_owned(),
            verse: verse.to_owned(),
            from_endpoint: from_endpoint.to_owned(),
            to_endpoint: to_endpoint.to_owned(),
            reason: reason.to_owned(),
            message: promotion_message(verse, from_endpoint, to_endpoint),
        }
    }
}

/// Append-only JSONL send ledger.
#[derive(Debug)]
pub struct SendLedger {
    path: PathBuf,
    file: File,
}

impl SendLedger {
    /// Opens (or creates) a ledger file for append.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Self { path, file })
    }

    /// Appends one send row and flushes.
    pub async fn append(&mut self, row: &LedgerSendRow) -> Result<(), std::io::Error> {
        self.append_json(row).await
    }

    /// Appends one authority / promotion row and flushes.
    pub async fn append_authority(
        &mut self,
        row: &LedgerAuthorityRow,
    ) -> Result<(), std::io::Error> {
        self.append_json(row).await
    }

    async fn append_json<T: Serialize>(&mut self, row: &T) -> Result<(), std::io::Error> {
        let mut line = serde_json::to_string(row).map_err(std::io::Error::other)?;
        line.push('\n');
        self.file.write_all(line.as_bytes()).await?;
        self.file.flush().await?;
        Ok(())
    }

    /// Ledger path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Sink-side committed line for reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkCommitRow {
    /// Logical offset assigned by the lab sink.
    pub offset: u64,
    /// Payload digest.
    pub payload_digest: String,
    /// Raw line (JSON envelope).
    pub line: String,
}
