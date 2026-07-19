//! Evidence ledger (producer → Canon leg, Phase 2).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::client::AckStatus;

/// One send-attempt row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerSendRow {
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
            producer_id: producer_id.to_owned(),
            verse: verse.to_owned(),
            incarnation: incarnation.to_owned(),
            seq,
            payload_digest: payload_digest.to_owned(),
            ack_status,
            unacked,
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

    /// Appends one row and flushes.
    pub async fn append(&mut self, row: &LedgerSendRow) -> Result<(), std::io::Error> {
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
