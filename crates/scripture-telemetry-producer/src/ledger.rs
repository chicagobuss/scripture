//! Evidence ledger (producer → Canon leg, Phase 2).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::client::AckStatus;

/// Builds a greppable producer-side failover observation (not a promotion claim).
///
/// WP §13 *promotion* with authority scope belongs to the orchestrator / Scribe
/// fence; this string only records that *this client* advanced its connect chain.
#[must_use]
pub fn failover_message(
    verse: &str,
    from_endpoint: &str,
    to_endpoint: &str,
    reason: &str,
) -> String {
    format!("Verse `{verse}` failover {from_endpoint}→{to_endpoint} ({reason})")
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

/// Producer-side failover observation (not Serving Authority / promotion).
///
/// Written when this client advances its configured connect chain after
/// Denied or exhausted Unacked. Correlates with an orchestrator/Scribe
/// promotion event; it does not *witness* one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerFailoverRow {
    /// Discriminator (`failover`).
    pub row_type: String,
    /// Verse whose client connection advanced.
    pub verse: String,
    /// Previous connect target.
    pub from_endpoint: String,
    /// New connect target.
    pub to_endpoint: String,
    /// Why the producer advanced (`denied`, `unacked_exhausted`, …).
    pub reason: String,
    /// Greppable message: `Verse \`name\` failover A→B (denied)`.
    pub message: String,
}

impl LedgerFailoverRow {
    /// Builds a failover observation row.
    #[must_use]
    pub fn verse_failover(
        verse: &str,
        from_endpoint: &str,
        to_endpoint: &str,
        reason: &str,
    ) -> Self {
        Self {
            row_type: "failover".to_owned(),
            verse: verse.to_owned(),
            from_endpoint: from_endpoint.to_owned(),
            to_endpoint: to_endpoint.to_owned(),
            reason: reason.to_owned(),
            message: failover_message(verse, from_endpoint, to_endpoint, reason),
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

    /// Appends one producer-side failover observation and flushes.
    pub async fn append_failover(&mut self, row: &LedgerFailoverRow) -> Result<(), std::io::Error> {
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
