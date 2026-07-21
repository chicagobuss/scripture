//! Experimental async client for Scripture Producer Wire v1.
//!
//! The durable boundary is [`scripture::ProducerOutbox`], not a successful TCP
//! write. Every submit is staged and fsynced before this client opens a network
//! connection. A timeout therefore returns [`SubmitOutcome::Ambiguous`] with
//! the exact bytes retained for retry; it never advances producer sequence.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use scripture::{
    MAX_FRAME_BYTES, PendingWireSubmission, ProducerOutbox, ProducerOutboxError,
    ProducerOutboxIdentity, ProducerWireError, ProducerWireErrorCode, ProducerWireFrame,
    decode_producer_wire_frame, encode_producer_wire_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Exact committed result for one native submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedSubmission {
    /// Stable producer epoch.
    pub producer_epoch: u32,
    /// Stable sequence in that epoch.
    pub sequence: u64,
    /// First committed record offset.
    pub first_offset: u64,
    /// Offset after the committed record span.
    pub next_offset: u64,
}

/// An explicit result at the producer boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Scribe returned a matching committed receipt.
    Committed(CommittedSubmission),
    /// Scribe explicitly refused the same durable submission.
    Refused {
        /// Machine-readable Scribe reason.
        code: ProducerWireErrorCode,
        /// Bounded peer diagnostic.
        message: String,
    },
    /// Transport/reply uncertainty. Exact bytes remain in the outbox.
    Ambiguous {
        /// Bounded local diagnostic; not a negative ACK.
        message: String,
    },
}

/// Construction and local durability failures.
#[derive(Debug, thiserror::Error)]
pub enum ProducerClientError {
    /// Outbox could not establish its durable local boundary.
    #[error("producer outbox: {0}")]
    Outbox(#[from] ProducerOutboxError),
    /// Submission could not be encoded before staging.
    #[error("producer-wire: {0}")]
    Wire(#[from] ProducerWireError),
}

/// One producer client bound to one stable producer identity and logical target.
///
/// Routes are mutable (`retarget`), but the outbox target label is not. That
/// makes a HA route change legal while preventing accidental reuse of a
/// producer transcript for another Canon/Verse.
pub struct ProducerWireClient {
    outbox: ProducerOutbox,
    endpoint: String,
    connect_timeout: Duration,
    ack_timeout: Duration,
}

impl ProducerWireClient {
    /// Opens the durable outbox before any network attempt.
    pub fn open(
        outbox_root: impl AsRef<Path>,
        identity: ProducerOutboxIdentity,
        max_outbox_bytes: usize,
        endpoint: impl Into<String>,
        connect_timeout: Duration,
        ack_timeout: Duration,
    ) -> Result<Self, ProducerClientError> {
        Ok(Self {
            outbox: ProducerOutbox::open(outbox_root, identity, max_outbox_bytes)?,
            endpoint: endpoint.into(),
            connect_timeout,
            ack_timeout,
        })
    }

    /// Current mutable Scribe route.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Changes only the Scribe route. Durable identity and target remain fixed.
    pub fn retarget(&mut self, endpoint: impl Into<String>) {
        self.endpoint = endpoint.into();
    }

    /// The durable producer identity/target binding.
    #[must_use]
    pub fn identity(&self) -> &ProducerOutboxIdentity {
        self.outbox.identity()
    }

    /// Stages and then attempts one atomic Wire submission.
    ///
    /// Older pending submissions are always sent first. If one is ambiguous or
    /// refused, this call returns its outcome and does not send a later sequence
    /// around it.
    pub async fn submit(
        &mut self,
        records: Vec<Bytes>,
    ) -> Result<SubmitOutcome, ProducerClientError> {
        let sequence = self.outbox.next_sequence()?;
        let encoded = encode_producer_wire_frame(&ProducerWireFrame::Submit { sequence, records })?;
        self.outbox.stage_submit(&encoded)?;
        self.flush_until(sequence).await
    }

    /// Retries existing pending work in sequence order.
    ///
    /// A successful result means the last pending submission that was flushed;
    /// callers that need a particular sequence should use [`Self::submit`].
    pub async fn flush(&mut self) -> Result<Option<SubmitOutcome>, ProducerClientError> {
        let Some(last) = self
            .outbox
            .pending_submissions()
            .last()
            .map(|entry| entry.sequence)
        else {
            return Ok(None);
        };
        Ok(Some(self.flush_until(last).await?))
    }

    async fn flush_until(
        &mut self,
        wanted_sequence: u64,
    ) -> Result<SubmitOutcome, ProducerClientError> {
        for pending in self.outbox.pending_submissions() {
            match self.forward(&pending).await {
                SubmitOutcome::Committed(receipt) => {
                    self.outbox
                        .mark_committed(receipt.producer_epoch, receipt.sequence)?;
                    if receipt.sequence == wanted_sequence {
                        return Ok(SubmitOutcome::Committed(receipt));
                    }
                }
                outcome => return Ok(outcome),
            }
        }
        // The only way this happens is an internal contradiction between the
        // just-staged sequence and the durable pending transcript. Do not claim
        // success: the outbox remains the evidence to inspect/recover.
        Ok(SubmitOutcome::Ambiguous {
            message: "staged submission disappeared before a matching receipt".into(),
        })
    }

    async fn forward(&self, pending: &PendingWireSubmission) -> SubmitOutcome {
        let result = async {
            let mut stream = timeout(self.connect_timeout, TcpStream::connect(&self.endpoint))
                .await
                .map_err(|_| "connect timeout".to_owned())?
                .map_err(|error| format!("connect: {error}"))?;
            let hello = self
                .outbox
                .hello_frame()
                .map_err(|error| format!("outbox hello: {error}"))?;
            stream
                .write_all(&hello)
                .await
                .map_err(|error| format!("hello write: {error}"))?;
            stream
                .write_all(&pending.encoded_submit)
                .await
                .map_err(|error| format!("submit write: {error}"))?;
            stream
                .flush()
                .await
                .map_err(|error| format!("flush: {error}"))?;
            timeout(self.ack_timeout, read_frame(&mut stream))
                .await
                .map_err(|_| "ACK timeout".to_owned())?
        }
        .await;
        match result {
            Ok(ProducerWireFrame::Ack {
                producer_epoch,
                sequence,
                first_offset,
                next_offset,
            }) if producer_epoch == self.outbox.identity().producer_epoch
                && sequence == pending.sequence
                && first_offset < next_offset =>
            {
                SubmitOutcome::Committed(CommittedSubmission {
                    producer_epoch,
                    sequence,
                    first_offset,
                    next_offset,
                })
            }
            Ok(ProducerWireFrame::Ack { .. }) => SubmitOutcome::Ambiguous {
                message: "Scribe ACK identity/range mismatch".into(),
            },
            Ok(ProducerWireFrame::Error { code, message, .. }) => {
                SubmitOutcome::Refused { code, message }
            }
            Ok(frame) => SubmitOutcome::Ambiguous {
                message: format!("expected Producer Wire ACK/Error, received {frame:?}"),
            },
            Err(message) => SubmitOutcome::Ambiguous { message },
        }
    }
}

async fn read_frame(stream: &mut TcpStream) -> Result<ProducerWireFrame, String> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|error| format!("ACK prefix: {error}"))?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_FRAME_BYTES {
        return Err("peer declared oversized Producer Wire frame".into());
    }
    let mut bytes = vec![0_u8; length + 4];
    bytes[..4].copy_from_slice(&prefix);
    stream
        .read_exact(&mut bytes[4..])
        .await
        .map_err(|error| format!("ACK body: {error}"))?;
    decode_producer_wire_frame(&bytes).map_err(|error| format!("ACK decode: {error}"))
}

#[cfg(test)]
mod tests {
    use scripture::ProducerId;

    use super::{CommittedSubmission, ProducerWireClient, SubmitOutcome};

    #[test]
    fn client_type_keeps_route_separate_from_durable_target_identity() {
        let root =
            std::env::temp_dir().join(format!("scripture-producer-client-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let identity = scripture::ProducerOutboxIdentity {
            producer_id: ProducerId::from_bytes(*b"producerclient01"),
            producer_epoch: 1,
            target: "canon/metrics/verse/node-a".into(),
        };
        let mut client = ProducerWireClient::open(
            &root,
            identity,
            1024 * 1024,
            "scribe-a:9001",
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(1),
        )
        .expect("open outbox");
        client.retarget("scribe-b:9001");
        assert_eq!(client.endpoint(), "scribe-b:9001");
        assert_eq!(client.identity().target, "canon/metrics/verse/node-a");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn committed_outcome_carries_exact_identity_and_range() {
        let outcome = SubmitOutcome::Committed(CommittedSubmission {
            producer_epoch: 3,
            sequence: 7,
            first_offset: 11,
            next_offset: 12,
        });
        assert!(matches!(
            outcome,
            SubmitOutcome::Committed(CommittedSubmission {
                sequence: 7,
                first_offset: 11,
                next_offset: 12,
                ..
            })
        ));
    }
}
