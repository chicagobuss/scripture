//! Transport-neutral submission primitives for a single Scripture journal.
//!
//! A [`JournalActor`] owns the sole v0 [`scripture::JournalWriter`]. Network
//! listeners submit records through a bounded [`JournalHandle`] and may only
//! report success once the returned [`AckFuture`] resolves. This is deliberately
//! a single-process, lab-grade service: it has no writer fencing, failover, or
//! cross-process restart guarantee.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use scripture::{AppendAck, CodecError, JournalWriter, Record, WriteError};
use tokio::sync::{mpsc, oneshot};

/// Errors exposed by the service submission boundary.
///
/// `Unavailable` is intentionally coarse. A kernel failure can leave a zombie
/// write durable while making the actor unable to assign another safe range;
/// callers receive no false acknowledgement and must recover at a later,
/// explicitly fenced generation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServiceError {
    /// A request did not name any records.
    #[error("cannot submit an empty record batch")]
    EmptyBatch,
    /// The submitted record cannot be represented by the durable format.
    #[error("invalid record submission")]
    InvalidRequest,
    /// The bounded submission queue is closed or the actor has terminally
    /// failed. A prior failed append may still be visible after recovery.
    #[error("journal service is unavailable")]
    Unavailable,
    /// The log is sealed. The named slot is informational only.
    #[error("journal is sealed after durable write at slot {slot}")]
    Sealed { slot: u64 },
}

/// Future returned by [`JournalHandle::submit`].
///
/// It resolves only after the containing batch is durably acknowledged, or
/// with a terminal service error. Dropping it never cancels the durable work.
#[must_use = "durability is learned by awaiting the acknowledgement"]
pub struct AckFuture {
    receiver: oneshot::Receiver<Result<AppendAck, ServiceError>>,
}

impl Future for AckFuture {
    type Output = Result<AppendAck, ServiceError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(context)
            .map(|result| result.unwrap_or(Err(ServiceError::Unavailable)))
    }
}

struct Submission {
    records: Vec<Record>,
    acknowledgement: oneshot::Sender<Result<AppendAck, ServiceError>>,
}

/// Cloneable bounded submission endpoint for one journal.
#[derive(Clone)]
pub struct JournalHandle {
    sender: mpsc::Sender<Submission>,
}

impl JournalHandle {
    /// Stages a non-empty record batch for durable append.
    ///
    /// Waiting to enqueue is the service's first backpressure mechanism. This
    /// v1 slice intentionally emits one durable batch per submission; batching
    /// policy will be introduced behind this boundary rather than in a wire
    /// protocol.
    pub async fn submit(&self, records: Vec<Record>) -> Result<AckFuture, ServiceError> {
        if records.is_empty() {
            return Err(ServiceError::EmptyBatch);
        }
        let (acknowledgement, receiver) = oneshot::channel();
        self.sender
            .send(Submission {
                records,
                acknowledgement,
            })
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        Ok(AckFuture { receiver })
    }
}

/// The single task that owns a v0 `JournalWriter`.
///
/// Run this future exactly once. On the first kernel failure it enters a
/// terminal state and resolves the failed request and every later submission
/// as unavailable (or sealed), so no client acknowledgement is left pending.
pub struct JournalActor {
    writer: JournalWriter,
    receiver: mpsc::Receiver<Submission>,
}

impl JournalActor {
    /// Creates a bounded actor and its cloneable client endpoint.
    #[must_use]
    pub fn new(writer: JournalWriter, queue_capacity: usize) -> (JournalHandle, Self) {
        let (sender, receiver) = mpsc::channel(queue_capacity);
        (JournalHandle { sender }, Self { writer, receiver })
    }

    /// Drives submissions until every handle is dropped.
    ///
    /// The actor deliberately does not attempt to restart its writer. The
    /// recovery helper's same-process preconditions are not a daemon recovery
    /// protocol; a future VirtualLog/fencing layer owns that transition.
    pub async fn run(mut self) {
        let mut terminal: Option<ServiceError> = None;
        while let Some(submission) = self.receiver.recv().await {
            if let Some(error) = &terminal {
                let _ = submission.acknowledgement.send(Err(error.clone()));
                continue;
            }
            match self.writer.append_batch(submission.records).await {
                Ok(acknowledgement) => {
                    let _ = submission.acknowledgement.send(Ok(acknowledgement));
                }
                Err(WriteError::Log(holylog::atomic::AtomicLogError::Sealed { address })) => {
                    let error = ServiceError::Sealed { slot: address };
                    terminal = Some(error.clone());
                    let _ = submission.acknowledgement.send(Err(error));
                }
                Err(WriteError::Log(_)) | Err(WriteError::Codec(CodecError::OffsetOverflow)) => {
                    terminal = Some(ServiceError::Unavailable);
                    let _ = submission
                        .acknowledgement
                        .send(Err(ServiceError::Unavailable));
                }
                Err(WriteError::EmptyBatch)
                | Err(WriteError::TooManyRecords)
                | Err(WriteError::Codec(_))
                | Err(WriteError::JournalMismatch { .. }) => {
                    let _ = submission
                        .acknowledgement
                        .send(Err(ServiceError::InvalidRequest));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use holylog::atomic::AtomicLog;
    use holylog::drive::LogDrive;
    use holylog::memory::InMemoryLogDrive;
    use scripture::{AttributeValue, JournalId, JournalWriter, Record, RecordOffset};

    use super::{JournalActor, ServiceError};

    fn writer() -> JournalWriter {
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let log = AtomicLog::builder(drive, 4).build().expect("build log");
        JournalWriter::new(
            JournalId::from_bytes(*b"service-test!!!!"),
            log,
            RecordOffset::new(0),
        )
    }

    fn record(number: i64) -> Record {
        Record::new(
            [("number".into(), AttributeValue::I64(number))],
            Bytes::from(format!("record-{number}")),
        )
    }

    #[tokio::test]
    async fn concurrent_submitters_receive_dense_durable_acknowledgements() {
        let (handle, actor) = JournalActor::new(writer(), 4);
        let actor = tokio::spawn(actor.run());
        let first = handle.submit(vec![record(1)]).await.expect("enqueue first");
        let second = handle
            .submit(vec![record(2), record(3)])
            .await
            .expect("enqueue second");
        let first = first.await.expect("first durable");
        let second = second.await.expect("second durable");
        assert_eq!(first.first_offset.get(), 0);
        assert_eq!(first.next_offset.get(), 1);
        assert_eq!(second.first_offset.get(), 1);
        assert_eq!(second.next_offset.get(), 3);
        drop(handle);
        actor.await.expect("actor exits");
    }

    #[tokio::test]
    async fn sealed_failure_transitions_the_actor_and_never_strands_later_acknowledgements() {
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let log = AtomicLog::builder(drive, 4).build().expect("build log");
        log.seal().await.expect("seal");
        let writer = JournalWriter::new(
            JournalId::from_bytes(*b"service-test!!!!"),
            log,
            RecordOffset::new(0),
        );
        let (handle, actor) = JournalActor::new(writer, 4);
        let actor = tokio::spawn(actor.run());
        let first = handle.submit(vec![record(1)]).await.expect("enqueue first");
        let second = handle
            .submit(vec![record(2)])
            .await
            .expect("enqueue second");
        assert!(matches!(first.await, Err(ServiceError::Sealed { .. })));
        assert!(matches!(second.await, Err(ServiceError::Sealed { .. })));
        drop(handle);
        actor.await.expect("actor exits");
    }

    #[tokio::test]
    async fn rejected_codec_input_does_not_wedge_later_valid_submissions() {
        let (handle, actor) = JournalActor::new(writer(), 4);
        let actor = tokio::spawn(actor.run());
        let invalid = Record::new(
            [("value".into(), AttributeValue::F64(f64::NAN))],
            Bytes::new(),
        );
        let invalid = handle.submit(vec![invalid]).await.expect("enqueue invalid");
        assert_eq!(invalid.await, Err(ServiceError::InvalidRequest));
        let valid = handle.submit(vec![record(1)]).await.expect("enqueue valid");
        assert_eq!(valid.await.expect("valid durable").first_offset.get(), 0);
        drop(handle);
        actor.await.expect("actor exits");
    }
}
