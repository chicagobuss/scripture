//! Single-owner chunk driver: admit, seal, append, committed-only receipts.
//!
//! Phase 1 is deliberately narrow: one journal per chunk, one in-process owner,
//! depth-one append pipeline, and no VirtualLog cutover. The pure model in
//! `tests/driver_model.rs` is the behavioral oracle.

mod admission;
mod metrics;
mod policy;
mod receipt;
mod run_loop;
mod state;

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::SinkExt;
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::future::BoxFuture;

use crate::chunk::{ChunkError, ChunkId, CohortId, ProducerId, WriterId};
use crate::chunklog::{ChunkLogError, ChunkLogWriter, RecoveredChunk};
use crate::clock::{Clock, Timer};
use crate::model::{JournalId, Record, RecordOffset};
use crate::trace::Ledger;

pub use metrics::DriverMetrics;
pub use policy::{ChunkPolicy, PolicyError};
pub use receipt::ReceiptFuture;

use receipt::SharedLedger;
use state::{BlockedSubmission, Command, DedupWindow, OpenChunk, SealedWork};

/// Acknowledgement level reported on a receipt.
///
/// Phase 1 only ever emits [`AckLevel::Committed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckLevel {
    /// One node held the bytes in memory. Never a durability claim.
    Accepted,
    /// A memory quorum held the bytes. Never a durability claim.
    Replicated,
    /// A local-disk quorum fsynced the bytes within a spool cell.
    Journaled,
    /// Holylog acknowledged the containing immutable chunk.
    Committed,
}

/// One producer submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    /// Stable producer identity across reconnects.
    pub producer_id: ProducerId,
    /// Producer incarnation; fences zombies.
    pub producer_epoch: u32,
    /// Strictly increasing per `(producer_id, producer_epoch, journal)`.
    pub sequence: u64,
    /// Records carried by this submission. Must be non-empty.
    pub records: Vec<Record>,
}

/// Committed receipt returned to a submitter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    /// Achieved acknowledgement level. Phase 1: always [`AckLevel::Committed`].
    pub level: AckLevel,
    /// Journal that received the records.
    pub journal_id: JournalId,
    /// First dense offset allocated to the submission.
    pub first_offset: RecordOffset,
    /// Offset after the submission's last record.
    pub next_offset: RecordOffset,
    /// Immutable chunk that carries the records.
    pub chunk_id: ChunkId,
    /// Holylog slot of that chunk.
    pub slot: u64,
    /// True when this receipt was replayed from the dedup window.
    pub deduplicated: bool,
}

/// Errors at the driver boundary.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    /// Producer skipped a sequence under the current epoch.
    #[error("out of sequence: expected {expected}, got {actual}")]
    OutOfSequence {
        /// Next expected sequence.
        expected: u64,
        /// Sequence offered.
        actual: u64,
    },
    /// A lower epoch arrived after a higher one was admitted.
    #[error("fenced producer: seen epoch {seen_epoch}, request epoch {request_epoch}")]
    FencedProducer {
        /// Highest epoch admitted for this producer.
        seen_epoch: u32,
        /// Epoch on the rejected request.
        request_epoch: u32,
    },
    /// The submission lies outside the bounded dedup window.
    #[error("indeterminate producer submission")]
    Indeterminate {
        /// Producer.
        producer_id: ProducerId,
        /// Sequence offered.
        sequence: u64,
    },
    /// A single record exceeds the hard per-record ceiling.
    #[error("record too large: {bytes} > {max}")]
    RecordTooLarge {
        /// Encoded contribution of the record.
        bytes: usize,
        /// Policy ceiling.
        max: usize,
    },
    /// An empty record list is never admitted.
    #[error("submission carries no records")]
    EmptySubmission,
    /// The append outcome is unknown; the owner is poisoned.
    #[error("append outcome uncertain for chunk {chunk_id:?}")]
    Uncertain {
        /// Chunk whose fate is unknown.
        chunk_id: ChunkId,
    },
    /// Provably never appended; safe for the producer to retry elsewhere.
    #[error("submission was not written")]
    NotWritten,
    /// A prior uncertain append poisoned this owner.
    #[error("chunk driver is poisoned")]
    Poisoned,
    /// The actor is gone; the submission was never admitted.
    #[error("chunk driver is unavailable")]
    Unavailable,
    /// Policy construction failed.
    #[error(transparent)]
    Policy(#[from] PolicyError),
    /// Chunk codec failure.
    #[error(transparent)]
    Codec(#[from] ChunkError),
    /// Chunk-log boundary failure.
    #[error(transparent)]
    Log(#[from] ChunkLogError),
}

pub(super) type AdmissionReply =
    Result<oneshot::Receiver<Result<Receipt, DriverError>>, DriverError>;
pub(super) type AdmissionSender = oneshot::Sender<AdmissionReply>;

/// Cloneable client endpoint for one journal owner.
#[derive(Clone)]
pub struct ChunkDriverHandle {
    commands: mpsc::Sender<Command>,
    metrics: Arc<Mutex<DriverMetrics>>,
    ledger: SharedLedger,
}

impl ChunkDriverHandle {
    /// Reserves, buffers, and returns a future that resolves only on commit or
    /// a terminal non-commit outcome. Awaiting this call is admission
    /// backpressure; the returned [`ReceiptFuture`] is independent.
    pub async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, DriverError> {
        let (admission_tx, admission_rx) = oneshot::channel();
        self.commands
            .clone()
            .send(Command::Submit {
                submission,
                admission: admission_tx,
            })
            .await
            .map_err(|_| DriverError::Unavailable)?;
        let receiver = admission_rx
            .await
            .unwrap_or(Err(DriverError::Unavailable))?;
        Ok(ReceiptFuture { receiver })
    }

    /// Seals and appends the open chunk now.
    pub async fn flush(&self) -> Result<(), DriverError> {
        let (responder, receiver) = oneshot::channel();
        self.commands
            .clone()
            .send(Command::Flush { responder })
            .await
            .map_err(|_| DriverError::Unavailable)?;
        receiver.await.unwrap_or(Err(DriverError::Unavailable))
    }

    /// Snapshot of owner counters.
    #[must_use]
    pub fn metrics(&self) -> DriverMetrics {
        self.metrics.lock().map(|guard| *guard).unwrap_or_default()
    }

    /// Snapshot of protocol events and logical effects observed by this owner.
    #[must_use]
    pub fn ledger(&self) -> Ledger {
        self.ledger.snapshot()
    }
}

/// The single task that owns the writer, open chunk, and reservation.
pub struct ChunkDriverActor<C, T> {
    journal_id: JournalId,
    cohort_id: CohortId,
    writer_id: WriterId,
    generation: u64,
    writer: ChunkLogWriter,
    policy: ChunkPolicy,
    clock: C,
    timer: T,
    commands: mpsc::Receiver<Command>,
    open: Option<OpenChunk>,
    /// Depth-one: at most one sealed chunk waiting to append or appending.
    pending_append: Option<SealedWork>,
    blocked: VecDeque<BlockedSubmission>,
    dedup: DedupWindow,
    admitted_seq: BTreeMap<(ProducerId, u32), u64>,
    known_producers: BTreeMap<ProducerId, u32>,
    reserved_bytes: usize,
    next_chunk: u64,
    poisoned: bool,
    ledger: SharedLedger,
    metrics: Arc<Mutex<DriverMetrics>>,
    /// Incomplete age-bound sleep retained across command-winning selects.
    age_sleep: Option<BoxFuture<'static, ()>>,
    /// Deadline for which [`Self::age_sleep`] was created, if any.
    age_sleep_deadline: Option<Duration>,
}

impl<C: Clock, T: Timer> ChunkDriverActor<C, T> {
    /// Creates a bounded actor and its cloneable handle.
    #[allow(clippy::too_many_arguments)] // construction surface matches the phase-1 work order
    pub fn new(
        journal_id: JournalId,
        cohort_id: CohortId,
        writer_id: WriterId,
        generation: u64,
        writer: ChunkLogWriter,
        recovered: &[RecoveredChunk],
        policy: ChunkPolicy,
        clock: C,
        timer: T,
        queue_capacity: usize,
    ) -> Result<(ChunkDriverHandle, Self), DriverError> {
        policy.validate()?;
        let (sender, receiver) = mpsc::channel(queue_capacity.max(1));
        let metrics = Arc::new(Mutex::new(DriverMetrics {
            bytes_at_risk: policy.bytes_at_risk(),
            ..DriverMetrics::default()
        }));
        let mut actor = Self {
            journal_id,
            cohort_id,
            writer_id,
            generation,
            writer,
            policy,
            clock,
            timer,
            commands: receiver,
            open: None,
            pending_append: None,
            blocked: VecDeque::new(),
            dedup: DedupWindow::new(),
            admitted_seq: BTreeMap::new(),
            known_producers: BTreeMap::new(),
            reserved_bytes: 0,
            next_chunk: 0,
            poisoned: false,
            ledger: SharedLedger::default(),
            metrics: Arc::clone(&metrics),
            age_sleep: None,
            age_sleep_deadline: None,
        };
        actor.rebuild_dedup(recovered);
        Ok((
            ChunkDriverHandle {
                commands: sender,
                metrics,
                ledger: actor.ledger.clone(),
            },
            actor,
        ))
    }

    /// Shared trace ledger for deterministic harness assertions.
    #[must_use]
    pub fn ledger(&self) -> Ledger {
        self.ledger.snapshot()
    }

    fn rebuild_dedup(&mut self, recovered: &[RecoveredChunk]) {
        for chunk in recovered {
            for submission in &chunk.frame.submissions {
                let key = (submission.producer_id, submission.producer_epoch);
                let first = chunk
                    .frame
                    .offsets_for(
                        submission.producer_id,
                        submission.producer_epoch,
                        submission.sequence,
                    )
                    .map(|(first, _)| first)
                    .unwrap_or(chunk.first_offset);
                let entry = self
                    .dedup
                    .entry(key)
                    .or_insert((submission.sequence, BTreeMap::new()));
                entry.0 = entry.0.max(submission.sequence);
                entry.1.insert(
                    submission.sequence,
                    (first, submission.record_count, chunk.chunk_id, chunk.slot),
                );
                self.admitted_seq.insert(key, entry.0);
                self.known_producers
                    .entry(submission.producer_id)
                    .and_modify(|epoch| *epoch = (*epoch).max(submission.producer_epoch))
                    .or_insert(submission.producer_epoch);
            }
        }
    }
}
