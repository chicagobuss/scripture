//! Single-owner chunk driver: admit, seal, append, committed-only receipts.
//!
//! Phase 1 is deliberately narrow: one journal per chunk, one in-process owner,
//! depth-one append pipeline, and no VirtualLog cutover. The pure model in
//! `tests/driver_model.rs` is the behavioral oracle.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::channel::{mpsc, oneshot};
use futures::future::{self, BoxFuture, Either};
use futures::{SinkExt, StreamExt};

use crate::chunk::{
    ChunkError, ChunkHeader, ChunkId, CohortId, Frame, ProducerId, SealedChunk, SubmissionRef,
    WriterId, encoded_chunk_len, seal_single_frame_chunk,
};
use crate::chunklog::{ChunkLogError, ChunkLogWriter, RecoveredChunk, RecoveryBound};
use crate::clock::{Clock, Timer};
use crate::model::{JournalId, Record, RecordOffset};
use crate::trace::{Effect, Event, Ledger, RejectReason, TerminalOutcome};

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

/// Hard limits the owner publishes and enforces by reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPolicy {
    /// Seal when the open chunk would reach this encoded size.
    pub max_chunk_bytes: usize,
    /// Hard reject a single record above this encoded contribution.
    pub max_record_bytes: usize,
    /// Seal when the open chunk reaches this many records.
    pub max_chunk_records: usize,
    /// Seal when the open chunk reaches this monotonic age.
    pub max_chunk_age: Duration,
    /// Reservation ceiling for unsealed buffered bytes.
    pub max_buffered_bytes: usize,
    /// Pipeline depth. Phase 1 requires exactly one.
    pub max_inflight_chunks: usize,
    /// Admission deadline for uncommitted work (not a resolution promise).
    pub max_uncommitted_age: Duration,
    /// Bounds the durable dedup-window rebuild.
    pub recovery_scan: RecoveryBound,
}

/// Why a [`ChunkPolicy`] refused to construct.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    /// A hard limit was zero or otherwise nonsensical.
    #[error("chunk policy has a non-positive hard limit")]
    InvalidLimit,
    /// Phase 1 only implements depth-one append; deeper pipelines are rejected.
    #[error(
        "phase 1 requires max_inflight_chunks == 1 (got {max_inflight_chunks}); deeper pipelines are not implemented"
    )]
    PhaseOneRequiresInflightOne {
        /// Configured pipeline depth.
        max_inflight_chunks: usize,
    },
    /// A max-sized record could not fit in a max-sized chunk after framing.
    #[error(
        "max_record_bytes {max_record_bytes} plus framing overhead ({overhead}) exceeds max_chunk_bytes {max_chunk_bytes}"
    )]
    RecordCannotFitChunk {
        /// Configured per-record ceiling.
        max_record_bytes: usize,
        /// Configured per-chunk ceiling.
        max_chunk_bytes: usize,
        /// Framing overhead for a one-record chunk.
        overhead: usize,
    },
}

impl ChunkPolicy {
    /// Validates hard limits and the record-fits-chunk invariant.
    ///
    /// Phase 1 requires [`Self::max_inflight_chunks`] `== 1` so
    /// [`Self::bytes_at_risk`] matches the implemented loss window.
    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.max_chunk_bytes == 0
            || self.max_record_bytes == 0
            || self.max_chunk_records == 0
            || self.max_buffered_bytes == 0
            || self.max_inflight_chunks == 0
            || self.recovery_scan.max_chunks() == 0
            || self.max_chunk_age.is_zero()
            || self.max_uncommitted_age.is_zero()
        {
            return Err(PolicyError::InvalidLimit);
        }
        if self.max_inflight_chunks != 1 {
            return Err(PolicyError::PhaseOneRequiresInflightOne {
                max_inflight_chunks: self.max_inflight_chunks,
            });
        }
        let _ = self
            .max_buffered_bytes
            .checked_add(
                self.max_inflight_chunks
                    .checked_mul(self.max_chunk_bytes)
                    .ok_or(PolicyError::InvalidLimit)?,
            )
            .ok_or(PolicyError::InvalidLimit)?;
        let overhead = worst_case_framing_overhead()?;
        let needed = self
            .max_record_bytes
            .checked_add(overhead)
            .ok_or(PolicyError::InvalidLimit)?;
        if needed > self.max_chunk_bytes {
            return Err(PolicyError::RecordCannotFitChunk {
                max_record_bytes: self.max_record_bytes,
                max_chunk_bytes: self.max_chunk_bytes,
                overhead,
            });
        }
        Ok(())
    }

    /// Hard bytes-at-risk bound:
    /// `max_buffered_bytes + max_inflight_chunks * max_chunk_bytes`.
    ///
    /// Phase 1 validates `max_inflight_chunks == 1`, so this is
    /// `max_buffered_bytes + max_chunk_bytes`: unsealed buffer plus one sealed
    /// chunk whose append may still be in flight.
    ///
    /// Construction rejects configurations where this arithmetic overflows.
    ///
    /// There is deliberately no `age_at_risk`: provider latency is unbounded,
    /// and publishing a number would be a lie (decision 0011).
    #[must_use]
    pub const fn bytes_at_risk(&self) -> usize {
        self.max_buffered_bytes + self.max_inflight_chunks * self.max_chunk_bytes
    }
}

fn worst_case_framing_overhead() -> Result<usize, PolicyError> {
    // One empty-attribute record with a zero-length payload measures framing
    // overhead; callers add max_record_bytes on top.
    let record = Record::new([], Bytes::new());
    let frame = Frame {
        journal_id: JournalId::from_bytes([0; 16]),
        base_offset: RecordOffset::new(0),
        records: vec![record],
        submissions: vec![SubmissionRef {
            producer_id: ProducerId::from_bytes([0; 16]),
            producer_epoch: 0,
            sequence: 0,
            first_record: 0,
            record_count: 1,
        }],
    };
    let total = encoded_chunk_len(&[frame]).map_err(|_| PolicyError::InvalidLimit)?;
    Ok(total)
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

/// Snapshot of owner counters. Numbers only — no actor internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriverMetrics {
    /// Declared policy bytes-at-risk bound ([`ChunkPolicy::bytes_at_risk`]).
    pub bytes_at_risk: usize,
    /// Bytes currently reserved (open chunk plus sealed-but-uncommitted work).
    pub reserved_bytes: usize,
    /// Chunks sealed and awaiting or undergoing append.
    pub inflight_chunks: usize,
    /// Successful dedup hits.
    pub dedup_hits: u64,
    /// Submissions admitted since construction/recovery.
    pub admitted: u64,
    /// Submissions rejected before admission.
    pub rejected: u64,
    /// True after the actor emits [`crate::trace::Event::OwnerPoisoned`].
    ///
    /// Survives while `run` continues in the poisoned drain loop, so a service
    /// can observe poison from [`ChunkDriverHandle::metrics`] without waiting
    /// for a later client request.
    pub poisoned: bool,
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

/// Future returned by [`ChunkDriverHandle::submit`].
///
/// Dropping it never cancels an accepted submission.
#[must_use = "receipts are learned by awaiting this future"]
pub struct ReceiptFuture {
    receiver: oneshot::Receiver<Result<Receipt, DriverError>>,
}

impl Future for ReceiptFuture {
    type Output = Result<Receipt, DriverError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(context)
            .map(|result| result.unwrap_or(Err(DriverError::Unavailable)))
    }
}

type AdmissionReply = Result<oneshot::Receiver<Result<Receipt, DriverError>>, DriverError>;
type AdmissionSender = oneshot::Sender<AdmissionReply>;

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

enum Command {
    Submit {
        submission: Submission,
        admission: AdmissionSender,
    },
    Flush {
        responder: oneshot::Sender<Result<(), DriverError>>,
    },
}

struct BlockedSubmission {
    submission: Submission,
    admission: AdmissionSender,
    encoded_bytes: usize,
}

struct PlacedSubmission {
    submission: Submission,
    first_offset: RecordOffset,
    #[allow(dead_code)] // retained for reservation accounting / metrics follow-ups
    encoded_bytes: usize,
    waiters: Vec<oneshot::Sender<Result<Receipt, DriverError>>>,
}

struct OpenChunk {
    placed: Vec<PlacedSubmission>,
    encoded_bytes: usize,
    started_at: Duration,
}

struct SealedWork {
    sealed: SealedChunk,
    placed: Vec<PlacedSubmission>,
    encoded_bytes: usize,
    sealed_at: Duration,
}

type DedupEntry = (u64, BTreeMap<u64, (RecordOffset, u32, ChunkId, u64)>);
type DedupWindow = BTreeMap<(ProducerId, u32), DedupEntry>;

/// Cloneable trace recorder shared by the actor and its handles.
///
/// It exists so deterministic integration tests can inspect a completed
/// `run(self)` without making the core's ledger globally mutable.
#[derive(Clone, Debug, Default)]
struct SharedLedger(Arc<Mutex<Ledger>>);

impl SharedLedger {
    fn event(&self, event: Event) {
        if let Ok(mut ledger) = self.0.lock() {
            ledger.event(event);
        }
    }

    fn effect(&self, scope: crate::trace::CostScope, effect: Effect) {
        if let Ok(mut ledger) = self.0.lock() {
            ledger.effect(scope, effect);
        }
    }

    fn snapshot(&self) -> Ledger {
        self.0
            .lock()
            .map(|ledger| ledger.clone())
            .unwrap_or_default()
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

    /// Drains commands, seals on bounds, and owns every append future.
    pub async fn run(mut self) -> Result<(), DriverError> {
        loop {
            if self.poisoned {
                self.poison_blocked();
                while let Some(command) = self.commands.next().await {
                    self.reject_command(command, DriverError::Poisoned);
                }
                return Ok(());
            }

            // Depth one: if a sealed chunk is waiting, append it before taking
            // more seal decisions. Commands may still queue in the channel.
            if self.pending_append.is_some() {
                self.append_pending().await?;
                continue;
            }

            if self.age_due() {
                self.clear_age_sleep();
                self.seal_open();
                continue;
            }

            self.refresh_age_sleep();

            let next_command = self.commands.next();
            let command = if let Some(sleep) = self.age_sleep.take() {
                match future::select(next_command, sleep).await {
                    Either::Left((command, sleep)) => {
                        self.age_sleep = Some(sleep);
                        command
                    }
                    Either::Right(((), _command)) => {
                        self.age_sleep_deadline = None;
                        if self.age_due() {
                            self.seal_open();
                        }
                        continue;
                    }
                }
            } else {
                next_command.await
            };

            let Some(command) = command else {
                // All handles dropped: flush remaining work then exit.
                self.clear_age_sleep();
                if self.open.is_some() {
                    self.seal_open();
                }
                if self.pending_append.is_some() {
                    self.append_pending().await?;
                }
                while let Some(blocked) = self.blocked.pop_front() {
                    let _ = blocked.admission.send(Err(DriverError::Unavailable));
                }
                return Ok(());
            };
            self.handle_command(command).await?;
        }
    }

    fn age_deadline(&self) -> Option<Duration> {
        let open = self.open.as_ref()?;
        if open.placed.is_empty() {
            return None;
        }
        Some(
            open.started_at
                .checked_add(self.policy.max_chunk_age)
                .unwrap_or(Duration::MAX),
        )
    }

    fn age_due(&self) -> bool {
        self.open.as_ref().is_some_and(|open| {
            !open.placed.is_empty()
                && self.clock.now().saturating_sub(open.started_at) >= self.policy.max_chunk_age
        })
    }

    fn clear_age_sleep(&mut self) {
        self.age_sleep = None;
        self.age_sleep_deadline = None;
    }

    fn refresh_age_sleep(&mut self) {
        let deadline = self.age_deadline();
        if self.age_sleep_deadline == deadline {
            if deadline.is_some() && self.age_sleep.is_none() {
                // Previously completed or cleared; recreate for the same deadline.
                if let Some(deadline) = deadline {
                    self.age_sleep = Some(self.timer.sleep_until(deadline));
                }
            }
            return;
        }
        self.age_sleep = None;
        self.age_sleep_deadline = deadline;
        if let Some(deadline) = deadline {
            self.age_sleep = Some(self.timer.sleep_until(deadline));
        }
    }

    async fn handle_command(&mut self, command: Command) -> Result<(), DriverError> {
        match command {
            Command::Submit {
                submission,
                admission,
            } => {
                self.admit(submission, admission);
                Ok(())
            }
            Command::Flush { responder } => {
                if self.poisoned {
                    let _ = responder.send(Err(DriverError::Poisoned));
                    return Ok(());
                }
                if self.open.is_some() {
                    self.clear_age_sleep();
                    self.seal_open();
                }
                if self.pending_append.is_some() {
                    self.append_pending().await?;
                }
                if self.poisoned {
                    let _ = responder.send(Err(DriverError::Poisoned));
                } else {
                    let _ = responder.send(Ok(()));
                }
                Ok(())
            }
        }
    }

    fn reject_command(&mut self, command: Command, error: DriverError) {
        match command {
            Command::Submit { admission, .. } => {
                let _ = admission.send(Err(error));
            }
            Command::Flush { responder } => {
                let _ = responder.send(Err(error));
            }
        }
    }

    fn poison_blocked(&mut self) {
        while let Some(blocked) = self.blocked.pop_front() {
            let _ = blocked.admission.send(Err(DriverError::Poisoned));
        }
    }

    fn oldest_uncommitted_at(&self) -> Option<Duration> {
        let open_started = self.open.as_ref().and_then(|open| {
            if open.placed.is_empty() {
                None
            } else {
                Some(open.started_at)
            }
        });
        let pending_sealed = self
            .pending_append
            .as_ref()
            .map(|pending| pending.sealed_at);
        match (open_started, pending_sealed) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    fn admission_age_blocked(&self) -> bool {
        let Some(oldest) = self.oldest_uncommitted_at() else {
            return false;
        };
        self.clock.now().saturating_sub(oldest) >= self.policy.max_uncommitted_age
    }

    fn should_block_admission(&self, encoded_bytes: usize) -> bool {
        if self.admission_age_blocked() {
            return true;
        }
        let open_bytes = self.open.as_ref().map_or(0, |open| open.encoded_bytes);
        let pending_bytes = self
            .pending_append
            .as_ref()
            .map_or(0, |pending| pending.encoded_bytes);
        let at_risk = open_bytes
            .saturating_add(pending_bytes)
            .saturating_add(encoded_bytes);
        if at_risk > self.policy.bytes_at_risk() {
            return true;
        }
        if open_bytes.saturating_add(encoded_bytes) > self.policy.max_buffered_bytes {
            return true;
        }
        if self.pending_append.is_some()
            && open_bytes.saturating_add(encoded_bytes) > self.policy.max_chunk_bytes
        {
            return true;
        }
        false
    }

    fn drain_blocked(&mut self) {
        while let Some(blocked) = self.blocked.pop_front() {
            if self.poisoned {
                let _ = blocked.admission.send(Err(DriverError::Poisoned));
                continue;
            }
            // Joins and dedup replays do not consume reservation. Prefer the
            // full admit path for those even when the buffer is still full,
            // otherwise a parked retry of an identity just admitted would sit
            // behind capacity instead of joining the open waiter.
            if !self.resolves_without_new_reservation(&blocked.submission)
                && self.should_block_admission(blocked.encoded_bytes)
            {
                self.blocked.push_front(blocked);
                return;
            }
            self.admit(blocked.submission, blocked.admission);
        }
    }

    fn resolves_without_new_reservation(&self, submission: &Submission) -> bool {
        let key = (submission.producer_id, submission.producer_epoch);
        if let Some((highest, _)) = self.dedup.get(&key)
            && submission.sequence <= *highest
        {
            return true;
        }
        if let Some(open) = self.open.as_ref() {
            for placed in &open.placed {
                if placed.submission.producer_id == submission.producer_id
                    && placed.submission.producer_epoch == submission.producer_epoch
                    && placed.submission.sequence == submission.sequence
                {
                    return true;
                }
            }
        }
        if let Some(pending) = self.pending_append.as_ref() {
            for placed in &pending.placed {
                if placed.submission.producer_id == submission.producer_id
                    && placed.submission.producer_epoch == submission.producer_epoch
                    && placed.submission.sequence == submission.sequence
                {
                    return true;
                }
            }
        }
        false
    }

    fn admit(&mut self, submission: Submission, admission: AdmissionSender) {
        if self.poisoned {
            let _ = admission.send(Err(DriverError::Poisoned));
            self.bump_rejected();
            return;
        }
        if submission.records.is_empty() {
            let _ = admission.send(Err(DriverError::EmptySubmission));
            self.bump_rejected();
            return;
        }

        if let Err(error) = self.validate_per_record_bytes(&submission) {
            self.ledger.event(Event::SubmissionRejected {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
                reason: RejectReason::RecordTooLarge,
            });
            let _ = admission.send(Err(error));
            self.bump_rejected();
            return;
        }

        let encoded_bytes = match self.submission_encoded_bytes(&submission) {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = admission.send(Err(error));
                self.bump_rejected();
                return;
            }
        };

        match self.known_producers.get(&submission.producer_id).copied() {
            Some(highest) if submission.producer_epoch < highest => {
                self.ledger.event(Event::SubmissionRejected {
                    producer_id: submission.producer_id,
                    sequence: submission.sequence,
                    reason: RejectReason::FencedProducer,
                });
                let _ = admission.send(Err(DriverError::FencedProducer {
                    seen_epoch: highest,
                    request_epoch: submission.producer_epoch,
                }));
                self.bump_rejected();
                return;
            }
            _ => {}
        }

        let key = (submission.producer_id, submission.producer_epoch);
        if let Some((highest, window)) = self.dedup.get(&key)
            && submission.sequence <= *highest
        {
            if let Some((first_offset, records, chunk_id, slot)) =
                window.get(&submission.sequence).copied()
            {
                let next_offset = first_offset
                    .checked_add(records as usize)
                    .unwrap_or(first_offset);
                self.ledger.event(Event::SubmissionDeduplicated {
                    producer_id: submission.producer_id,
                    producer_epoch: submission.producer_epoch,
                    sequence: submission.sequence,
                    first_offset,
                });
                self.ledger.event(Event::ReceiptReleased {
                    producer_id: submission.producer_id,
                    producer_epoch: submission.producer_epoch,
                    sequence: submission.sequence,
                    first_offset,
                    records,
                });
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.dedup_hits = metrics.dedup_hits.saturating_add(1);
                }
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(Ok(Receipt {
                    level: AckLevel::Committed,
                    journal_id: self.journal_id,
                    first_offset,
                    next_offset,
                    chunk_id,
                    slot,
                    deduplicated: true,
                }));
                let _ = admission.send(Ok(rx));
                return;
            }
            self.ledger.event(Event::SubmissionRejected {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
                reason: RejectReason::IndeterminateProducer,
            });
            let _ = admission.send(Err(DriverError::Indeterminate {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
            }));
            self.bump_rejected();
            return;
        }

        // Duplicate still buffered / in flight: join the original waiter.
        if let Some(open) = self.open.as_mut() {
            for placed in &mut open.placed {
                if placed.submission.producer_id == submission.producer_id
                    && placed.submission.producer_epoch == submission.producer_epoch
                    && placed.submission.sequence == submission.sequence
                {
                    let (tx, rx) = oneshot::channel();
                    placed.waiters.push(tx);
                    let _ = admission.send(Ok(rx));
                    return;
                }
            }
        }
        if let Some(pending) = self.pending_append.as_mut() {
            for placed in &mut pending.placed {
                if placed.submission.producer_id == submission.producer_id
                    && placed.submission.producer_epoch == submission.producer_epoch
                    && placed.submission.sequence == submission.sequence
                {
                    let (tx, rx) = oneshot::channel();
                    placed.waiters.push(tx);
                    let _ = admission.send(Ok(rx));
                    return;
                }
            }
        }

        let expected = self
            .admitted_seq
            .get(&key)
            .map(|highest| highest.saturating_add(1))
            .or_else(|| {
                self.dedup
                    .get(&key)
                    .map(|(highest, _)| highest.saturating_add(1))
            })
            .unwrap_or(0);
        // New higher epoch begins at zero.
        let expected = if self
            .known_producers
            .get(&submission.producer_id)
            .copied()
            .is_none_or(|highest| submission.producer_epoch > highest)
        {
            0
        } else {
            expected
        };
        if submission.sequence != expected {
            self.ledger.event(Event::SubmissionRejected {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
                reason: RejectReason::OutOfSequence,
            });
            let _ = admission.send(Err(DriverError::OutOfSequence {
                expected,
                actual: submission.sequence,
            }));
            self.bump_rejected();
            return;
        }

        if self.should_block_admission(encoded_bytes) {
            self.blocked.push_back(BlockedSubmission {
                submission,
                admission,
                encoded_bytes,
            });
            return;
        }

        self.admit_ready(submission, admission, encoded_bytes);
    }

    fn admit_ready(
        &mut self,
        submission: Submission,
        admission: AdmissionSender,
        encoded_bytes: usize,
    ) {
        let open_bytes = self.open.as_ref().map_or(0, |open| open.encoded_bytes);
        let pending_bytes = self
            .pending_append
            .as_ref()
            .map_or(0, |pending| pending.encoded_bytes);

        let first_offset = self.writer.next_offset();
        let first_offset = self.open.as_ref().map_or(first_offset, |open| {
            open.placed.last().map_or(first_offset, |last| {
                last.first_offset
                    .checked_add(last.submission.records.len())
                    .unwrap_or(last.first_offset)
            })
        });

        let record_count = submission.records.len() as u32;
        let (tx, rx) = oneshot::channel();
        let placed = PlacedSubmission {
            submission: submission.clone(),
            first_offset,
            encoded_bytes,
            waiters: vec![tx],
        };

        let now = self.clock.now();
        let open = self.open.get_or_insert_with(|| OpenChunk {
            placed: Vec::new(),
            encoded_bytes: 0,
            started_at: now,
        });
        open.placed.push(placed);
        open.encoded_bytes += encoded_bytes;
        self.reserved_bytes = open_bytes + pending_bytes + encoded_bytes;
        let key = (submission.producer_id, submission.producer_epoch);
        self.admitted_seq.insert(key, submission.sequence);
        self.known_producers
            .entry(submission.producer_id)
            .and_modify(|epoch| *epoch = (*epoch).max(submission.producer_epoch))
            .or_insert(submission.producer_epoch);

        self.ledger.event(Event::SubmissionAdmitted {
            producer_id: submission.producer_id,
            producer_epoch: submission.producer_epoch,
            sequence: submission.sequence,
            records: record_count,
        });
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.admitted = metrics.admitted.saturating_add(1);
            metrics.reserved_bytes = self.reserved_bytes;
            metrics.bytes_at_risk = self.policy.bytes_at_risk();
        }

        let _ = admission.send(Ok(rx));

        let records: usize = self
            .open
            .as_ref()
            .map(|open| open.placed.iter().map(|p| p.submission.records.len()).sum())
            .unwrap_or(0);
        let open_bytes = self.open.as_ref().map_or(0, |o| o.encoded_bytes);
        if records >= self.policy.max_chunk_records || open_bytes >= self.policy.max_chunk_bytes {
            self.clear_age_sleep();
            self.seal_open();
        }
    }

    fn validate_per_record_bytes(&self, submission: &Submission) -> Result<(), DriverError> {
        let empty = self.solo_record_chunk_len(&Record::new([], Bytes::new()))?;
        for record in &submission.records {
            let solo = self.solo_record_chunk_len(record)?;
            let contribution = solo.saturating_sub(empty);
            if contribution > self.policy.max_record_bytes {
                return Err(DriverError::RecordTooLarge {
                    bytes: contribution,
                    max: self.policy.max_record_bytes,
                });
            }
        }
        Ok(())
    }

    fn solo_record_chunk_len(&self, record: &Record) -> Result<usize, DriverError> {
        let frame = Frame {
            journal_id: self.journal_id,
            base_offset: RecordOffset::new(0),
            records: vec![record.clone()],
            submissions: vec![SubmissionRef {
                producer_id: ProducerId::from_bytes([0; 16]),
                producer_epoch: 0,
                sequence: 0,
                first_record: 0,
                record_count: 1,
            }],
        };
        Ok(encoded_chunk_len(std::slice::from_ref(&frame))?)
    }

    fn submission_encoded_bytes(&self, submission: &Submission) -> Result<usize, DriverError> {
        // Conservative reservation: full solo-chunk size for this submission.
        let frame = Frame {
            journal_id: self.journal_id,
            base_offset: RecordOffset::new(0),
            records: submission.records.clone(),
            submissions: vec![SubmissionRef {
                producer_id: submission.producer_id,
                producer_epoch: submission.producer_epoch,
                sequence: submission.sequence,
                first_record: 0,
                record_count: u32::try_from(submission.records.len())
                    .map_err(|_| ChunkError::Oversized)?,
            }],
        };
        Ok(encoded_chunk_len(std::slice::from_ref(&frame))?)
    }

    fn seal_open(&mut self) {
        let Some(open) = self.open.take() else {
            return;
        };
        self.clear_age_sleep();
        if open.placed.is_empty() {
            return;
        }
        let base_offset = open.placed[0].first_offset;
        let mut records = Vec::new();
        let mut submissions = Vec::new();
        let mut first_record = 0_u32;
        for placed in &open.placed {
            let count = u32::try_from(placed.submission.records.len()).unwrap_or(u32::MAX);
            submissions.push(SubmissionRef {
                producer_id: placed.submission.producer_id,
                producer_epoch: placed.submission.producer_epoch,
                sequence: placed.submission.sequence,
                first_record,
                record_count: count,
            });
            records.extend(placed.submission.records.iter().cloned());
            first_record = first_record.saturating_add(count);
        }
        let chunk_id = ChunkId::from_bytes({
            let mut bytes = [0_u8; 16];
            bytes[..8].copy_from_slice(&self.next_chunk.to_be_bytes());
            self.next_chunk = self.next_chunk.wrapping_add(1);
            bytes
        });
        let created_at_micros = u64::try_from(self.clock.now().as_micros()).unwrap_or(u64::MAX);
        let sealed_at = self.clock.now();
        let sealed = match seal_single_frame_chunk(
            ChunkHeader {
                chunk_id,
                cohort_id: self.cohort_id,
                generation: self.generation,
                writer_id: self.writer_id,
                created_at_micros,
            },
            vec![Frame {
                journal_id: self.journal_id,
                base_offset,
                records,
                submissions,
            }],
        ) {
            Ok(sealed) => sealed,
            Err(error) => {
                for placed in open.placed {
                    for waiter in placed.waiters {
                        let _ = waiter.send(Err(DriverError::Codec(error.clone())));
                    }
                }
                self.publish_reserved();
                self.drain_blocked();
                return;
            }
        };
        let bytes = sealed.bytes.len();
        self.ledger.event(Event::ChunkSealed {
            chunk_id,
            records: first_record,
            bytes,
        });
        self.pending_append = Some(SealedWork {
            sealed,
            placed: open.placed,
            encoded_bytes: open.encoded_bytes,
            sealed_at,
        });
        self.publish_reserved();
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.inflight_chunks = 1;
            metrics.bytes_at_risk = self.policy.bytes_at_risk();
        }
    }

    fn publish_reserved(&mut self) {
        let open_bytes = self.open.as_ref().map_or(0, |open| open.encoded_bytes);
        let pending_bytes = self
            .pending_append
            .as_ref()
            .map_or(0, |pending| pending.encoded_bytes);
        self.reserved_bytes = open_bytes + pending_bytes;
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.reserved_bytes = self.reserved_bytes;
            metrics.bytes_at_risk = self.policy.bytes_at_risk();
        }
    }

    async fn append_pending(&mut self) -> Result<(), DriverError> {
        let Some(pending) = self.pending_append.take() else {
            return Ok(());
        };
        self.ledger.event(Event::AppendIssued {
            chunk_id: pending.sealed.chunk_id,
        });
        match self.writer.append(&pending.sealed).await {
            Ok(ack) => {
                self.ledger.event(Event::AppendAcknowledged {
                    chunk_id: pending.sealed.chunk_id,
                    slot: ack.slot,
                });
                self.ledger
                    .effect(crate::trace::CostScope::Logical, Effect::ChunkCommitted);
                for placed in pending.placed {
                    let records = placed.submission.records.len() as u32;
                    let next_offset = placed
                        .first_offset
                        .checked_add(placed.submission.records.len())
                        .unwrap_or(placed.first_offset);
                    let key = (
                        placed.submission.producer_id,
                        placed.submission.producer_epoch,
                    );
                    let entry = self
                        .dedup
                        .entry(key)
                        .or_insert((placed.submission.sequence, BTreeMap::new()));
                    entry.0 = entry.0.max(placed.submission.sequence);
                    entry.1.insert(
                        placed.submission.sequence,
                        (
                            placed.first_offset,
                            records,
                            pending.sealed.chunk_id,
                            ack.slot,
                        ),
                    );
                    self.ledger.event(Event::ReceiptReleased {
                        producer_id: placed.submission.producer_id,
                        producer_epoch: placed.submission.producer_epoch,
                        sequence: placed.submission.sequence,
                        first_offset: placed.first_offset,
                        records,
                    });
                    let receipt = Receipt {
                        level: AckLevel::Committed,
                        journal_id: self.journal_id,
                        first_offset: placed.first_offset,
                        next_offset,
                        chunk_id: pending.sealed.chunk_id,
                        slot: ack.slot,
                        deduplicated: false,
                    };
                    for waiter in placed.waiters {
                        let _ = waiter.send(Ok(receipt.clone()));
                    }
                }
                self.publish_reserved();
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.inflight_chunks = 0;
                }
                self.drain_blocked();
                Ok(())
            }
            Err(_) => {
                self.ledger.event(Event::AppendUncertain {
                    chunk_id: pending.sealed.chunk_id,
                });
                self.ledger.event(Event::OwnerPoisoned);
                self.poisoned = true;
                for placed in pending.placed {
                    self.ledger.event(Event::WaiterFailed {
                        producer_id: placed.submission.producer_id,
                        producer_epoch: placed.submission.producer_epoch,
                        sequence: placed.submission.sequence,
                        outcome: TerminalOutcome::Uncertain,
                    });
                    for waiter in placed.waiters {
                        let _ = waiter.send(Err(DriverError::Uncertain {
                            chunk_id: pending.sealed.chunk_id,
                        }));
                    }
                }
                if let Some(open) = self.open.take() {
                    self.clear_age_sleep();
                    for placed in open.placed {
                        self.ledger.event(Event::WaiterFailed {
                            producer_id: placed.submission.producer_id,
                            producer_epoch: placed.submission.producer_epoch,
                            sequence: placed.submission.sequence,
                            outcome: TerminalOutcome::NotWritten,
                        });
                        for waiter in placed.waiters {
                            let _ = waiter.send(Err(DriverError::NotWritten));
                        }
                    }
                }
                self.poison_blocked();
                self.reserved_bytes = 0;
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.poisoned = true;
                    metrics.inflight_chunks = 0;
                    metrics.reserved_bytes = 0;
                    metrics.bytes_at_risk = self.policy.bytes_at_risk();
                }
                // Do not return Err: run must continue into the poisoned drain loop
                // so later Submit callers observe Poisoned, not Unavailable.
                Ok(())
            }
        }
    }

    fn bump_rejected(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.rejected = metrics.rejected.saturating_add(1);
        }
    }
}
