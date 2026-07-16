//! The shared event and effect vocabulary.
//!
//! One set of names is used by the pure reference model, by the real driver, and
//! (when they exist) by the formal specs. That is what makes drift *observable*:
//! if the implementation takes a transition the model never permits, the two
//! traces disagree in the same words rather than in two private dialects.
//!
//! # Why effects are separate from events
//!
//! An [`Event`] is something that happened to the protocol. An [`Effect`] is
//! something that costs money. They are deliberately different types, because
//! the family's central claim — that this design is cheap — is not a protocol
//! property and cannot be checked by reasoning about the protocol.
//!
//! The scopes matter and must never be conflated:
//!
//! - **logical**: what the caller asked for (a chunk committed, a record read).
//! - **adapter**: what the kernel asked the object store for (a PUT, a GET, a
//!   tail scan). This is what a schedule test can assert.
//! - **physical**: what the provider actually billed. Pagination, redirects and
//!   SDK retries live below the adapter, so one adapter tail scan is *not*
//!   necessarily one billable request. Only hosted measurement can see this.
//!
//! A cost invariant is therefore stated at the adapter scope and *attested* at
//! the physical scope. Neither substitutes for the other.

use crate::chunk::{ChunkId, ProducerId};
use crate::model::{JournalId, RecordOffset};

/// A protocol event, in the vocabulary the model and the implementation share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A submission was admitted: it is now owned by the driver and will commit
    /// or receive a terminal outcome. This is the linearization point for
    /// caller cancellation (scripture 0011).
    SubmissionAdmitted {
        /// The producer.
        producer_id: ProducerId,
        /// Its incarnation.
        producer_epoch: u32,
        /// Its sequence.
        sequence: u64,
        /// How many records it carried.
        records: u32,
    },
    /// A submission was refused before admission. Nothing was reserved.
    SubmissionRejected {
        /// The producer.
        producer_id: ProducerId,
        /// Its sequence.
        sequence: u64,
        /// Why.
        reason: RejectReason,
    },
    /// A duplicate submission was answered from the dedup window with the
    /// *original* offsets.
    SubmissionDeduplicated {
        /// The producer.
        producer_id: ProducerId,
        /// Its incarnation. Part of the identity: `(producer, epoch, sequence)`.
        producer_epoch: u32,
        /// Its sequence.
        sequence: u64,
        /// The offsets the first attempt received.
        first_offset: RecordOffset,
    },
    /// A chunk's bytes were finalized. They may not change after this.
    ChunkSealed {
        /// The chunk.
        chunk_id: ChunkId,
        /// How many records it carries.
        records: u32,
        /// How many bytes it encodes to.
        bytes: usize,
    },
    /// An append was issued to the kernel. Exactly one of the three outcomes
    /// below must follow.
    AppendIssued {
        /// The chunk being appended.
        chunk_id: ChunkId,
    },
    /// The kernel acknowledged the append. The chunk is committed.
    AppendAcknowledged {
        /// The chunk.
        chunk_id: ChunkId,
        /// The slot it landed in.
        slot: u64,
    },
    /// The append's outcome is unknown. It is never retried: a retry would
    /// acquire a new slot while the abandoned one blocks every later
    /// `complete_slot` forever (scripture 0010).
    AppendUncertain {
        /// The chunk whose fate is unknown.
        chunk_id: ChunkId,
    },
    /// A receipt was released to a submitter. Only ever from the committed
    /// state.
    ReceiptReleased {
        /// The producer.
        producer_id: ProducerId,
        /// Its incarnation. A receipt that cannot say which epoch it answers is
        /// ambiguous across a producer restart, because a submission's identity
        /// is `(producer, epoch, sequence)` — not `(producer, sequence)`.
        producer_epoch: u32,
        /// Its sequence.
        sequence: u64,
        /// Where its records landed.
        first_offset: RecordOffset,
        /// How many.
        records: u32,
    },
    /// A waiter was resolved with a terminal, non-committing outcome. Every
    /// admitted submission must reach either this or `ReceiptReleased` — no
    /// caller may hang forever (scripture 0011).
    WaiterFailed {
        /// The producer.
        producer_id: ProducerId,
        /// Its incarnation. Part of the identity.
        producer_epoch: u32,
        /// Its sequence.
        sequence: u64,
        /// Whether the record may still be in the log.
        outcome: TerminalOutcome,
    },
    /// The driver observed an uncertain append and stopped accepting work.
    OwnerPoisoned,
    /// A new owner rebuilt its state from durable bytes.
    OwnerRecovered {
        /// The next offset it will allocate.
        next_offset: RecordOffset,
        /// How many chunks its bounded scan read.
        scanned_chunks: usize,
    },
    /// A generation was sealed and a successor published.
    MembershipPublished {
        /// The generation that is now active.
        generation: u64,
        /// The first global position the successor owns.
        boundary: u64,
    },
    /// A chunk became visible in the log.
    ChunkVisible {
        /// The chunk.
        chunk_id: ChunkId,
        /// Its slot.
        slot: u64,
    },
    /// A chunk is durable in the object store but lies at or above a cutover
    /// boundary, so it is unmapped and will never be committed (holylog 0005).
    ChunkExcluded {
        /// The chunk that will never be read.
        chunk_id: ChunkId,
    },
}

/// Why a submission was refused before admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// A gap in the producer's sequence.
    OutOfSequence,
    /// An older incarnation of the producer.
    FencedProducer,
    /// The producer is not in the recovered window; the owner cannot know
    /// whether this is a new producer or a zombie, and says so.
    IndeterminateProducer,
    /// A record larger than the policy admits. Rejected rather than sealed
    /// alone, because sealing it alone would breach the bytes-at-risk ceiling.
    RecordTooLarge,
    /// The driver is poisoned and accepts nothing.
    DriverStopped,
}

/// How a waiter ended without a receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalOutcome {
    /// The append's outcome is unknown: the record may or may not be in the log.
    /// Recovery decides. Never retried blindly.
    Uncertain,
    /// The driver stopped before this submission's chunk was ever appended, so
    /// the record is provably *not* in the log. Cleanly retryable — and
    /// deliberately a different outcome from `Uncertain`, because collapsing
    /// them would force a caller to reconcile a record we know was never
    /// written.
    NotWritten,
}

/// The scope at which an effect is observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CostScope {
    /// What the caller asked for.
    Logical,
    /// What the kernel asked the object store for. Schedule tests assert here.
    Adapter,
}

/// A billable-ish action. Adapter effects are the ones a cost invariant names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Effect {
    /// A chunk was committed (logical).
    ChunkCommitted,
    /// A record was delivered to a reader (logical).
    RecordDelivered,
    /// A data object was written.
    DataPut,
    /// A data object was read.
    DataGet,
    /// The seal was read. Irreducible for an open log (holylog 0006).
    SealGet,
    /// The trim point was read.
    TrimGet,
    /// A tail scan — a LIST. The most expensive request class most object
    /// stores bill, and the one a warm read path must never issue.
    TailScan,
    /// The membership register was read.
    RegisterRead,
    /// The membership register was swapped.
    RegisterCas,
}

/// An append-only ledger of events and effects.
///
/// The model and the implementation both write to one of these, so a generated
/// schedule can assert cost invariants — "a warm read emits no `TailScan`" — as
/// invariants rather than as after-the-fact metrics.
#[derive(Debug, Default, Clone)]
pub struct Ledger {
    events: Vec<Event>,
    effects: Vec<(CostScope, Effect)>,
}

impl Ledger {
    /// Creates an empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a protocol event.
    pub fn event(&mut self, event: Event) {
        self.events.push(event);
    }

    /// Records a cost effect.
    pub fn effect(&mut self, scope: CostScope, effect: Effect) {
        self.effects.push((scope, effect));
    }

    /// Every event, in order.
    #[must_use]
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Counts one adapter effect.
    #[must_use]
    pub fn adapter_count(&self, effect: Effect) -> usize {
        self.effects
            .iter()
            .filter(|(scope, seen)| *scope == CostScope::Adapter && *seen == effect)
            .count()
    }

    /// Counts one logical effect.
    #[must_use]
    pub fn logical_count(&self, effect: Effect) -> usize {
        self.effects
            .iter()
            .filter(|(scope, seen)| *scope == CostScope::Logical && *seen == effect)
            .count()
    }

    /// Counts matching events.
    #[must_use]
    pub fn count_events(&self, predicate: impl Fn(&Event) -> bool) -> usize {
        self.events.iter().filter(|event| predicate(event)).count()
    }
}

/// A journal-scoped view, so a cost invariant can name its journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalScope(pub JournalId);
