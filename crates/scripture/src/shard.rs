//! Deterministic shard metadata reducer for the tdv2 data-plane model.
//!
//! This is intentionally narrow: offset allocation, producer-event deduplication,
//! and immutable blob references. It does not own authority, routing, or HA.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::chunk::ProducerId;
use crate::model::{JournalId, RecordOffset};

/// Stable producer event identity for at-least-once retry accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    /// Producer that originated the event.
    pub producer_id: ProducerId,
    /// Producer incarnation; fences zombies.
    pub producer_epoch: u32,
    /// Strictly increasing per `(producer_id, producer_epoch, journal)`.
    pub sequence: u64,
}

/// Immutable payload reference assigned by the data plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRef {
    /// Object-store blob identifier.
    pub blob_id: String,
    /// Checksum of the referenced byte range.
    pub checksum: [u8; 32],
    /// Inclusive start offset within the blob.
    pub start: u64,
    /// Exclusive end offset within the blob.
    pub end: u64,
}

/// One append command accepted into shard metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendDataRef {
    /// Durable event identity supplied by the producer.
    pub event_id: EventId,
    /// Target journal for dense offset assignment.
    pub journal_id: JournalId,
    /// Immutable reference to committed payload bytes.
    pub data_ref: DataRef,
    /// Opaque payload bytes (for local verification / tests).
    pub payload: Bytes,
}

/// Commands applied deterministically to shard metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardCommand {
    /// Assign offsets and record a new immutable reference.
    Append(AppendDataRef),
}

/// Materialized shard metadata after replaying commands in order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShardState {
    /// Next dense offset to assign per journal.
    pub next_offset: BTreeMap<JournalId, RecordOffset>,
    /// Events already committed (for dedup on retry).
    pub committed_events: BTreeSet<EventId>,
    /// Assigned offsets for committed events.
    pub event_offsets: BTreeMap<EventId, RecordOffset>,
    /// Ordered data references per journal.
    pub refs: BTreeMap<JournalId, Vec<DataRef>>,
}

/// Reducer errors.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ShardError {
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
}

/// Pure deterministic reducer over shard commands.
#[derive(Debug, Default)]
pub struct ShardReducer {
    state: ShardState,
    /// Highest admitted epoch per producer.
    producer_epochs: BTreeMap<ProducerId, u32>,
    /// Next expected sequence per `(producer, epoch)`.
    next_sequence: BTreeMap<(ProducerId, u32), u64>,
}

impl ShardReducer {
    /// Empty reducer at genesis.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current materialized state.
    #[must_use]
    pub fn state(&self) -> &ShardState {
        &self.state
    }

    /// Applies one command, returning the assigned offset (or the original on dedup).
    pub fn apply(&mut self, command: ShardCommand) -> Result<RecordOffset, ShardError> {
        match command {
            ShardCommand::Append(append) => self.apply_append(append),
        }
    }

    fn apply_append(&mut self, append: AppendDataRef) -> Result<RecordOffset, ShardError> {
        let event_id = append.event_id;
        if let Some(offset) = self.state.event_offsets.get(&event_id) {
            return Ok(*offset);
        }

        let seen_epoch = self
            .producer_epochs
            .get(&event_id.producer_id)
            .copied()
            .unwrap_or(0);
        if event_id.producer_epoch < seen_epoch {
            return Err(ShardError::FencedProducer {
                seen_epoch,
                request_epoch: event_id.producer_epoch,
            });
        }
        if event_id.producer_epoch > seen_epoch {
            self.producer_epochs
                .insert(event_id.producer_id, event_id.producer_epoch);
            self.next_sequence
                .insert((event_id.producer_id, event_id.producer_epoch), 0);
        }

        let key = (event_id.producer_id, event_id.producer_epoch);
        let expected = self.next_sequence.get(&key).copied().unwrap_or(0);
        if event_id.sequence != expected {
            return Err(ShardError::OutOfSequence {
                expected,
                actual: event_id.sequence,
            });
        }
        self.next_sequence.insert(key, expected + 1);

        let offset = *self
            .state
            .next_offset
            .entry(append.journal_id)
            .or_insert(RecordOffset::new(0));
        self.state
            .next_offset
            .insert(append.journal_id, offset.checked_add(1).expect("offset overflow"));
        self.state.committed_events.insert(event_id);
        self.state.event_offsets.insert(event_id, offset);
        self.state
            .refs
            .entry(append.journal_id)
            .or_default()
            .push(append.data_ref);
        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::ProducerId;

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"shard-journal!!!")
    }

    fn producer() -> ProducerId {
        ProducerId::from_bytes(*b"shard-producer!!")
    }

    fn append(sequence: u64) -> AppendDataRef {
        AppendDataRef {
            event_id: EventId {
                producer_id: producer(),
                producer_epoch: 1,
                sequence,
            },
            journal_id: journal(),
            data_ref: DataRef {
                blob_id: format!("blob-{sequence}"),
                checksum: [0; 32],
                start: 0,
                end: 4,
            },
            payload: Bytes::from_static(b"test"),
        }
    }

    #[test]
    fn assigns_dense_offsets_and_dedups_retries() {
        let mut reducer = ShardReducer::new();
        let first = reducer
            .apply(ShardCommand::Append(append(0)))
            .expect("first");
        let retry = reducer
            .apply(ShardCommand::Append(append(0)))
            .expect("dedup");
        let second = reducer
            .apply(ShardCommand::Append(append(1)))
            .expect("second");
        assert_eq!(first, RecordOffset::new(0));
        assert_eq!(retry, RecordOffset::new(0));
        assert_eq!(second, RecordOffset::new(1));
    }

    #[test]
    fn rejects_out_of_sequence() {
        let mut reducer = ShardReducer::new();
        assert!(reducer
            .apply(ShardCommand::Append(append(1)))
            .is_err());
    }
}
