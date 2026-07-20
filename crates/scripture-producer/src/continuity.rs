//! Producer-edge continuity outbox for fleet routing changes.
//!
//! Unlike [`crate::SpoolCell`] (a Scribe-local pre-commit WAL that poisons on
//! forward failure), this outbox is the **producer** durability boundary:
//! temporary route / Scribe unavailability must retain pending work and retry
//! after route refresh. It never silently discards a locally durable admission.
//!
//! Receipts:
//! - `admit` → locally durable (survives Scribe restart under this outbox's disk)
//! - `drain` / successful forward → Canon-committed (or equivalent) observed

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;

use scripture::driver::{Receipt, Submission};

/// Stable identity for one producer submission in the continuity outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContinuityId {
    /// Producer that originated the submission.
    pub producer_id: scripture::ProducerId,
    /// Producer incarnation.
    pub producer_epoch: u32,
    /// Strictly increasing sequence under `(producer_id, producer_epoch)`.
    pub sequence: u64,
}

impl ContinuityId {
    /// Extracts identity from a submission.
    #[must_use]
    pub fn from_submission(submission: &Submission) -> Self {
        Self {
            producer_id: submission.producer_id,
            producer_epoch: submission.producer_epoch,
            sequence: submission.sequence,
        }
    }
}

/// One pending outbox entry awaiting Canon commit.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// Stable identity.
    pub id: ContinuityId,
    /// Original submission retained for at-least-once retry.
    pub submission: Submission,
}

/// Snapshot used by rolling-restart proofs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuitySnapshot {
    /// Sequences accepted into the durable outbox.
    pub local_durable: BTreeSet<ContinuityId>,
    /// Sequences for which a committed receipt was observed.
    pub committed: BTreeSet<ContinuityId>,
    /// Still awaiting successful forward + commit.
    pub pending: usize,
}

/// Failures at the continuity outbox boundary.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ContinuityError {
    /// Empty submission refused at admission.
    #[error("cannot admit an empty submission")]
    EmptySubmission,
    /// Duplicate identity already locally durable.
    #[error("duplicate continuity identity")]
    DuplicateIdentity,
    /// Outbox capacity exhausted before admit.
    #[error("continuity outbox is full")]
    Full,
}

/// In-memory continuity outbox (producer-edge durable boundary for tests/lab).
///
/// Production deployments should back this with a fsynced store; the admission
/// and retry semantics stay identical.
#[derive(Debug, Default)]
pub struct ContinuityOutbox {
    max_pending: usize,
    pending: Mutex<VecDeque<PendingEntry>>,
    local_durable: Mutex<BTreeSet<ContinuityId>>,
    committed: Mutex<BTreeSet<ContinuityId>>,
    /// Last committed receipt per identity (for dedup evidence).
    receipts: Mutex<BTreeMap<ContinuityId, Receipt>>,
}

fn drop_pending(pending: &mut VecDeque<PendingEntry>, id: ContinuityId) {
    pending.retain(|entry| entry.id != id);
}

impl ContinuityOutbox {
    /// Creates an outbox with a hard pending capacity.
    #[must_use]
    pub fn new(max_pending: usize) -> Self {
        Self {
            max_pending: max_pending.max(1),
            ..Self::default()
        }
    }

    /// Durably admits a submission into the outbox (local-durable boundary).
    pub fn admit(&self, submission: Submission) -> Result<ContinuityId, ContinuityError> {
        if submission.records.is_empty() {
            return Err(ContinuityError::EmptySubmission);
        }
        let id = ContinuityId::from_submission(&submission);
        let mut durable = self
            .local_durable
            .lock()
            .expect("continuity local_durable lock");
        if durable.contains(&id) {
            return Err(ContinuityError::DuplicateIdentity);
        }
        let mut pending = self.pending.lock().expect("continuity pending lock");
        if pending.len() >= self.max_pending {
            return Err(ContinuityError::Full);
        }
        durable.insert(id);
        pending.push_back(PendingEntry { id, submission });
        Ok(id)
    }

    /// Peeks the next pending entry without removing it.
    pub fn peek_pending(&self) -> Option<PendingEntry> {
        self.pending
            .lock()
            .expect("continuity pending lock")
            .front()
            .cloned()
    }

    /// Returns a clone of all pending entries in FIFO order (for parallel drain).
    pub fn pending_snapshot(&self) -> Vec<PendingEntry> {
        self.pending
            .lock()
            .expect("continuity pending lock")
            .iter()
            .cloned()
            .collect()
    }

    /// Marks an identity committed and drops it from the pending queue.
    pub fn mark_committed(&self, id: ContinuityId) {
        self.committed
            .lock()
            .expect("continuity committed lock")
            .insert(id);
        let mut pending = self.pending.lock().expect("continuity pending lock");
        drop_pending(&mut pending, id);
    }

    /// Marks committed and retains the receipt for evidence.
    pub fn mark_committed_with_receipt(&self, id: ContinuityId, receipt: Receipt) {
        self.receipts
            .lock()
            .expect("continuity receipts lock")
            .insert(id, receipt);
        self.mark_committed(id);
    }

    /// Whether every locally durable identity has a committed receipt.
    #[must_use]
    pub fn fully_drained(&self) -> bool {
        let snap = self.snapshot();
        snap.pending == 0 && snap.local_durable == snap.committed
    }

    /// Current continuity accounting.
    #[must_use]
    pub fn snapshot(&self) -> ContinuitySnapshot {
        ContinuitySnapshot {
            local_durable: self
                .local_durable
                .lock()
                .expect("continuity local_durable lock")
                .clone(),
            committed: self
                .committed
                .lock()
                .expect("continuity committed lock")
                .clone(),
            pending: self.pending.lock().expect("continuity pending lock").len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use scripture::{ProducerId, Record, Submission};

    use super::*;

    fn submission(sequence: u64) -> Submission {
        Submission {
            producer_id: ProducerId::from_bytes(*b"cont-producer!!!"),
            producer_epoch: 1,
            sequence,
            records: vec![Record::new([], Bytes::from_static(b"x"))],
        }
    }

    #[test]
    fn admit_retains_until_committed() {
        let outbox = ContinuityOutbox::new(8);
        let id = outbox.admit(submission(0)).expect("admit");
        assert_eq!(outbox.snapshot().pending, 1);
        outbox.mark_committed(id);
        assert!(outbox.fully_drained());
    }

    #[test]
    fn rejects_duplicate_and_empty() {
        let outbox = ContinuityOutbox::new(8);
        outbox.admit(submission(0)).expect("first");
        assert!(matches!(
            outbox.admit(submission(0)),
            Err(ContinuityError::DuplicateIdentity)
        ));
        let empty = Submission {
            producer_id: ProducerId::from_bytes(*b"cont-producer!!!"),
            producer_epoch: 1,
            sequence: 1,
            records: vec![],
        };
        assert!(matches!(
            outbox.admit(empty),
            Err(ContinuityError::EmptySubmission)
        ));
    }
}
