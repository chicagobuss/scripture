//! Producer-edge continuity outbox for fleet routing changes.
//!
//! Unlike [`crate::SpoolCell`] (a Scribe-local pre-commit WAL that poisons on
//! forward failure), this outbox is the **producer** durability boundary:
//! temporary route / Scribe unavailability must retain pending work and retry
//! after route refresh. It never silently discards a locally durable admission.
//!
//! Durability contract (foundation / decision 0015):
//! - [`Self::admit`] returns only after the submission frame is appended **and
//!   synced** (`fsync` on [`crate::FileSpoolStorage`]).
//! - [`Self::mark_committed`] appends a progress frame and syncs before dropping
//!   the pending entry from the reconstructed view.
//! - Reopening the same store rebuilds pending = submissions without progress.
//!
//! Receipts:
//! - `admit` → locally durable (survives process restart under this outbox's disk)
//! - `drain` / successful forward → Canon-committed (or equivalent) observed

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use scripture::driver::{Receipt, Submission};
use scripture::model::JournalId;

use crate::file::FileSpoolStorage;
use crate::frame::SpoolFrame;
use crate::progress::ProgressIdentity;
use crate::storage::{InMemorySpoolStorage, SpoolError, SpoolStorage, ValidFrame};

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

    fn from_progress(identity: ProgressIdentity) -> Self {
        Self {
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
            sequence: identity.sequence,
        }
    }

    fn to_progress(self, journal_id: JournalId) -> ProgressIdentity {
        ProgressIdentity {
            journal_id,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            sequence: self.sequence,
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
#[derive(Debug, thiserror::Error)]
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
    /// Durable store append/sync/open failure.
    #[error(transparent)]
    Store(#[from] SpoolError),
}

/// Producer-edge continuity outbox backed by a [`SpoolStorage`] WAL.
///
/// [`Self::admit`] is the local-durable boundary: it does not return until
/// `append_frame` + `sync` succeed. Temporary forward failures leave the
/// durable submission in place for retry (no poison).
pub struct ContinuityOutbox<S: SpoolStorage> {
    journal_id: JournalId,
    max_pending: usize,
    storage: Mutex<S>,
    pending: Mutex<VecDeque<PendingEntry>>,
    local_durable: Mutex<BTreeSet<ContinuityId>>,
    committed: Mutex<BTreeSet<ContinuityId>>,
    /// Last committed receipt per identity (process-local evidence only).
    receipts: Mutex<BTreeMap<ContinuityId, Receipt>>,
}

fn drop_pending(pending: &mut VecDeque<PendingEntry>, id: ContinuityId) {
    pending.retain(|entry| entry.id != id);
}

fn rebuild_indexes(
    frames: &[ValidFrame],
) -> (
    VecDeque<PendingEntry>,
    BTreeSet<ContinuityId>,
    BTreeSet<ContinuityId>,
) {
    let mut submissions = BTreeMap::<ContinuityId, Submission>::new();
    let mut committed = BTreeSet::new();
    for valid in frames {
        match &valid.frame {
            SpoolFrame::Submission { submission, .. } => {
                let id = ContinuityId::from_submission(submission);
                submissions.insert(id, submission.clone());
            }
            SpoolFrame::Progress(identity) => {
                committed.insert(ContinuityId::from_progress(*identity));
            }
        }
    }
    let mut local_durable = BTreeSet::new();
    let mut pending = VecDeque::new();
    for (id, submission) in submissions {
        local_durable.insert(id);
        if !committed.contains(&id) {
            pending.push_back(PendingEntry { id, submission });
        }
    }
    // local_durable includes both pending and committed admissions.
    local_durable.extend(committed.iter().copied());
    (pending, local_durable, committed)
}

impl ContinuityOutbox<InMemorySpoolStorage> {
    /// In-memory outbox for fast tests (sync is a no-op durability stand-in).
    #[must_use]
    pub fn memory(journal_id: JournalId, max_pending: usize) -> Self {
        Self::from_storage(journal_id, max_pending, InMemorySpoolStorage::default())
            .expect("empty in-memory continuity outbox")
    }

    /// Backward-compatible lab constructor (synthetic journal id).
    #[must_use]
    pub fn new(max_pending: usize) -> Self {
        Self::memory(JournalId::from_bytes(*b"cont-outbox-jrnl"), max_pending)
    }
}

impl ContinuityOutbox<FileSpoolStorage> {
    /// Opens a directory-backed outbox; `admit` fsyncs before returning.
    ///
    /// Reopening the same path recovers pending submissions that lack progress.
    pub fn open_file(
        journal_id: JournalId,
        max_pending: usize,
        root: impl AsRef<Path>,
    ) -> Result<Self, ContinuityError> {
        let storage = FileSpoolStorage::open(root)?;
        Self::from_storage(journal_id, max_pending, storage)
    }
}

impl<S: SpoolStorage> ContinuityOutbox<S> {
    /// Builds an outbox over an existing store, reconstructing pending state.
    pub fn from_storage(
        journal_id: JournalId,
        max_pending: usize,
        storage: S,
    ) -> Result<Self, ContinuityError> {
        let (frames, _tail) = storage.scan_valid_frames()?;
        let (pending, local_durable, committed) = rebuild_indexes(&frames);
        Ok(Self {
            journal_id,
            max_pending: max_pending.max(1),
            storage: Mutex::new(storage),
            pending: Mutex::new(pending),
            local_durable: Mutex::new(local_durable),
            committed: Mutex::new(committed),
            receipts: Mutex::new(BTreeMap::new()),
        })
    }

    /// Durably admits a submission (append + sync before return).
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

        let frame = SpoolFrame::Submission {
            journal_id: self.journal_id,
            submission: submission.clone(),
        };
        {
            let mut storage = self.storage.lock().expect("continuity storage lock");
            storage.append_frame(&frame)?;
            storage.sync()?;
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

    /// Returns a clone of all pending entries in FIFO order.
    pub fn pending_snapshot(&self) -> Vec<PendingEntry> {
        self.pending
            .lock()
            .expect("continuity pending lock")
            .iter()
            .cloned()
            .collect()
    }

    /// Appends progress + sync, then drops the identity from pending.
    pub fn mark_committed(&self, id: ContinuityId) -> Result<(), ContinuityError> {
        let progress = SpoolFrame::Progress(id.to_progress(self.journal_id));
        {
            let mut storage = self.storage.lock().expect("continuity storage lock");
            storage.append_frame(&progress)?;
            storage.sync()?;
        }
        self.committed
            .lock()
            .expect("continuity committed lock")
            .insert(id);
        let mut pending = self.pending.lock().expect("continuity pending lock");
        drop_pending(&mut pending, id);
        Ok(())
    }

    /// Marks committed, syncs progress, and retains the receipt in-process.
    pub fn mark_committed_with_receipt(
        &self,
        id: ContinuityId,
        receipt: Receipt,
    ) -> Result<(), ContinuityError> {
        self.mark_committed(id)?;
        self.receipts
            .lock()
            .expect("continuity receipts lock")
            .insert(id, receipt);
        Ok(())
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
    use std::time::{SystemTime, UNIX_EPOCH};

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

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"cont-outbox-jrnl")
    }

    fn tempfile_dir(tag: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("scripture-cont-{tag}-{nanos}"));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("mkdir");
        path
    }

    #[test]
    fn admit_retains_until_committed() {
        let outbox = ContinuityOutbox::memory(journal(), 8);
        let id = outbox.admit(submission(0)).expect("admit");
        assert_eq!(outbox.snapshot().pending, 1);
        outbox.mark_committed(id).expect("commit");
        assert!(outbox.fully_drained());
    }

    #[test]
    fn rejects_duplicate_and_empty() {
        let outbox = ContinuityOutbox::memory(journal(), 8);
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

    #[test]
    fn file_outbox_survives_reopen_with_pending_and_progress() {
        let dir = tempfile_dir("survive");
        let id = {
            let outbox = ContinuityOutbox::open_file(journal(), 8, &dir).expect("open");
            let first = outbox.admit(submission(0)).expect("admit 0");
            let second = outbox.admit(submission(1)).expect("admit 1");
            outbox.mark_committed(first).expect("commit 0");
            assert_eq!(outbox.snapshot().pending, 1);
            second
        };

        let reopened = ContinuityOutbox::open_file(journal(), 8, &dir).expect("reopen");
        let snap = reopened.snapshot();
        assert!(snap.committed.contains(&ContinuityId::from_submission(
            &submission(0)
        )));
        assert_eq!(snap.pending, 1);
        assert_eq!(reopened.peek_pending().expect("pending").id, id);
        reopened.mark_committed(id).expect("commit 1");
        assert!(reopened.fully_drained());

        let final_open = ContinuityOutbox::open_file(journal(), 8, &dir).expect("final");
        assert!(final_open.fully_drained());
        assert_eq!(final_open.snapshot().pending, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
