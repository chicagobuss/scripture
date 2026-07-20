//! Pre-commit spool WAL: early `spooled` receipts feeding [`BlobEnvelopeSource`].
//!
//! Persist a generation-free envelope, **fsync before** issuing `spooled`, replay
//! idempotently under the currently active Verse generation, and retire the entry
//! only after observing `committed`. Capacity is reserved before the ack; a full
//! spool with `on_full: reject` never acknowledges then evicts.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use scripture::{
    AchievedProfile, AdmitPlan, ChunkId, CohortId, JournalId, ProducerReceipt, ProgressIdentity,
    ReceiptPolicyError, ReceiptRequirement, ScribeSpoolCapability, SpoolConfig, SpoolError,
    SpoolFrame, SpoolOnFull, SpoolStorage, SpooledReceipt, Submission, SubmissionRef,
    VerseReceiptPolicy, encoded_frame_bytes, plan_admission, scan_and_classify,
};

use crate::blob_writer::{BlobEnvelope, BlobEnvelopeSource, BlobWriterError};

/// Configuration for a pre-commit envelope WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreCommitSpoolConfig {
    /// Physical WAL limits (shared with S1a storage).
    pub wal: SpoolConfig,
    /// Scribe-local capability; required when issuing `spooled`.
    pub capability: ScribeSpoolCapability,
}

impl PreCommitSpoolConfig {
    /// Validates WAL limits and the published loss budget.
    pub fn validate(&self) -> Result<(), PreCommitSpoolError> {
        self.wal.validate().map_err(PreCommitSpoolError::Spool)?;
        self.capability
            .validate()
            .map_err(PreCommitSpoolError::Policy)?;
        Ok(())
    }
}

/// Pre-commit WAL failures.
#[derive(Debug, thiserror::Error)]
pub enum PreCommitSpoolError {
    /// Underlying spool storage failure.
    #[error(transparent)]
    Spool(#[from] SpoolError),
    /// Receipt policy / capability rejection.
    #[error(transparent)]
    Policy(#[from] ReceiptPolicyError),
    /// Internal invariant.
    #[error("pre-commit spool invariant: {0}")]
    Invariant(String),
}

impl From<PreCommitSpoolError> for BlobWriterError {
    fn from(value: PreCommitSpoolError) -> Self {
        BlobWriterError::Source(value.to_string())
    }
}

#[derive(Debug, Clone)]
struct PendingEntry {
    identity: ProgressIdentity,
    envelope: BlobEnvelope,
    /// True once yielded by [`BlobEnvelopeSource`] at least once (replay may
    /// yield again until `committed` retires the entry).
    drained: bool,
}

/// Ordering probe for tests that assert fsync precedes the ack.
#[derive(Debug, Default, Clone)]
pub struct AdmitOrderLog {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl AdmitOrderLog {
    /// Creates an empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of recorded events.
    #[must_use]
    pub fn snapshot(&self) -> Vec<&'static str> {
        self.events.lock().expect("order").clone()
    }

    fn push(&self, event: &'static str) {
        self.events.lock().expect("order").push(event);
    }
}

/// Pre-commit envelope WAL with restart replay and early `spooled` acks.
pub struct PreCommitSpool<S> {
    config: PreCommitSpoolConfig,
    storage: Mutex<S>,
    pending: Mutex<VecDeque<PendingEntry>>,
    known: Mutex<BTreeSet<ProgressIdentity>>,
    committed: Mutex<BTreeSet<ProgressIdentity>>,
    reserved_bytes: Mutex<usize>,
    order: Option<AdmitOrderLog>,
}

impl<S: SpoolStorage> PreCommitSpool<S> {
    /// Opens storage, rebuilds pending envelopes from durable frames, and serves.
    ///
    /// Unlike the S1a cell, a non-empty WAL is normal: pending entries are
    /// replayed through [`BlobEnvelopeSource`].
    pub fn open(config: PreCommitSpoolConfig, mut storage: S) -> Result<Self, PreCommitSpoolError> {
        config.validate()?;
        let (frames, _tail) = storage.scan_valid_frames()?;
        let mut pending = VecDeque::new();
        let mut known = BTreeSet::new();
        let mut committed = BTreeSet::new();
        let mut by_id: BTreeMap<ProgressIdentity, BlobEnvelope> = BTreeMap::new();

        for entry in &frames {
            match &entry.frame {
                SpoolFrame::PreCommit {
                    verse_key,
                    chunk_id,
                    journal_id,
                    cohort_id,
                    submission,
                } => {
                    let identity = ProgressIdentity {
                        journal_id: *journal_id,
                        producer_id: submission.producer_id,
                        producer_epoch: submission.producer_epoch,
                        sequence: submission.sequence,
                    };
                    known.insert(identity);
                    by_id.insert(
                        identity,
                        envelope_from_parts(
                            verse_key.clone(),
                            *chunk_id,
                            *journal_id,
                            *cohort_id,
                            submission.clone(),
                        ),
                    );
                }
                SpoolFrame::Progress(identity) => {
                    committed.insert(*identity);
                }
                SpoolFrame::Submission { .. } => {
                    // S1a frames are not replayed by this path.
                }
            }
        }

        for (identity, envelope) in by_id {
            if !committed.contains(&identity) {
                pending.push_back(PendingEntry {
                    identity,
                    envelope,
                    drained: false,
                });
            }
        }

        storage.set_faults(Default::default());
        Ok(Self {
            config,
            storage: Mutex::new(storage),
            pending: Mutex::new(pending),
            known: Mutex::new(known),
            committed: Mutex::new(committed),
            reserved_bytes: Mutex::new(0),
            order: None,
        })
    }

    /// Attaches an ordering probe (tests assert fsync-before-ack).
    pub fn with_order_log(mut self, order: AdmitOrderLog) -> Self {
        self.order = Some(order);
        self
    }

    /// Published loss budget for disclosure (`scripture doctor`).
    #[must_use]
    pub fn loss_budget(&self) -> std::time::Duration {
        self.config.capability.loss_budget
    }

    /// True when `identity` still awaits `committed` (durable frame not retired).
    #[must_use]
    pub fn is_pending(&self, identity: &ProgressIdentity) -> bool {
        self.pending
            .lock()
            .expect("pending")
            .iter()
            .any(|entry| entry.identity == *identity)
    }

    /// Durable pending count (survives process restart via WAL scan).
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.lock().expect("pending").len()
    }

    /// Plans and, when appropriate, persists + fsyncs then returns `spooled`.
    ///
    /// When the plan is [`AdmitPlan::WaitForCommitted`], returns `Ok(None)` so
    /// the caller waits on the Canon path — never issues a weaker ack.
    pub fn admit_for_receipt(
        &self,
        policy: &VerseReceiptPolicy,
        requested: Option<ReceiptRequirement>,
        envelope: BlobEnvelope,
    ) -> Result<Option<ProducerReceipt>, PreCommitSpoolError> {
        match plan_admission(policy, requested, Some(&self.config.capability))? {
            AdmitPlan::WaitForCommitted => Ok(None),
            AdmitPlan::IssueSpooled => {
                let receipt = self.persist_and_ack_spooled(envelope)?;
                Ok(Some(ProducerReceipt::Spooled(receipt)))
            }
        }
    }

    /// Persist + fsync + `spooled` without re-checking Verse policy (caller already planned).
    pub fn persist_and_ack_spooled(
        &self,
        envelope: BlobEnvelope,
    ) -> Result<SpooledReceipt, PreCommitSpoolError> {
        let identity = identity_of(&envelope)?;
        {
            let known = self.known.lock().expect("known");
            if known.contains(&identity) {
                return Err(SpoolError::DuplicateIdentity.into());
            }
        }

        let frame = SpoolFrame::PreCommit {
            verse_key: envelope.verse_key.clone(),
            chunk_id: envelope.chunk_id,
            journal_id: envelope.journal_id,
            cohort_id: envelope.cohort_id,
            submission: submission_of(&envelope)?,
        };
        let encoded = encoded_frame_bytes(&frame)?;
        if encoded.len() > self.config.wal.max_frame_bytes {
            return Err(SpoolError::CapacityExceeded.into());
        }

        self.reserve(encoded.len())?;
        {
            let mut storage = self.storage.lock().expect("storage");
            if let Some(order) = &self.order {
                order.push("append");
            }
            match storage.append_frame(&frame) {
                Ok(()) => {}
                Err(error) => {
                    self.release(encoded.len());
                    return Err(error.into());
                }
            }
            if let Some(order) = &self.order {
                order.push("sync");
            }
            if let Err(error) = storage.sync() {
                self.release(encoded.len());
                return Err(error.into());
            }
        }

        self.known.lock().expect("known").insert(identity);
        self.pending
            .lock()
            .expect("pending")
            .push_back(PendingEntry {
                identity,
                envelope,
                drained: false,
            });

        if let Some(order) = &self.order {
            order.push("ack");
        }
        Ok(SpooledReceipt::new(
            self.config.capability.scribe_id.clone(),
            identity,
        ))
    }

    /// Records that Canon commit was observed; retires the pending entry.
    ///
    /// The durable Progress frame is written so a restarted process will not
    /// replay the envelope. Physical byte reclamation is left to segment GC.
    pub fn observe_committed(&self, identity: ProgressIdentity) -> Result<(), PreCommitSpoolError> {
        if self
            .committed
            .lock()
            .expect("committed")
            .contains(&identity)
        {
            return Ok(());
        }
        let progress = SpoolFrame::Progress(identity);
        let encoded = encoded_frame_bytes(&progress)?;
        self.reserve(encoded.len())?;
        {
            let mut storage = self.storage.lock().expect("storage");
            if let Err(error) = storage.append_frame(&progress) {
                self.release(encoded.len());
                return Err(error.into());
            }
            if let Err(error) = storage.sync() {
                self.release(encoded.len());
                return Err(error.into());
            }
        }
        self.committed.lock().expect("committed").insert(identity);
        self.pending
            .lock()
            .expect("pending")
            .retain(|entry| entry.identity != identity);
        Ok(())
    }

    /// Scan-time classification (diagnostics).
    pub fn recovery_pending_unclassified(&self) -> Result<usize, PreCommitSpoolError> {
        let storage = self.storage.lock().expect("storage");
        Ok(scan_and_classify(&*storage)?.pending_unclassified)
    }

    fn reserve(&self, bytes: usize) -> Result<(), PreCommitSpoolError> {
        let mut reserved = self.reserved_bytes.lock().expect("reserved");
        let storage = self.storage.lock().expect("storage");
        let used = storage
            .used_bytes()
            .checked_add(*reserved)
            .ok_or(SpoolError::CapacityExceeded)?;
        let next = used
            .checked_add(bytes)
            .ok_or(SpoolError::CapacityExceeded)?;
        if next > self.config.wal.max_wal_bytes
            || storage.frame_count().saturating_add(1) > self.config.wal.max_frames
        {
            match self.config.capability.on_full {
                SpoolOnFull::Reject => return Err(SpoolError::CapacityExceeded.into()),
            }
        }
        *reserved = reserved
            .checked_add(bytes)
            .ok_or(SpoolError::CapacityExceeded)?;
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let mut reserved = self.reserved_bytes.lock().expect("reserved");
        *reserved = reserved.saturating_sub(bytes);
    }
}

#[async_trait]
impl<S: SpoolStorage + Send> BlobEnvelopeSource for PreCommitSpool<S> {
    async fn next_envelope(&mut self) -> Result<Option<BlobEnvelope>, BlobWriterError> {
        let mut pending = self.pending.lock().expect("pending");
        // Prefer not-yet-drained entries; allow re-drain of still-pending ones
        // only when the queue is entirely drained-but-uncommitted (idempotent
        // second drain yields the same envelopes again until observe_committed).
        if let Some(entry) = pending.iter_mut().find(|entry| !entry.drained) {
            entry.drained = true;
            return Ok(Some(entry.envelope.clone()));
        }
        if let Some(entry) = pending.front_mut() {
            entry.drained = true;
            return Ok(Some(entry.envelope.clone()));
        }
        Ok(None)
    }
}

/// Reset drain markers so a second drain pass can run (idempotent drain test).
pub fn reset_drain_markers<S>(spool: &PreCommitSpool<S>) {
    for entry in spool.pending.lock().expect("pending").iter_mut() {
        entry.drained = false;
    }
}

fn identity_of(envelope: &BlobEnvelope) -> Result<ProgressIdentity, PreCommitSpoolError> {
    let submission = envelope.submissions.first().ok_or_else(|| {
        PreCommitSpoolError::Invariant("envelope requires at least one submission".into())
    })?;
    Ok(ProgressIdentity {
        journal_id: envelope.journal_id,
        producer_id: submission.producer_id,
        producer_epoch: submission.producer_epoch,
        sequence: submission.sequence,
    })
}

fn submission_of(envelope: &BlobEnvelope) -> Result<Submission, PreCommitSpoolError> {
    let submission = envelope.submissions.first().ok_or_else(|| {
        PreCommitSpoolError::Invariant("envelope requires at least one submission".into())
    })?;
    if envelope.submissions.len() != 1 {
        return Err(PreCommitSpoolError::Invariant(
            "pre-commit V1 stores one submission per envelope".into(),
        ));
    }
    Ok(Submission {
        producer_id: submission.producer_id,
        producer_epoch: submission.producer_epoch,
        sequence: submission.sequence,
        records: envelope.records.clone(),
    })
}

fn envelope_from_parts(
    verse_key: String,
    chunk_id: ChunkId,
    journal_id: JournalId,
    cohort_id: CohortId,
    submission: Submission,
) -> BlobEnvelope {
    let record_count = u32::try_from(submission.records.len()).unwrap_or(u32::MAX);
    BlobEnvelope {
        verse_key,
        chunk_id,
        journal_id,
        cohort_id,
        records: submission.records,
        submissions: vec![SubmissionRef {
            producer_id: submission.producer_id,
            producer_epoch: submission.producer_epoch,
            sequence: submission.sequence,
            first_record: 0,
            record_count,
        }],
    }
}

/// Evaluate a producer request when this Scribe has **no** spool mounted.
///
/// A `spooled`-permitting Verse still serves: wait for `committed` (stronger
/// satisfies) rather than refusing activation.
pub fn plan_without_spool(
    policy: &VerseReceiptPolicy,
    requested: Option<ReceiptRequirement>,
) -> Result<AdmitPlan, ReceiptPolicyError> {
    plan_admission(policy, requested, None)
}

/// Build a `committed` producer receipt that reports the stronger achieved profile
/// when the producer only asked for `spooled`.
pub fn committed_receipt_for(
    envelope: &BlobEnvelope,
    first_offset: scripture::RecordOffset,
    next_offset: scripture::RecordOffset,
    slot: u64,
    canon_revision: u64,
) -> Result<ProducerReceipt, PreCommitSpoolError> {
    let submission = envelope.submissions.first().ok_or_else(|| {
        PreCommitSpoolError::Invariant("envelope requires at least one submission".into())
    })?;
    Ok(ProducerReceipt::Committed(scripture::CommittedReceipt {
        profile: AchievedProfile::Committed,
        journal_id: envelope.journal_id,
        first_offset,
        next_offset,
        chunk_id: envelope.chunk_id,
        slot,
        canon_revision,
        producer_id: submission.producer_id,
        producer_epoch: submission.producer_epoch,
        sequence: submission.sequence,
    }))
}
