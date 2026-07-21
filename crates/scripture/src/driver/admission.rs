//! Producer admission: epoch/sequence, dedup, reservation, parked work.

use std::time::Duration;

use bytes::Bytes;
use futures::channel::oneshot;

use crate::chunk::{ChunkError, Frame, ProducerId, SubmissionRef, encoded_chunk_len};
use crate::model::{Record, RecordOffset, canonical_records_digest};
use crate::trace::{Event, RejectReason};

use super::state::{BlockedSubmission, OpenChunk, PendingAppend, PlacedSubmission};
use super::{AckLevel, AdmissionSender, ChunkDriverActor, DriverError, Receipt, Submission};

impl<C: crate::clock::Clock, T: crate::clock::Timer> ChunkDriverActor<C, T> {
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
            .and_then(|pending| match pending {
                PendingAppend::DepthOne(work) => Some(work.sealed_at),
                PendingAppend::BlobSink { .. } => None,
            });
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
            .map_or(0, PendingAppend::encoded_bytes);
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

    pub(super) fn drain_blocked(&mut self) {
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
            for placed in pending.placed() {
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

    pub(super) fn admit(&mut self, submission: Submission, admission: AdmissionSender) {
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
        let canonical_digest = canonical_records_digest(&submission.records);

        // A submission is one atomic unit. If it cannot fit in a single chunk,
        // reject it — never split it across chunks (that would invent two
        // identities for one producer sequence).
        if submission.records.len() > self.policy.max_chunk_records
            || encoded_bytes > self.policy.max_chunk_bytes
        {
            self.ledger.event(Event::SubmissionRejected {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
                reason: RejectReason::SubmissionTooLarge,
            });
            let _ = admission.send(Err(DriverError::SubmissionTooLarge {
                records: submission.records.len(),
                encoded_bytes,
                max_records: self.policy.max_chunk_records,
                max_bytes: self.policy.max_chunk_bytes,
            }));
            self.bump_rejected();
            return;
        }

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
            if let Some(receipt) = window.get(&submission.sequence) {
                if receipt.submission_digest != canonical_digest {
                    self.ledger.event(Event::SubmissionRejected {
                        producer_id: submission.producer_id,
                        sequence: submission.sequence,
                        reason: RejectReason::OutOfSequence,
                    });
                    let _ = admission.send(Err(DriverError::IdentityConflict {
                        producer_id: submission.producer_id,
                        producer_epoch: submission.producer_epoch,
                        sequence: submission.sequence,
                    }));
                    self.bump_rejected();
                    return;
                }
                let first_offset = receipt.first_offset;
                let records = receipt.record_count;
                let chunk_id = receipt.chunk_id;
                let slot = receipt.slot;
                let canon_revision = receipt.canon_revision;
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
                    canon_revision,
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
                    if canonical_records_digest(&placed.submission.records) != canonical_digest {
                        let _ = admission.send(Err(DriverError::IdentityConflict {
                            producer_id: submission.producer_id,
                            producer_epoch: submission.producer_epoch,
                            sequence: submission.sequence,
                        }));
                        self.bump_rejected();
                        return;
                    }
                    let (tx, rx) = oneshot::channel();
                    placed.waiters.push(tx);
                    let _ = admission.send(Ok(rx));
                    return;
                }
            }
        }
        if let Some(pending) = self.pending_append.as_mut() {
            for placed in pending.placed_mut() {
                if placed.submission.producer_id == submission.producer_id
                    && placed.submission.producer_epoch == submission.producer_epoch
                    && placed.submission.sequence == submission.sequence
                {
                    if canonical_records_digest(&placed.submission.records) != canonical_digest {
                        let _ = admission.send(Err(DriverError::IdentityConflict {
                            producer_id: submission.producer_id,
                            producer_epoch: submission.producer_epoch,
                            sequence: submission.sequence,
                        }));
                        self.bump_rejected();
                        return;
                    }
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
            .map_or(0, PendingAppend::encoded_bytes);

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
    fn bump_rejected(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.rejected = metrics.rejected.saturating_add(1);
        }
    }
}
