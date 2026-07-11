use holylog::atomic::{AtomicLog, AtomicLogError, LogEntry};

use crate::batch::{Batch, CodecError, decode_batch, encode_batch};
use crate::model::{Checkpoint, JournalId, JournalRecord, Record, RecordOffset, ResumeHint};

/// Durable acknowledgement for one appended batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendAck {
    /// Holylog slot holding the batch.
    pub slot: u64,
    /// First acknowledged record offset.
    pub first_offset: RecordOffset,
    /// Offset immediately after the acknowledged records.
    pub next_offset: RecordOffset,
    /// Number of acknowledged records.
    pub record_count: u32,
}

/// Writer failures.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// Empty batches have no record range and are not written.
    #[error("cannot append an empty batch")]
    EmptyBatch,
    /// Record count does not fit the durable format.
    #[error("batch has too many records")]
    TooManyRecords,
    /// Durable format encoding failed.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// Holylog rejected or failed the append.
    #[error(transparent)]
    Log(#[from] AtomicLogError),
}

/// Single in-process v0 writer. This type is deliberately not `Clone` and is
/// not a distributed writer-exclusion mechanism.
pub struct JournalWriter {
    journal_id: JournalId,
    log: AtomicLog,
    next_offset: RecordOffset,
}

impl JournalWriter {
    /// Constructs the sole v0 writer at an explicitly established next offset.
    /// Cross-process restart requires a future fenced recovery protocol.
    #[must_use]
    pub fn new(journal_id: JournalId, log: AtomicLog, next_offset: RecordOffset) -> Self {
        Self {
            journal_id,
            log,
            next_offset,
        }
    }

    /// Returns the next record offset this writer will allocate.
    #[must_use]
    pub const fn next_offset(&self) -> RecordOffset {
        self.next_offset
    }

    /// Canonically encodes and durably appends one non-empty batch.
    pub async fn append_batch(&mut self, records: Vec<Record>) -> Result<AppendAck, WriteError> {
        if records.is_empty() {
            return Err(WriteError::EmptyBatch);
        }
        let record_count = u32::try_from(records.len()).map_err(|_| WriteError::TooManyRecords)?;
        let next_offset = self
            .next_offset
            .checked_add(records.len())
            .ok_or(CodecError::OffsetOverflow)?;
        let bytes = encode_batch(self.journal_id, self.next_offset, &records)?;
        let slot = self.log.append(bytes).await?.get();
        let ack = AppendAck {
            slot,
            first_offset: self.next_offset,
            next_offset,
            record_count,
        };
        self.next_offset = next_offset;
        Ok(ack)
    }

    /// Manually advances the logical trim point by Holylog slots.
    pub async fn trim_to_slot(&self, slot: u64) -> Result<u64, WriteError> {
        Ok(self.log.prefix_trim(slot).await?)
    }
}

/// A reader encountered a logical trim below its requested slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimGap {
    /// Slot the reader attempted.
    pub requested_slot: u64,
    /// First slot not logically trimmed.
    pub new_start_slot: u64,
    /// Offset expected before discovering the gap.
    pub expected_offset: RecordOffset,
}

/// One pull-reader outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadEvent {
    /// Next ordered record.
    Record(JournalRecord),
    /// Explicit loss of the requested prefix.
    Gap(TrimGap),
    /// No record exists below the latest checked tail.
    CaughtUp { next_offset: RecordOffset },
}

/// Checkpoint construction failures.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ReaderCheckpointError {
    /// Checkpoint belongs to another journal.
    #[error("checkpoint belongs to journal {actual}, expected {expected}")]
    JournalMismatch {
        expected: JournalId,
        actual: JournalId,
    },
    /// A non-zero offset needs a physical hint until a directory/index exists.
    #[error("checkpoint at non-zero offset requires a resume hint in v0")]
    MissingResumeHint,
}

/// Direct-reader failures.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// Holylog read/tail operation failed.
    #[error(transparent)]
    Log(#[from] AtomicLogError),
    /// Durable bytes failed format validation.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// Batch belongs to a different journal.
    #[error("batch belongs to journal {actual}, expected {expected}")]
    JournalMismatch {
        expected: JournalId,
        actual: JournalId,
    },
    /// Batch offsets do not continue from the reader's expected offset.
    #[error("batch starts at offset {actual}, expected {expected}")]
    OffsetDiscontinuity { expected: u64, actual: u64 },
    /// Resume hint points outside its durable batch.
    #[error("resume hint record index {record_index} is outside batch at slot {slot}")]
    InvalidResumeHint { slot: u64, record_index: u32 },
}

/// Client-direct ordered pull reader.
pub struct JournalReader {
    journal_id: JournalId,
    log: AtomicLog,
    slot: u64,
    record_index: u32,
    next_offset: RecordOffset,
    checked_tail: u64,
    cached: Option<(u64, Batch)>,
    after_gap: bool,
}

impl JournalReader {
    /// Starts at the beginning of a journal.
    #[must_use]
    pub fn from_start(journal_id: JournalId, log: AtomicLog) -> Self {
        Self {
            journal_id,
            log,
            slot: 0,
            record_index: 0,
            next_offset: RecordOffset::new(0),
            checked_tail: 0,
            cached: None,
            after_gap: false,
        }
    }

    /// Resumes from a consumer-owned next-record checkpoint.
    pub fn from_checkpoint(
        journal_id: JournalId,
        log: AtomicLog,
        checkpoint: Checkpoint,
    ) -> Result<Self, ReaderCheckpointError> {
        if checkpoint.journal_id != journal_id {
            return Err(ReaderCheckpointError::JournalMismatch {
                expected: journal_id,
                actual: checkpoint.journal_id,
            });
        }
        let hint = match checkpoint.resume_hint {
            Some(hint) => hint,
            None if checkpoint.next_offset.get() == 0 => ResumeHint::new(0, 0),
            None => return Err(ReaderCheckpointError::MissingResumeHint),
        };
        Ok(Self {
            journal_id,
            log,
            slot: hint.slot(),
            record_index: hint.record_index(),
            next_offset: checkpoint.next_offset,
            checked_tail: 0,
            cached: None,
            after_gap: false,
        })
    }

    /// Captures the next record to consume. The physical hint is replaceable.
    #[must_use]
    pub const fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            journal_id: self.journal_id,
            next_offset: self.next_offset,
            resume_hint: Some(ResumeHint::new(self.slot, self.record_index)),
        }
    }

    /// Explicitly refreshes the checked Holylog slot tail.
    pub async fn refresh_tail(&mut self) -> Result<u64, ReadError> {
        self.checked_tail = self.log.check_tail().await?.tail;
        Ok(self.checked_tail)
    }

    async fn load_batch(&mut self) -> Result<Option<ReadEvent>, ReadError> {
        if self.slot >= self.checked_tail {
            self.refresh_tail().await?;
            if self.slot >= self.checked_tail {
                return Ok(Some(ReadEvent::CaughtUp {
                    next_offset: self.next_offset,
                }));
            }
        }
        match self.log.read_next(self.slot, self.checked_tail).await {
            Ok(LogEntry { payload, .. }) => {
                let batch = decode_batch(&payload)?;
                if batch.journal_id != self.journal_id {
                    return Err(ReadError::JournalMismatch {
                        expected: self.journal_id,
                        actual: batch.journal_id,
                    });
                }
                let index = usize::try_from(self.record_index).map_err(|_| {
                    ReadError::InvalidResumeHint {
                        slot: self.slot,
                        record_index: self.record_index,
                    }
                })?;
                if index > batch.records.len() {
                    return Err(ReadError::InvalidResumeHint {
                        slot: self.slot,
                        record_index: self.record_index,
                    });
                }
                let actual = batch
                    .base_offset
                    .checked_add(index)
                    .ok_or(CodecError::OffsetOverflow)?;
                if self.after_gap {
                    self.next_offset = actual;
                    self.after_gap = false;
                } else if actual != self.next_offset {
                    return Err(ReadError::OffsetDiscontinuity {
                        expected: self.next_offset.get(),
                        actual: actual.get(),
                    });
                }
                self.cached = Some((self.slot, batch));
                Ok(None)
            }
            Err(AtomicLogError::Trimmed { trim_point, .. }) => {
                let gap = TrimGap {
                    requested_slot: self.slot,
                    new_start_slot: trim_point,
                    expected_offset: self.next_offset,
                };
                self.slot = trim_point;
                self.record_index = 0;
                self.cached = None;
                self.after_gap = true;
                Ok(Some(ReadEvent::Gap(gap)))
            }
            Err(error) => Err(ReadError::Log(error)),
        }
    }

    /// Pulls one record, explicit gap, or caught-up marker.
    pub async fn read_next(&mut self) -> Result<ReadEvent, ReadError> {
        loop {
            if self.cached.is_none()
                && let Some(event) = self.load_batch().await?
            {
                return Ok(event);
            }
            let Some((cached_slot, batch)) = &self.cached else {
                continue;
            };
            let index =
                usize::try_from(self.record_index).map_err(|_| ReadError::InvalidResumeHint {
                    slot: self.slot,
                    record_index: self.record_index,
                })?;
            if index == batch.records.len() {
                self.slot = cached_slot
                    .checked_add(1)
                    .ok_or(CodecError::OffsetOverflow)?;
                self.record_index = 0;
                self.cached = None;
                continue;
            }
            let record = batch.records[index].clone();
            let offset = self.next_offset;
            self.next_offset = self
                .next_offset
                .checked_add(1)
                .ok_or(CodecError::OffsetOverflow)?;
            self.record_index = self
                .record_index
                .checked_add(1)
                .ok_or(CodecError::Oversized)?;
            return Ok(ReadEvent::Record(JournalRecord {
                offset,
                attributes: record.attributes,
                payload: record.payload,
            }));
        }
    }
}
