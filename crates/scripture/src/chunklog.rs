//! The real Holylog boundary for immutable Scripture chunks.
//!
//! A [`ChunkLogWriter`] owns offset allocation for one journal in one AtomicLog
//! generation.  It deliberately has no retry path: an AtomicLog append whose
//! caller does not observe `Ok` may have acquired a slot, and retrying it on the
//! same log can permanently wedge later completion.  The future driver owns
//! this writer and poisons on every non-OK append outcome.

use holylog::atomic::{AtomicLog, AtomicLogError};

use crate::chunk::{ChunkDigest, ChunkError, ChunkId, CohortId, SealedChunk, decode_index};
use crate::model::{JournalId, RecordOffset};

/// A bounded number of tail chunks to inspect during owner recovery.
///
/// The bound limits producer-dedup reconstruction in the future driver.  It
/// also makes the recovery cost explicit: at most this many Holylog reads,
/// after one checked-tail operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryBound(usize);

impl RecoveryBound {
    /// Creates a non-zero recovery bound.
    pub const fn new(max_chunks: usize) -> Option<Self> {
        if max_chunks == 0 {
            None
        } else {
            Some(Self(max_chunks))
        }
    }

    /// The maximum number of chunks the recovery path may inspect.
    #[must_use]
    pub const fn max_chunks(self) -> usize {
        self.0
    }
}

/// Durable acknowledgement of a sealed chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkAppendAck {
    /// Holylog slot that contains the chunk.
    pub slot: u64,
    /// Stable identifier of the committed immutable bytes.
    pub chunk_id: ChunkId,
    /// First record offset in the chunk's sole journal frame.
    pub first_offset: RecordOffset,
    /// Offset after the chunk's last record.
    pub next_offset: RecordOffset,
    /// Number of records in the chunk.
    pub record_count: u32,
}

/// One recovered durable chunk, retained for the driver's bounded dedup scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredChunk {
    /// The Holylog position where the chunk became visible.
    pub slot: u64,
    /// The decoded immutable index.
    pub chunk_id: ChunkId,
    /// The start of the record span.
    pub first_offset: RecordOffset,
    /// Number of records in the span.
    pub record_count: u32,
}

/// Result of rebuilding one writer from a bounded durable suffix.
#[derive(Debug)]
pub struct ChunkLogRecovery {
    /// The rebuilt single-owner writer.
    pub writer: ChunkLogWriter,
    /// Durable suffix in ascending slot order, for producer dedup recovery.
    pub chunks: Vec<RecoveredChunk>,
}

/// Errors at the chunk-to-Holylog boundary.
#[derive(Debug, thiserror::Error)]
pub enum ChunkLogError {
    /// Holylog failed, rejected the append, or observed a seal.
    #[error(transparent)]
    Log(#[from] AtomicLogError),
    /// Chunk bytes are malformed.
    #[error(transparent)]
    Chunk(#[from] ChunkError),
    /// A chunk belongs to another cohort.
    #[error("chunk cohort does not match this writer")]
    CohortMismatch,
    /// A chunk was sealed for another AtomicLog generation.
    #[error("chunk generation {actual} does not match writer generation {expected}")]
    GenerationMismatch {
        /// Writer generation.
        expected: u64,
        /// Chunk generation.
        actual: u64,
    },
    /// Phase 1 requires exactly one frame for this writer's journal.
    #[error("phase-1 chunk must contain exactly one frame for journal {journal}")]
    JournalFrameMismatch {
        /// The writer's journal.
        journal: JournalId,
    },
    /// The chunk frame does not begin at the writer's next dense offset.
    #[error("chunk starts at offset {actual}, expected {expected}")]
    OffsetDiscontinuity {
        /// Expected next offset.
        expected: u64,
        /// Actual chunk base offset.
        actual: u64,
    },
    /// A recovered suffix is not dense for this journal.
    #[error("recovered chunk starts at offset {actual}, expected {expected}")]
    RecoveredOffsetDiscontinuity {
        /// Expected next offset in the scanned suffix.
        expected: u64,
        /// Actual offset.
        actual: u64,
    },
    /// A prior append had an unknown outcome; this writer cannot be reused.
    #[error("chunk writer is poisoned; recover a fenced successor")]
    Poisoned,
    /// The public sealed-chunk carrier did not agree with its immutable bytes.
    #[error("sealed chunk metadata does not match its bytes")]
    SealedMetadataMismatch,
}

/// One non-cloneable owner of chunk appends for a journal and generation.
#[derive(Debug)]
pub struct ChunkLogWriter {
    journal_id: JournalId,
    cohort_id: CohortId,
    generation: u64,
    log: AtomicLog,
    next_offset: RecordOffset,
    poisoned: bool,
}

impl ChunkLogWriter {
    /// Constructs a writer after ownership and its initial offset are known.
    #[must_use]
    pub fn new(
        journal_id: JournalId,
        cohort_id: CohortId,
        generation: u64,
        log: AtomicLog,
        next_offset: RecordOffset,
    ) -> Self {
        Self {
            journal_id,
            cohort_id,
            generation,
            log,
            next_offset,
            poisoned: false,
        }
    }

    /// The next dense record offset this writer will allocate.
    #[must_use]
    pub const fn next_offset(&self) -> RecordOffset {
        self.next_offset
    }

    /// Appends exactly one sealed frame for this writer's journal.
    ///
    /// Any non-OK result poisons the writer before awaiting Holylog.  The
    /// caller must discard it and recover under a fenced successor generation;
    /// it must not retry on this AtomicLog.
    pub async fn append(&mut self, chunk: &SealedChunk) -> Result<ChunkAppendAck, ChunkLogError> {
        if self.poisoned {
            return Err(ChunkLogError::Poisoned);
        }
        let index = decode_index(&chunk.bytes)?;
        if index.header.chunk_id != chunk.chunk_id || ChunkDigest::of(&chunk.bytes) != chunk.digest
        {
            return Err(ChunkLogError::SealedMetadataMismatch);
        }
        if index.header.cohort_id != self.cohort_id {
            return Err(ChunkLogError::CohortMismatch);
        }
        if index.header.generation != self.generation {
            return Err(ChunkLogError::GenerationMismatch {
                expected: self.generation,
                actual: index.header.generation,
            });
        }
        let [frame] = index.frames.as_slice() else {
            return Err(ChunkLogError::JournalFrameMismatch {
                journal: self.journal_id,
            });
        };
        if frame.journal_id != self.journal_id {
            return Err(ChunkLogError::JournalFrameMismatch {
                journal: self.journal_id,
            });
        }
        if frame.base_offset != self.next_offset {
            return Err(ChunkLogError::OffsetDiscontinuity {
                expected: self.next_offset.get(),
                actual: frame.base_offset.get(),
            });
        }
        let next_offset = frame
            .base_offset
            .checked_add(frame.record_count as usize)
            .ok_or(ChunkError::OffsetOverflow)?;

        self.poisoned = true;
        let slot = self.log.append(chunk.bytes.clone()).await?;
        self.poisoned = false;
        self.next_offset = next_offset;
        Ok(ChunkAppendAck {
            slot: slot.get(),
            chunk_id: chunk.chunk_id,
            first_offset: frame.base_offset,
            next_offset,
            record_count: frame.record_count,
        })
    }

    /// Rebuilds the writer and returns a bounded durable suffix for dedup.
    pub async fn recover(
        journal_id: JournalId,
        cohort_id: CohortId,
        generation: u64,
        log: AtomicLog,
        bound: RecoveryBound,
    ) -> Result<ChunkLogRecovery, ChunkLogError> {
        let tail = log.check_tail().await?.tail;
        let start = tail.saturating_sub(bound.max_chunks() as u64);
        let mut chunks = Vec::new();
        for slot in start..tail {
            let entry = log.read_next(slot, tail).await?;
            let index = decode_index(&entry.payload)?;
            if index.header.cohort_id != cohort_id {
                return Err(ChunkLogError::CohortMismatch);
            }
            if index.header.generation != generation {
                return Err(ChunkLogError::GenerationMismatch {
                    expected: generation,
                    actual: index.header.generation,
                });
            }
            let [frame] = index.frames.as_slice() else {
                return Err(ChunkLogError::JournalFrameMismatch {
                    journal: journal_id,
                });
            };
            if frame.journal_id != journal_id {
                return Err(ChunkLogError::JournalFrameMismatch {
                    journal: journal_id,
                });
            }
            chunks.push(RecoveredChunk {
                slot,
                chunk_id: index.header.chunk_id,
                first_offset: frame.base_offset,
                record_count: frame.record_count,
            });
        }

        let mut next_offset = chunks
            .first()
            .map_or(RecordOffset::new(0), |chunk| chunk.first_offset);
        for chunk in &chunks {
            if chunk.first_offset != next_offset {
                return Err(ChunkLogError::RecoveredOffsetDiscontinuity {
                    expected: next_offset.get(),
                    actual: chunk.first_offset.get(),
                });
            }
            next_offset = next_offset
                .checked_add(chunk.record_count as usize)
                .ok_or(ChunkError::OffsetOverflow)?;
        }
        let writer = Self::new(journal_id, cohort_id, generation, log, next_offset);
        Ok(ChunkLogRecovery { writer, chunks })
    }
}
