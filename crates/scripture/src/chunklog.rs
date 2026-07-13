//! The real Holylog boundary for immutable Scripture chunks.
//!
//! A [`ChunkLogWriter`] owns offset allocation for one journal in one AtomicLog
//! generation.  It deliberately has no retry path: an AtomicLog append whose
//! caller does not observe `Ok` may have acquired a slot, and retrying it on the
//! same log can permanently wedge later completion.  The future driver owns
//! this writer and poisons on every non-OK append outcome.

use holylog::atomic::{AtomicLog, AtomicLogError, SealStatus};
use holylog::virtual_log::{VirtualLog, VirtualLogError};

use crate::canon::{
    CanonAuthorityError, CanonAuthoritySnapshot, LineId, OwnerId, observe_canon_authority,
};
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
    /// Holylog position that contains the chunk: a local AtomicLog address for
    /// Phase 1, or a global VirtualLog position for a fenced Canon Line.
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
    /// The decoded immutable identity.
    pub chunk_id: ChunkId,
    /// Content digest of the durable bytes.
    pub digest: ChunkDigest,
    /// Chunk header generation (Canon revision for VirtualLog-backed writers).
    ///
    /// Preserved so deduplicated receipts after recovery can name the generation
    /// that originally accepted the submission, not the active successor.
    pub generation: u64,
    /// The start of the record span.
    pub first_offset: RecordOffset,
    /// Number of records in the span.
    pub record_count: u32,
    /// The sole decoded frame, including its [`crate::chunk::SubmissionRef`]s.
    ///
    /// Callers rebuild the producer dedup window from these spans without a
    /// second Holylog read.
    pub frame: crate::chunk::FrameRef,
}

/// Result of rebuilding one writer from a bounded durable suffix.
#[derive(Debug)]
pub struct ChunkLogRecovery {
    /// The rebuilt single-owner writer.
    pub writer: ChunkLogWriter,
    /// Durable suffix in ascending slot order, for producer dedup recovery.
    pub chunks: Vec<RecoveredChunk>,
}

/// VirtualLog recovery result for one fenced Canon observation.
///
/// [`Self::authority`] is the observation used to start this attempt, not a
/// forever lease. A later register advance or seal fence invalidates it.
#[derive(Debug)]
pub struct VirtualChunkLogRecovery {
    /// Writer bound to the active Canon revision.
    pub writer: ChunkLogWriter,
    /// Bounded durable suffix in global logical order.
    pub chunks: Vec<RecoveredChunk>,
    /// Fresh authority observation that authorized this recovery attempt.
    pub authority: CanonAuthoritySnapshot,
}

/// Errors at the chunk-to-Holylog boundary.
#[derive(Debug, thiserror::Error)]
pub enum ChunkLogError {
    /// An AtomicLog-backed Holylog failed, rejected the append, or observed a seal.
    #[error(transparent)]
    AtomicLog(#[from] AtomicLogError),
    /// A VirtualLog-backed Holylog failed or fenced this owner at a seal.
    #[error(transparent)]
    VirtualLog(#[from] VirtualLogError),
    /// Fresh Canon authority observation refused this owner.
    #[error(transparent)]
    Authority(#[from] CanonAuthorityError),
    /// The cached VirtualLog membership no longer names this writer generation.
    #[error("VirtualLog generation changed: expected {expected}, observed {actual}")]
    VirtualGenerationChanged {
        /// Generation encoded in this writer's chunks.
        expected: u64,
        /// Membership revision currently cached by VirtualLog.
        actual: u64,
    },
    /// Canon/VirtualLog revision changed while recovery was reading.
    #[error(
        "Canon recovery invalidated: started at revision {expected}, observed {observed} before returning a writer"
    )]
    StaleCanonRecovery {
        /// Revision that authorized the attempt.
        expected: u64,
        /// Revision observed at the end of the attempt.
        observed: u64,
    },
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
    /// A recovered VirtualLog chunk claims a generation after the inspected Canon.
    #[error(
        "recovered chunk generation {actual} is after active Canon revision {active}; authority is inconsistent"
    )]
    FutureChunkGeneration {
        /// Active Canon revision from the recovery observation.
        active: u64,
        /// Generation encoded in the chunk header.
        actual: u64,
    },
    /// Recovered VirtualLog history moved backward to an older chunk generation.
    #[error(
        "recovered chunk generation regressed from {previous} to {actual}; refusing stale or corrupt history"
    )]
    RecoveredGenerationRegression {
        /// Highest generation already observed earlier in logical order.
        previous: u64,
        /// Lower generation carried by the later recovered chunk.
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
    log: ChunkLog,
    next_offset: RecordOffset,
    poisoned: bool,
}

/// The Holylog scope used by one chunk writer.
///
/// The Atomic variant is retained for the Phase 1 local-owner laboratory. The
/// Virtual variant routes through the Conflux generation chain and receives a
/// seal fence whenever a Canon cutover has replaced its Line.
#[derive(Debug)]
enum ChunkLog {
    Atomic(AtomicLog),
    Virtual(VirtualLog),
}

impl ChunkLog {
    async fn append(
        &self,
        bytes: bytes::Bytes,
        expected_generation: u64,
    ) -> Result<u64, ChunkLogError> {
        match self {
            Self::Atomic(log) => Ok(log.append(bytes).await?.get()),
            Self::Virtual(log) => {
                // This catches an in-process owner whose shared VirtualLog
                // cache was advanced by a reconfigurer. A remote stale owner
                // keeps its old cache and is fenced by the predecessor seal in
                // VirtualLog::append instead.
                let actual = log.cached_membership().await?.revision;
                if actual != expected_generation {
                    return Err(ChunkLogError::VirtualGenerationChanged {
                        expected: expected_generation,
                        actual,
                    });
                }
                Ok(log.append(bytes).await?.position)
            }
        }
    }

    async fn checked_tail(&self) -> Result<u64, ChunkLogError> {
        match self {
            Self::Atomic(log) => Ok(log.check_tail().await?.tail),
            Self::Virtual(log) => Ok(log.check_tail().await?.tail),
        }
    }

    async fn read_payload(&self, min: u64, max: u64) -> Result<bytes::Bytes, ChunkLogError> {
        match self {
            Self::Atomic(log) => Ok(log.read_next(min, max).await?.payload),
            Self::Virtual(log) => Ok(log.read_next(min, max).await?.payload),
        }
    }
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
            log: ChunkLog::Atomic(log),
            next_offset,
            poisoned: false,
        }
    }

    /// Constructs a writer over a fenced Holylog VirtualLog generation.
    ///
    /// The caller must obtain and validate a fresh Scripture Canon fence before
    /// construction. If a later Canon cutover advances a shared membership
    /// cache, the next append returns [`ChunkLogError::VirtualGenerationChanged`].
    /// A remote stale owner instead reaches the sealed predecessor and returns
    /// [`VirtualLogError::StaleGeneration`]. In either case the writer is
    /// poisoned and must be replaced through recovery rather than retried.
    #[must_use]
    pub fn new_virtual(
        journal_id: JournalId,
        cohort_id: CohortId,
        generation: u64,
        log: VirtualLog,
        next_offset: RecordOffset,
    ) -> Self {
        Self {
            journal_id,
            cohort_id,
            generation,
            log: ChunkLog::Virtual(log),
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
        let slot = self
            .log
            .append(chunk.bytes.clone(), self.generation)
            .await?;
        self.poisoned = false;
        self.next_offset = next_offset;
        Ok(ChunkAppendAck {
            slot,
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
        let (writer, chunks) = Self::recover_from_log(
            journal_id,
            cohort_id,
            generation,
            ChunkLog::Atomic(log),
            bound,
            GenerationPolicy::Exact(generation),
        )
        .await?;
        Ok(ChunkLogRecovery { writer, chunks })
    }

    /// Rebuilds a VirtualLog-backed writer after a fenced Canon cutover.
    ///
    /// Starts from a fresh linearizable VirtualLog state and validates the
    /// Canon fence identity/owner before reading a bounded suffix. Historical
    /// chunk headers with generation `<=` the active Canon revision are
    /// accepted; a future generation fails closed. The returned writer always
    /// encodes the **active** Canon revision for new appends.
    ///
    /// If the register advances or the active generation is observed sealed
    /// while recovery reads, the attempt returns
    /// [`ChunkLogError::StaleCanonRecovery`] and never a writer.
    pub async fn recover_virtual(
        journal_id: JournalId,
        cohort_id: CohortId,
        expected_line_id: LineId,
        expected_owner_id: OwnerId,
        log: VirtualLog,
        bound: RecoveryBound,
    ) -> Result<VirtualChunkLogRecovery, ChunkLogError> {
        let authority =
            observe_canon_authority(&log, journal_id, expected_line_id, expected_owner_id).await?;
        let active_revision = authority.revision();

        let (writer, chunks) = Self::recover_from_log(
            journal_id,
            cohort_id,
            active_revision,
            ChunkLog::Virtual(log.clone()),
            bound,
            GenerationPolicy::AtMost(active_revision),
        )
        .await?;

        // Re-inspect before returning: never merge observations across revisions.
        let closing = log.state().await?;
        if closing.revision != active_revision {
            return Err(ChunkLogError::StaleCanonRecovery {
                expected: active_revision,
                observed: closing.revision,
            });
        }
        let closing_fence = crate::canon::CanonFence::from_virtual_log_state(&closing)
            .map_err(CanonAuthorityError::from)?;
        if closing_fence != authority.fence {
            return Err(ChunkLogError::StaleCanonRecovery {
                expected: active_revision,
                observed: closing.revision,
            });
        }

        Ok(VirtualChunkLogRecovery {
            writer,
            chunks,
            authority,
        })
    }

    async fn recover_from_log(
        journal_id: JournalId,
        cohort_id: CohortId,
        generation: u64,
        log: ChunkLog,
        bound: RecoveryBound,
        generation_policy: GenerationPolicy,
    ) -> Result<(Self, Vec<RecoveredChunk>), ChunkLogError> {
        let tail = match &log {
            ChunkLog::Atomic(_) => log.checked_tail().await?,
            ChunkLog::Virtual(virtual_log) => {
                let check = virtual_log.check_tail().await?;
                if check.seal_status == SealStatus::Sealed {
                    return Err(ChunkLogError::StaleCanonRecovery {
                        expected: generation,
                        observed: check.revision,
                    });
                }
                if check.revision != generation {
                    return Err(ChunkLogError::StaleCanonRecovery {
                        expected: generation,
                        observed: check.revision,
                    });
                }
                check.tail
            }
        };
        let start = tail.saturating_sub(bound.max_chunks() as u64);
        let mut chunks = Vec::new();
        let mut previous_virtual_generation = None;
        for slot in start..tail {
            let payload = match log.read_payload(slot, tail).await {
                Ok(payload) => payload,
                Err(ChunkLogError::VirtualLog(VirtualLogError::StaleGeneration { .. })) => {
                    let observed = match &log {
                        ChunkLog::Virtual(virtual_log) => virtual_log
                            .state()
                            .await
                            .map(|state| state.revision)
                            .unwrap_or_else(|_| generation.saturating_add(1)),
                        ChunkLog::Atomic(_) => generation.saturating_add(1),
                    };
                    return Err(ChunkLogError::StaleCanonRecovery {
                        expected: generation,
                        observed,
                    });
                }
                Err(error) => return Err(error),
            };
            let index = decode_index(&payload)?;
            if index.header.cohort_id != cohort_id {
                return Err(ChunkLogError::CohortMismatch);
            }
            match generation_policy {
                GenerationPolicy::Exact(expected) => {
                    if index.header.generation != expected {
                        return Err(ChunkLogError::GenerationMismatch {
                            expected,
                            actual: index.header.generation,
                        });
                    }
                }
                GenerationPolicy::AtMost(active) => {
                    if index.header.generation > active {
                        return Err(ChunkLogError::FutureChunkGeneration {
                            active,
                            actual: index.header.generation,
                        });
                    }
                    if let Some(previous) = previous_virtual_generation
                        && index.header.generation < previous
                    {
                        return Err(ChunkLogError::RecoveredGenerationRegression {
                            previous,
                            actual: index.header.generation,
                        });
                    }
                    previous_virtual_generation = Some(index.header.generation);
                }
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
                digest: ChunkDigest::of(&payload),
                generation: index.header.generation,
                first_offset: frame.base_offset,
                record_count: frame.record_count,
                frame: frame.clone(),
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
        let writer = Self {
            journal_id,
            cohort_id,
            generation,
            log,
            next_offset,
            poisoned: false,
        };
        Ok((writer, chunks))
    }
}

#[derive(Debug, Clone, Copy)]
enum GenerationPolicy {
    Exact(u64),
    AtMost(u64),
}
