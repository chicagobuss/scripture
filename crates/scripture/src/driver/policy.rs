//! Chunk policy hard limits and validation.

use std::time::Duration;

use bytes::Bytes;

use crate::chunk::{Frame, ProducerId, SubmissionRef, encoded_chunk_len};
use crate::chunklog::RecoveryBound;
use crate::model::{JournalId, Record, RecordOffset};

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
