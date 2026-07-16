//! Private owner work queues and dedup window types.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::channel::oneshot;

use crate::chunk::{ChunkId, ProducerId, SealedChunk};
use crate::model::RecordOffset;

use super::{AdmissionSender, DriverError, Receipt, Submission};

pub(super) enum Command {
    Submit {
        submission: Submission,
        admission: AdmissionSender,
    },
    Flush {
        responder: oneshot::Sender<Result<(), DriverError>>,
    },
}

pub(super) struct BlockedSubmission {
    pub(super) submission: Submission,
    pub(super) admission: AdmissionSender,
    pub(super) encoded_bytes: usize,
}

pub(super) struct PlacedSubmission {
    pub(super) submission: Submission,
    pub(super) first_offset: RecordOffset,
    #[allow(dead_code)] // retained for reservation accounting / metrics follow-ups
    pub(super) encoded_bytes: usize,
    pub(super) waiters: Vec<oneshot::Sender<Result<Receipt, DriverError>>>,
}

pub(super) struct OpenChunk {
    pub(super) placed: Vec<PlacedSubmission>,
    pub(super) encoded_bytes: usize,
    pub(super) started_at: Duration,
}

pub(super) struct SealedWork {
    pub(super) sealed: SealedChunk,
    pub(super) placed: Vec<PlacedSubmission>,
    pub(super) encoded_bytes: usize,
    pub(super) sealed_at: Duration,
}

/// Dedup window value: highest committed sequence, then per-sequence
/// `(first_offset, record_count, chunk_id, slot, canon_revision)`.
pub(super) type DedupEntry = (u64, BTreeMap<u64, (RecordOffset, u32, ChunkId, u64, u64)>);
pub(super) type DedupWindow = BTreeMap<(ProducerId, u32), DedupEntry>;
