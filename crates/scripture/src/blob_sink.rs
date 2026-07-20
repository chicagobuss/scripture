//! Scribe-level shared blob sink seam.
//!
//! Cross-Verse accumulation lives outside the driver: assignments enqueue
//! generation-free envelopes and receive committed receipts only after a cut
//! PUT and per-Verse fenced reference append. Nothing in the shared buffer is
//! acknowledged — that is what distinguishes it from the pre-commit spool.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::channel::oneshot;

use crate::chunk::{ChunkId, CohortId, SealedChunk, SubmissionRef};
use crate::chunklog::ChunkAppendAck;
use crate::dataref::DataRef;
use crate::driver::ChunkDriverHandle;
use crate::driver::DriverError;
use crate::model::{JournalId, Record, RecordOffset};

/// Stable envelope offered to a shared Scribe blob sink before generation binding.
#[derive(Debug, Clone)]
pub struct PendingBlobEnvelope {
    /// Assignment / Verse key used to route seal and append operations.
    pub verse_key: String,
    /// Stable chunk identity carried through to producer receipts.
    pub chunk_id: ChunkId,
    /// Dense offset allocated when the chunk was sealed for buffering.
    pub base_offset: RecordOffset,
    /// Journal carried by this one-frame chunk.
    pub journal_id: JournalId,
    /// Cohort policy for this chunk.
    pub cohort_id: CohortId,
    /// Records to seal at cut time under the active generation.
    pub records: Vec<Record>,
    /// Producer submission spans sealed with the records.
    pub submissions: Vec<SubmissionRef>,
}

/// One sealed chunk and DataRef awaiting a fenced log append during a cut.
#[derive(Debug, Clone)]
pub struct BlobSinkAppendItem {
    /// Sealed bytes for this placement.
    pub sealed: SealedChunk,
    /// Pointer into the shared blob for this placement.
    pub data_ref: DataRef,
}

/// Submission handed to a shared blob sink from one assignment driver.
pub struct BlobSinkSubmit {
    /// Envelope to buffer until the next cut.
    pub envelope: PendingBlobEnvelope,
    /// Encoded bytes reserved while the envelope waits in the sink.
    pub encoded_bytes: usize,
    /// Completes with the fenced append ack for this chunk only.
    pub completion: oneshot::Sender<Result<ChunkAppendAck, DriverError>>,
}

/// Shared Scribe blob sink fed by many assignment drivers.
pub trait BlobCommitSink: Send + Sync {
    /// Enqueues one envelope. Returns `BufferFull` when the Scribe ceiling or
    /// per-assignment fair share would be exceeded — never acknowledges.
    fn submit(
        self: Arc<Self>,
        item: BlobSinkSubmit,
    ) -> Pin<Box<dyn Future<Output = Result<(), DriverError>> + Send>>;

    /// Registers one assignment driver for seal/append during cuts.
    fn register_driver(&self, verse_key: &str, handle: ChunkDriverHandle) {
        let _ = (verse_key, handle);
    }
}
