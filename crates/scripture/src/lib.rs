//! Scripture's native durable-journal API over the Holylog kernel.
//!
//! This crate is intentionally small: canonical record batches, dense record
//! offsets, a single-writer append surface, direct ordered reads, explicit trim
//! gaps, and consumer-owned checkpoints. Directory services, filtering,
//! consumer groups, and cross-process writer fencing are not v0 features.

pub mod batch;
pub mod canon;
pub mod chunk;
pub mod chunklog;
pub mod clock;
pub mod driver;
pub mod journal;
pub mod model;
pub mod trace;

pub use batch::{Batch, CodecError, decode_batch, encode_batch, encoded_batch_len};
pub use canon::{
    CanonAuthorityError, CanonAuthoritySnapshot, CanonFence, CanonFenceError, CanonOwner, LineId,
    OwnerEndpoint, OwnerId, WitnessedCanonAuthority, observe_canon_authority,
    observe_canon_authority_witnessed,
};
pub use chunk::{
    Chunk, ChunkDigest, ChunkError, ChunkHeader, ChunkId, ChunkIndex, CohortId, Frame, FrameRef,
    ProducerId, SealedChunk, SubmissionRef, WriterId, decode_chunk, decode_frame, decode_index,
    encoded_chunk_len, seal_chunk, seal_single_frame_chunk,
};
pub use chunklog::{
    ChunkAppendAck, ChunkLogError, ChunkLogRecovery, ChunkLogWriter, RecoveredChunk, RecoveryBound,
    VirtualChunkLogRecovery,
};
pub use clock::{
    BatchAccumulator, BatchPolicy, Clock, ManualClock, ManualTimer, PushResult, SystemClock,
    SystemTimer, Timer,
};
pub use driver::{
    AckLevel, ChunkDriverActor, ChunkDriverHandle, ChunkPolicy, DriverError, DriverMetrics,
    PolicyError, Receipt, ReceiptFuture, Submission,
};
pub use journal::{
    AppendAck, JournalReader, JournalWriter, ReadError, ReadEvent, ReaderCheckpointError,
    RetentionAuthority, TrimGap, WriteError,
};
pub use model::{
    AttributeValue, Checkpoint, JournalId, JournalRecord, Record, RecordOffset, ResumeHint,
};
pub use trace::{CostScope, Effect, Event, Ledger, RejectReason, TerminalOutcome};
