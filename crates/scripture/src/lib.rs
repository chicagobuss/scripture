//! Scripture's native durable-journal API over the Holylog kernel.
//!
//! This crate is intentionally small: canonical record batches, dense record
//! offsets, a single-writer append surface, direct ordered reads, explicit trim
//! gaps, and consumer-owned checkpoints. Directory services, filtering,
//! consumer groups, and cross-process writer fencing are not v0 features.

pub mod batch;
pub mod blob_store;
pub mod canon;
pub mod chunk;
pub mod chunklog;
pub mod clock;
pub mod dataref;
pub mod driver;
pub mod journal;
pub mod model;
pub mod receipt;
pub mod sequencer_key;
pub mod serving_authority;
pub mod spool;
pub mod trace;

pub use batch::{Batch, CodecError, decode_batch, encode_batch, encoded_batch_len};
pub use blob_store::{
    ChunkBlobStore, DEFAULT_STAGING_BLOB_PREFIX, DataRefBlobConfig, commit_sealed_as_data_ref,
};
pub use canon::{
    CanonAuthorityError, CanonAuthoritySnapshot, CanonFence, CanonFenceError, CanonOwner,
    OwnedSequencerBinding, OwnerEndpoint, OwnerId, VerseId, WitnessedCanonAuthority,
    observe_canon_authority, observe_canon_authority_witnessed,
};
pub use chunk::{
    Chunk, ChunkDigest, ChunkError, ChunkHeader, ChunkId, ChunkIndex, CohortId, Frame, FrameRef,
    ProducerId, SealedChunk, SubmissionRef, WriterId, decode_chunk, decode_frame, decode_index,
    encoded_chunk_len, next_sealed_chunk_len, scan_sealed_chunk_ids, seal_chunk,
    seal_single_frame_chunk,
};
pub use chunklog::{
    ChunkAppendAck, ChunkLogError, ChunkLogRecovery, ChunkLogWriter, RecoveredChunk, RecoveryBound,
    VirtualChunkLogRecovery,
};
pub use clock::{
    BatchAccumulator, BatchPolicy, Clock, ManualClock, ManualTimer, PushResult, SystemClock,
    SystemTimer, Timer,
};
pub use dataref::{
    DataRef, DataRefError, LogPayload, MAX_BLOB_KEY_BYTES, decode_data_ref, decode_log_payload,
    decode_reference_batch, encode_data_ref, encode_reference_batch,
};
pub use driver::{
    AckLevel, ChunkDriverActor, ChunkDriverHandle, ChunkPolicy, DriverError, DriverMetrics,
    PolicyError, Receipt, ReceiptFuture, Submission,
};
pub use holylog::remote_sequencer::SequencerEpoch;
pub use holylog::remote_sequencer::SequencerRequestKey;
pub use journal::{
    AppendAck, JournalReader, JournalWriter, ReadError, ReadEvent, ReaderCheckpointError,
    RetentionAuthority, TrimGap, WriteError,
};
pub use model::{
    AttributeValue, Checkpoint, JournalId, JournalRecord, Record, RecordOffset, ResumeHint,
};
pub use receipt::{
    AchievedProfile, AdmitPlan, CommittedReceipt, ProducerReceipt, ReceiptPolicyError,
    ReceiptRequirement, ScribeSpoolCapability, SpoolFsyncPolicy, SpoolOnFull, SpooledReceipt,
    VerseReceiptPolicy, plan_admission, profile_satisfies, raise_to_floor,
};
pub use sequencer_key::{sequencer_request_key_for_chunk, sequencer_request_key_for_submission};
pub use serving_authority::{
    AuthorityKey, AuthorityState, JournalGenerationRef, RouteHint, ServingAuthorityError,
    ServingAuthorityRecord, ServingPublication, TransitionId, TransitionKind, WriterAuthority,
    WriterTerm,
};
pub use spool::{
    FileSpoolStorage, FrameClassification, FrameKind, InMemorySpoolStorage, ProgressIdentity,
    RecoveryClassification, RecoveryReport, ScanTail, SpoolCell, SpoolCellHandle, SpoolCellState,
    SpoolConfig, SpoolError, SpoolFrame, SpoolFrameError, SpoolPoisonCause, SpoolReceiptFuture,
    SpoolStorage, SpoolStorageFaults, ValidFrame, classify_frames, encoded_frame_bytes,
    scan_and_classify,
};
pub use spool::{decode_frame as decode_spool_frame, encode_frame as encode_spool_frame};
pub use trace::{CostScope, Effect, Event, Ledger, RejectReason, TerminalOutcome};
