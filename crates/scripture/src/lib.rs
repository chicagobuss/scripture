//! Scripture's native durable-journal API over the Holylog kernel.
//!
//! This crate is intentionally small: canonical record batches, dense record
//! offsets, a single-writer append surface, direct ordered reads, explicit trim
//! gaps, and consumer-owned checkpoints. Directory services, filtering,
//! consumer groups, and cross-process writer fencing are not v0 features.

pub mod batch;
pub mod clock;
pub mod journal;
pub mod model;

pub use batch::{Batch, CodecError, decode_batch, encode_batch, encoded_batch_len};
pub use clock::{BatchAccumulator, BatchPolicy, Clock, ManualClock, PushResult, SystemClock};
pub use journal::{
    AppendAck, JournalReader, JournalWriter, ReadError, ReadEvent, ReaderCheckpointError,
    RetentionAuthority, TrimGap, WriteError,
};
pub use model::{
    AttributeValue, Checkpoint, JournalId, JournalRecord, Record, RecordOffset, ResumeHint,
};
