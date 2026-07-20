//! Producer-side durability for Scripture.
//!
//! This crate owns the edge boundary: local outbox / spool before a submission
//! reaches a stateless Scribe. Core Scripture admission exposes only
//! [`scripture::driver::AckLevel::Committed`] receipts; `local_durable` lives here.

mod cell;
mod continuity;
mod file;
mod frame;
mod progress;
mod recovery;
mod storage;

pub use cell::{SpoolCell, SpoolCellHandle, SpoolCellState, SpoolReceiptFuture};
pub use continuity::{
    ContinuityError, ContinuityId, ContinuityOutbox, ContinuitySnapshot, PendingEntry,
};
pub use file::FileSpoolStorage;
pub use frame::{FrameKind, SpoolFrame, SpoolFrameError, decode_frame, encode_frame};
pub use progress::ProgressIdentity;
pub use recovery::{
    FrameClassification, RecoveryClassification, RecoveryReport, classify_frames, scan_and_classify,
};
pub use storage::{
    InMemorySpoolStorage, ScanTail, SpoolConfig, SpoolError, SpoolPoisonCause, SpoolStorage,
    SpoolStorageFaults, ValidFrame,
};
