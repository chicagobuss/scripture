//! Submission spool (S1a): local WAL of submissions, progress evidence, fail-closed recovery.
//!
//! Governed by decision 0013. Public receipts remain committed-only; a single local
//! disk is not a `Journaled` quorum and never auto-resubmits after restart.

mod cell;
mod file;
mod frame;
mod progress;
mod recovery;
mod storage;

pub use cell::{SpoolCell, SpoolCellHandle, SpoolCellState, SpoolReceiptFuture};
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
