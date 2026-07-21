//! Submission spool (S1a): local WAL of submissions, progress evidence, fail-closed recovery.
//!
//! Governed by decision 0013 for the committed-only cell path. Public early
//! `spooled` receipts use the pre-commit envelope frame and
//! `scripture_runtime::pre_commit_spool`, which replays through the active
//! generation rather than fail-closing on non-empty history.

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
    SpoolStorageFaults, ValidFrame, encoded_frame_bytes,
};
