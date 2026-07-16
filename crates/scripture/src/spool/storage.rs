//! Spool storage trait, config, in-memory reference implementation.

use std::io;
use std::sync::Mutex;

use bytes::Bytes;

use super::frame::{SpoolFrame, SpoolFrameError, decode_frame, encode_frame};

/// Physical limits reserved before append (never accept-then-drop).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolConfig {
    /// Maximum total bytes retained across unretired segments.
    pub max_wal_bytes: usize,
    /// Maximum number of valid frames retained.
    pub max_frames: usize,
    /// Maximum encoded size of one frame (length prefix + body).
    pub max_frame_bytes: usize,
    /// Maximum completions queued after WAL admission (channel capacity).
    ///
    /// A submit reserves a completion slot **before** WAL append. When the queue
    /// is full, admission fails with [`SpoolError::CapacityExceeded`] and does
    /// not write. Default matches the former hard-coded 64.
    pub max_inflight_completions: usize,
}

impl Default for SpoolConfig {
    fn default() -> Self {
        Self {
            max_wal_bytes: 64 * 1024 * 1024,
            max_frames: 65_536,
            max_frame_bytes: 1024 * 1024,
            max_inflight_completions: 64,
        }
    }
}

impl SpoolConfig {
    /// Validates positive limits.
    pub fn validate(&self) -> Result<(), SpoolError> {
        if self.max_wal_bytes == 0
            || self.max_frames == 0
            || self.max_frame_bytes < 16
            || self.max_inflight_completions == 0
        {
            return Err(SpoolError::InvalidConfig);
        }
        Ok(())
    }
}

/// Controlled test faults for storage implementations.
#[derive(Debug, Clone, Default)]
pub struct SpoolStorageFaults {
    /// Next `append_frame` fails with a capacity/IO error.
    pub fail_next_append: bool,
    /// Next `sync` fails.
    pub fail_next_sync: bool,
    /// After a successful encode, append only this many prefix bytes (torn write).
    pub tear_after_bytes: Option<usize>,
}

/// One CRC-valid decoded frame with its on-disk byte offset/size for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidFrame {
    /// Byte offset of the length prefix in the active stream.
    pub offset: u64,
    /// Encoded size including length prefix.
    pub encoded_len: usize,
    /// Decoded frame.
    pub frame: SpoolFrame,
}

/// Why a live cell stopped admitting submissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpoolPoisonCause {
    /// `forward` failed after a durable submission WAL frame.
    ForwardFailed,
    /// Wrapped committed receipt failed after a durable submission WAL frame.
    ReceiptFailed,
    /// Local progress frame append/sync failed after remote commit.
    ProgressFailed,
}

/// Spool storage errors.
#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    /// Config rejected.
    #[error("invalid spool config")]
    InvalidConfig,
    /// Append would exceed a physical limit.
    #[error("spool capacity exceeded")]
    CapacityExceeded,
    /// Frame codec failure.
    #[error(transparent)]
    Frame(#[from] SpoolFrameError),
    /// Underlying IO failure.
    #[error("spool io error: {0}")]
    Io(#[from] io::Error),
    /// Another process already owns this spool directory.
    #[error("spool directory is locked by another process")]
    Locked,
    /// Serving is refused; operator must classify recovery.
    #[error("spool recovery required")]
    RecoveryRequired,
    /// Duplicate submission identity while Serving.
    #[error("duplicate submission identity in spool")]
    DuplicateIdentity,
    /// Cell is not in Serving state.
    #[error("spool cell is not serving")]
    NotServing,
    /// Live cell poisoned after a durable WAL frame; admissions blocked.
    #[error("spool cell is poisoned ({cause:?})")]
    Poisoned {
        /// Why the cell stopped serving.
        cause: SpoolPoisonCause,
    },
    /// Completer queue closed / cell runtime unavailable.
    #[error("spool cell is unavailable")]
    Unavailable,
    /// Injected or unexpected durable persistence failure after remote commit.
    #[error("spool progress persistence failed")]
    ProgressFailed,
    /// Wrapped driver admission/commit failure.
    #[error("spool forward failed: {0}")]
    Forward(String),
}

/// Small durable append/sync/scan surface.
pub trait SpoolStorage: Send {
    /// Appends one length-delimited frame (may buffer until sync).
    fn append_frame(&mut self, frame: &SpoolFrame) -> Result<(), SpoolError>;

    /// Makes prior appends durable.
    fn sync(&mut self) -> Result<(), SpoolError>;

    /// Returns CRC-valid frames in append order; stops before a terminal tear.
    ///
    /// Implementations must not panic on arbitrary durable bytes.
    fn scan_valid_frames(&self) -> Result<(Vec<ValidFrame>, ScanTail), SpoolError>;

    /// Current reserved/used byte estimate for capacity accounting.
    fn used_bytes(&self) -> usize;

    /// Number of valid frames currently retained.
    fn frame_count(&self) -> usize;

    /// Test-only fault injection.
    fn set_faults(&mut self, faults: SpoolStorageFaults);
}

/// What the scanner observed after the last valid frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanTail {
    /// Clean EOF after valid frames (or empty file).
    CleanEof,
    /// Truncated or CRC-invalid bytes at the end with nothing after.
    TornTerminal {
        /// Absolute byte offset where the tear begins.
        offset: u64,
        /// Residual trailing byte count.
        residual_bytes: usize,
    },
    /// A length-delimited region failed validation and bytes continue after it.
    CorruptMiddle {
        /// Absolute byte offset of the bad frame.
        offset: u64,
    },
}

/// Reference-only in-memory storage.
#[derive(Debug, Default)]
pub struct InMemorySpoolStorage {
    bytes: Mutex<Vec<u8>>,
    faults: SpoolStorageFaults,
    valid_count: usize,
}

impl InMemorySpoolStorage {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl SpoolStorage for InMemorySpoolStorage {
    fn append_frame(&mut self, frame: &SpoolFrame) -> Result<(), SpoolError> {
        if self.faults.fail_next_append {
            self.faults.fail_next_append = false;
            return Err(SpoolError::CapacityExceeded);
        }
        let encoded = encode_frame(frame)?;
        let full_len = encoded.len();
        let to_write = if let Some(tear) = self.faults.tear_after_bytes.take() {
            encoded.slice(..tear.min(full_len))
        } else {
            encoded
        };
        let mut guard = self
            .bytes
            .lock()
            .map_err(|_| SpoolError::Io(io::Error::other("spool memory poisoned")))?;
        guard.extend_from_slice(&to_write);
        if to_write.len() == full_len {
            self.valid_count = self.valid_count.saturating_add(1);
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), SpoolError> {
        if self.faults.fail_next_sync {
            self.faults.fail_next_sync = false;
            return Err(SpoolError::Io(io::Error::other("injected sync failure")));
        }
        Ok(())
    }

    fn scan_valid_frames(&self) -> Result<(Vec<ValidFrame>, ScanTail), SpoolError> {
        let guard = self
            .bytes
            .lock()
            .map_err(|_| SpoolError::Io(io::Error::other("spool memory poisoned")))?;
        Ok(scan_bytes(&guard))
    }

    fn used_bytes(&self) -> usize {
        self.bytes.lock().map(|guard| guard.len()).unwrap_or(0)
    }

    fn frame_count(&self) -> usize {
        self.valid_count
    }

    fn set_faults(&mut self, faults: SpoolStorageFaults) {
        self.faults = faults;
    }
}

/// Shared scanner for memory and file backends.
pub(super) fn scan_bytes(bytes: &[u8]) -> (Vec<ValidFrame>, ScanTail) {
    let mut frames = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        match decode_frame(&bytes[offset..]) {
            Ok((frame, consumed)) => {
                frames.push(ValidFrame {
                    offset: offset as u64,
                    encoded_len: consumed,
                    frame,
                });
                offset += consumed;
            }
            Err(error) => {
                let residual = bytes.len() - offset;
                if residual >= 4 {
                    let claimed = u32::from_be_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]) as usize;
                    if let Some(total) = claimed.checked_add(4)
                        && offset + total < bytes.len()
                        && !matches!(error, SpoolFrameError::Truncated)
                    {
                        return (
                            frames,
                            ScanTail::CorruptMiddle {
                                offset: offset as u64,
                            },
                        );
                    }
                }
                return (
                    frames,
                    ScanTail::TornTerminal {
                        offset: offset as u64,
                        residual_bytes: residual,
                    },
                );
            }
        }
    }
    (frames, ScanTail::CleanEof)
}

/// Encode helper for capacity checks without mutating storage.
pub(super) fn encoded_frame_bytes(frame: &SpoolFrame) -> Result<Bytes, SpoolError> {
    Ok(encode_frame(frame)?)
}
