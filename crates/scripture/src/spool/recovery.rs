//! Crash classification without replay (decision 0013 / S1b-lite).

use std::collections::BTreeSet;

use super::frame::SpoolFrame;
use super::progress::ProgressIdentity;
use super::storage::{ScanTail, SpoolError, SpoolStorage, ValidFrame};

/// Per-submission classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameClassification {
    /// Submission frame with a matching durable progress frame.
    CommittedLocally,
    /// Submission frame without durable progress (remote fate unknown).
    PendingUnclassified,
}

/// Scanner-level outcome for one spool directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryClassification {
    /// Empty/clean spool suitable for Serving.
    CleanEmpty,
    /// History requires operator / successor protocol; do not serve.
    RecoveryRequired {
        /// Valid frame classifications.
        frames: Vec<(ProgressIdentity, FrameClassification)>,
        /// Terminal tear, if any.
        torn_terminal: bool,
        /// Corrupt mid-history or illegal progress/duplicate.
        corrupt_history: bool,
    },
    /// Typed IO surfaced during scan.
    IoFailure,
}

/// Operator-readable report (counts only; no payloads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Overall classification.
    pub classification: RecoveryClassification,
    /// Count of CommittedLocally frames.
    pub committed_locally: usize,
    /// Count of PendingUnclassified frames.
    pub pending_unclassified: usize,
    /// Terminal torn frame observed.
    pub torn_terminal: bool,
    /// Corrupt history observed.
    pub corrupt_history: bool,
    /// Valid frames scanned.
    pub valid_frames: usize,
}

impl RecoveryReport {
    /// True when Serving is forbidden.
    #[must_use]
    pub fn recovery_required(&self) -> bool {
        !matches!(self.classification, RecoveryClassification::CleanEmpty)
    }
}

/// Classify an already-scanned frame list + tail.
pub fn classify_frames(frames: &[ValidFrame], tail: &ScanTail) -> RecoveryReport {
    let torn_terminal = matches!(tail, ScanTail::TornTerminal { .. });
    let mut corrupt_history = matches!(tail, ScanTail::CorruptMiddle { .. });
    let mut submissions: BTreeSet<ProgressIdentity> = BTreeSet::new();
    let mut progress: BTreeSet<ProgressIdentity> = BTreeSet::new();
    let mut ordered: Vec<ProgressIdentity> = Vec::new();

    for entry in frames {
        match &entry.frame {
            SpoolFrame::Submission {
                journal_id,
                submission,
            }
            | SpoolFrame::PreCommit {
                journal_id,
                submission,
                ..
            } => {
                let identity = ProgressIdentity {
                    journal_id: *journal_id,
                    producer_id: submission.producer_id,
                    producer_epoch: submission.producer_epoch,
                    sequence: submission.sequence,
                };
                if !submissions.insert(identity) {
                    corrupt_history = true;
                }
                ordered.push(identity);
            }
            SpoolFrame::Progress(identity) => {
                if !progress.insert(*identity) {
                    corrupt_history = true;
                }
            }
        }
    }

    for identity in &progress {
        if !submissions.contains(identity) {
            corrupt_history = true;
        }
    }

    let mut classified = Vec::with_capacity(ordered.len());
    let mut committed_locally = 0_usize;
    let mut pending_unclassified = 0_usize;
    for identity in ordered {
        let class = if progress.contains(&identity) {
            committed_locally += 1;
            FrameClassification::CommittedLocally
        } else {
            pending_unclassified += 1;
            FrameClassification::PendingUnclassified
        };
        classified.push((identity, class));
    }

    if frames.is_empty() && !torn_terminal && !corrupt_history {
        return RecoveryReport {
            classification: RecoveryClassification::CleanEmpty,
            committed_locally: 0,
            pending_unclassified: 0,
            torn_terminal: false,
            corrupt_history: false,
            valid_frames: 0,
        };
    }

    RecoveryReport {
        classification: RecoveryClassification::RecoveryRequired {
            frames: classified,
            torn_terminal,
            corrupt_history,
        },
        committed_locally,
        pending_unclassified,
        torn_terminal,
        corrupt_history,
        valid_frames: frames.len(),
    }
}

/// Scan storage and classify.
pub fn scan_and_classify(storage: &impl SpoolStorage) -> Result<RecoveryReport, SpoolError> {
    match storage.scan_valid_frames() {
        Ok((frames, tail)) => Ok(classify_frames(&frames, &tail)),
        Err(SpoolError::Io(_)) => Ok(RecoveryReport {
            classification: RecoveryClassification::IoFailure,
            committed_locally: 0,
            pending_unclassified: 0,
            torn_terminal: false,
            corrupt_history: false,
            valid_frames: 0,
        }),
        Err(error) => Err(error),
    }
}
