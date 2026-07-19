//! Host lifecycle: acquire fence → reconcile → apply → CAS checkpoint.

use crate::config::BatchBoundsConfig;
use crate::progress::{
    AcquiredBinding, BindingToken, ConsumerBinding, ConsumerCheckpoint, ConsumerProgressStore,
    ProgressError, ProgressVersion,
};
use crate::types::{SourceOffset, SourceRange};
use crate::workload::{
    OutputCommit, ReconcileOutcome, Workload, WorkloadError, validate_output_commit,
};

/// Outcome of processing one source range under the contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// Applied new output and advanced the checkpoint.
    Applied {
        /// Durable output commit.
        commit: OutputCommit,
        /// New progress version after CAS.
        progress_version: ProgressVersion,
    },
    /// Reconciled a previously committed range and advanced without duplicating.
    Replayed {
        /// Existing output commit.
        commit: OutputCommit,
        /// New progress version after CAS.
        progress_version: ProgressVersion,
    },
    /// Empty range; no output work.
    Empty,
}

/// Runs the non-negotiable consumer workload lifecycle.
pub struct WorkloadHost<S> {
    progress: S,
}

impl<S: ConsumerProgressStore> WorkloadHost<S> {
    /// Creates a host over a durable progress store.
    #[must_use]
    pub const fn new(progress: S) -> Self {
        Self { progress }
    }

    /// Acquires or renews the durable fence for this binding.
    ///
    /// Must succeed before any `reconcile` / `apply`. Race losers receive
    /// [`HostError::FenceHeld`].
    pub fn acquire_binding(
        &self,
        binding: ConsumerBinding,
        owner_token: &BindingToken,
    ) -> Result<AcquiredBinding, HostError> {
        self.progress
            .acquire_or_renew(binding, owner_token)
            .map_err(HostError::from)
    }

    /// Processes one bounded, validated source range under an acquired fence.
    pub fn process_range(
        &self,
        workload: &dyn Workload,
        range: &SourceRange,
        fence: &AcquiredBinding,
        batch: &BatchBoundsConfig,
    ) -> Result<ProcessOutcome, HostError> {
        range
            .validate()
            .map_err(|error| HostError::InvalidRange(error.to_string()))?;
        if range.is_empty() {
            return Ok(ProcessOutcome::Empty);
        }
        enforce_batch_limits(range, batch)?;

        let metadata = workload.metadata();
        if fence.binding.workload_id != metadata.workload_id
            || fence.binding.canon_id != range.canon_id
            || fence.binding.verse_id != range.verse_id
        {
            return Err(HostError::StaleBinding);
        }

        // Re-assert fence ownership before any output side effect.
        let fence = self
            .progress
            .acquire_or_renew(fence.binding.clone(), &fence.owner_token)
            .map_err(HostError::from)?;

        let observed = self
            .progress
            .observe(&metadata.workload_id, &range.canon_id, &range.verse_id)
            .map_err(HostError::from)?;
        let expected_version = match &observed {
            Some((checkpoint, version)) => {
                if checkpoint.binding.binding_epoch != fence.binding.binding_epoch {
                    return Err(HostError::StaleBinding);
                }
                if checkpoint.binding.workload_id != metadata.workload_id
                    || checkpoint.binding.canon_id != range.canon_id
                    || checkpoint.binding.verse_id != range.verse_id
                {
                    return Err(HostError::StaleBinding);
                }
                if checkpoint.next_offset != range.first_offset {
                    return Err(HostError::NonContiguous {
                        checkpoint_next: checkpoint.next_offset,
                        range_first: range.first_offset,
                    });
                }
                Some(*version)
            }
            None => None,
        };

        let reconcile = workload.reconcile(range, &fence)?;
        let (commit, replayed) = match reconcile {
            ReconcileOutcome::Absent => {
                let commit = workload.apply(range, &fence)?;
                validate_output_commit(&commit, range, &fence, &metadata.workload_id)
                    .map_err(|error| HostError::OutputMismatch(error.to_string()))?;
                (commit, false)
            }
            ReconcileOutcome::AlreadyCommitted(commit) => {
                validate_output_commit(&commit, range, &fence, &metadata.workload_id)
                    .map_err(|error| HostError::OutputMismatch(error.to_string()))?;
                (commit, true)
            }
            ReconcileOutcome::Indeterminate { detail } => {
                return Err(HostError::Indeterminate(detail));
            }
        };

        let checkpoint = ConsumerCheckpoint {
            binding: fence.binding.clone(),
            owner_token: fence.owner_token.clone(),
            next_offset: range.next_offset,
        };
        let progress_version = self
            .progress
            .compare_and_swap(checkpoint, expected_version, &fence.owner_token)
            .map_err(HostError::from)?;

        Ok(if replayed {
            ProcessOutcome::Replayed {
                commit,
                progress_version,
            }
        } else {
            ProcessOutcome::Applied {
                commit,
                progress_version,
            }
        })
    }
}

fn enforce_batch_limits(range: &SourceRange, batch: &BatchBoundsConfig) -> Result<(), HostError> {
    if let Some(wall) = batch.max_wall_ms
        && wall != 0
    {
        return Err(HostError::BatchLimits(
            "max_wall_ms is declared but not implemented; omit or set 0".into(),
        ));
    }
    let record_count = u32::try_from(range.records.len()).unwrap_or(u32::MAX);
    if record_count > batch.max_records {
        return Err(HostError::BatchLimits(format!(
            "records {record_count} exceed max_records {}",
            batch.max_records
        )));
    }
    let mut bytes = 0u64;
    for record in &range.records {
        bytes = bytes.saturating_add(u64::try_from(record.payload.len()).unwrap_or(u64::MAX));
    }
    if bytes > batch.max_bytes {
        return Err(HostError::BatchLimits(format!(
            "payload bytes {bytes} exceed max_bytes {}",
            batch.max_bytes
        )));
    }
    Ok(())
}

/// Host orchestration errors (distinct from workload/output classes).
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// Invalid delivered range.
    #[error("invalid source range: {0}")]
    InvalidRange(String),
    /// Binding epoch / identity mismatch.
    #[error("stale consumer binding")]
    StaleBinding,
    /// Another owner holds the durable fence.
    #[error("consumer binding fence held by another owner")]
    FenceHeld,
    /// Declared batch limits exceeded or unimplemented time bound.
    #[error("batch limits: {0}")]
    BatchLimits(String),
    /// Range does not start at the checkpoint next-offset.
    #[error(
        "non-contiguous range: checkpoint next_offset={checkpoint_next}, range first={range_first}"
    )]
    NonContiguous {
        /// Checkpoint next offset.
        checkpoint_next: SourceOffset,
        /// Range first offset.
        range_first: SourceOffset,
    },
    /// Output commit does not match the delivered range / workload / fence.
    #[error("output mismatch: {0}")]
    OutputMismatch(String),
    /// Indeterminate output: do not advance progress.
    #[error("indeterminate output: {0}")]
    Indeterminate(String),
    /// Progress store error.
    #[error(transparent)]
    Progress(ProgressError),
    /// Workload error.
    #[error(transparent)]
    Workload(#[from] WorkloadError),
}

impl From<ProgressError> for HostError {
    fn from(error: ProgressError) -> Self {
        match error {
            ProgressError::FenceHeld => Self::FenceHeld,
            ProgressError::StaleBinding | ProgressError::InvalidEpoch => Self::StaleBinding,
            other => Self::Progress(other),
        }
    }
}
