//! Host lifecycle: acquire fence → reconcile → apply → CAS register advance.

use crate::config::BatchBoundsConfig;
use crate::progress::{
    AcquiredBinding, BindingKey, BindingToken, ConsumerProgressStore, ProgressError,
    ProgressVersion,
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

    /// Acquires or renews the durable fence for this binding key.
    ///
    /// Epoch is assigned by the progress register (bump on every fresh token).
    /// Must succeed before any `reconcile` / `apply`.
    pub async fn acquire_binding(
        &self,
        key: BindingKey,
        owner_token: &BindingToken,
    ) -> Result<AcquiredBinding, HostError> {
        self.progress
            .acquire_or_renew(key, owner_token)
            .await
            .map_err(HostError::from)
    }

    /// Processes one bounded, validated source range under an acquired fence.
    pub async fn process_range(
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

        // Re-assert fence ownership without takeover. A zombie must not bump the
        // epoch by calling acquire_or_renew after another process has taken over.
        let observed = self
            .progress
            .observe(&metadata.workload_id, &range.canon_id, &range.verse_id)
            .await
            .map_err(HostError::from)?
            .ok_or(HostError::StaleBinding)?;
        let (register, _version) = observed;
        if register.binding_token != fence.owner_token {
            return Err(HostError::FenceHeld);
        }
        if register.binding.binding_epoch != fence.binding.binding_epoch {
            return Err(HostError::StaleBinding);
        }
        if register.frontier != range.first_offset {
            return Err(HostError::NonContiguous {
                checkpoint_next: register.frontier,
                range_first: range.first_offset,
            });
        }

        let reconcile = workload.reconcile(range, fence)?;
        let (commit, replayed) = match reconcile {
            ReconcileOutcome::Absent => {
                let commit = workload.apply(range, fence, register.last_commit_ref.as_deref())?;
                validate_output_commit(&commit, range, fence, &metadata.workload_id)
                    .map_err(|error| HostError::OutputMismatch(error.to_string()))?;
                (commit, false)
            }
            ReconcileOutcome::AlreadyCommitted(commit) => {
                validate_output_commit(&commit, range, fence, &metadata.workload_id)
                    .map_err(|error| HostError::OutputMismatch(error.to_string()))?;
                (commit, true)
            }
            ReconcileOutcome::Indeterminate { detail } => {
                return Err(HostError::Indeterminate(detail));
            }
        };

        let (_register, progress_version) = self
            .progress
            .advance(
                fence,
                range.next_offset,
                commit.last_commit_ref().to_owned(),
            )
            .await
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
