//! Workload trait and factory surface.

use crate::progress::AcquiredBinding;
use crate::types::{SourceRange, WorkloadId};

/// Durable identity of one committed workload output for an exact source range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCommit {
    /// Workload that produced the output.
    pub workload_id: WorkloadId,
    /// Binding epoch that authorized this commit.
    pub binding_epoch: u64,
    /// Fence owner token embedded in the durable output.
    pub owner_token: String,
    /// Exact source range covered by this commit.
    pub source_range: SourceRange,
    /// Non-secret opaque output identity (path, digest, snapshot id, …).
    ///
    /// Written into the progress register as `last_commit_ref` on advance.
    pub output_identity: String,
}

impl OutputCommit {
    /// Opaque commit reference stored on the progress register after advance.
    #[must_use]
    pub fn last_commit_ref(&self) -> &str {
        &self.output_identity
    }
}

/// Result of reconciling a possibly previously completed range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileOutcome {
    /// No durable output for this range yet.
    Absent,
    /// Output already committed for this exact range.
    AlreadyCommitted(OutputCommit),
    /// Output state cannot be decided; fail closed.
    Indeterminate {
        /// Human-readable detail (never secrets).
        detail: String,
    },
}

/// Static workload metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadMetadata {
    /// Workload identity.
    pub workload_id: WorkloadId,
    /// Kind label (`json_arrow_parquet`, …).
    pub kind: String,
}

/// In-process consumer workload: reconcile then apply over a bounded range.
pub trait Workload: Send + Sync {
    /// Metadata for evidence / config.
    fn metadata(&self) -> &WorkloadMetadata;

    /// Inspect durable output for `range` before replaying apply.
    fn reconcile(
        &self,
        range: &SourceRange,
        fence: &AcquiredBinding,
    ) -> Result<ReconcileOutcome, WorkloadError>;

    /// Durably commit output for `range`, embedding workload id, fence, and exact range.
    ///
    /// `previous_commit_ref` is the register's observed `last_commit_ref` before this
    /// apply (if any). It is recorded in the manifest chain — never recovered by LIST.
    fn apply(
        &self,
        range: &SourceRange,
        fence: &AcquiredBinding,
        previous_commit_ref: Option<&str>,
    ) -> Result<OutputCommit, WorkloadError>;
}

/// Static in-process construction from validated config.
pub trait WorkloadFactory: Send + Sync {
    /// Builds a workload instance.
    fn build(&self) -> Result<Box<dyn Workload>, WorkloadError>;
}

/// Workload failures.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadError {
    /// Configuration / schema validation failed.
    #[error("workload config: {0}")]
    Config(String),
    /// Source range validation failed.
    #[error("invalid source range: {0}")]
    InvalidRange(String),
    /// Declared batch bounds exceeded.
    #[error("batch limits exceeded: {0}")]
    BatchLimits(String),
    /// Malformed record under `fail_batch` policy.
    #[error("malformed record at offset {offset}: {detail}")]
    MalformedRecord {
        /// Offending offset.
        offset: u64,
        /// Detail without payload secrets.
        detail: String,
    },
    /// Schema mismatch / decode failure.
    #[error("schema/decode: {0}")]
    Schema(String),
    /// Output I/O failed; treat as indeterminate if durability is unclear.
    #[error("output I/O: {0}")]
    OutputIo(String),
    /// Indeterminate output must fail closed.
    #[error("indeterminate output: {0}")]
    Indeterminate(String),
}

/// Shared exact validation for apply results and AlreadyCommitted reconciles.
pub fn validate_output_commit(
    commit: &OutputCommit,
    range: &SourceRange,
    fence: &AcquiredBinding,
    workload_id: &WorkloadId,
) -> Result<(), WorkloadError> {
    if commit.workload_id != *workload_id {
        return Err(WorkloadError::Config(
            "commit workload_id does not match workload metadata".into(),
        ));
    }
    if commit.binding_epoch != fence.binding.binding_epoch {
        return Err(WorkloadError::Config(
            "commit binding_epoch does not match fence".into(),
        ));
    }
    if commit.owner_token != fence.owner_token.as_str() {
        return Err(WorkloadError::Config(
            "commit owner_token does not match fence".into(),
        ));
    }
    if commit.source_range.first_offset != range.first_offset
        || commit.source_range.next_offset != range.next_offset
        || commit.source_range.canon_id != range.canon_id
        || commit.source_range.verse_id != range.verse_id
        || commit.source_range.schema_ref != range.schema_ref
    {
        return Err(WorkloadError::Config(
            "commit source_range does not match delivered range".into(),
        ));
    }
    Ok(())
}
