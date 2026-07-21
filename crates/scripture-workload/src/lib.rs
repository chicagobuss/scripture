//! Consumer Workload Contract: durable Canon/Verse history → external effects.
//!
//! Product-facing terms use **Canon** / **Verse**. Internal `JournalId` remains
//! an implementation substrate elsewhere in Scripture; this crate does not put
//! Arrow into the Holylog/Scripture core.

#![forbid(unsafe_code)]

mod canon_source;
mod config;
mod evidence_bundle;
mod host;
mod materializer;
mod parquet_summary;
mod progress;
mod progress_object_store;
mod types;
mod workload;

pub use canon_source::{
    CanonHistorySource, CanonSourceError, CohortChunk, CohortVerseFrame, MemoryCanonSource,
};
pub use config::{
    ArrowFieldConfig, ArrowSchemaConfig, BatchBoundsConfig, CheckpointConfig, DecoderConfig,
    MalformedPolicy, MaterializerOutputConfig, WorkloadConfig, WorkloadKind, WorkloadsFile,
};
pub use evidence_bundle::{
    BundleEmitError, IcebergEvidenceState, IcebergVerifiedEvidence, RunBundleEmit,
    VerticalVerifyReport, emit_run_bundle_v1, verify_vertical_equality,
};
pub use host::{HostError, ProcessOutcome, WorkloadHost};
pub use materializer::{JsonArrowParquetMaterializer, MaterializerError, ParquetCommitManifest};
pub use parquet_summary::{
    ParquetOutputSummary, SourceOffsetDigest, SummaryError, canon_range_digests, payload_digest,
    summarize_canonical_parquet, walk_manifest_chain,
};
pub use progress::{
    AcquiredBinding, BindingKey, BindingToken, ConsumerBinding, ConsumerProgressStore,
    InMemoryProgressStore, ProgressError, ProgressRegister, ProgressVersion,
};
pub use progress_object_store::{
    MAX_PROGRESS_COMMIT_REF_BYTES, MAX_PROGRESS_KEY_COMPONENT_BYTES, MAX_PROGRESS_RECORD_BYTES,
    MAX_PROGRESS_TOKEN_BYTES, ObjectStoreProgressStore, PROGRESS_CODEC_HEADER_BYTES,
    ProgressStoreConfigError,
};
pub use types::{
    CanonRecord, CanonRef, SchemaRef, SourceOffset, SourceRange, TypeError, VerseRef, WorkloadId,
};
pub use workload::{
    OutputCommit, ReconcileOutcome, Workload, WorkloadError, WorkloadFactory, WorkloadMetadata,
    validate_output_commit,
};
