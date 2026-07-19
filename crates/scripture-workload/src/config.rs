//! Declarative workload configuration (no exact-once claim fields).

use serde::{Deserialize, Serialize};

use crate::materializer::JsonArrowParquetMaterializer;
use crate::types::{CanonRef, SchemaRef, TypeError, VerseRef, WorkloadId};
use crate::workload::{Workload, WorkloadError, WorkloadFactory};

/// Top-level optional `workloads` document / config fragment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkloadsFile {
    /// Declared workloads (empty preserves single-scribe configs unchanged).
    #[serde(default)]
    pub workloads: Vec<WorkloadConfig>,
}

/// One workload binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadConfig {
    /// Stable workload id.
    pub id: String,
    /// Workload kind.
    pub kind: WorkloadKind,
    /// Source Canon.
    pub canon_id: String,
    /// Source Verse.
    pub verse_id: String,
    /// Binding epoch.
    pub binding_epoch: u64,
    /// Checkpoint store identity (host-interpreted; opaque here).
    pub checkpoint: CheckpointConfig,
    /// Batch bounds.
    pub batch: BatchBoundsConfig,
    /// Decoder / schema.
    pub decoder: DecoderConfig,
    /// Output configuration.
    pub output: MaterializerOutputConfig,
    /// Malformed-record policy.
    #[serde(default)]
    pub malformed: MalformedPolicy,
}

/// Checkpoint location identity (not Serving Authority).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// Opaque store key / path prefix.
    pub identity: String,
}

/// Batch record/byte/time bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchBoundsConfig {
    /// Max records per delivered range.
    pub max_records: u32,
    /// Max payload bytes per delivered range.
    pub max_bytes: u64,
    /// Optional max wall time in milliseconds (host advisory).
    #[serde(default)]
    pub max_wall_ms: Option<u64>,
}

/// Decoder configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecoderConfig {
    /// Decoder kind (`newline_json`).
    pub kind: String,
    /// Schema reference string.
    pub schema_ref: String,
    /// Arrow schema declaration for the JSON decoder.
    pub arrow_schema: ArrowSchemaConfig,
}

/// Arrow schema declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrowSchemaConfig {
    /// Ordered fields.
    pub fields: Vec<ArrowFieldConfig>,
}

/// One Arrow field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrowFieldConfig {
    /// Field name.
    pub name: String,
    /// Logical type: `utf8` | `int64` | `bool` | `float64`.
    pub data_type: String,
    /// Nullability.
    #[serde(default)]
    pub nullable: bool,
}

/// Materializer output configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializerOutputConfig {
    /// Directory for Parquet files + commit manifests.
    pub directory: String,
}

/// Malformed-record policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MalformedPolicy {
    /// Fail the entire batch (initial required policy).
    #[default]
    FailBatch,
}

/// Known workload kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    /// Newline-JSON → Arrow → Parquet reference materializer.
    JsonArrowParquet,
}

impl WorkloadConfig {
    /// Validates and builds a factory for this config.
    pub fn factory(&self) -> Result<Box<dyn WorkloadFactory>, WorkloadError> {
        match self.kind {
            WorkloadKind::JsonArrowParquet => {
                let workload_id = WorkloadId::new(&self.id).map_err(type_err)?;
                let canon_id = CanonRef::new(&self.canon_id).map_err(type_err)?;
                let verse_id = VerseRef::new(&self.verse_id).map_err(type_err)?;
                let schema_ref = SchemaRef::new(&self.decoder.schema_ref).map_err(type_err)?;
                if self.binding_epoch == 0 {
                    return Err(WorkloadError::Config(
                        "binding_epoch must be nonzero".into(),
                    ));
                }
                if self.decoder.kind != "newline_json" {
                    return Err(WorkloadError::Config(format!(
                        "unsupported decoder kind {}",
                        self.decoder.kind
                    )));
                }
                if self.batch.max_records == 0 {
                    return Err(WorkloadError::Config(
                        "batch.max_records must be >= 1".into(),
                    ));
                }
                if matches!(self.batch.max_wall_ms, Some(ms) if ms != 0) {
                    return Err(WorkloadError::Config(
                        "batch.max_wall_ms is not implemented; omit or set 0".into(),
                    ));
                }
                let materializer = JsonArrowParquetMaterializer::new(
                    workload_id,
                    canon_id,
                    verse_id,
                    schema_ref,
                    self.decoder.arrow_schema.clone(),
                    self.output.directory.clone(),
                    self.malformed,
                )?;
                Ok(Box::new(materializer))
            }
        }
    }
}

impl WorkloadFactory for JsonArrowParquetMaterializer {
    fn build(&self) -> Result<Box<dyn Workload>, WorkloadError> {
        Ok(Box::new(self.clone()))
    }
}

fn type_err(error: TypeError) -> WorkloadError {
    WorkloadError::Config(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_workloads_yaml() {
        let raw = r#"
workloads:
  - id: parquet-events
    kind: json_arrow_parquet
    canon_id: events
    verse_id: v0
    binding_epoch: 1
    checkpoint:
      identity: local://checkpoints/parquet-events
    batch:
      max_records: 100
      max_bytes: 1048576
    decoder:
      kind: newline_json
      schema_ref: events.v1
      arrow_schema:
        fields:
          - name: id
            data_type: utf8
          - name: amount
            data_type: int64
            nullable: true
    output:
      directory: /tmp/scripture-parquet
    malformed: fail_batch
"#;
        let parsed: WorkloadsFile = serde_yaml::from_str(raw).expect("parse");
        assert_eq!(parsed.workloads.len(), 1);
        assert_eq!(parsed.workloads[0].kind, WorkloadKind::JsonArrowParquet);
        assert_eq!(parsed.workloads[0].malformed, MalformedPolicy::FailBatch);
    }
}
