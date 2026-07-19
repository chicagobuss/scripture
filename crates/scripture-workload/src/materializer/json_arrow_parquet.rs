//! Newline-JSON → Arrow RecordBatch → Parquet reference materializer.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};

use crate::config::{ArrowSchemaConfig, MalformedPolicy};
use crate::progress::AcquiredBinding;
use crate::types::{CanonRef, SchemaRef, SourceOffset, SourceRange, VerseRef, WorkloadId};
use crate::workload::{OutputCommit, ReconcileOutcome, Workload, WorkloadError, WorkloadMetadata};

/// Non-secret commit manifest written beside a Parquet file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetCommitManifest {
    /// Workload identity (readable).
    pub workload_id: String,
    /// Binding epoch that authorized this commit.
    pub binding_epoch: u64,
    /// Fence owner token.
    pub owner_token: String,
    /// Schema reference.
    pub schema_ref: String,
    /// Canon id (readable; not used as a path component).
    pub canon_id: String,
    /// Verse id (readable; not used as a path component).
    pub verse_id: String,
    /// Inclusive first offset.
    pub first_offset: u64,
    /// Exclusive next offset.
    pub next_offset: u64,
    /// Relative Parquet file name (safe key based).
    pub parquet_file: String,
    /// BLAKE3 digest of the Parquet file bytes (`blake3:<hex>`).
    pub parquet_digest: String,
}

/// JSON→Arrow→Parquet materializer.
#[derive(Debug, Clone)]
pub struct JsonArrowParquetMaterializer {
    metadata: WorkloadMetadata,
    canon_id: CanonRef,
    verse_id: VerseRef,
    schema_ref: SchemaRef,
    arrow_schema: Arc<Schema>,
    field_specs: Vec<FieldSpec>,
    output_dir: PathBuf,
    malformed: MalformedPolicy,
}

type FieldSpec = (String, DataType, bool);

impl JsonArrowParquetMaterializer {
    /// Builds a materializer from validated pieces.
    pub fn new(
        workload_id: WorkloadId,
        canon_id: CanonRef,
        verse_id: VerseRef,
        schema_ref: SchemaRef,
        schema_config: ArrowSchemaConfig,
        output_dir: impl Into<PathBuf>,
        malformed: MalformedPolicy,
    ) -> Result<Self, WorkloadError> {
        let (arrow_schema, field_specs) = build_arrow_schema(&schema_config)?;
        Ok(Self {
            metadata: WorkloadMetadata {
                workload_id,
                kind: "json_arrow_parquet".into(),
            },
            canon_id,
            verse_id,
            schema_ref,
            arrow_schema: Arc::new(arrow_schema),
            field_specs,
            output_dir: output_dir.into(),
            malformed,
        })
    }

    /// Stable path-safe key derived from workload + Canon + Verse + epoch + range.
    ///
    /// Epoch is part of the key so stale-epoch PUTs cannot clobber a valid object.
    #[must_use]
    pub fn object_key(&self, range: &SourceRange, binding_epoch: u64) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.metadata.workload_id.as_str().as_bytes());
        hasher.update(b"\0");
        hasher.update(range.canon_id.as_str().as_bytes());
        hasher.update(b"\0");
        hasher.update(range.verse_id.as_str().as_bytes());
        hasher.update(b"\0");
        hasher.update(&binding_epoch.to_le_bytes());
        hasher.update(b"\0");
        hasher.update(&range.first_offset.get().to_le_bytes());
        hasher.update(&range.next_offset.get().to_le_bytes());
        hasher.finalize().to_hex().to_string()
    }

    fn parquet_path(&self, range: &SourceRange, binding_epoch: u64) -> PathBuf {
        self.output_dir
            .join(format!("{}.parquet", self.object_key(range, binding_epoch)))
    }

    fn manifest_path(&self, range: &SourceRange, binding_epoch: u64) -> PathBuf {
        self.output_dir.join(format!(
            "{}.commit.json",
            self.object_key(range, binding_epoch)
        ))
    }

    fn decode_batch(&self, range: &SourceRange) -> Result<RecordBatch, WorkloadError> {
        let mut builders: Vec<ColumnBuilder> = self
            .field_specs
            .iter()
            .map(|(name, data_type, nullable)| ColumnBuilder::new(name, data_type, *nullable))
            .collect();

        for record in &range.records {
            let value: serde_json::Value = match serde_json::from_slice(&record.payload) {
                Ok(value) => value,
                Err(error) => {
                    return match self.malformed {
                        MalformedPolicy::FailBatch => Err(WorkloadError::MalformedRecord {
                            offset: record.offset.get(),
                            detail: format!("json parse: {error}"),
                        }),
                    };
                }
            };
            let object = match value.as_object() {
                Some(object) => object,
                None => {
                    return Err(WorkloadError::MalformedRecord {
                        offset: record.offset.get(),
                        detail: "json root must be an object".into(),
                    });
                }
            };
            for builder in &mut builders {
                builder.append_from_object(object, record.offset)?;
            }
        }

        let columns: Vec<ArrayRef> = builders.into_iter().map(ColumnBuilder::finish).collect();
        RecordBatch::try_new(Arc::clone(&self.arrow_schema), columns).map_err(|error| {
            WorkloadError::Schema(format!("record batch construction failed: {error}"))
        })
    }

    fn write_parquet_publish(
        &self,
        range: &SourceRange,
        fence: &AcquiredBinding,
        batch: &RecordBatch,
    ) -> Result<(String, String), WorkloadError> {
        fs::create_dir_all(&self.output_dir)
            .map_err(|error| WorkloadError::OutputIo(format!("create output dir: {error}")))?;
        let epoch = fence.binding.binding_epoch;
        let final_path = self.parquet_path(range, epoch);
        if final_path.exists() {
            return Err(WorkloadError::Indeterminate(
                "final parquet already exists without successful reconcile".into(),
            ));
        }

        let unique = BindingUnique::new();
        let tmp_name = format!("{}.{unique}.parquet.tmp", self.object_key(range, epoch));
        let tmp_path = self.output_dir.join(tmp_name);
        {
            let file = File::create(&tmp_path)
                .map_err(|error| WorkloadError::OutputIo(format!("create parquet tmp: {error}")))?;
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&self.arrow_schema), None)
                .map_err(|error| WorkloadError::OutputIo(format!("parquet writer: {error}")))?;
            writer
                .write(batch)
                .map_err(|error| WorkloadError::OutputIo(format!("parquet write: {error}")))?;
            writer
                .close()
                .map_err(|error| WorkloadError::OutputIo(format!("parquet close: {error}")))?;
        }
        sync_file(&tmp_path)?;
        sync_dir(&self.output_dir)?;

        // No-clobber final publication.
        publish_no_clobber(&tmp_path, &final_path)?;
        sync_file(&final_path)?;
        sync_dir(&self.output_dir)?;

        let bytes = fs::read(&final_path).map_err(|error| {
            WorkloadError::OutputIo(format!("read parquet for digest: {error}"))
        })?;
        let digest = format!("blake3:{}", blake3::hash(&bytes).to_hex());
        let file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| WorkloadError::OutputIo("parquet file name utf8".into()))?
            .to_owned();
        Ok((file_name, digest))
    }

    fn write_manifest_publish(
        &self,
        range: &SourceRange,
        fence: &AcquiredBinding,
        parquet_file: &str,
        digest: &str,
    ) -> Result<(), WorkloadError> {
        let manifest = ParquetCommitManifest {
            workload_id: self.metadata.workload_id.as_str().to_owned(),
            binding_epoch: fence.binding.binding_epoch,
            owner_token: fence.owner_token.as_str().to_owned(),
            schema_ref: self.schema_ref.as_str().to_owned(),
            canon_id: range.canon_id.as_str().to_owned(),
            verse_id: range.verse_id.as_str().to_owned(),
            first_offset: range.first_offset.get(),
            next_offset: range.next_offset.get(),
            parquet_file: parquet_file.to_owned(),
            parquet_digest: digest.to_owned(),
        };
        let path = self.manifest_path(range, fence.binding.binding_epoch);
        if path.exists() {
            return Err(WorkloadError::Indeterminate(
                "commit manifest already exists".into(),
            ));
        }
        let unique = BindingUnique::new();
        let tmp = self.output_dir.join(format!(
            "{}.{unique}.commit.json.tmp",
            self.object_key(range, fence.binding.binding_epoch)
        ));
        let body = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| WorkloadError::OutputIo(format!("encode manifest: {error}")))?;
        {
            let mut file = File::create(&tmp).map_err(|error| {
                WorkloadError::OutputIo(format!("create manifest tmp: {error}"))
            })?;
            file.write_all(&body)
                .map_err(|error| WorkloadError::OutputIo(format!("write manifest: {error}")))?;
            file.sync_all()
                .map_err(|error| WorkloadError::OutputIo(format!("sync manifest: {error}")))?;
        }
        sync_dir(&self.output_dir)?;
        publish_no_clobber(&tmp, &path)?;
        sync_file(&path)?;
        sync_dir(&self.output_dir)?;
        Ok(())
    }

    fn read_manifest(&self, path: &Path) -> Result<Option<ParquetCommitManifest>, WorkloadError> {
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)
            .map_err(|error| WorkloadError::Indeterminate(format!("read manifest: {error}")))?;
        let manifest: ParquetCommitManifest = serde_json::from_slice(&bytes).map_err(|error| {
            WorkloadError::Indeterminate(format!("corrupt commit manifest: {error}"))
        })?;
        Ok(Some(manifest))
    }

    fn any_tmp_for_key(
        &self,
        range: &SourceRange,
        binding_epoch: u64,
    ) -> Result<bool, WorkloadError> {
        let prefix = format!("{}.", self.object_key(range, binding_epoch));
        let entries = fs::read_dir(&self.output_dir)
            .map_err(|error| WorkloadError::Indeterminate(format!("list output dir: {error}")))?;
        for entry in entries {
            let entry = entry
                .map_err(|error| WorkloadError::Indeterminate(format!("dir entry: {error}")))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix)
                && (name.ends_with(".parquet.tmp")
                    || name.ends_with(".commit.json.tmp")
                    || name.ends_with(".partial")
                    || name.contains(".partial"))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl Workload for JsonArrowParquetMaterializer {
    fn metadata(&self) -> &WorkloadMetadata {
        &self.metadata
    }

    fn reconcile(
        &self,
        range: &SourceRange,
        fence: &AcquiredBinding,
    ) -> Result<ReconcileOutcome, WorkloadError> {
        range
            .validate()
            .map_err(|error| WorkloadError::InvalidRange(error.to_string()))?;
        if range.canon_id != self.canon_id || range.verse_id != self.verse_id {
            return Err(WorkloadError::Config(
                "range canon/verse does not match materializer binding".into(),
            ));
        }
        if range.schema_ref != self.schema_ref {
            return Err(WorkloadError::Schema(format!(
                "range schema_ref {} != materializer {}",
                range.schema_ref.as_str(),
                self.schema_ref.as_str()
            )));
        }
        if fence.binding.workload_id != self.metadata.workload_id
            || fence.binding.canon_id != self.canon_id
            || fence.binding.verse_id != self.verse_id
        {
            return Err(WorkloadError::Config(
                "fence does not match materializer binding".into(),
            ));
        }

        if !self.output_dir.exists() {
            return Ok(ReconcileOutcome::Absent);
        }

        // Never delete unknown partials; their presence is indeterminate.
        if self.any_tmp_for_key(range, fence.binding.binding_epoch)? {
            return Ok(ReconcileOutcome::Indeterminate {
                detail: "partial/tmp output artifacts present; fail closed (not deleted)".into(),
            });
        }

        let manifest_path = self.manifest_path(range, fence.binding.binding_epoch);
        let parquet_path = self.parquet_path(range, fence.binding.binding_epoch);

        match self.read_manifest(&manifest_path)? {
            None => {
                if parquet_path.exists() {
                    return Ok(ReconcileOutcome::Indeterminate {
                        detail: "parquet exists without valid commit manifest".into(),
                    });
                }
                Ok(ReconcileOutcome::Absent)
            }
            Some(manifest) => {
                // Epoch must match the current fence. Stale-epoch objects live
                // under different keys and are never adopted here.
                if manifest.workload_id != self.metadata.workload_id.as_str()
                    || manifest.canon_id != range.canon_id.as_str()
                    || manifest.verse_id != range.verse_id.as_str()
                    || manifest.first_offset != range.first_offset.get()
                    || manifest.next_offset != range.next_offset.get()
                    || manifest.schema_ref != range.schema_ref.as_str()
                    || manifest.schema_ref != self.schema_ref.as_str()
                    || manifest.binding_epoch != fence.binding.binding_epoch
                    || manifest.owner_token != fence.owner_token.as_str()
                {
                    // schema_ref / identity mismatch: refuse reconcile adoption.
                    if manifest.schema_ref != range.schema_ref.as_str()
                        || manifest.schema_ref != self.schema_ref.as_str()
                    {
                        return Err(WorkloadError::Schema(format!(
                            "commit manifest schema_ref {} does not match range/materializer",
                            manifest.schema_ref
                        )));
                    }
                    return Ok(ReconcileOutcome::Indeterminate {
                        detail: "commit manifest does not match delivered range/fence epoch".into(),
                    });
                }
                if !parquet_path.exists() {
                    return Ok(ReconcileOutcome::Indeterminate {
                        detail: "commit manifest present but parquet missing".into(),
                    });
                }
                let bytes = fs::read(&parquet_path).map_err(|error| {
                    WorkloadError::Indeterminate(format!("read parquet for reconcile: {error}"))
                })?;
                let digest = format!("blake3:{}", blake3::hash(&bytes).to_hex());
                if digest != manifest.parquet_digest {
                    return Ok(ReconcileOutcome::Indeterminate {
                        detail: "parquet digest mismatch vs commit manifest".into(),
                    });
                }
                Ok(ReconcileOutcome::AlreadyCommitted(OutputCommit {
                    workload_id: self.metadata.workload_id.clone(),
                    binding_epoch: fence.binding.binding_epoch,
                    owner_token: fence.owner_token.as_str().to_owned(),
                    source_range: range.clone(),
                    output_identity: format!(
                        "parquet:{}#{}",
                        manifest.parquet_file, manifest.parquet_digest
                    ),
                }))
            }
        }
    }

    fn apply(
        &self,
        range: &SourceRange,
        fence: &AcquiredBinding,
    ) -> Result<OutputCommit, WorkloadError> {
        match self.reconcile(range, fence)? {
            ReconcileOutcome::Absent => {}
            ReconcileOutcome::AlreadyCommitted(_) => {
                return Err(WorkloadError::Config(
                    "apply called after AlreadyCommitted reconcile".into(),
                ));
            }
            ReconcileOutcome::Indeterminate { detail } => {
                return Err(WorkloadError::Indeterminate(detail));
            }
        }
        let batch = self.decode_batch(range)?;
        let (file_name, digest) = self.write_parquet_publish(range, fence, &batch)?;
        self.write_manifest_publish(range, fence, &file_name, &digest)?;
        Ok(OutputCommit {
            workload_id: self.metadata.workload_id.clone(),
            binding_epoch: fence.binding.binding_epoch,
            owner_token: fence.owner_token.as_str().to_owned(),
            source_range: range.clone(),
            output_identity: format!("parquet:{file_name}#{digest}"),
        })
    }
}

struct BindingUnique(String);

impl BindingUnique {
    fn new() -> Self {
        let mut bytes = [0u8; 8];
        let _ = getrandom::fill(&mut bytes);
        Self(bytes.iter().map(|b| format!("{b:02x}")).collect())
    }
}

impl std::fmt::Display for BindingUnique {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn publish_no_clobber(tmp: &Path, final_path: &Path) -> Result<(), WorkloadError> {
    // Exclusive create of final; if it exists, leave tmp in place (indeterminate).
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o644)
        .open(final_path)
    {
        Ok(mut dest) => {
            let bytes = fs::read(tmp).map_err(|error| {
                WorkloadError::OutputIo(format!("read tmp for publish: {error}"))
            })?;
            dest.write_all(&bytes)
                .map_err(|error| WorkloadError::OutputIo(format!("write final: {error}")))?;
            dest.sync_all()
                .map_err(|error| WorkloadError::OutputIo(format!("sync final: {error}")))?;
            // Best-effort remove our unique tmp after successful publish.
            let _ = fs::remove_file(tmp);
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Err(
            WorkloadError::Indeterminate("final object already exists (no-clobber)".into()),
        ),
        Err(error) => Err(WorkloadError::OutputIo(format!(
            "exclusive publish failed: {error}"
        ))),
    }
}

fn sync_file(path: &Path) -> Result<(), WorkloadError> {
    let file = File::open(path)
        .map_err(|error| WorkloadError::OutputIo(format!("open for sync: {error}")))?;
    file.sync_all()
        .map_err(|error| WorkloadError::OutputIo(format!("fsync file: {error}")))
}

fn sync_dir(path: &Path) -> Result<(), WorkloadError> {
    let dir = File::open(path)
        .map_err(|error| WorkloadError::OutputIo(format!("open dir for sync: {error}")))?;
    dir.sync_all()
        .map_err(|error| WorkloadError::OutputIo(format!("fsync dir: {error}")))
}

enum ColumnBuilder {
    Utf8 {
        name: String,
        nullable: bool,
        builder: StringBuilder,
    },
    Int64 {
        name: String,
        nullable: bool,
        builder: Int64Builder,
    },
    Bool {
        name: String,
        nullable: bool,
        builder: BooleanBuilder,
    },
    Float64 {
        name: String,
        nullable: bool,
        builder: Float64Builder,
    },
}

impl ColumnBuilder {
    fn new(name: &str, data_type: &DataType, nullable: bool) -> Self {
        match data_type {
            DataType::Utf8 => Self::Utf8 {
                name: name.to_owned(),
                nullable,
                builder: StringBuilder::new(),
            },
            DataType::Int64 => Self::Int64 {
                name: name.to_owned(),
                nullable,
                builder: Int64Builder::new(),
            },
            DataType::Boolean => Self::Bool {
                name: name.to_owned(),
                nullable,
                builder: BooleanBuilder::new(),
            },
            DataType::Float64 => Self::Float64 {
                name: name.to_owned(),
                nullable,
                builder: Float64Builder::new(),
            },
            _ => Self::Utf8 {
                name: name.to_owned(),
                nullable,
                builder: StringBuilder::new(),
            },
        }
    }

    fn append_from_object(
        &mut self,
        object: &serde_json::Map<String, serde_json::Value>,
        offset: SourceOffset,
    ) -> Result<(), WorkloadError> {
        let (name, nullable) = match self {
            Self::Utf8 { name, nullable, .. }
            | Self::Int64 { name, nullable, .. }
            | Self::Bool { name, nullable, .. }
            | Self::Float64 { name, nullable, .. } => (name.clone(), *nullable),
        };
        let value = object.get(&name);
        match (self, value) {
            (Self::Utf8 { builder, .. }, Some(serde_json::Value::String(text))) => {
                builder.append_value(text);
            }
            (Self::Utf8 { builder, .. }, None | Some(serde_json::Value::Null)) if nullable => {
                builder.append_null();
            }
            (Self::Int64 { builder, .. }, Some(serde_json::Value::Number(number))) => {
                let Some(int) = number.as_i64() else {
                    return Err(WorkloadError::MalformedRecord {
                        offset: offset.get(),
                        detail: format!("field {name} is not int64"),
                    });
                };
                builder.append_value(int);
            }
            (Self::Int64 { builder, .. }, None | Some(serde_json::Value::Null)) if nullable => {
                builder.append_null();
            }
            (Self::Bool { builder, .. }, Some(serde_json::Value::Bool(flag))) => {
                builder.append_value(*flag);
            }
            (Self::Bool { builder, .. }, None | Some(serde_json::Value::Null)) if nullable => {
                builder.append_null();
            }
            (Self::Float64 { builder, .. }, Some(serde_json::Value::Number(number))) => {
                let Some(float) = number.as_f64() else {
                    return Err(WorkloadError::MalformedRecord {
                        offset: offset.get(),
                        detail: format!("field {name} is not float64"),
                    });
                };
                builder.append_value(float);
            }
            (Self::Float64 { builder, .. }, None | Some(serde_json::Value::Null)) if nullable => {
                builder.append_null();
            }
            _ => {
                return Err(WorkloadError::MalformedRecord {
                    offset: offset.get(),
                    detail: format!("field {name} missing or wrong type"),
                });
            }
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8 { mut builder, .. } => Arc::new(builder.finish()),
            Self::Int64 { mut builder, .. } => Arc::new(builder.finish()),
            Self::Bool { mut builder, .. } => Arc::new(builder.finish()),
            Self::Float64 { mut builder, .. } => Arc::new(builder.finish()),
        }
    }
}

fn build_arrow_schema(
    config: &ArrowSchemaConfig,
) -> Result<(Schema, Vec<FieldSpec>), WorkloadError> {
    if config.fields.is_empty() {
        return Err(WorkloadError::Config(
            "arrow_schema.fields must be non-empty".into(),
        ));
    }
    let mut fields = Vec::with_capacity(config.fields.len());
    let mut specs = Vec::with_capacity(config.fields.len());
    for field in &config.fields {
        let data_type = match field.data_type.as_str() {
            "utf8" => DataType::Utf8,
            "int64" => DataType::Int64,
            "bool" => DataType::Boolean,
            "float64" => DataType::Float64,
            other => {
                return Err(WorkloadError::Config(format!(
                    "unsupported arrow data_type {other}"
                )));
            }
        };
        fields.push(Field::new(&field.name, data_type.clone(), field.nullable));
        specs.push((field.name.clone(), data_type, field.nullable));
    }
    Ok((Schema::new(fields), specs))
}

/// Materializer-local error alias for callers that prefer this name.
pub type MaterializerError = WorkloadError;
