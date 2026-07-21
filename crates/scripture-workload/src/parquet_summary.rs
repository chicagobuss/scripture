//! Independent Parquet summary selected via register/manifest chain — never prefix LIST.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::materializer::ParquetCommitManifest;
use crate::progress::ProgressRegister;
use crate::types::SourceOffset;

const MAX_CHAIN_LEN: usize = 256;

type ParquetReadStats = (u64, Vec<String>, Vec<String>, Vec<SourceOffsetDigest>);

/// One offset/digest pair from the canonical Parquet chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOffsetDigest {
    /// Dense Canon offset.
    pub offset: u64,
    /// Payload digest at that offset.
    pub digest: String,
}

/// Independent summary of canonical Parquet output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetOutputSummary {
    /// Always `present` when this struct is produced from a successful read.
    pub status: String,
    /// Binding epoch from the head (register-selected) manifest.
    pub binding_epoch: u64,
    /// Row count from an independent Parquet read across the chain.
    pub row_count: u64,
    /// Schema field names observed in the files.
    pub schema_fields: Vec<String>,
    /// Source payload digests in chain/source order.
    pub source_digests: Vec<String>,
    /// Offset-aligned digests for frontier verification.
    pub source_offset_digests: Vec<SourceOffsetDigest>,
    /// Canonical data object paths (chain order).
    pub data_objects: Vec<String>,
    /// Head manifest file name.
    pub canonical_manifest: String,
    /// Inclusive first offset across the full chain.
    pub first_offset: u64,
    /// Exclusive next offset (must equal register frontier).
    pub next_offset: u64,
    /// Manifest chain length.
    pub manifest_count: u64,
    /// Discipline note.
    pub note: String,
}

/// Errors while summarizing canonical output.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SummaryError {
    /// Register has no commit ref — nothing canonical yet.
    #[error("register has no last_commit_ref; canonical output absent")]
    NoCommitRef,
    /// Manifest / path resolution failed.
    #[error("manifest: {0}")]
    Manifest(String),
    /// Parquet I/O or decode failed.
    #[error("parquet: {0}")]
    Parquet(String),
    /// Chain integrity failed.
    #[error("manifest chain: {0}")]
    Chain(String),
    /// Register frontier does not match the chain terminal.
    #[error(
        "frontier {frontier} != chain terminal next_offset {terminal_next}; refusing LIST-free summary"
    )]
    FrontierMismatch {
        /// Register frontier.
        frontier: u64,
        /// Terminal manifest next_offset.
        terminal_next: u64,
    },
}

/// BLAKE3 hex digest of opaque Canon payload bytes (producer/Canon/Parquet join key).
#[must_use]
pub fn payload_digest(payload: &[u8]) -> String {
    format!("{}", blake3::hash(payload).to_hex())
}

/// Resolves canonical output through the progress register + manifest chain only.
pub fn summarize_canonical_parquet(
    output_dir: &Path,
    register: &ProgressRegister,
) -> Result<ParquetOutputSummary, SummaryError> {
    let commit_ref = register
        .last_commit_ref
        .as_deref()
        .ok_or(SummaryError::NoCommitRef)?;
    let chain = walk_manifest_chain(output_dir, commit_ref, register)?;
    let head = chain.last().expect("non-empty chain");
    if head.1.next_offset != register.frontier.get() {
        return Err(SummaryError::FrontierMismatch {
            frontier: register.frontier.get(),
            terminal_next: head.1.next_offset,
        });
    }

    let tail = chain.first().expect("non-empty");
    let mut row_count = 0u64;
    let mut schema_fields = Vec::new();
    let mut source_digests = Vec::new();
    let mut source_offset_digests = Vec::new();
    let mut data_objects = Vec::new();

    for (_path, manifest) in &chain {
        validate_parquet_file_name(&manifest.parquet_file)?;
        let parquet_path = output_dir.join(&manifest.parquet_file);
        ensure_path_inside(output_dir, &parquet_path)?;
        let bytes = fs::read(&parquet_path).map_err(|error| {
            SummaryError::Parquet(format!("read {}: {error}", parquet_path.display()))
        })?;
        let digest = format!("blake3:{}", blake3::hash(&bytes).to_hex());
        if digest != manifest.parquet_digest {
            return Err(SummaryError::Parquet(format!(
                "digest mismatch for {}",
                manifest.parquet_file
            )));
        }
        let (rows, fields, digests, offset_digests) =
            read_parquet_stats(&parquet_path, manifest.first_offset)?;
        if !schema_fields.is_empty() && schema_fields != fields {
            return Err(SummaryError::Parquet(
                "schema_fields differ across canonical chain".into(),
            ));
        }
        schema_fields = fields;
        row_count = row_count.saturating_add(rows);
        source_digests.extend(digests);
        source_offset_digests.extend(offset_digests);
        data_objects.push(manifest.parquet_file.clone());
    }

    Ok(ParquetOutputSummary {
        status: "present".into(),
        binding_epoch: head.1.binding_epoch,
        row_count,
        schema_fields,
        source_digests,
        source_offset_digests,
        data_objects,
        canonical_manifest: head
            .0
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("manifest")
            .to_owned(),
        first_offset: tail.1.first_offset,
        next_offset: head.1.next_offset,
        manifest_count: u64::try_from(chain.len()).unwrap_or(u64::MAX),
        note: "Summary walks register-selected manifest chain backward and reads every canonical Parquet file — never prefix LIST."
            .into(),
    })
}

/// Walk backward from `head_commit_ref`, validate continuity, return oldest-first chain.
pub fn walk_manifest_chain(
    output_dir: &Path,
    head_commit_ref: &str,
    register: &ProgressRegister,
) -> Result<Vec<(PathBuf, ParquetCommitManifest)>, SummaryError> {
    let output_dir = output_dir
        .canonicalize()
        .map_err(|error| SummaryError::Manifest(format!("canonicalize output dir: {error}")))?;
    let mut backward: Vec<(PathBuf, ParquetCommitManifest)> = Vec::new();
    let mut visited = HashSet::new();
    let mut current = Some(head_commit_ref.to_owned());

    while let Some(commit_ref) = current {
        if backward.len() >= MAX_CHAIN_LEN {
            return Err(SummaryError::Chain("chain exceeds max length".into()));
        }
        if !visited.insert(commit_ref.clone()) {
            return Err(SummaryError::Chain(
                "cycle detected in manifest chain".into(),
            ));
        }
        let (path, manifest) = resolve_manifest_for_commit_ref(&output_dir, &commit_ref)?;
        ensure_path_inside(&output_dir, &path)?;
        validate_parquet_file_name(&manifest.parquet_file)?;
        if backward.last().is_some() {
            let newer = backward.last().expect("just pushed").1.clone();
            if manifest.next_offset != newer.first_offset {
                return Err(SummaryError::Chain(format!(
                    "continuity gap: {}.next_offset {} != {}.first_offset {}",
                    manifest.parquet_file,
                    manifest.next_offset,
                    newer.parquet_file,
                    newer.first_offset
                )));
            }
            if manifest.canon_id != newer.canon_id
                || manifest.verse_id != newer.verse_id
                || manifest.schema_ref != newer.schema_ref
            {
                return Err(SummaryError::Chain(
                    "identity mismatch across manifest chain".into(),
                ));
            }
        }
        current = manifest.previous_commit_ref.clone();
        backward.push((path, manifest));
    }

    if backward.is_empty() {
        return Err(SummaryError::Chain("empty manifest chain".into()));
    }

    // Head manifest must match the register-selected binding identity.
    // Manifest epoch may be older than the register epoch after takeover (carried commit ref).
    let head_manifest = &backward[0].1;
    if head_manifest.workload_id != register.binding.workload_id.as_str()
        || head_manifest.canon_id != register.binding.canon_id.as_str()
        || head_manifest.verse_id != register.binding.verse_id.as_str()
    {
        return Err(SummaryError::Chain(
            "head manifest binding identity does not match register".into(),
        ));
    }
    if head_manifest.binding_epoch > register.binding.binding_epoch {
        return Err(SummaryError::Chain(format!(
            "head manifest binding_epoch {} > register epoch {} (stale register snapshot)",
            head_manifest.binding_epoch, register.binding.binding_epoch
        )));
    }

    backward.reverse();
    Ok(backward)
}

fn validate_parquet_file_name(name: &str) -> Result<(), SummaryError> {
    if name.is_empty() {
        return Err(SummaryError::Manifest(
            "parquet_file must not be empty".into(),
        ));
    }
    if name.contains('\0') {
        return Err(SummaryError::Manifest("parquet_file contains NUL".into()));
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err(SummaryError::Manifest(format!(
            "parquet_file must be a single safe file name: {name}"
        )));
    }
    if Path::new(name).is_absolute() {
        return Err(SummaryError::Manifest(format!(
            "parquet_file must be relative: {name}"
        )));
    }
    Ok(())
}

fn ensure_path_inside(root: &Path, path: &Path) -> Result<(), SummaryError> {
    let canonical = path.canonicalize().map_err(|error| {
        SummaryError::Manifest(format!("canonicalize {}: {error}", path.display()))
    })?;
    if !canonical.starts_with(root) {
        return Err(SummaryError::Chain(format!(
            "manifest path escapes output dir: {}",
            path.display()
        )));
    }
    Ok(())
}

fn resolve_manifest_for_commit_ref(
    output_dir: &Path,
    commit_ref: &str,
) -> Result<(PathBuf, ParquetCommitManifest), SummaryError> {
    if commit_ref.contains('\0') {
        return Err(SummaryError::Manifest("commit ref contains NUL".into()));
    }
    if let Some(rest) = commit_ref.strip_prefix("parquet:") {
        let file = rest
            .split('#')
            .next()
            .ok_or_else(|| SummaryError::Manifest("empty parquet commit ref".into()))?;
        if file.contains('/') || file.contains('\\') || file.contains("..") {
            return Err(SummaryError::Manifest(
                "escaped parquet file in commit ref".into(),
            ));
        }
        let stem = file
            .strip_suffix(".parquet")
            .ok_or_else(|| SummaryError::Manifest(format!("expected .parquet in {file}")))?;
        let path = output_dir.join(format!("{stem}.commit.json"));
        let manifest = read_manifest(&path)?;
        if manifest.parquet_file != file {
            return Err(SummaryError::Manifest(format!(
                "manifest parquet_file {} != commit ref {file}",
                manifest.parquet_file
            )));
        }
        return Ok((path, manifest));
    }
    if Path::new(commit_ref).is_absolute() {
        return Err(SummaryError::Manifest(
            "absolute commit ref rejected; use register-relative identity".into(),
        ));
    }
    if commit_ref.contains("..") || commit_ref.contains('\\') {
        return Err(SummaryError::Manifest("escaped commit ref path".into()));
    }
    let path = output_dir.join(commit_ref);
    let manifest = read_manifest(&path)?;
    Ok((path, manifest))
}

fn read_manifest(path: &Path) -> Result<ParquetCommitManifest, SummaryError> {
    let bytes = fs::read(path)
        .map_err(|error| SummaryError::Manifest(format!("read {}: {error}", path.display())))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| SummaryError::Manifest(format!("decode {}: {error}", path.display())))
}

fn read_parquet_stats(path: &Path, base_offset: u64) -> Result<ParquetReadStats, SummaryError> {
    let file = fs::File::open(path)
        .map_err(|error| SummaryError::Parquet(format!("open {}: {error}", path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| SummaryError::Parquet(format!("builder: {error}")))?;
    let fields: Vec<String> = builder
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let digest_index = fields.iter().position(|name| name == "source_digest");
    let reader = builder
        .build()
        .map_err(|error| SummaryError::Parquet(format!("reader: {error}")))?;
    let mut row_count = 0u64;
    let mut digests = Vec::new();
    let mut offset_digests = Vec::new();
    let mut offset_cursor = base_offset;
    for batch in reader {
        let batch = batch.map_err(|error| SummaryError::Parquet(format!("batch: {error}")))?;
        row_count = row_count.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        if let Some(index) = digest_index {
            let column = batch.column(index);
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SummaryError::Parquet("source_digest is not utf8".into()))?;
            for index in 0..array.len() {
                if array.is_valid(index) {
                    let digest = array.value(index).to_owned();
                    digests.push(digest.clone());
                    offset_digests.push(SourceOffsetDigest {
                        offset: offset_cursor,
                        digest,
                    });
                    offset_cursor = offset_cursor.saturating_add(1);
                }
            }
        }
    }
    Ok((row_count, fields, digests, offset_digests))
}

/// Builds digests for a delivered Canon range (independent of Parquet).
#[must_use]
pub fn canon_range_digests(
    first_offset: SourceOffset,
    payloads: impl IntoIterator<Item = impl AsRef<[u8]>>,
) -> Vec<(u64, String)> {
    payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let offset = first_offset.get() + u64::try_from(index).unwrap_or(u64::MAX);
            (offset, payload_digest(payload.as_ref()))
        })
        .collect()
}
