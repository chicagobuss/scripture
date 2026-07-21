//! Local `run-bundle-v1` emission and three-way verifier (no cloud, no live scrape).

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::parquet_summary::{ParquetOutputSummary, payload_digest};
use crate::types::{CanonRef, SourceOffset, VerseRef};

/// Iceberg evidence state — never guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergEvidenceState {
    /// Real table commit verified.
    Verified,
    /// Config present but not proven.
    ConfiguredNotVerified,
    /// No Iceberg work in this run.
    Absent,
    /// Live/cloud layer not executed.
    NotRun,
}

/// Iceberg table evidence when state is [`IcebergEvidenceState::Verified`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergVerifiedEvidence {
    /// Fully qualified table identity.
    pub table_ident: String,
    /// Committed snapshot id.
    pub snapshot_id: String,
}

/// Inputs for emitting a local run-bundle-v1 directory.
#[derive(Debug, Clone)]
pub struct RunBundleEmit {
    /// Run id.
    pub run_id: String,
    /// Collection timestamp (RFC3339).
    pub collected_at: String,
    /// Scripture git SHA (or placeholder).
    pub scripture_rev: String,
    /// Holylog git SHA (or placeholder).
    pub holylog_rev: String,
    /// Lab namespace label.
    pub namespace: String,
    /// Object prefixes (inventory observation only).
    pub object_prefixes: Vec<String>,
    /// Producer ledger JSONL rows (already serialized objects).
    pub producer_ledger_rows: Vec<serde_json::Value>,
    /// Message index rows.
    pub message_rows: Vec<serde_json::Value>,
    /// Scribe log rows by id.
    pub scribe_logs: Vec<(String, Vec<serde_json::Value>)>,
    /// Object inventory JSON.
    pub object_inventory: serde_json::Value,
    /// Progress register JSON.
    pub register: serde_json::Value,
    /// Output manifests (canonical + optional stale).
    pub manifests: Vec<(String /* relative name */, serde_json::Value)>,
    /// Parquet summary.
    pub parquet_summary: ParquetOutputSummary,
    /// Iceberg state.
    pub iceberg: IcebergEvidenceState,
    /// Iceberg detail.
    pub iceberg_detail: String,
    /// Optional Holylog oracle JSON evidence (written to `holylog-oracle.json`).
    pub holylog_oracle: Option<serde_json::Value>,
    /// Required when `iceberg` is [`IcebergEvidenceState::Verified`].
    pub iceberg_verified: Option<IcebergVerifiedEvidence>,
}

/// Emit a run-bundle-v1 directory. Does not contact cloud or Kubernetes.
pub fn emit_run_bundle_v1(
    out_dir: &Path,
    emit: &RunBundleEmit,
) -> Result<PathBuf, BundleEmitError> {
    validate_emit(emit)?;
    fs::create_dir_all(out_dir).map_err(io_err)?;
    fs::create_dir_all(out_dir.join("scribes")).map_err(io_err)?;
    fs::create_dir_all(out_dir.join("outputs/manifests")).map_err(io_err)?;

    write_jsonl(
        out_dir.join("producer-ledger.jsonl"),
        &emit.producer_ledger_rows,
    )?;
    write_jsonl(out_dir.join("messages.jsonl"), &emit.message_rows)?;
    for (id, rows) in &emit.scribe_logs {
        validate_path_component(id, "scribe id")?;
        write_jsonl(out_dir.join(format!("scribes/{id}.jsonl")), rows)?;
    }
    write_json(out_dir.join("objects.json"), &emit.object_inventory)?;
    write_json(out_dir.join("outputs/register.json"), &emit.register)?;
    let mut manifest_paths = Vec::new();
    for (name, body) in &emit.manifests {
        validate_path_component(name, "manifest name")?;
        let rel = format!("outputs/manifests/{name}");
        write_json(out_dir.join(&rel), body)?;
        manifest_paths.push(rel);
    }
    write_json(
        out_dir.join("outputs/parquet-summary.json"),
        &emit.parquet_summary,
    )?;

    let (table_ident, snapshot_id) = match emit.iceberg {
        IcebergEvidenceState::Verified => {
            let evidence = emit.iceberg_verified.as_ref().ok_or_else(|| {
                BundleEmitError::Validation(
                    "iceberg verified requires table_ident and snapshot_id evidence".into(),
                )
            })?;
            (Some(evidence.table_ident.clone()), Some(evidence.snapshot_id.clone()))
        }
        _ => (None, None),
    };
    write_json(
        out_dir.join("outputs/iceberg.json"),
        &json!({
            "state": match emit.iceberg {
                IcebergEvidenceState::Verified => "verified",
                IcebergEvidenceState::ConfiguredNotVerified => "configured_not_verified",
                IcebergEvidenceState::Absent => "absent",
                IcebergEvidenceState::NotRun => "not_run",
            },
            "detail": emit.iceberg_detail,
            "table_ident": table_ident,
            "snapshot_id": snapshot_id
        }),
    )?;

    let scribe_paths: Vec<String> = emit
        .scribe_logs
        .iter()
        .map(|(id, _)| format!("scribes/{id}.jsonl"))
        .collect();

    let mut inputs = json!({
        "producer_ledger": "producer-ledger.jsonl",
        "messages": "messages.jsonl",
        "scribe_logs": scribe_paths,
        "object_inventory": "objects.json",
        "outputs_register": "outputs/register.json",
        "outputs_manifests": manifest_paths,
        "parquet_summary": "outputs/parquet-summary.json",
        "iceberg": "outputs/iceberg.json"
    });

    let mut verdicts = vec![
        verdict(
            "producer ledger observations",
            "observed",
            "producer-ledger.jsonl",
        ),
        verdict("message index (digest-first)", "observed", "messages.jsonl"),
        verdict("object inventory observation", "observed", "objects.json"),
        verdict(
            "consumer register frontier",
            "observed",
            "outputs/register.json",
        ),
        verdict(
            "Parquet independent summary",
            "observed",
            "outputs/parquet-summary.json",
        ),
        verdict("Iceberg table", iceberg_verdict(emit.iceberg), "outputs/iceberg.json"),
    ];
    for path in &scribe_paths {
        verdicts.push(verdict("Scribe structured log", "observed", path));
    }
    for path in &manifest_paths {
        verdicts.push(verdict("output manifest", "observed", path));
    }
    if let Some(oracle) = &emit.holylog_oracle {
        write_json(out_dir.join("holylog-oracle.json"), oracle)?;
        inputs["holylog_oracle"] = json!("holylog-oracle.json");
        verdicts.push(verdict("Holylog oracle", "observed", "holylog-oracle.json"));
    } else {
        verdicts.push(json!({
            "label": "Holylog oracle",
            "verdict": "not_run",
            "source": "not supplied"
        }));
    }
    verdicts.push(json!({
        "label": "live cloud / k0s drill",
        "verdict": "not_run",
        "source": "not supplied"
    }));

    let manifest = json!({
        "schema_version": 1,
        "run_id": emit.run_id,
        "collected_at": emit.collected_at,
        "revisions": {
            "scripture": emit.scripture_rev,
            "holylog": emit.holylog_rev
        },
        "scope": {
            "namespace": emit.namespace,
            "object_prefixes": emit.object_prefixes
        },
        "policy": { "payload_previews": "off" },
        "inputs": inputs,
        "verdicts": verdicts
    });
    write_json(out_dir.join("manifest.json"), &manifest)?;
    Ok(out_dir.to_path_buf())
}

fn iceberg_verdict(state: IcebergEvidenceState) -> &'static str {
    match state {
        IcebergEvidenceState::Verified => "pass",
        IcebergEvidenceState::Absent => "observed",
        IcebergEvidenceState::ConfiguredNotVerified | IcebergEvidenceState::NotRun => "not_run",
    }
}

fn validate_emit(emit: &RunBundleEmit) -> Result<(), BundleEmitError> {
    for (id, _) in &emit.scribe_logs {
        validate_path_component(id, "scribe id")?;
    }
    for (name, _) in &emit.manifests {
        validate_path_component(name, "manifest name")?;
    }
    if matches!(emit.iceberg, IcebergEvidenceState::Verified) && emit.iceberg_verified.is_none() {
        return Err(BundleEmitError::Validation(
            "iceberg verified requires iceberg_verified evidence".into(),
        ));
    }
    Ok(())
}

fn validate_path_component(component: &str, label: &str) -> Result<(), BundleEmitError> {
    if component.is_empty() {
        return Err(BundleEmitError::Validation(format!("{label} must not be empty")));
    }
    if component.contains('\0') {
        return Err(BundleEmitError::Validation(format!("{label} contains NUL")));
    }
    if component.contains('/') || component.contains('\\') {
        return Err(BundleEmitError::Validation(format!(
            "{label} must not contain path separators: {component}"
        )));
    }
    if component.contains("..") {
        return Err(BundleEmitError::Validation(format!(
            "{label} must not contain traversal: {component}"
        )));
    }
    if Path::new(component).is_absolute() {
        return Err(BundleEmitError::Validation(format!(
            "{label} must be relative: {component}"
        )));
    }
    Ok(())
}

fn verdict(label: &str, verdict: &str, source: &str) -> serde_json::Value {
    json!({ "label": label, "verdict": verdict, "source": source })
}

/// Three-way equality over the covered frontier: producer digests ≡ Canon ≡ Parquet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerticalVerifyReport {
    /// Overall verdict.
    pub verdict: String,
    /// Producer digests in frontier order.
    pub producer_digests: Vec<String>,
    /// Canon digests in frontier order.
    pub canon_digests: Vec<String>,
    /// Parquet `source_digest` column values.
    pub parquet_digests: Vec<String>,
    /// Detail lines.
    pub notes: Vec<String>,
}

/// Verifies producer → Canon → Parquet digest equality over the covered interval.
///
/// Coverage is `[parquet_summary.first_offset, parquet_summary.next_offset)`, which must
/// equal `[frontier_base, frontier)` for trim/bootstrap scenarios where the chain does
/// not begin at offset 0.
pub fn verify_vertical_equality(
    producer_payloads: &[(u64, &[u8])],
    canon_payloads: &[(u64, &[u8])],
    parquet_summary: &ParquetOutputSummary,
    frontier: SourceOffset,
    _canon: &CanonRef,
    _verse: &VerseRef,
) -> VerticalVerifyReport {
    let first = parquet_summary.first_offset;
    let next = parquet_summary.next_offset;
    let limit = frontier.get();

    let producer_map = match offset_digest_map(producer_payloads, first, next) {
        Ok(map) => map,
        Err(note) => {
            return fail_report(note);
        }
    };
    let canon_map = match offset_digest_map(canon_payloads, first, next) {
        Ok(map) => map,
        Err(note) => {
            return fail_report(note);
        }
    };
    let parquet_map = match parquet_offset_digest_map(parquet_summary, first, next) {
        Ok(map) => map,
        Err(note) => {
            return fail_report(note);
        }
    };

    let producer_digests = ordered_digests(&producer_map);
    let canon_digests = ordered_digests(&canon_map);
    let parquet_digests = ordered_digests(&parquet_map);

    let mut notes = Vec::new();
    if producer_map != canon_map {
        notes.push("producer digests ≠ Canon digests over covered offsets".into());
    }
    if canon_map != parquet_map {
        notes.push("Canon digests ≠ Parquet source_digest offsets over covered interval".into());
    }
    if !covers_interval_exactly_once(&producer_map, first, next) {
        notes.push(format!(
            "producer offsets do not cover [{first}, {next}) exactly once"
        ));
    }
    if !covers_interval_exactly_once(&canon_map, first, next) {
        notes.push(format!(
            "Canon offsets do not cover [{first}, {next}) exactly once"
        ));
    }
    if !covers_interval_exactly_once(&parquet_map, first, next) {
        notes.push(format!(
            "Parquet offsets do not cover [{first}, {next}) exactly once"
        ));
    }
    if next != limit {
        notes.push(format!(
            "parquet chain next_offset {next} != register frontier {limit}"
        ));
    }
    if u64::try_from(parquet_digests.len()).unwrap_or(u64::MAX) != parquet_summary.row_count {
        notes.push("parquet row_count does not match source_digest length".into());
    }
    if parquet_summary.binding_epoch == 0 {
        notes.push("binding_epoch must be nonzero".into());
    }

    let equal = notes.is_empty();
    if equal {
        notes.push(format!(
            "producer ≡ Canon ≡ Parquet over covered offsets [{first}, {next})"
        ));
        notes.push(
            "register/manifest chain covers the covered interval exactly once (no prefix LIST)"
                .into(),
        );
    }

    VerticalVerifyReport {
        verdict: if equal { "pass".into() } else { "fail".into() },
        producer_digests,
        canon_digests,
        parquet_digests,
        notes,
    }
}

fn fail_report(note: String) -> VerticalVerifyReport {
    VerticalVerifyReport {
        verdict: "fail".into(),
        producer_digests: Vec::new(),
        canon_digests: Vec::new(),
        parquet_digests: Vec::new(),
        notes: vec![note],
    }
}

fn offset_digest_map(
    payloads: &[(u64, &[u8])],
    first: u64,
    next: u64,
) -> Result<BTreeMap<u64, String>, String> {
    let mut map = BTreeMap::new();
    for (offset, payload) in payloads {
        if *offset >= first && *offset < next {
            if map.contains_key(offset) {
                return Err(format!("duplicate offset {offset} in payload stream"));
            }
            map.insert(*offset, payload_digest(payload));
        }
    }
    Ok(map)
}

fn parquet_offset_digest_map(
    summary: &ParquetOutputSummary,
    first: u64,
    next: u64,
) -> Result<BTreeMap<u64, String>, String> {
    let mut map = BTreeMap::new();
    for item in &summary.source_offset_digests {
        if item.offset >= first && item.offset < next {
            if map.contains_key(&item.offset) {
                return Err(format!(
                    "duplicate offset {} in parquet source_offset_digests",
                    item.offset
                ));
            }
            map.insert(item.offset, item.digest.clone());
        }
    }
    Ok(map)
}

fn covers_interval_exactly_once(map: &BTreeMap<u64, String>, first: u64, next: u64) -> bool {
    if next < first {
        return false;
    }
    let span = next.saturating_sub(first);
    if map.len() != usize::try_from(span).unwrap_or(usize::MAX) {
        return false;
    }
    for offset in first..next {
        if !map.contains_key(&offset) {
            return false;
        }
    }
    true
}

fn ordered_digests(map: &BTreeMap<u64, String>) -> Vec<String> {
    map.values().cloned().collect()
}

/// Bundle emission errors.
#[derive(Debug, thiserror::Error)]
pub enum BundleEmitError {
    /// Filesystem I/O.
    #[error("io: {0}")]
    Io(String),
    /// JSON encode.
    #[error("json: {0}")]
    Json(String),
    /// Validation failed before write.
    #[error("validation: {0}")]
    Validation(String),
}

fn io_err(error: std::io::Error) -> BundleEmitError {
    BundleEmitError::Io(error.to_string())
}

fn write_json(path: PathBuf, value: &impl Serialize) -> Result<(), BundleEmitError> {
    let body = serde_json::to_vec_pretty(value)
        .map_err(|error| BundleEmitError::Json(error.to_string()))?;
    fs::write(&path, body).map_err(io_err)?;
    Ok(())
}

fn write_jsonl(path: PathBuf, rows: &[serde_json::Value]) -> Result<(), BundleEmitError> {
    let mut file = fs::File::create(&path).map_err(io_err)?;
    for row in rows {
        let line =
            serde_json::to_string(row).map_err(|error| BundleEmitError::Json(error.to_string()))?;
        writeln!(file, "{line}").map_err(io_err)?;
    }
    Ok(())
}
