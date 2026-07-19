//! In-process pipeline helpers for Phase 1 component tests.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::buffer::{BufferedLine, DropOldestBuffer};
use crate::config::{ProducerConfig, SourceKind};
use crate::envelope::{MetricEnvelope, SeqAllocator};
use crate::normalize::normalize_samples;
use crate::scrape::parse_openmetrics;

/// One prepared outbound record plus its envelope.
#[derive(Debug, Clone)]
pub struct PreparedRecord {
    /// Envelope before serialization.
    pub envelope: MetricEnvelope,
    /// Buffered line ready to send.
    pub buffered: BufferedLine,
}

/// Turns an OpenMetrics body into buffered lines for one verse/source.
pub fn prepare_scrape(
    config: &ProducerConfig,
    verse: &str,
    kind: SourceKind,
    body: &str,
    seqs: &mut SeqAllocator,
) -> Result<(Vec<PreparedRecord>, u64, u64), PrepareError> {
    let source = config
        .scrape
        .sources
        .iter()
        .find(|source| source.verse == verse)
        .ok_or_else(|| PrepareError::UnknownVerse(verse.to_owned()))?;
    if source.kind != kind {
        return Err(PrepareError::KindMismatch);
    }
    let samples =
        parse_openmetrics(body).map_err(|error| PrepareError::Parse(error.to_string()))?;
    let batch = normalize_samples(
        &samples,
        &config.normalize,
        config.scrape.max_series_per_scrape,
    );
    let collected_at = rfc3339_now();
    let mut prepared = Vec::new();
    for sample in &batch.samples {
        let seq = seqs.allocate();
        let envelope = MetricEnvelope::from_sample(
            &config.producer_id,
            &config.canon,
            verse,
            seq,
            &collected_at,
            kind,
            sample,
            BTreeMap::from([("host.name".into(), verse.to_owned())]),
        );
        let line = envelope
            .to_line()
            .map_err(|error| PrepareError::Serialize(error.to_string()))?;
        let digest = MetricEnvelope::payload_digest(&line);
        prepared.push(PreparedRecord {
            envelope,
            buffered: BufferedLine {
                verse: verse.to_owned(),
                seq,
                line,
                payload_digest: digest,
            },
        });
    }
    Ok((
        prepared,
        batch.counters.dropped_series,
        batch.counters.dropped_metric_not_allowlisted,
    ))
}

/// Pushes prepared records into a drop-oldest buffer.
pub fn enqueue(buffer: &mut DropOldestBuffer, records: &[PreparedRecord]) -> usize {
    let mut dropped = 0;
    for record in records {
        dropped += buffer
            .push(
                record.buffered.seq,
                record.buffered.line.clone(),
                record.buffered.payload_digest.clone(),
            )
            .len();
    }
    dropped
}

/// Deduplicates committed payloads by `(producer_id, verse, seq)`.
#[must_use]
pub fn dedup_committed_lines(lines: &[String]) -> Vec<String> {
    let mut seen: BTreeSet<(String, String, u64)> = BTreeSet::new();
    let mut out = Vec::new();
    for line in lines {
        let Ok(envelope) = serde_json::from_str::<MetricEnvelope>(line) else {
            continue;
        };
        let key = envelope.dedup_key();
        if seen.insert(key) {
            out.push(line.clone());
        }
    }
    out
}

fn rfc3339_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    // Fixed Zulu formatting without chrono dependency.
    format!("{secs}Z")
}

/// Prepare failures.
#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    /// Unknown verse.
    #[error("unknown verse {0}")]
    UnknownVerse(String),
    /// Kind does not match config.
    #[error("source kind mismatch")]
    KindMismatch,
    /// OpenMetrics parse failure.
    #[error("parse: {0}")]
    Parse(String),
    /// JSON serialize failure.
    #[error("serialize: {0}")]
    Serialize(String),
}
