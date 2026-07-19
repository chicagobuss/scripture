//! In-process pipeline helpers for Phase 1 component tests.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::buffer::{BufferedLine, DropOldestBuffer};
use crate::config::{ProducerConfig, SourceKind};
use crate::envelope::{EnvelopeContext, MetricEnvelope, SeqAllocator, format_rfc3339_utc};
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

/// Counters from prepare.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PrepareCounters {
    /// Series dropped by the per-scrape cap.
    pub dropped_series: u64,
    /// Series dropped because metric was not allowlisted.
    pub dropped_metric_not_allowlisted: u64,
    /// Malformed sample lines skipped by the parser.
    pub unparseable_lines: u64,
}

/// Turns an OpenMetrics body into buffered lines for one verse/source.
pub fn prepare_scrape(
    config: &ProducerConfig,
    verse: &str,
    kind: SourceKind,
    body: &str,
    seqs: &mut SeqAllocator,
) -> Result<(Vec<PreparedRecord>, PrepareCounters), PrepareError> {
    let source = config
        .scrape
        .sources
        .iter()
        .find(|source| source.verse == verse)
        .ok_or_else(|| PrepareError::UnknownVerse(verse.to_owned()))?;
    if source.kind != kind {
        return Err(PrepareError::KindMismatch);
    }
    let parsed = parse_openmetrics(body).map_err(|error| PrepareError::Parse(error.to_string()))?;
    let batch = normalize_samples(
        &parsed.samples,
        &config.normalize,
        config.scrape.max_series_per_scrape,
    );
    let collected_at = rfc3339_now();
    let mut prepared = Vec::new();
    for sample in &batch.samples {
        let seq = seqs.allocate();
        let envelope = MetricEnvelope::from_sample(
            EnvelopeContext {
                producer_id: &config.producer_id,
                canon: &config.canon,
                verse,
                incarnation: seqs.incarnation,
                seq,
                collected_at: &collected_at,
                kind,
                resource: BTreeMap::from([("host.name".into(), verse.to_owned())]),
            },
            sample,
        );
        let line = envelope
            .to_line()
            .map_err(|error| PrepareError::Serialize(error.to_string()))?;
        let digest = MetricEnvelope::payload_digest(&line);
        prepared.push(PreparedRecord {
            envelope,
            buffered: BufferedLine {
                verse: verse.to_owned(),
                incarnation: seqs.incarnation,
                seq,
                line,
                payload_digest: digest,
            },
        });
    }
    Ok((
        prepared,
        PrepareCounters {
            dropped_series: batch.counters.dropped_series,
            dropped_metric_not_allowlisted: batch.counters.dropped_metric_not_allowlisted,
            unparseable_lines: parsed.unparseable_lines,
        },
    ))
}

/// Pushes prepared records into a drop-oldest buffer.
pub fn enqueue(buffer: &mut DropOldestBuffer, records: &[PreparedRecord]) -> usize {
    let mut dropped = 0;
    for record in records {
        dropped += buffer
            .push(
                record.envelope.incarnation,
                record.buffered.seq,
                record.buffered.line.clone(),
                record.buffered.payload_digest.clone(),
            )
            .len();
    }
    dropped
}

/// Result of deduplicating committed payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupResult {
    /// First occurrence of each `(producer_id, verse, incarnation, seq)`.
    pub lines: Vec<String>,
    /// Committed lines that were not valid envelopes.
    pub unparseable_committed: u64,
}

/// Deduplicates committed payloads by `(producer_id, verse, incarnation, seq)`.
#[must_use]
pub fn dedup_committed_lines(lines: &[String]) -> DedupResult {
    let mut seen: BTreeSet<(String, String, u64, u64)> = BTreeSet::new();
    let mut out = Vec::new();
    let mut unparseable_committed = 0_u64;
    for line in lines {
        let Ok(envelope) = serde_json::from_str::<MetricEnvelope>(line) else {
            unparseable_committed += 1;
            continue;
        };
        let key = envelope.dedup_key();
        if seen.insert(key) {
            out.push(line.clone());
        }
    }
    DedupResult {
        lines: out,
        unparseable_committed,
    }
}

fn rfc3339_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    format_rfc3339_utc(secs)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collected_at_is_rfc3339() {
        let stamp = rfc3339_now();
        assert!(
            stamp.len() == 20 && stamp.ends_with('Z') && stamp.contains('T'),
            "got {stamp}"
        );
    }
}
