//! OTel-shaped JSON envelope (internal; not OTLP).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::SourceKind;
use crate::normalize::NormalizedSample;

/// Schema reference stamped on every record.
pub const SCHEMA_REF: &str = "otel-shaped-metrics.v1";

/// One newline-delimited telemetry record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricEnvelope {
    /// Envelope frame version.
    pub envelope_version: u32,
    /// Schema identity for consumer validation.
    pub schema_ref: String,
    /// Stable producer identity.
    pub producer_id: String,
    /// Canon name.
    pub canon: String,
    /// Verse lane.
    pub verse: String,
    /// Per-Verse monotonic sequence (idempotency key component).
    pub seq: u64,
    /// Collection time (RFC3339).
    pub collected_at: String,
    /// Source identity.
    pub source: SourceMeta,
    /// OTel-shaped metric body (not OTLP wire).
    pub otel: OtelBody,
}

/// Source metadata in the envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMeta {
    /// Closed source kind.
    pub kind: String,
    /// Static target identity (usually the verse name).
    pub target: String,
}

/// Minimal OTel-shaped metric body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtelBody {
    /// Resource attributes.
    pub resource: BTreeMap<String, String>,
    /// Instrumentation scope.
    pub scope: ScopeMeta,
    /// Single metric datapoint.
    pub metric: MetricPoint,
}

/// Scope metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeMeta {
    /// Scope name.
    pub name: String,
}

/// One metric datapoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricPoint {
    /// Metric name.
    pub name: String,
    /// Metric type (`sum`, `gauge`, …).
    #[serde(rename = "type")]
    pub metric_type: String,
    /// Whether a sum is monotonic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monotonic: Option<bool>,
    /// Datapoint payload.
    pub data_point: DataPoint,
}

/// Datapoint fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataPoint {
    /// Attribute labels.
    pub attributes: BTreeMap<String, String>,
    /// Event time as unix nanos string (JSON-safe).
    pub time_unix_nano: String,
    /// Double value.
    pub as_double: f64,
}

impl MetricEnvelope {
    /// Builds an envelope for one normalized sample.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn from_sample(
        producer_id: &str,
        canon: &str,
        verse: &str,
        seq: u64,
        collected_at: &str,
        kind: SourceKind,
        sample: &NormalizedSample,
        resource: BTreeMap<String, String>,
    ) -> Self {
        let (metric_type, monotonic) = match sample.metric_type.as_str() {
            "counter" => ("sum".to_owned(), Some(true)),
            "gauge" => ("gauge".to_owned(), None),
            other => (other.to_owned(), None),
        };
        let time_unix_nano = sample
            .timestamp_ms
            .map(|ms| (ms as i128 * 1_000_000).to_string())
            .unwrap_or_else(|| "0".to_owned());
        Self {
            envelope_version: 1,
            schema_ref: SCHEMA_REF.to_owned(),
            producer_id: producer_id.to_owned(),
            canon: canon.to_owned(),
            verse: verse.to_owned(),
            seq,
            collected_at: collected_at.to_owned(),
            source: SourceMeta {
                kind: kind.as_str().to_owned(),
                target: verse.to_owned(),
            },
            otel: OtelBody {
                resource,
                scope: ScopeMeta {
                    name: format!("scripture.scrape.{}", kind.as_str()),
                },
                metric: MetricPoint {
                    name: sample.name.clone(),
                    metric_type,
                    monotonic,
                    data_point: DataPoint {
                        attributes: sample.labels.clone(),
                        time_unix_nano,
                        as_double: sample.value,
                    },
                },
            },
        }
    }

    /// Dedup key `(producer_id, verse, seq)`.
    #[must_use]
    pub fn dedup_key(&self) -> (String, String, u64) {
        (self.producer_id.clone(), self.verse.clone(), self.seq)
    }

    /// Serializes to a single raw-lines payload (no embedded newlines).
    pub fn to_line(&self) -> Result<String, serde_json::Error> {
        let line = serde_json::to_string(self)?;
        debug_assert!(!line.contains('\n'));
        Ok(line)
    }

    /// Blake3 digest of the line bytes (for evidence ledgers).
    #[must_use]
    pub fn payload_digest(line: &str) -> String {
        let hash = blake3::hash(line.as_bytes());
        hash.to_hex().to_string()
    }
}

/// Assigns monotonic `seq` values within one `(producer_id, verse)` stream.
#[derive(Debug, Default)]
pub struct SeqAllocator {
    next: u64,
}

impl SeqAllocator {
    /// Returns the next sequence number (starts at 0).
    pub fn allocate(&mut self) -> u64 {
        let seq = self.next;
        self.next = self.next.saturating_add(1);
        seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceKind;

    #[test]
    fn envelope_round_trips_and_has_schema_ref() {
        let sample = NormalizedSample {
            name: "node_cpu_seconds_total".into(),
            labels: BTreeMap::from([("cpu".into(), "0".into())]),
            value: 1.25,
            metric_type: "counter".into(),
            timestamp_ms: Some(1_000),
        };
        let envelope = MetricEnvelope::from_sample(
            "fleet-collector-0",
            "fleet-telemetry",
            "node-node-a",
            7,
            "2026-07-19T04:12:30Z",
            SourceKind::NodeExporter,
            &sample,
            BTreeMap::from([("host.name".into(), "node-a".into())]),
        );
        let line = envelope.to_line().expect("line");
        assert!(!line.contains('\n'));
        let parsed: MetricEnvelope = serde_json::from_str(&line).expect("json");
        assert_eq!(parsed.schema_ref, SCHEMA_REF);
        assert_eq!(
            parsed.dedup_key(),
            ("fleet-collector-0".into(), "node-node-a".into(), 7)
        );
    }
}
