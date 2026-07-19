//! OTel-shaped JSON envelope (internal; not OTLP).

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::config::SourceKind;
use crate::normalize::NormalizedSample;

/// Schema reference stamped on every record.
///
/// v1 includes a 128-bit random `incarnation` string in the dedup key before any
/// durable Phase-2 Canon records exist. Bump on incompatible changes.
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
    /// Process/restart incarnation — 128-bit random hex; disambiguates `seq`.
    pub incarnation: String,
    /// Per-Verse monotonic sequence within one incarnation.
    pub seq: u64,
    /// Collection time (RFC3339 UTC, e.g. `2026-07-19T04:12:30Z`).
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

/// Inputs for [`MetricEnvelope::from_sample`] (keeps the call site under clippy's arg limit).
#[derive(Debug, Clone)]
pub struct EnvelopeContext<'a> {
    /// Stable producer identity.
    pub producer_id: &'a str,
    /// Canon name.
    pub canon: &'a str,
    /// Verse lane.
    pub verse: &'a str,
    /// Restart incarnation (128-bit hex).
    pub incarnation: &'a str,
    /// Sequence within the incarnation.
    pub seq: u64,
    /// RFC3339 collection timestamp.
    pub collected_at: &'a str,
    /// Collection instant as unix nanos (fallback for samples without timestamps).
    pub collected_at_unix_nano: u64,
    /// Source kind.
    pub kind: SourceKind,
    /// Resource attributes.
    pub resource: BTreeMap<String, String>,
}

impl MetricEnvelope {
    /// Builds an envelope for one normalized sample.
    #[must_use]
    pub fn from_sample(ctx: EnvelopeContext<'_>, sample: &NormalizedSample) -> Self {
        let (metric_type, monotonic) = match sample.metric_type.as_str() {
            "counter" => ("sum".to_owned(), Some(true)),
            "gauge" => ("gauge".to_owned(), None),
            other => (other.to_owned(), None),
        };
        // Prefer the sample timestamp; otherwise the scrape collection instant
        // (never the unix epoch — node-exporter samples usually omit timestamps).
        let time_unix_nano = sample
            .timestamp_ms
            .map(|ms| (i128::from(ms) * 1_000_000).to_string())
            .unwrap_or_else(|| ctx.collected_at_unix_nano.to_string());
        Self {
            envelope_version: 1,
            schema_ref: SCHEMA_REF.to_owned(),
            producer_id: ctx.producer_id.to_owned(),
            canon: ctx.canon.to_owned(),
            verse: ctx.verse.to_owned(),
            incarnation: ctx.incarnation.to_owned(),
            seq: ctx.seq,
            collected_at: ctx.collected_at.to_owned(),
            source: SourceMeta {
                kind: ctx.kind.as_str().to_owned(),
                target: ctx.verse.to_owned(),
            },
            otel: OtelBody {
                resource: ctx.resource,
                scope: ScopeMeta {
                    name: format!("scripture.scrape.{}", ctx.kind.as_str()),
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

    /// Dedup key `(producer_id, verse, incarnation, seq)`.
    #[must_use]
    pub fn dedup_key(&self) -> (String, String, String, u64) {
        (
            self.producer_id.clone(),
            self.verse.clone(),
            self.incarnation.clone(),
            self.seq,
        )
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

/// Assigns monotonic `seq` values within one `(producer_id, verse, incarnation)`.
#[derive(Debug)]
pub struct SeqAllocator {
    /// Process/restart incarnation (128-bit random hex).
    pub incarnation: String,
    next: u64,
}

impl SeqAllocator {
    /// Creates an allocator with a fresh 128-bit random incarnation.
    ///
    /// Panics only if the OS entropy source fails — the process cannot safely
    /// mint collision-resistant restart ids without it.
    #[must_use]
    pub fn new() -> Self {
        Self {
            incarnation: random_incarnation_hex(),
            next: 0,
        }
    }

    /// Creates an allocator with an explicit incarnation (tests).
    #[must_use]
    pub fn with_incarnation(incarnation: impl Into<String>) -> Self {
        Self {
            incarnation: incarnation.into(),
            next: 0,
        }
    }

    /// Returns the next sequence number within this incarnation (starts at 0).
    pub fn allocate(&mut self) -> u64 {
        let seq = self.next;
        self.next = self.next.saturating_add(1);
        seq
    }
}

impl Default for SeqAllocator {
    /// Same as [`SeqAllocator::new`] — fresh random incarnation.
    ///
    /// Prefer constructing allocators once at process start (see runner setup).
    /// Do not call this lazily per scrape; a second mint would change incarnation.
    fn default() -> Self {
        Self::new()
    }
}

/// 32-char lowercase hex of 16 random bytes.
#[must_use]
pub fn random_incarnation_hex() -> String {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).unwrap_or_else(|error| {
        panic!("OS entropy required for producer incarnation: {error}");
    });
    let mut out = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Current unix time in nanoseconds (for `collected_at` / `time_unix_nano`).
#[must_use]
pub fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Formats a unix-second timestamp as RFC3339 UTC (`YYYY-MM-DDTHH:MM:SSZ`).
#[must_use]
pub fn format_rfc3339_utc(secs: u64) -> String {
    const SECS_PER_DAY: u64 = 86_400;
    const DAYS_FROM_UNIX_TO_CIVIL: i64 = 719_468; // 1970-01-01 relative to civil algorithm epoch

    let days = (secs / SECS_PER_DAY) as i64;
    let tod = secs % SECS_PER_DAY;
    let hour = tod / 3600;
    let minute = (tod % 3600) / 60;
    let second = tod % 60;

    let z = days + DAYS_FROM_UNIX_TO_CIVIL;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
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
            EnvelopeContext {
                producer_id: "fleet-collector-0",
                canon: "fleet-telemetry",
                verse: "node-node-a",
                incarnation: "aabbccddeeff00112233445566778899",
                seq: 7,
                collected_at: "2026-07-19T04:12:30Z",
                collected_at_unix_nano: 1_784_434_350_000_000_000,
                kind: SourceKind::NodeExporter,
                resource: BTreeMap::from([("host.name".into(), "node-a".into())]),
            },
            &sample,
        );
        let line = envelope.to_line().expect("line");
        assert!(!line.contains('\n'));
        let parsed: MetricEnvelope = serde_json::from_str(&line).expect("json");
        assert_eq!(parsed.schema_ref, SCHEMA_REF);
        assert_eq!(
            parsed.dedup_key(),
            (
                "fleet-collector-0".into(),
                "node-node-a".into(),
                "aabbccddeeff00112233445566778899".into(),
                7
            )
        );
        assert_eq!(parsed.otel.metric.data_point.time_unix_nano, "1000000000");
    }

    #[test]
    fn missing_sample_timestamp_uses_collection_instant() {
        let sample = NormalizedSample {
            name: "node_memory_MemAvailable_bytes".into(),
            labels: BTreeMap::new(),
            value: 4096.0,
            metric_type: "gauge".into(),
            timestamp_ms: None,
        };
        let collected = 1_784_434_350_000_000_000_u64;
        let envelope = MetricEnvelope::from_sample(
            EnvelopeContext {
                producer_id: "fleet-collector-0",
                canon: "fleet-telemetry",
                verse: "node-node-a",
                incarnation: "00112233445566778899aabbccddeeff",
                seq: 0,
                collected_at: "2026-07-19T04:12:30Z",
                collected_at_unix_nano: collected,
                kind: SourceKind::NodeExporter,
                resource: BTreeMap::new(),
            },
            &sample,
        );
        assert_eq!(
            envelope.otel.metric.data_point.time_unix_nano,
            collected.to_string()
        );
        assert_ne!(envelope.otel.metric.data_point.time_unix_nano, "0");
    }

    #[test]
    fn rfc3339_formats_known_instant() {
        // 2026-07-19T04:12:30Z
        assert_eq!(format_rfc3339_utc(1_784_434_350), "2026-07-19T04:12:30Z");
    }

    #[test]
    fn distinct_incarnations_do_not_collide() {
        let mut a = SeqAllocator::with_incarnation("inc-a");
        let mut b = SeqAllocator::with_incarnation("inc-b");
        assert_eq!(a.allocate(), 0);
        assert_eq!(b.allocate(), 0);
        assert_ne!(a.incarnation, b.incarnation);
    }

    #[test]
    fn random_incarnation_is_32_hex_chars() {
        let a = random_incarnation_hex();
        let b = random_incarnation_hex();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }
}
