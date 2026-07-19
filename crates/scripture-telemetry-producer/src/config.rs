//! Static producer configuration (experimental).

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Closed set of scrape source kinds (no plugin ABI).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceKind {
    /// Prometheus node-exporter `/metrics`.
    NodeExporter,
    /// kube-state-metrics `/metrics`.
    KubeStateMetrics,
}

impl SourceKind {
    /// Stable wire label for envelopes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeExporter => "node-exporter",
            Self::KubeStateMetrics => "kube-state-metrics",
        }
    }
}

/// One statically allowlisted scrape source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrapeSource {
    /// Verse lane that receives this source's samples.
    pub verse: String,
    /// Closed source kind.
    pub kind: SourceKind,
    /// Exact scrape URL (no templates / wildcards).
    pub url: String,
}

/// Per-Verse raw-lines ingress endpoint (primary + optional failover list).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressEndpoint {
    /// Verse identity.
    pub verse: String,
    /// Primary `host:port` of the serving assignment's raw-lines listener.
    pub connect: String,
    /// Ordered failover endpoints tried after Denied / exhausted Unacked
    /// (Phase 3 A→B promotion). Empty = single-endpoint resend only.
    #[serde(default)]
    pub failover_connects: Vec<String>,
}

impl IngressEndpoint {
    /// Primary followed by failovers (deduplicated, order preserved).
    #[must_use]
    pub fn connect_chain(&self) -> Vec<String> {
        let mut chain = Vec::with_capacity(1 + self.failover_connects.len());
        chain.push(self.connect.clone());
        for endpoint in &self.failover_connects {
            if !chain.iter().any(|existing| existing == endpoint) {
                chain.push(endpoint.clone());
            }
        }
        chain
    }
}

/// Scrape loop bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrapeConfig {
    /// Interval between scrapes.
    #[serde(with = "humantime_serde_compat::duration")]
    pub interval: Duration,
    /// Per-request timeout.
    #[serde(with = "humantime_serde_compat::duration")]
    pub timeout: Duration,
    /// Hard response body cap; oversized scrapes fail closed.
    pub max_response_bytes: usize,
    /// Soft cardinality cap; excess series dropped + counted.
    pub max_series_per_scrape: usize,
    /// Static allowlist.
    pub sources: Vec<ScrapeSource>,
}

/// Normalize policy (metric/label allowlists).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizeConfig {
    /// Envelope schema reference.
    pub schema_ref: String,
    /// Metric names that survive normalize.
    pub metric_allowlist: BTreeSet<String>,
    /// Label keys that survive normalize.
    pub label_allowlist: BTreeSet<String>,
    /// Labels dropped even if allowlisted (ephemeral / PII risk).
    pub label_drop: BTreeSet<String>,
}

/// Per-Verse send buffer bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferConfig {
    /// Max pending records per Verse.
    pub max_records_per_verse: usize,
    /// Max pending payload bytes per Verse.
    pub max_bytes_per_verse: usize,
}

/// Top-level telemetry producer config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerConfig {
    /// Stable producer identity — never derived from hostname/pod/IP.
    pub producer_id: String,
    /// Canon name (v1: `fleet-telemetry`).
    pub canon: String,
    /// Per-Verse ingress endpoints.
    pub endpoints: Vec<IngressEndpoint>,
    /// Scrape configuration.
    pub scrape: ScrapeConfig,
    /// Normalize policy.
    pub normalize: NormalizeConfig,
    /// Buffer bounds.
    pub buffer: BufferConfig,
    /// TCP connect timeout.
    #[serde(with = "humantime_serde_compat::duration")]
    pub connect_timeout: Duration,
    /// Whether to resend unacked records after reconnect (at-least-once).
    pub resend_unacked_on_reconnect: bool,
    /// After scrape loop stops, how long send tasks may keep draining before
    /// exiting with pending (honest unacked) rows left in the buffer.
    #[serde(
        default = "defaults::drain_deadline",
        with = "humantime_serde_compat::duration"
    )]
    pub drain_deadline: Duration,
    /// Initial backoff after Denied / Unacked before retry or failover.
    #[serde(
        default = "defaults::retry_initial_backoff",
        with = "humantime_serde_compat::duration"
    )]
    pub retry_initial_backoff: Duration,
    /// Cap for exponential retry backoff.
    #[serde(
        default = "defaults::retry_max_backoff",
        with = "humantime_serde_compat::duration"
    )]
    pub retry_max_backoff: Duration,
}

mod defaults {
    use std::time::Duration;

    pub(crate) fn drain_deadline() -> Duration {
        Duration::from_secs(10)
    }

    pub(crate) fn retry_initial_backoff() -> Duration {
        Duration::from_millis(50)
    }

    pub(crate) fn retry_max_backoff() -> Duration {
        Duration::from_secs(2)
    }
}

/// On-disk YAML root (`telemetry_producer:`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerConfigFile {
    /// Nested producer config.
    pub telemetry_producer: ProducerConfig,
}

impl ProducerConfig {
    /// Loads and validates a YAML config file with a `telemetry_producer:` root.
    pub fn load_yaml_path(path: impl AsRef<std::path::Path>) -> Result<Self, LoadError> {
        let raw = std::fs::read_to_string(path.as_ref()).map_err(LoadError::Io)?;
        Self::from_yaml_str(&raw)
    }

    /// Parses YAML text with a `telemetry_producer:` root and validates.
    pub fn from_yaml_str(raw: &str) -> Result<Self, LoadError> {
        let file: ProducerConfigFile =
            serde_yaml::from_str(raw).map_err(|error| LoadError::Yaml(error.to_string()))?;
        file.telemetry_producer
            .validate()
            .map_err(LoadError::Validate)?;
        Ok(file.telemetry_producer)
    }

    /// Minimal Phase-1 single-source config for tests / local drills.
    #[must_use]
    pub fn phase1_single_source(producer_id: &str, scrape_url: &str, connect: &str) -> Self {
        Self {
            producer_id: producer_id.to_owned(),
            canon: "fleet-telemetry".to_owned(),
            endpoints: vec![IngressEndpoint {
                verse: "node-node-a".to_owned(),
                connect: connect.to_owned(),
                failover_connects: Vec::new(),
            }],
            scrape: ScrapeConfig {
                interval: Duration::from_secs(30),
                timeout: Duration::from_secs(5),
                max_response_bytes: 8_000_000,
                max_series_per_scrape: 5000,
                sources: vec![ScrapeSource {
                    verse: "node-node-a".to_owned(),
                    kind: SourceKind::NodeExporter,
                    url: scrape_url.to_owned(),
                }],
            },
            normalize: NormalizeConfig {
                schema_ref: crate::SCHEMA_REF.to_owned(),
                metric_allowlist: BTreeSet::from([
                    "node_cpu_seconds_total".into(),
                    "node_memory_MemAvailable_bytes".into(),
                ]),
                label_allowlist: BTreeSet::from(["instance".into(), "cpu".into(), "mode".into()]),
                label_drop: BTreeSet::from([
                    "pod_uid".into(),
                    "container_id".into(),
                    "id".into(),
                    "uid".into(),
                    "__address__".into(),
                ]),
            },
            buffer: BufferConfig {
                max_records_per_verse: 20_000,
                max_bytes_per_verse: 32 * 1024 * 1024,
            },
            connect_timeout: Duration::from_secs(3),
            resend_unacked_on_reconnect: true,
            drain_deadline: defaults::drain_deadline(),
            retry_initial_backoff: defaults::retry_initial_backoff(),
            retry_max_backoff: defaults::retry_max_backoff(),
        }
    }

    /// Validates config invariants (stable producer_id, no wildcards, …).
    pub fn validate(&self) -> Result<(), ValidateError> {
        if self.producer_id.trim().is_empty() {
            return Err(ValidateError::EmptyProducerId);
        }
        if self.producer_id.contains('/') || self.producer_id.chars().any(char::is_whitespace) {
            return Err(ValidateError::InvalidProducerId(self.producer_id.clone()));
        }
        if self.canon.trim().is_empty() {
            return Err(ValidateError::EmptyCanon);
        }
        if self.scrape.sources.is_empty() {
            return Err(ValidateError::NoSources);
        }
        for source in &self.scrape.sources {
            if source.verse.trim().is_empty() {
                return Err(ValidateError::EmptyVerse);
            }
            if source.url.contains('*') || source.url.contains('{') {
                return Err(ValidateError::WildcardUrl(source.url.clone()));
            }
            if source.url.starts_with("https://") {
                return Err(ValidateError::TlsNotSupported(source.url.clone()));
            }
            if !source.url.starts_with("http://") {
                return Err(ValidateError::BadUrl(source.url.clone()));
            }
            if !self
                .endpoints
                .iter()
                .any(|endpoint| endpoint.verse == source.verse)
            {
                return Err(ValidateError::MissingEndpoint(source.verse.clone()));
            }
        }
        let mut source_verses = BTreeSet::new();
        for source in &self.scrape.sources {
            if !source_verses.insert(source.verse.clone()) {
                return Err(ValidateError::DuplicateSourceVerse(source.verse.clone()));
            }
        }
        let mut endpoint_verses = BTreeSet::new();
        for endpoint in &self.endpoints {
            if endpoint.verse.trim().is_empty() {
                return Err(ValidateError::EmptyVerse);
            }
            if !endpoint_verses.insert(endpoint.verse.clone()) {
                return Err(ValidateError::DuplicateEndpointVerse(
                    endpoint.verse.clone(),
                ));
            }
            if endpoint.connect.trim().is_empty() {
                return Err(ValidateError::EmptyConnect(endpoint.verse.clone()));
            }
            for failover in &endpoint.failover_connects {
                if failover.trim().is_empty() {
                    return Err(ValidateError::EmptyConnect(endpoint.verse.clone()));
                }
            }
        }
        if self.scrape.max_response_bytes == 0 || self.scrape.max_series_per_scrape == 0 {
            return Err(ValidateError::ZeroCap);
        }
        if self.buffer.max_records_per_verse == 0 || self.buffer.max_bytes_per_verse == 0 {
            return Err(ValidateError::ZeroCap);
        }
        Ok(())
    }
}

/// YAML load failures.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// Filesystem read failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// YAML parse failure.
    #[error("yaml: {0}")]
    Yaml(String),
    /// Semantic validation failure.
    #[error(transparent)]
    Validate(#[from] ValidateError),
}

/// Config validation failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidateError {
    /// `producer_id` missing.
    #[error("producer_id must be non-empty and config-pinned")]
    EmptyProducerId,
    /// `producer_id` contains illegal characters.
    #[error("invalid producer_id {0:?}")]
    InvalidProducerId(String),
    /// Canon missing.
    #[error("canon must be non-empty")]
    EmptyCanon,
    /// No scrape sources.
    #[error("at least one scrape source is required")]
    NoSources,
    /// Empty verse name.
    #[error("verse must be non-empty")]
    EmptyVerse,
    /// Wildcard / templated URL rejected.
    #[error("wildcard/templated scrape URL rejected: {0}")]
    WildcardUrl(String),
    /// Non-http URL.
    #[error("scrape URL must be http:// (plain HTTP): {0}")]
    BadUrl(String),
    /// TLS not implemented in v1 (in-cluster targets are plain HTTP).
    #[error("TLS not supported in v1 (use http://): {0}")]
    TlsNotSupported(String),
    /// Source verse has no ingress endpoint.
    #[error("no ingress endpoint for verse {0}")]
    MissingEndpoint(String),
    /// Duplicate verse among scrape sources.
    #[error("duplicate scrape source verse {0}")]
    DuplicateSourceVerse(String),
    /// Duplicate verse among ingress endpoints.
    #[error("duplicate ingress endpoint verse {0}")]
    DuplicateEndpointVerse(String),
    /// Empty connect / failover address.
    #[error("empty connect for verse {0}")]
    EmptyConnect(String),
    /// Zero resource cap.
    #[error("resource caps must be >= 1")]
    ZeroCap,
}

/// Tiny serde helpers for `Duration` as humantime strings without a dep.
pub(crate) mod humantime_serde_compat {
    pub(crate) mod duration {
        use std::time::Duration;

        use serde::{Deserialize, Deserializer, Serializer};

        pub(crate) fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let millis = value.as_millis();
            if millis.is_multiple_of(1000) {
                serializer.serialize_str(&format!("{}s", millis / 1000))
            } else {
                serializer.serialize_str(&format!("{millis}ms"))
            }
        }

        pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
        where
            D: Deserializer<'de>,
        {
            let raw = String::deserialize(deserializer)?;
            parse(&raw).map_err(serde::de::Error::custom)
        }

        fn parse(raw: &str) -> Result<Duration, String> {
            // Check "ms" before bare "s" — every "Nms" also ends in 's'.
            if let Some(millis) = raw.strip_suffix("ms") {
                let n: u64 = millis
                    .parse()
                    .map_err(|_| format!("invalid duration {raw}"))?;
                return Ok(Duration::from_millis(n));
            }
            if let Some(seconds) = raw.strip_suffix('s') {
                let n: u64 = seconds
                    .parse()
                    .map_err(|_| format!("invalid duration {raw}"))?;
                return Ok(Duration::from_secs(n));
            }
            Err(format!("expected Ns or Nms, got {raw}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_producer_id() {
        let mut config = ProducerConfig::phase1_single_source(
            "fleet-collector-0",
            "http://x/metrics",
            "127.0.0.1:9000",
        );
        config.producer_id.clear();
        assert!(matches!(
            config.validate(),
            Err(ValidateError::EmptyProducerId)
        ));
    }

    #[test]
    fn rejects_wildcard_url() {
        let mut config = ProducerConfig::phase1_single_source(
            "fleet-collector-0",
            "http://x/metrics",
            "127.0.0.1:9000",
        );
        config.scrape.sources[0].url = "http://*.svc/metrics".into();
        assert!(matches!(
            config.validate(),
            Err(ValidateError::WildcardUrl(_))
        ));
    }

    #[test]
    fn rejects_https_url() {
        let mut config = ProducerConfig::phase1_single_source(
            "fleet-collector-0",
            "http://x/metrics",
            "127.0.0.1:9000",
        );
        config.scrape.sources[0].url = "https://x/metrics".into();
        assert!(matches!(
            config.validate(),
            Err(ValidateError::TlsNotSupported(_))
        ));
    }

    #[test]
    fn rejects_duplicate_source_verse() {
        let mut config = ProducerConfig::phase1_single_source(
            "fleet-collector-0",
            "http://x/metrics",
            "127.0.0.1:9000",
        );
        config.scrape.sources.push(ScrapeSource {
            verse: "node-node-a".into(),
            kind: SourceKind::NodeExporter,
            url: "http://y/metrics".into(),
        });
        assert!(matches!(
            config.validate(),
            Err(ValidateError::DuplicateSourceVerse(_))
        ));
    }

    #[test]
    fn rejects_duplicate_endpoint_verse() {
        let mut config = ProducerConfig::phase1_single_source(
            "fleet-collector-0",
            "http://x/metrics",
            "127.0.0.1:9000",
        );
        config.endpoints.push(IngressEndpoint {
            verse: "node-node-a".into(),
            connect: "127.0.0.1:9001".into(),
            failover_connects: Vec::new(),
        });
        assert!(matches!(
            config.validate(),
            Err(ValidateError::DuplicateEndpointVerse(_))
        ));
    }

    #[test]
    fn duration_ms_round_trips() {
        use super::humantime_serde_compat::duration;
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Wrap {
            #[serde(with = "duration")]
            d: std::time::Duration,
        }

        let original = Wrap {
            d: std::time::Duration::from_millis(1500),
        };
        let yaml = serde_yaml::to_string(&original).expect("ser");
        assert!(yaml.contains("1500ms"), "serialized as {yaml}");
        let parsed: Wrap = serde_yaml::from_str(&yaml).expect("de");
        assert_eq!(parsed, original);

        let secs = Wrap {
            d: std::time::Duration::from_secs(3),
        };
        let yaml = serde_yaml::to_string(&secs).expect("ser");
        assert!(yaml.contains("3s"), "serialized as {yaml}");
        let parsed: Wrap = serde_yaml::from_str(&yaml).expect("de");
        assert_eq!(parsed, secs);
    }

    #[test]
    fn loads_telemetry_producer_yaml_root() {
        let yaml = r#"
telemetry_producer:
  producer_id: fleet-collector-0
  canon: fleet-telemetry
  endpoints:
    - verse: node-node-a
      connect: 127.0.0.1:9101
  scrape:
    interval: 30s
    timeout: 5s
    max_response_bytes: 8000000
    max_series_per_scrape: 5000
    sources:
      - verse: node-node-a
        kind: node-exporter
        url: "http://127.0.0.1:9100/metrics"
  normalize:
    schema_ref: otel-shaped-metrics.v1
    metric_allowlist: [node_cpu_seconds_total]
    label_allowlist: [cpu, mode]
    label_drop: [pod_uid]
  buffer:
    max_records_per_verse: 100
    max_bytes_per_verse: 1048576
  connect_timeout: 3s
  resend_unacked_on_reconnect: true
"#;
        let config = ProducerConfig::from_yaml_str(yaml).expect("load");
        assert_eq!(config.producer_id, "fleet-collector-0");
        assert_eq!(config.scrape.sources[0].kind, SourceKind::NodeExporter);
    }
}
