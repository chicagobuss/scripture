//! Metric/label allowlists and per-scrape series caps.

use std::collections::BTreeMap;

use crate::config::NormalizeConfig;
use crate::scrape::OpenMetricsSample;

/// Counters emitted during normalize.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NormalizeCounters {
    /// Series dropped because metric name was not allowlisted.
    pub dropped_metric_not_allowlisted: u64,
    /// Series dropped after `max_series_per_scrape`.
    pub dropped_series: u64,
}

/// One normalized sample ready for enveloping.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedSample {
    /// Metric name (allowlisted).
    pub name: String,
    /// Filtered labels.
    pub labels: BTreeMap<String, String>,
    /// Numeric value.
    pub value: f64,
    /// Metric type hint.
    pub metric_type: String,
    /// Optional source timestamp.
    pub timestamp_ms: Option<i64>,
}

/// Normalize output for one scrape.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedBatch {
    /// Kept samples (≤ series cap).
    pub samples: Vec<NormalizedSample>,
    /// Drop / filter counters.
    pub counters: NormalizeCounters,
}

/// Applies allowlists and the per-scrape series cap.
#[must_use]
pub fn normalize_samples(
    samples: &[OpenMetricsSample],
    config: &NormalizeConfig,
    max_series_per_scrape: usize,
) -> NormalizedBatch {
    let mut counters = NormalizeCounters::default();
    let mut kept = Vec::new();

    for sample in samples {
        if !config.metric_allowlist.contains(&sample.name) {
            counters.dropped_metric_not_allowlisted += 1;
            continue;
        }
        if kept.len() >= max_series_per_scrape {
            counters.dropped_series += 1;
            continue;
        }
        let mut labels = BTreeMap::new();
        for (key, value) in &sample.labels {
            if config.label_drop.contains(key) {
                continue;
            }
            if !config.label_allowlist.contains(key) {
                continue;
            }
            labels.insert(key.clone(), value.clone());
        }
        kept.push(NormalizedSample {
            name: sample.name.clone(),
            labels,
            value: sample.value,
            metric_type: sample.metric_type.clone(),
            timestamp_ms: sample.timestamp_ms,
        });
    }

    NormalizedBatch {
        samples: kept,
        counters,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProducerConfig;
    use crate::scrape::OpenMetricsSample;

    #[test]
    fn drops_unlisted_metrics_and_caps_series() {
        let config = ProducerConfig::phase1_single_source("p", "http://x/m", "127.0.0.1:1");
        let samples = vec![
            OpenMetricsSample {
                name: "node_cpu_seconds_total".into(),
                labels: BTreeMap::from([
                    ("cpu".into(), "0".into()),
                    ("mode".into(), "idle".into()),
                    ("pod_uid".into(), "abc".into()),
                ]),
                value: 1.0,
                timestamp_ms: None,
                metric_type: "counter".into(),
            },
            OpenMetricsSample {
                name: "unlisted_metric".into(),
                labels: BTreeMap::new(),
                value: 2.0,
                timestamp_ms: None,
                metric_type: "gauge".into(),
            },
            OpenMetricsSample {
                name: "node_memory_MemAvailable_bytes".into(),
                labels: BTreeMap::new(),
                value: 3.0,
                timestamp_ms: None,
                metric_type: "gauge".into(),
            },
        ];
        let batch = normalize_samples(&samples, &config.normalize, 1);
        assert_eq!(batch.samples.len(), 1);
        assert!(!batch.samples[0].labels.contains_key("pod_uid"));
        assert_eq!(batch.counters.dropped_metric_not_allowlisted, 1);
        assert_eq!(batch.counters.dropped_series, 1);
    }
}
