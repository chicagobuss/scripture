//! Experimental fleet telemetry producer (WP Phase 1–3).
//!
//! Scrapes a static OpenMetrics allowlist, normalizes to OTel-shaped JSON with
//! a stable `(producer_id, verse, incarnation, seq)` dedup key, and appends over
//! the product-internal raw-lines ingress. Lab surface only — not OTLP, not a
//! public producer protocol, not exactly-once.

#![deny(missing_docs)]

mod buffer;
mod client;
mod config;
mod envelope;
mod ledger;
mod normalize;
mod pipeline;
mod runner;
mod scrape;

pub use buffer::{BufferedLine, DropOldestBuffer, DroppedRecord};
pub use client::{AckStatus, RawLinesClient, SendLogEntry, send_log_entry};
pub use config::{
    BufferConfig, IngressEndpoint, LoadError, NormalizeConfig, ProducerConfig, ProducerConfigFile,
    ScrapeConfig, ScrapeSource, SourceKind, ValidateError,
};
pub use envelope::{
    EnvelopeContext, MetricEnvelope, MetricPoint, OtelBody, SCHEMA_REF, SeqAllocator,
    format_rfc3339_utc, random_incarnation_hex, unix_nanos_now,
};
pub use ledger::{LedgerFailoverRow, LedgerSendRow, SendLedger, SinkCommitRow, failover_message};
pub use normalize::{NormalizeCounters, NormalizedBatch, normalize_samples};
pub use pipeline::{
    DedupResult, PrepareCounters, PreparedRecord, dedup_committed_lines, enqueue, prepare_scrape,
};
pub use runner::{RunError, RunOptions, RunnerCounters, run_producer};
pub use scrape::{
    OpenMetricsSample, ParseError, ParseResult, ScrapeError, parse_openmetrics, scrape_url,
};
