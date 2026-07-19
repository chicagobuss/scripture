//! Experimental fleet telemetry producer (WP Phase 1).
//!
//! Scrapes a static OpenMetrics allowlist, normalizes to OTel-shaped JSON with
//! a stable `(producer_id, verse, seq)` dedup key, and appends over the
//! product-internal raw-lines ingress. Lab surface only — not OTLP, not a
//! public producer protocol, not exactly-once.

#![deny(missing_docs)]

mod buffer;
mod client;
mod config;
mod envelope;
mod normalize;
mod pipeline;
mod scrape;

pub use buffer::{DropOldestBuffer, DroppedRecord};
pub use client::{AckStatus, RawLinesClient, SendLogEntry, send_log_entry};
pub use config::{
    BufferConfig, IngressEndpoint, NormalizeConfig, ProducerConfig, ScrapeConfig, ScrapeSource,
    SourceKind, ValidateError,
};
pub use envelope::{
    EnvelopeContext, MetricEnvelope, MetricPoint, OtelBody, SCHEMA_REF, SeqAllocator,
    format_rfc3339_utc,
};
pub use normalize::{NormalizeCounters, NormalizedBatch, normalize_samples};
pub use pipeline::{
    DedupResult, PrepareCounters, PreparedRecord, dedup_committed_lines, enqueue, prepare_scrape,
};
pub use scrape::{
    OpenMetricsSample, ParseError, ParseResult, ScrapeError, parse_openmetrics, scrape_url,
};
