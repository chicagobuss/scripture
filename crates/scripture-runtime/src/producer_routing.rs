//! Directory-backed producer routing over the temporary raw-lines ingress.
//!
//! This is the routing half of producer continuity: given `(canon, verse)`,
//! resolve ranked Scribe endpoints from the fleet directory, send a record,
//! and on refusal / disconnect / lost reply refresh the route and retry while
//! retaining the uncommitted record. It is **not** a durable outbox — local
//! spool and `local_durable` receipts belong elsewhere.
//!
//! # Known limitation — store credentials on the producer
//!
//! [`resolve_route`] and [`DirectoryRouteSource`] read the fleet directory
//! from object storage, so a producer using this client needs store
//! credentials. That is acceptable for an internal client and wrong as a
//! long-term public producer story: the product intent is that a producer asks
//! a **Scribe** for a route, not the store.
//!
//! Follow-up: a Scribe-side route endpoint (bootstrap / relay) that returns
//! ranked candidates without requiring producers to hold object-store
//! credentials. Until then, this module is store-backed because that works
//! today.
//!
//! Delivery claim is at-least-once with a stable [`RecordId`]. A lost committed
//! reply may yield a duplicate Canon entry; callers identify duplicates by id.
//!
//! HA ingress returns no endpoint hint on refusal (unlike legacy Canon
//! `not-owner … endpoint=…`). This client falls back to the directory rather
//! than expecting a redirect. The wire protocol is unchanged.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use crate::directory::{
    self, DirectoryError, DirectoryRecord, RankedCandidate, list_all, rank_candidates,
};

/// Stable identity for one outbound record across retries.
///
/// Generated before the first send attempt and retained until a committed ACK
/// is observed (or attempts are exhausted). Callers that embed the id in the
/// payload can detect at-least-once duplicates after a lost reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId([u8; 16]);

impl RecordId {
    /// Mint a fresh random identity.
    #[must_use]
    pub fn new() -> Self {
        let mut bytes = [0_u8; 16];
        // Fall back to zeros only if the OS RNG is unavailable; tests still
        // pass explicit ids when they need deterministic assertions.
        let _ = getrandom::fill(&mut bytes);
        Self(bytes)
    }

    /// Build from an explicit 16-byte identity.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Raw identity bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Lower-hex encoding for logs and test assertions.
    #[must_use]
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Default for RecordId {
    fn default() -> Self {
        Self::new()
    }
}

/// One record ready to send: stable id plus opaque payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundRecord {
    /// Identity preserved across route refresh and retry.
    pub id: RecordId,
    /// Opaque payload (must not contain newlines; the wire is line-oriented).
    pub payload: Vec<u8>,
}

impl OutboundRecord {
    /// Mint a fresh id for `payload`.
    #[must_use]
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            id: RecordId::new(),
            payload: payload.into(),
        }
    }

    /// Use a caller-supplied identity (tests, outbox replay).
    #[must_use]
    pub fn with_id(id: RecordId, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            id,
            payload: payload.into(),
        }
    }
}

/// Ranked endpoints for one `(canon, verse)` plus when they were resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerRoute {
    /// Canon identity this route was resolved for.
    pub canon: String,
    /// Verse identity this route was resolved for.
    pub verse: String,
    /// Ranked candidates, best first ([`rank_candidates`] order).
    pub candidates: Vec<RankedCandidate>,
    /// Wall-clock resolve time (ms since Unix epoch).
    pub resolved_at_ms: u64,
}

impl ProducerRoute {
    /// Endpoints in try order.
    pub fn endpoints(&self) -> impl Iterator<Item = &str> {
        self.candidates.iter().map(|c| c.endpoint.as_str())
    }
}

/// Committed raw-lines acknowledgement (`OK first_offset next_offset`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedAck {
    /// First logical offset covered by the commit.
    pub first_offset: u64,
    /// Next logical offset after the commit.
    pub next_offset: u64,
    /// Identity of the record that received this ACK (same across retries).
    pub record_id: RecordId,
    /// Endpoint that returned the committed ACK.
    pub endpoint: String,
}

/// Bounded retry / timeout policy for [`RoutingProducer::send`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum send attempts (connect + exchange) before exhaustion.
    pub max_attempts: u32,
    /// Timeout for establishing a TCP connection.
    pub connect_timeout: Duration,
    /// Timeout waiting for one ACK line after a successful write.
    pub ack_timeout: Duration,
    /// Backoff after a Transitioning (recovery-gap) refusal.
    pub transitioning_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            connect_timeout: Duration::from_secs(2),
            ack_timeout: Duration::from_secs(5),
            transitioning_backoff: Duration::from_millis(50),
        }
    }
}

/// Failures from route resolution or exhausted send retries.
#[derive(Debug)]
pub enum ProducerRoutingError {
    /// Fleet directory could not be listed or decoded.
    Directory(DirectoryError),
    /// No candidates for the requested `(canon, verse)`.
    NoCandidates {
        /// Requested Canon.
        canon: String,
        /// Requested Verse.
        verse: String,
    },
    /// Payload contains a newline (illegal for the line-oriented wire).
    PayloadContainsNewline {
        /// Record that was refused before any network attempt.
        record_id: RecordId,
    },
    /// Retry budget exhausted; the record was retained but not ACKed.
    Exhausted {
        /// Record that remains uncommitted.
        record_id: RecordId,
        /// Attempts consumed.
        attempts: u32,
        /// Last failure observed (never a silent drop).
        last_failure: String,
    },
    /// Dial / parse failure for an advertise string.
    Endpoint(String),
}

impl std::fmt::Display for ProducerRoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Directory(error) => write!(f, "producer routing directory: {error}"),
            Self::NoCandidates { canon, verse } => {
                write!(f, "no directory candidates for canon={canon} verse={verse}")
            }
            Self::PayloadContainsNewline { record_id } => {
                write!(
                    f,
                    "payload for record {} contains a newline",
                    record_id.to_hex()
                )
            }
            Self::Exhausted {
                record_id,
                attempts,
                last_failure,
            } => write!(
                f,
                "producer routing exhausted after {attempts} attempts for record {}: {last_failure}",
                record_id.to_hex()
            ),
            Self::Endpoint(reason) => write!(f, "producer routing endpoint: {reason}"),
        }
    }
}

impl std::error::Error for ProducerRoutingError {}

impl From<DirectoryError> for ProducerRoutingError {
    fn from(value: DirectoryError) -> Self {
        Self::Directory(value)
    }
}

/// Source of ranked routes for a `(canon, verse)`.
///
/// Production uses [`DirectoryRouteSource`]. Tests inject a scripted source so
/// failure classes can be exercised without a live object store.
pub trait RouteSource: Send + Sync {
    /// Resolve (or refresh) ranked candidates.
    fn resolve(
        &self,
        canon: &str,
        verse: &str,
    ) -> Pin<Box<dyn Future<Output = Result<ProducerRoute, ProducerRoutingError>> + Send + '_>>;
}

/// Store-backed route resolution via the fleet directory.
///
/// See the module-level credential-coupling note: producers need object-store
/// access today; a Scribe-side route endpoint is the intended follow-up.
#[derive(Clone)]
pub struct DirectoryRouteSource {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl DirectoryRouteSource {
    /// Bind to a store prefix that already publishes directory records.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }
}

impl RouteSource for DirectoryRouteSource {
    fn resolve(
        &self,
        canon: &str,
        verse: &str,
    ) -> Pin<Box<dyn Future<Output = Result<ProducerRoute, ProducerRoutingError>> + Send + '_>>
    {
        let canon = canon.to_owned();
        let verse = verse.to_owned();
        Box::pin(async move { resolve_route(&self.store, &self.prefix, &canon, &verse).await })
    }
}

/// List the fleet directory and rank candidates for `(canon, verse)`.
pub async fn resolve_route(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    canon: &str,
    verse: &str,
) -> Result<ProducerRoute, ProducerRoutingError> {
    let records = list_all(store, prefix).await?;
    Ok(route_from_records(
        &records,
        canon,
        verse,
        directory::now_ms(),
    ))
}

fn route_from_records(
    records: &[DirectoryRecord],
    canon: &str,
    verse: &str,
    now_ms: u64,
) -> ProducerRoute {
    ProducerRoute {
        canon: canon.to_owned(),
        verse: verse.to_owned(),
        candidates: rank_candidates(records, canon, verse, now_ms),
        resolved_at_ms: now_ms,
    }
}

/// Classified attempt outcome (internal; drives refresh / backoff policy).
#[derive(Debug, Clone, PartialEq, Eq)]
enum AttemptFailure {
    /// Deposed writer — refresh directory and try another candidate.
    DeposedWriter { detail: String },
    /// Recovery gap — back off; do not treat as permanent.
    Transitioning { detail: String },
    /// Dead peer or connect failure.
    DeadScribe { detail: String },
    /// Write succeeded (or may have) but the ACK never arrived.
    LostReply { detail: String },
    /// Other ERR / protocol noise — refresh and retry.
    Other { detail: String },
}

impl AttemptFailure {
    fn detail(&self) -> &str {
        match self {
            Self::DeposedWriter { detail }
            | Self::Transitioning { detail }
            | Self::DeadScribe { detail }
            | Self::LostReply { detail }
            | Self::Other { detail } => detail,
        }
    }
}

/// Observed live HA ingress refusal strings (Debug of [`AuthorityGateDenial`]).
const DEPOSED_MARKER: &str = r#"NotEffectiveWriter { state: "Serving" }"#;
const TRANSITIONING_MARKER: &str = r#"NotEffectiveWriter { state: "Transitioning" }"#;

fn classify_err_line(line: &str) -> AttemptFailure {
    if line.contains(DEPOSED_MARKER) {
        AttemptFailure::DeposedWriter {
            detail: line.to_owned(),
        }
    } else if line.contains(TRANSITIONING_MARKER) {
        AttemptFailure::Transitioning {
            detail: line.to_owned(),
        }
    } else {
        AttemptFailure::Other {
            detail: line.to_owned(),
        }
    }
}

/// Directory-backed (or injected) routing producer over raw-lines ingress.
pub struct RoutingProducer<S: RouteSource> {
    source: S,
    canon: String,
    verse: String,
    route: ProducerRoute,
    policy: RetryPolicy,
    /// Index into `route.candidates` for the next dial attempt.
    next_candidate: usize,
}

impl RoutingProducer<DirectoryRouteSource> {
    /// Resolve an initial route from the fleet directory and build a producer.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        canon: impl Into<String>,
        verse: impl Into<String>,
        policy: RetryPolicy,
    ) -> Result<Self, ProducerRoutingError> {
        let source = DirectoryRouteSource::new(store, prefix);
        Self::open_with_source(source, canon, verse, policy).await
    }
}

impl<S: RouteSource> RoutingProducer<S> {
    /// Build from an arbitrary [`RouteSource`] (tests inject scripted routes).
    pub async fn open_with_source(
        source: S,
        canon: impl Into<String>,
        verse: impl Into<String>,
        policy: RetryPolicy,
    ) -> Result<Self, ProducerRoutingError> {
        let canon = canon.into();
        let verse = verse.into();
        let route = source.resolve(&canon, &verse).await?;
        if route.candidates.is_empty() {
            return Err(ProducerRoutingError::NoCandidates { canon, verse });
        }
        Ok(Self {
            source,
            canon,
            verse,
            route,
            policy,
            next_candidate: 0,
        })
    }

    /// Current cached route (may be stale; authority remains the final gate).
    #[must_use]
    pub fn route(&self) -> &ProducerRoute {
        &self.route
    }

    /// Send `record`, refreshing and retrying until a committed ACK or exhaustion.
    ///
    /// On every failure class the record is retained: this method either returns
    /// [`CommittedAck`] (same [`RecordId`]) or [`ProducerRoutingError::Exhausted`]
    /// naming the last failure. It never reports success without an `OK` line.
    pub async fn send(
        &mut self,
        record: &OutboundRecord,
    ) -> Result<CommittedAck, ProducerRoutingError> {
        if record.payload.iter().any(|&b| b == b'\n' || b == b'\r') {
            return Err(ProducerRoutingError::PayloadContainsNewline {
                record_id: record.id,
            });
        }
        if self.policy.max_attempts == 0 {
            return Err(ProducerRoutingError::Exhausted {
                record_id: record.id,
                attempts: 0,
                last_failure: "retry policy max_attempts is 0".to_owned(),
            });
        }

        let mut last_failure = String::from("no attempt completed");
        for attempt in 1..=self.policy.max_attempts {
            if self.route.candidates.is_empty() {
                last_failure = format!(
                    "no directory candidates for canon={} verse={}",
                    self.canon, self.verse
                );
                let _ = self.refresh_route().await;
                continue;
            }
            if self.next_candidate >= self.route.candidates.len() {
                self.next_candidate = 0;
            }
            let endpoint = self.route.candidates[self.next_candidate].endpoint.clone();

            match self.exchange_once(&endpoint, &record.payload).await {
                Ok((first_offset, next_offset)) => {
                    return Ok(CommittedAck {
                        first_offset,
                        next_offset,
                        record_id: record.id,
                        endpoint,
                    });
                }
                Err(failure) => {
                    last_failure = failure.detail().to_owned();
                    match failure {
                        AttemptFailure::DeposedWriter { .. }
                        | AttemptFailure::DeadScribe { .. }
                        | AttemptFailure::LostReply { .. }
                        | AttemptFailure::Other { .. } => {
                            // Directory may have a better candidate after a
                            // promote or peer death; HA ingress gives no hint.
                            let _ = self.refresh_route().await;
                            self.next_candidate = 0;
                        }
                        AttemptFailure::Transitioning { .. } => {
                            // Recovery gap is transient; back off and retry
                            // without treating the refusal as permanent.
                            sleep(self.policy.transitioning_backoff).await;
                            let _ = self.refresh_route().await;
                        }
                    }
                }
            }
            let _ = attempt;
        }

        Err(ProducerRoutingError::Exhausted {
            record_id: record.id,
            attempts: self.policy.max_attempts,
            last_failure,
        })
    }

    async fn refresh_route(&mut self) -> Result<(), ProducerRoutingError> {
        let route = self.source.resolve(&self.canon, &self.verse).await?;
        self.route = route;
        self.next_candidate = 0;
        Ok(())
    }

    async fn exchange_once(
        &self,
        endpoint: &str,
        payload: &[u8],
    ) -> Result<(u64, u64), AttemptFailure> {
        let addr = match dial_address(endpoint) {
            Ok(addr) => addr,
            Err(reason) => {
                return Err(AttemptFailure::DeadScribe { detail: reason });
            }
        };

        let stream = match timeout(self.policy.connect_timeout, TcpStream::connect(&addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                return Err(AttemptFailure::DeadScribe {
                    detail: format!("connect {addr}: {error}"),
                });
            }
            Err(_) => {
                return Err(AttemptFailure::DeadScribe {
                    detail: format!("connect timeout to {addr}"),
                });
            }
        };

        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        let mut frame = Vec::with_capacity(payload.len() + 1);
        frame.extend_from_slice(payload);
        frame.push(b'\n');

        if let Err(error) = writer.write_all(&frame).await {
            return Err(AttemptFailure::DeadScribe {
                detail: format!("write to {endpoint}: {error}"),
            });
        }
        if let Err(error) = writer.flush().await {
            return Err(AttemptFailure::DeadScribe {
                detail: format!("flush to {endpoint}: {error}"),
            });
        }

        // From here a server may have committed; a closed socket before ACK is
        // LostReply (at-least-once), not a safe drop.
        let mut line = String::new();
        match timeout(self.policy.ack_timeout, reader.read_line(&mut line)).await {
            Ok(Ok(0)) => Err(AttemptFailure::LostReply {
                detail: format!("connection closed after send before ACK from {endpoint}"),
            }),
            Ok(Ok(_)) => {
                let trimmed = line.trim_end();
                if let Some(ack) = parse_ok(trimmed) {
                    Ok(ack)
                } else if trimmed.starts_with("ERR ") {
                    Err(classify_err_line(trimmed))
                } else {
                    Err(AttemptFailure::Other {
                        detail: format!("unexpected response from {endpoint}: {trimmed:?}"),
                    })
                }
            }
            Ok(Err(error)) => Err(AttemptFailure::LostReply {
                detail: format!("ACK read from {endpoint}: {error}"),
            }),
            Err(_) => Err(AttemptFailure::LostReply {
                detail: format!("ACK timeout from {endpoint}"),
            }),
        }
    }
}

/// Map an advertise string (`tcp://host:port` or `host:port`) to a dial target.
fn dial_address(endpoint: &str) -> Result<String, String> {
    let trimmed = endpoint.trim();
    let without_scheme = trimmed
        .strip_prefix("tcp://")
        .or_else(|| trimmed.strip_prefix("TCP://"))
        .unwrap_or(trimmed);
    if without_scheme.is_empty() {
        return Err(format!("empty endpoint after parsing {endpoint:?}"));
    }
    // Require host:port so TcpStream::connect gets a socket address.
    if !without_scheme.contains(':') {
        return Err(format!(
            "endpoint {endpoint:?} is missing a port (expected host:port)"
        ));
    }
    Ok(without_scheme.to_owned())
}

fn parse_ok(line: &str) -> Option<(u64, u64)> {
    let mut parts = line.split_whitespace();
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("OK"), Some(first), Some(next), None) => {
            let first = first.parse::<u64>().ok()?;
            let next = next.parse::<u64>().ok()?;
            Some((first, next))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use object_store::memory::InMemory;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::directory::{DirectoryAssignment, DirectoryRecord, publish};

    fn policy_fast() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 6,
            connect_timeout: Duration::from_millis(200),
            ack_timeout: Duration::from_millis(500),
            transitioning_backoff: Duration::from_millis(5),
        }
    }

    fn assignment(owner: &str, advertise: &str, serving: bool) -> DirectoryRecord {
        DirectoryRecord {
            format_version: 1,
            owner_id: owner.to_owned(),
            node_advertise: advertise.to_owned(),
            published_at_ms: 1_000,
            valid_for_ms: 60_000,
            assignments: vec![DirectoryAssignment {
                canon: "canon-a".to_owned(),
                verse: "verse-a".to_owned(),
                advertise: advertise.to_owned(),
                posture: "standby".to_owned(),
                disposition: if serving {
                    "Serving".to_owned()
                } else {
                    "Standby".to_owned()
                },
                admits_committed_acks: serving,
            }],
        }
    }

    struct ScriptedSource {
        routes: Mutex<VecDeque<ProducerRoute>>,
    }

    impl ScriptedSource {
        fn new(routes: Vec<ProducerRoute>) -> Self {
            Self {
                routes: Mutex::new(routes.into()),
            }
        }
    }

    impl RouteSource for ScriptedSource {
        fn resolve(
            &self,
            canon: &str,
            verse: &str,
        ) -> Pin<Box<dyn Future<Output = Result<ProducerRoute, ProducerRoutingError>> + Send + '_>>
        {
            let canon = canon.to_owned();
            let verse = verse.to_owned();
            Box::pin(async move {
                let mut guard = self.routes.lock().expect("script lock");
                if let Some(route) = guard.pop_front() {
                    Ok(route)
                } else {
                    Err(ProducerRoutingError::NoCandidates { canon, verse })
                }
            })
        }
    }

    fn route_with(endpoints: &[&str]) -> ProducerRoute {
        let candidates = endpoints
            .iter()
            .enumerate()
            .map(|(index, endpoint)| RankedCandidate {
                owner_id: format!("owner-{index}"),
                endpoint: (*endpoint).to_owned(),
                disposition: "Serving".to_owned(),
                claims_serving: true,
                fresh: true,
                age_ms: 0,
            })
            .collect();
        ProducerRoute {
            canon: "canon-a".to_owned(),
            verse: "verse-a".to_owned(),
            candidates,
            resolved_at_ms: 1,
        }
    }

    async fn bind_local() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        (listener, format!("tcp://{addr}"))
    }

    /// Scripted ingress: for each accepted connection, run one response step.
    async fn spawn_scripted_ingress(
        listener: TcpListener,
        steps: Vec<IngressStep>,
    ) -> tokio::task::JoinHandle<Vec<Vec<u8>>> {
        tokio::spawn(async move {
            let mut seen = Vec::new();
            for step in steps {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut buf = Vec::new();
                // Read one newline-terminated frame.
                loop {
                    let mut byte = [0_u8; 1];
                    if stream.read_exact(&mut byte).await.is_err() {
                        break;
                    }
                    if byte[0] == b'\n' {
                        break;
                    }
                    buf.push(byte[0]);
                }
                seen.push(buf.clone());
                match step {
                    IngressStep::Ok { first, next } => {
                        let line = format!("OK {first} {next}\n");
                        stream.write_all(line.as_bytes()).await.expect("ok");
                    }
                    IngressStep::Err(reason) => {
                        let line = format!("ERR {reason}\n");
                        stream.write_all(line.as_bytes()).await.expect("err");
                    }
                    IngressStep::CommitThenClose => {
                        // Simulate commit-then-lost-ACK: peer closes without OK.
                    }
                    IngressStep::ResetMidWrite => {
                        // Drop immediately after accept / partial read.
                        drop(stream);
                        continue;
                    }
                }
                let _ = stream.shutdown().await;
            }
            seen
        })
    }

    #[derive(Clone)]
    enum IngressStep {
        Ok { first: u64, next: u64 },
        Err(&'static str),
        CommitThenClose,
        ResetMidWrite,
    }

    #[test]
    fn dial_address_strips_tcp_scheme() {
        assert_eq!(
            dial_address("tcp://127.0.0.1:9001").expect("dial"),
            "127.0.0.1:9001"
        );
        assert_eq!(
            dial_address("10.0.0.2:9200").expect("dial"),
            "10.0.0.2:9200"
        );
    }

    #[test]
    fn classify_matches_live_refusal_strings() {
        let deposed =
            r#"ERR effective-writer gate denied: NotEffectiveWriter { state: "Serving" }"#;
        let transitioning =
            r#"ERR effective-writer gate denied: NotEffectiveWriter { state: "Transitioning" }"#;
        assert!(matches!(
            classify_err_line(deposed),
            AttemptFailure::DeposedWriter { .. }
        ));
        assert!(matches!(
            classify_err_line(transitioning),
            AttemptFailure::Transitioning { .. }
        ));
    }

    #[test]
    fn route_ranking_prefers_serving_over_standby() {
        let records = vec![
            assignment("b", "tcp://b:9002", false),
            assignment("a", "tcp://a:9001", true),
        ];
        let route = route_from_records(&records, "canon-a", "verse-a", 2_000);
        assert_eq!(route.candidates.len(), 2);
        assert_eq!(route.candidates[0].owner_id, "a");
        assert!(route.candidates[0].claims_serving);
    }

    #[tokio::test]
    async fn resolve_route_reads_directory_and_ranks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        publish(
            &store,
            "test",
            &assignment("b", "tcp://127.0.0.1:9002", false),
        )
        .await
        .expect("publish b");
        publish(
            &store,
            "test",
            &assignment("a", "tcp://127.0.0.1:9001", true),
        )
        .await
        .expect("publish a");

        let route = resolve_route(&store, "test", "canon-a", "verse-a")
            .await
            .expect("resolve");
        assert_eq!(route.candidates[0].endpoint, "tcp://127.0.0.1:9001");
        assert_eq!(route.canon, "canon-a");
        assert_eq!(route.verse, "verse-a");
    }

    #[tokio::test]
    async fn deposed_writer_refreshes_and_retries_next_candidate() {
        let (listener_a, endpoint_a) = bind_local().await;
        let (listener_b, endpoint_b) = bind_local().await;

        let deposed = r#"effective-writer gate denied: NotEffectiveWriter { state: "Serving" }"#;
        let handle_a = spawn_scripted_ingress(listener_a, vec![IngressStep::Err(deposed)]).await;
        let handle_b =
            spawn_scripted_ingress(listener_b, vec![IngressStep::Ok { first: 0, next: 1 }]).await;

        // First resolve sees only A; refresh returns B as serving.
        let source = ScriptedSource::new(vec![
            route_with(&[endpoint_a.as_str()]),
            route_with(&[endpoint_b.as_str()]),
        ]);
        let mut producer =
            RoutingProducer::open_with_source(source, "canon-a", "verse-a", policy_fast())
                .await
                .expect("open");

        let id = RecordId::from_bytes([7; 16]);
        let record = OutboundRecord::with_id(id, b"payload-deposed");
        let ack = producer.send(&record).await.expect("ack after refresh");
        assert_eq!(ack.record_id, id);
        assert_eq!(ack.first_offset, 0);
        assert_eq!(ack.next_offset, 1);
        assert_eq!(ack.endpoint, endpoint_b);

        let seen_a = handle_a.await.expect("join a");
        let seen_b = handle_b.await.expect("join b");
        assert_eq!(seen_a, vec![b"payload-deposed".to_vec()]);
        assert_eq!(seen_b, vec![b"payload-deposed".to_vec()]);
    }

    #[tokio::test]
    async fn transitioning_backs_off_and_retries_without_permanent_failure() {
        let (listener, endpoint) = bind_local().await;
        let transitioning =
            r#"effective-writer gate denied: NotEffectiveWriter { state: "Transitioning" }"#;
        let handle = spawn_scripted_ingress(
            listener,
            vec![
                IngressStep::Err(transitioning),
                IngressStep::Ok { first: 3, next: 4 },
            ],
        )
        .await;

        let source = ScriptedSource::new(vec![
            route_with(&[endpoint.as_str()]),
            route_with(&[endpoint.as_str()]),
            route_with(&[endpoint.as_str()]),
        ]);
        let mut producer =
            RoutingProducer::open_with_source(source, "canon-a", "verse-a", policy_fast())
                .await
                .expect("open");

        let record = OutboundRecord::with_id(RecordId::from_bytes([1; 16]), b"gap");
        let ack = producer.send(&record).await.expect("eventual ack");
        assert_eq!((ack.first_offset, ack.next_offset), (3, 4));
        let seen = handle.await.expect("join");
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], b"gap");
        assert_eq!(seen[1], b"gap");
    }

    #[tokio::test]
    async fn dead_scribe_refreshes_and_tries_next_candidate() {
        let (listener_b, endpoint_b) = bind_local().await;
        let handle_b = spawn_scripted_ingress(
            listener_b,
            vec![IngressStep::Ok {
                first: 10,
                next: 11,
            }],
        )
        .await;

        // Port with nothing listening — connection refused.
        let dead = "tcp://127.0.0.1:1";
        let source = ScriptedSource::new(vec![
            route_with(&[dead, endpoint_b.as_str()]),
            route_with(&[endpoint_b.as_str()]),
        ]);
        let mut producer =
            RoutingProducer::open_with_source(source, "canon-a", "verse-a", policy_fast())
                .await
                .expect("open");

        let id = RecordId::from_bytes([2; 16]);
        let record = OutboundRecord::with_id(id, b"after-dead");
        let ack = producer.send(&record).await.expect("ack on live peer");
        assert_eq!(ack.record_id, id);
        assert_eq!(ack.endpoint, endpoint_b);
        let seen = handle_b.await.expect("join");
        assert_eq!(seen, vec![b"after-dead".to_vec()]);
    }

    #[tokio::test]
    async fn lost_reply_retries_preserving_record_identity() {
        let (listener, endpoint) = bind_local().await;
        let handle = spawn_scripted_ingress(
            listener,
            vec![
                IngressStep::CommitThenClose,
                IngressStep::Ok { first: 5, next: 6 },
            ],
        )
        .await;

        let source = ScriptedSource::new(vec![
            route_with(&[endpoint.as_str()]),
            route_with(&[endpoint.as_str()]),
            route_with(&[endpoint.as_str()]),
        ]);
        let mut producer =
            RoutingProducer::open_with_source(source, "canon-a", "verse-a", policy_fast())
                .await
                .expect("open");

        let id = RecordId::from_bytes([9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9]);
        let payload = format!("event:{}", id.to_hex()).into_bytes();
        let record = OutboundRecord::with_id(id, payload.clone());
        let ack = producer.send(&record).await.expect("ack after lost reply");
        assert_eq!(ack.record_id, id);
        assert_eq!((ack.first_offset, ack.next_offset), (5, 6));

        let seen = handle.await.expect("join");
        assert_eq!(seen.len(), 2, "must retry rather than drop");
        assert_eq!(seen[0], payload);
        assert_eq!(seen[1], payload);
        assert_eq!(
            seen[0], seen[1],
            "retry must preserve the same record bytes/identity"
        );
    }

    #[tokio::test]
    async fn exhaustion_names_last_failure_and_does_not_claim_success() {
        let (listener, endpoint) = bind_local().await;
        let deposed = r#"effective-writer gate denied: NotEffectiveWriter { state: "Serving" }"#;
        let _handle = spawn_scripted_ingress(
            listener,
            vec![
                IngressStep::Err(deposed),
                IngressStep::Err(deposed),
                IngressStep::Err(deposed),
            ],
        )
        .await;

        let source = ScriptedSource::new(vec![
            route_with(&[endpoint.as_str()]),
            route_with(&[endpoint.as_str()]),
            route_with(&[endpoint.as_str()]),
            route_with(&[endpoint.as_str()]),
        ]);
        let policy = RetryPolicy {
            max_attempts: 3,
            ..policy_fast()
        };
        let mut producer = RoutingProducer::open_with_source(source, "canon-a", "verse-a", policy)
            .await
            .expect("open");

        let id = RecordId::from_bytes([3; 16]);
        let record = OutboundRecord::with_id(id, b"never-acked");
        let err = producer.send(&record).await.expect_err("must exhaust");
        match err {
            ProducerRoutingError::Exhausted {
                record_id,
                attempts,
                last_failure,
            } => {
                assert_eq!(record_id, id);
                assert_eq!(attempts, 3);
                assert!(
                    last_failure.contains("NotEffectiveWriter"),
                    "last failure must be named: {last_failure}"
                );
            }
            other => panic!("expected Exhausted, got {other}"),
        }
    }

    #[tokio::test]
    async fn directory_backed_open_and_send_happy_path() {
        let (listener, endpoint) = bind_local().await;
        let handle =
            spawn_scripted_ingress(listener, vec![IngressStep::Ok { first: 0, next: 1 }]).await;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        publish(&store, "prod", &assignment("a", &endpoint, true))
            .await
            .expect("publish");

        let mut producer =
            RoutingProducer::open(store, "prod", "canon-a", "verse-a", policy_fast())
                .await
                .expect("open");

        let record = OutboundRecord::new(b"hello");
        let id = record.id;
        let ack = producer.send(&record).await.expect("ack");
        assert_eq!(ack.record_id, id);
        assert_eq!(ack.endpoint, endpoint);
        let seen = handle.await.expect("join");
        assert_eq!(seen, vec![b"hello".to_vec()]);
    }

    #[tokio::test]
    async fn reset_mid_connection_is_treated_as_dead_scribe() {
        let (listener_a, endpoint_a) = bind_local().await;
        let (listener_b, endpoint_b) = bind_local().await;
        let handle_a = spawn_scripted_ingress(listener_a, vec![IngressStep::ResetMidWrite]).await;
        let handle_b =
            spawn_scripted_ingress(listener_b, vec![IngressStep::Ok { first: 1, next: 2 }]).await;

        let source = ScriptedSource::new(vec![
            route_with(&[endpoint_a.as_str()]),
            route_with(&[endpoint_b.as_str()]),
        ]);
        let mut producer =
            RoutingProducer::open_with_source(source, "canon-a", "verse-a", policy_fast())
                .await
                .expect("open");

        let record = OutboundRecord::with_id(RecordId::from_bytes([4; 16]), b"reset");
        let ack = producer.send(&record).await.expect("failover ack");
        assert_eq!(ack.endpoint, endpoint_b);
        let _ = handle_a.await;
        let seen_b = handle_b.await.expect("join b");
        assert_eq!(seen_b, vec![b"reset".to_vec()]);
    }

    /// Ensures scripted refreshes are actually consumed (compile/runtime guard).
    #[tokio::test]
    async fn refresh_failure_surfaces_when_directory_empties() {
        let (listener, endpoint) = bind_local().await;
        let deposed = r#"effective-writer gate denied: NotEffectiveWriter { state: "Serving" }"#;
        let _handle = spawn_scripted_ingress(listener, vec![IngressStep::Err(deposed)]).await;

        // After the first deposed refusal, refresh yields no candidates.
        let source = ScriptedSource::new(vec![route_with(&[endpoint.as_str()])]);
        let policy = RetryPolicy {
            max_attempts: 2,
            ..policy_fast()
        };
        let mut producer = RoutingProducer::open_with_source(source, "canon-a", "verse-a", policy)
            .await
            .expect("open");
        let record = OutboundRecord::new(b"alone");
        let err = producer.send(&record).await.expect_err("exhaust");
        assert!(matches!(err, ProducerRoutingError::Exhausted { .. }));
    }

    #[test]
    fn record_id_hex_is_stable() {
        let id = RecordId::from_bytes([0xab; 16]);
        assert_eq!(id.to_hex(), "ab".repeat(16));
    }
}
