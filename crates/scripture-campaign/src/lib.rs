//! Opt-in correctness-campaign facility (feature `correctness-campaign`).
//!
//! This module drives the *real* Scripture product recovery paths — the same
//! `VerseRuntime`, `HaServingSession`, `HolylogJournalFoundation`, and
//! `AuthorityCoordinator` used in the in-process integration tests — against a
//! selectable backend (in-memory or a real RustFS/S3-compatible endpoint) and
//! records the shared Holylog correctness trace vocabulary. It then runs the
//! shared `holylog_correctness` checker and returns a redacted evidence bundle:
//! an NDJSON trace, final Journal Foundation and Serving Authority observations,
//! and an explicit `Pass` / `Fail` / `Inconclusive` verdict.
//!
//! It is deliberately narrow:
//! - it is never compiled into the default serve path;
//! - it introduces no new fleet daemon, operator, CRD, or public producer
//!   protocol;
//! - it never logs payload plaintext or credential values — only content
//!   digests, byte lengths, identities, and control observations.
//!
//! Node/process separation across Kubernetes pods is a *deployment* property
//! supplied by the campaign topology (RustFS on one node, this driver on
//! another, the checker on a third). Within one campaign process the A/B roles
//! are in-process; this module makes no multi-node object-store durability,
//! availability, or replica-independence claim.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{
    ExclusiveClaimStore, InMemoryExclusiveClaimStore, LogletComponents, LogletObjectNamespaces,
};
use holylog::virtual_log::{
    ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver, VirtualLog,
};
use holylog_correctness::faults::{FaultableConditionalRegister, FaultableLogDrive, FaultableSeal};
use holylog_correctness::{
    ActorId, ActorTrace, ArmedFault, EventKind, FaultController, OperationId, RecordingSink, RunId,
    TraceEvent, TraceSink, Verdict, check_trace, payload_digest,
};
use holylog_object_store::{ObjectStoreExclusiveClaim, ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::ObjectStore;
use object_store::path::Path;
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, RouteHint, ServingAuthorityRecord, WriterTerm,
};
use scripture::{
    CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, ProducerId,
    Receipt, Record, RecoveryBound, Submission, SystemClock, SystemTimer, VerseId, WriterId,
};
use scripture_service::virtuallog_test_support::VirtualLogHarness;
use scripture_service::{
    AuthorityCoordinator, CanonTransitionOutcome, DeterministicTransitionIdGenerator,
    JournalFoundationTransition, VerseHandoffRequest, VerseRuntime, VerseRuntimeConfig,
};

use scripture_runtime::{
    BackendProfile, DurableLogletParts, HaServingSession, HolylogJournalFoundation, NodeIdentity,
    ObjectStorePartsFactory, PartsFactory, PartsFactoryError, ProcessLogletResolver,
    SharedMemoryPartsFactory, bootstrap_and_serve, promote_and_serve,
};

/// Weak-tail window used by the Foundation-path scenarios.
const FOUNDATION_K: u64 = 2;

/// Named campaign scenarios. These correspond to the initial RustFS sequence in
/// the Tracker campaign topology and reuse the proven in-process flows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// A serves; bounded committed ACKs are recorded and re-observed.
    BaselineCommittedAck,
    /// Applied root-CAS whose reply is lost; A fails closed, B serves after an
    /// exact membership readback.
    RootCasReplyLost,
    /// Writer payload lands durably then the writer wedges before a committed
    /// ACK; HA recovery seals the predecessor and B serves the successor.
    WriterDiesAfterPayload,
}

impl Scenario {
    /// All scenario tokens accepted by [`Scenario::parse`].
    pub const ALL: [&'static str; 3] = [
        "baseline-committed-ack",
        "root-cas-reply-lost",
        "writer-dies-after-payload",
    ];

    /// Parses a scenario token.
    pub fn parse(raw: &str) -> Result<Self, CampaignError> {
        match raw {
            "baseline-committed-ack" => Ok(Self::BaselineCommittedAck),
            "root-cas-reply-lost" => Ok(Self::RootCasReplyLost),
            "writer-dies-after-payload" => Ok(Self::WriterDiesAfterPayload),
            other => Err(CampaignError::UnknownScenario(other.to_owned())),
        }
    }

    /// Stable scenario token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BaselineCommittedAck => "baseline-committed-ack",
            Self::RootCasReplyLost => "root-cas-reply-lost",
            Self::WriterDiesAfterPayload => "writer-dies-after-payload",
        }
    }
}

/// Backend selection for a campaign run.
pub enum CampaignBackend {
    /// Deterministic in-memory backend (fast preflight of the trace/checker path).
    InMemory,
    /// Real RustFS / S3-compatible endpoint under a run-scoped exclusive prefix.
    RustFs(RustFsBackend),
}

/// Real object-store backend bound to one exclusive run prefix.
pub struct RustFsBackend {
    store: Arc<dyn ObjectStore>,
    root: String,
    metrics: Arc<ObjectStoreMetrics>,
}

impl CampaignBackend {
    /// Builds a RustFS backend over an already-connected store and a run-scoped
    /// exclusive root prefix (never the whole bucket).
    #[must_use]
    pub fn rustfs(store: Arc<dyn ObjectStore>, root: impl Into<String>) -> Self {
        Self::RustFs(RustFsBackend {
            store,
            root: root.into().trim_end_matches('/').to_owned(),
            metrics: Arc::new(ObjectStoreMetrics::default()),
        })
    }

    /// Stable backend label for reports.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::InMemory => "memory",
            Self::RustFs(_) => "rustfs",
        }
    }

    fn environment(&self, scenario: Scenario, run_id: &str) -> serde_json::Value {
        let backend = match self {
            Self::InMemory => serde_json::json!({ "kind": "memory" }),
            Self::RustFs(backend) => serde_json::json!({
                "kind": "rustfs",
                "endpoint_class": "s3-compatible",
                "run_prefix": backend.root,
                "register_capabilities": "amazon_s3",
            }),
        };
        serde_json::json!({
            "run_id": run_id,
            "scenario": scenario.as_str(),
            "backend": backend,
            "claims": [
                "exercises the Holylog object-store adapters and Scripture recovery path against the configured backend",
            ],
            "non_claims": [
                "single-process A/B roles; not a multi-node process-separation proof",
                "no object-store replica independence, provider durability, or multi-site availability claim",
                "no equivalence with R2/S3/GCS is established by this run",
            ],
        })
    }

    /// Untraced inner conditional register for the configured backend.
    fn inner_register(&self) -> Result<Arc<dyn ConditionalRegister>, CampaignError> {
        match self {
            Self::InMemory => Ok(Arc::new(InMemoryConditionalRegister::new())),
            Self::RustFs(backend) => {
                let path = Path::from(backend.root.clone()).join(register_path("verse").as_ref());
                let register = ObjectStoreConditionalRegister::new(
                    Arc::clone(&backend.store),
                    path,
                    BackendProfile::RustFs.register_capabilities(),
                )
                .map_err(|error| CampaignError::Backend(error.to_string()))?;
                Ok(Arc::new(register))
            }
        }
    }

    /// Fresh, untraced durable parts for one loglet on the configured backend.
    fn fresh_parts(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, CampaignError> {
        match self {
            Self::InMemory => Ok(DurableLogletParts::from_components(
                Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
                Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
                Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
            )),
            Self::RustFs(backend) => backend
                .parts_factory()
                .fresh(loglet_id)
                .map_err(|error| CampaignError::Backend(error.to_string())),
        }
    }

    /// Traced (faultable) durable components for the harness-path scenarios.
    fn traced_components(
        &self,
        loglet_id: &LogletId,
        faults: &Arc<FaultController>,
        trace: &ActorTrace,
    ) -> Result<LogletComponents, CampaignError> {
        let raw = self.fresh_parts(loglet_id)?;
        let drive = Arc::new(FaultableLogDrive::new(
            raw.drive(),
            Arc::clone(faults),
            trace.clone(),
            loglet_id.as_str(),
        )) as Arc<dyn LogDrive>;
        let seal = Arc::new(FaultableSeal::new(
            raw.seal(),
            Arc::clone(faults),
            trace.clone(),
            loglet_id.as_str(),
        )) as Arc<dyn Seal>;
        Ok(LogletComponents::new(drive, seal, raw.trim(), 0))
    }

    /// Shared, traced parts factory for the Foundation-path scenario.
    fn tracing_parts_factory(
        &self,
        faults: Arc<FaultController>,
        trace: ActorTrace,
    ) -> Arc<dyn PartsFactory> {
        let inner: Arc<dyn PartsFactory> = match self {
            Self::InMemory => Arc::new(SharedMemoryPartsFactory::default()),
            Self::RustFs(backend) => backend.parts_factory(),
        };
        Arc::new(TracingPartsFactory::new(inner, faults, trace))
    }

    /// Exclusive claim store for the Foundation-path scenario.
    fn claims(&self) -> Result<Arc<dyn ExclusiveClaimStore>, CampaignError> {
        match self {
            Self::InMemory => Ok(Arc::new(InMemoryExclusiveClaimStore::new())),
            Self::RustFs(backend) => {
                let claim = ObjectStoreExclusiveClaim::new(
                    Arc::clone(&backend.store),
                    BackendProfile::RustFs.drive_capabilities(),
                )
                .map_err(|error| CampaignError::Backend(error.to_string()))?;
                Ok(Arc::new(claim))
            }
        }
    }
}

impl RustFsBackend {
    fn parts_factory(&self) -> Arc<dyn PartsFactory> {
        Arc::new(ObjectStorePartsFactory::new(
            Arc::clone(&self.store),
            self.root.clone(),
            BackendProfile::RustFs.drive_capabilities(),
            WritePolicy::AtomicCreate,
            Arc::clone(&self.metrics),
        ))
    }
}

/// Parts factory that wraps an inner factory's durable drive/seal with the
/// correctness harness faultable/tracing wrappers while preserving the real
/// inner operations. Wrapped parts are cached so `open` returns the same
/// handles a wedged `fresh` produced.
struct TracingPartsFactory {
    inner: Arc<dyn PartsFactory>,
    faults: Arc<FaultController>,
    trace: ActorTrace,
    cache: Mutex<std::collections::BTreeMap<LogletId, DurableLogletParts>>,
}

impl TracingPartsFactory {
    fn new(inner: Arc<dyn PartsFactory>, faults: Arc<FaultController>, trace: ActorTrace) -> Self {
        Self {
            inner,
            faults,
            trace,
            cache: Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    fn wrap(&self, loglet_id: &LogletId, raw: DurableLogletParts) -> DurableLogletParts {
        let drive = Arc::new(FaultableLogDrive::new(
            raw.drive(),
            Arc::clone(&self.faults),
            self.trace.clone(),
            loglet_id.to_string(),
        )) as Arc<dyn LogDrive>;
        let seal = Arc::new(FaultableSeal::new(
            raw.seal(),
            Arc::clone(&self.faults),
            self.trace.clone(),
            loglet_id.to_string(),
        )) as Arc<dyn Seal>;
        DurableLogletParts::from_components(drive, seal, raw.trim())
    }
}

impl PartsFactory for TracingPartsFactory {
    fn fresh(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        let raw = self.inner.fresh(loglet_id)?;
        let wrapped = self.wrap(loglet_id, raw);
        self.cache
            .lock()
            .map_err(|_| PartsFactoryError::new("tracing parts cache poisoned"))?
            .insert(loglet_id.clone(), wrapped.clone());
        Ok(wrapped)
    }

    fn open(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        if let Some(existing) = self
            .cache
            .lock()
            .map_err(|_| PartsFactoryError::new("tracing parts cache poisoned"))?
            .get(loglet_id)
            .cloned()
        {
            return Ok(existing);
        }
        let raw = self.inner.open(loglet_id)?;
        let wrapped = self.wrap(loglet_id, raw);
        self.cache
            .lock()
            .map_err(|_| PartsFactoryError::new("tracing parts cache poisoned"))?
            .insert(loglet_id.clone(), wrapped.clone());
        Ok(wrapped)
    }

    fn namespaces(
        &self,
        loglet_id: &LogletId,
    ) -> Result<LogletObjectNamespaces, PartsFactoryError> {
        self.inner.namespaces(loglet_id)
    }
}

/// Redacted campaign evidence bundle.
pub struct CampaignReport {
    /// Run identity used across trace, prefix, and artifacts.
    pub run_id: String,
    /// Scenario token.
    pub scenario: &'static str,
    /// Backend label.
    pub backend: &'static str,
    /// Redacted backend/topology identity (no secrets).
    pub environment: serde_json::Value,
    /// Structured trace events in global order.
    pub events: Vec<TraceEvent>,
    /// Final Journal Foundation membership observation.
    pub final_root: serde_json::Value,
    /// Final Serving Authority observation (null for harness-path scenarios).
    pub final_authority: serde_json::Value,
    /// Shared checker verdict.
    pub verdict: Verdict,
}

impl CampaignReport {
    /// Serializes the trace as newline-delimited JSON.
    pub fn trace_ndjson(&self) -> Result<String, CampaignError> {
        let mut out = String::new();
        for event in &self.events {
            out.push_str(
                &serde_json::to_string(event)
                    .map_err(|error| CampaignError::Serialize(error.to_string()))?,
            );
            out.push('\n');
        }
        Ok(out)
    }

    /// Serializes the verdict as JSON.
    pub fn verdict_json(&self) -> Result<serde_json::Value, CampaignError> {
        serde_json::to_value(&self.verdict)
            .map_err(|error| CampaignError::Serialize(error.to_string()))
    }

    /// Whether the checker returned `Pass`.
    #[must_use]
    pub fn is_pass(&self) -> bool {
        matches!(self.verdict, Verdict::Pass)
    }

    /// Stable lowercase verdict label for logs and reports.
    #[must_use]
    pub fn verdict_label(&self) -> &'static str {
        match &self.verdict {
            Verdict::Pass => "pass",
            Verdict::Fail { .. } => "fail",
            Verdict::Inconclusive { .. } => "inconclusive",
        }
    }

    /// Process exit code: 0 Pass, 2 Fail, 3 Inconclusive.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match &self.verdict {
            Verdict::Pass => 0,
            Verdict::Fail { .. } => 2,
            Verdict::Inconclusive { .. } => 3,
        }
    }
}

/// Campaign execution failures.
#[derive(Debug, thiserror::Error)]
pub enum CampaignError {
    /// Unknown scenario token.
    #[error(
        "unknown scenario {0:?}; expected baseline-committed-ack|root-cas-reply-lost|writer-dies-after-payload"
    )]
    UnknownScenario(String),
    /// Backend construction failure.
    #[error("backend setup: {0}")]
    Backend(String),
    /// Scenario execution failure.
    #[error("scenario execution: {0}")]
    Scenario(String),
    /// Serialization failure.
    #[error("serialize: {0}")]
    Serialize(String),
}

/// Runs a named scenario against `backend`, returning a redacted evidence bundle.
pub async fn run_campaign(
    run_id: &str,
    scenario: Scenario,
    backend: CampaignBackend,
) -> Result<CampaignReport, CampaignError> {
    let environment = backend.environment(scenario, run_id);
    let backend_label = backend.label();
    let (events, final_root, final_authority) = match scenario {
        Scenario::BaselineCommittedAck => run_baseline(run_id, &backend).await?,
        Scenario::RootCasReplyLost => run_root_cas_reply_lost(run_id, &backend).await?,
        Scenario::WriterDiesAfterPayload => run_wedged_payload(run_id, &backend).await?,
    };
    let verdict = check_trace(&events);
    Ok(CampaignReport {
        run_id: run_id.to_owned(),
        scenario: scenario.as_str(),
        backend: backend_label,
        environment,
        events,
        final_root,
        final_authority,
        verdict,
    })
}

// ---- fixed campaign identities (non-secret) --------------------------------

fn journal() -> JournalId {
    JournalId::from_bytes(*b"cmpn-journal!!!!")
}
fn verse() -> VerseId {
    VerseId::from_bytes(*b"cmpn-verse!!!!!!")
}
fn owner_a() -> OwnerId {
    OwnerId::from_bytes(*b"cmpn-owner-a!!!!")
}
fn owner_b() -> OwnerId {
    OwnerId::from_bytes(*b"cmpn-owner-b!!!!")
}
fn producer() -> ProducerId {
    ProducerId::from_bytes(*b"cmpn-producer!!!")
}
fn key() -> AuthorityKey {
    AuthorityKey {
        journal_id: journal(),
        verse_id: verse(),
    }
}

fn config(owner: OwnerId) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: journal(),
        verse_id: verse(),
        owner_id: owner,
        cohort_id: CohortId::from_bytes(*b"cmpn-cohort!!!!!"),
        writer_id: WriterId::from_bytes(*b"cmpn-writer!!!!!"),
        policy: ChunkPolicy {
            max_chunk_bytes: 64 * 1024,
            max_record_bytes: 16 * 1024,
            max_chunk_records: 8,
            max_chunk_age: Duration::from_secs(60),
            max_buffered_bytes: 64 * 1024,
            max_inflight_chunks: 1,
            max_uncommitted_age: Duration::from_secs(60),
            recovery_scan: RecoveryBound::new(8).expect("recovery bound"),
        },
        recovery_bound: RecoveryBound::new(8).expect("recovery bound"),
        queue_capacity: 16,
    }
}

fn campaign_owner(owner: OwnerId) -> CanonOwner {
    CanonOwner::Owned {
        owner_id: owner,
        endpoint: OwnerEndpoint::new("tcp://campaign.local:9000").expect("endpoint"),
        sequencer: None,
        writer_term: None,
    }
}

fn campaign_fence(revision: u64, owner: OwnerId) -> CanonFence {
    CanonFence::new(revision, journal(), verse(), campaign_owner(owner))
}

fn emit_committed_ack(
    trace: &ActorTrace,
    operation: &str,
    receipt: &Receipt,
    loglet_id: &LogletId,
) {
    let bytes = receipt.chunk_id.as_bytes();
    trace.emit(
        Some(OperationId::new(operation.to_owned())),
        EventKind::ScriptureCommittedAck {
            logical_offset: receipt.first_offset.get(),
            digest: payload_digest(&bytes),
            size: bytes.len(),
            loglet_id: loglet_id.as_str().into(),
        },
    );
}

async fn membership_json(
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<dyn LogletResolver>,
) -> serde_json::Value {
    let log = VirtualLog::new(register, resolver);
    match log.observe_membership().await {
        Ok(observed) => {
            let active = observed.state.active();
            serde_json::json!({
                "revision": observed.state.revision,
                "active_loglet": active.map(|generation| generation.loglet_id.as_str().to_owned()),
                "active_start": active.map(|generation| generation.start),
                "generations": observed.state.generations.len(),
            })
        }
        Err(error) => serde_json::json!({ "error": error.to_string() }),
    }
}

async fn authority_json(
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<dyn LogletResolver>,
) -> serde_json::Value {
    let log = VirtualLog::new(register, resolver);
    match log.observe_membership().await {
        Ok(observed) => {
            if observed.state.application_fence.as_bytes().is_empty() {
                return serde_json::json!({ "state": "absent" });
            }
            match ServingAuthorityRecord::decode_application_fence(
                &observed.state.application_fence,
            ) {
                Ok(record) => match &record.state {
                    AuthorityState::Serving { authority, .. } => serde_json::json!({
                        "state": "serving",
                        "generation_revision": authority.generation_ref.virtual_log_revision,
                    }),
                    AuthorityState::Transitioning { .. } => {
                        serde_json::json!({ "state": "transitioning" })
                    }
                    other => serde_json::json!({ "state": format!("{other:?}") }),
                },
                Err(error) => serde_json::json!({ "error": error.to_string() }),
            }
        }
        Err(error) => serde_json::json!({ "error": error.to_string() }),
    }
}

async fn commit_record(
    runtime: &VerseRuntime,
    sequence: u64,
    payload: &'static [u8],
) -> Result<Receipt, CampaignError> {
    let pending = runtime
        .submit(Submission {
            producer_id: producer(),
            producer_epoch: 0,
            sequence,
            records: vec![Record::new([], Bytes::from_static(payload))],
        })
        .await
        .map_err(|error| CampaignError::Scenario(format!("admit: {error}")))?;
    runtime
        .flush()
        .await
        .map_err(|error| CampaignError::Scenario(format!("flush: {error}")))?;
    pending
        .await
        .map_err(|error| CampaignError::Scenario(format!("commit: {error}")))
}

async fn commit_session(
    session: &HaServingSession,
    sequence: u64,
    payload: &'static [u8],
) -> Result<Receipt, CampaignError> {
    let pending = session
        .submit(Submission {
            producer_id: producer(),
            producer_epoch: 0,
            sequence,
            records: vec![Record::new([], Bytes::from_static(payload))],
        })
        .await
        .map_err(|error| CampaignError::Scenario(format!("session admit: {error}")))?;
    session
        .flush()
        .await
        .map_err(|error| CampaignError::Scenario(format!("session flush: {error}")))?;
    pending
        .await
        .map_err(|error| CampaignError::Scenario(format!("session commit: {error}")))
}

// ---- scenarios -------------------------------------------------------------

type ScenarioOutput = (Vec<TraceEvent>, serde_json::Value, serde_json::Value);

async fn run_baseline(
    run_id: &str,
    backend: &CampaignBackend,
) -> Result<ScenarioOutput, CampaignError> {
    let run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let foundation_trace = ActorTrace::new(
        run.clone(),
        ActorId::new("foundation"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let root_faults = Arc::new(FaultController::new());
    let register = Arc::new(FaultableConditionalRegister::new(
        backend.inner_register()?,
        root_faults,
        foundation_trace.clone(),
    )) as Arc<dyn ConditionalRegister>;
    let harness = VirtualLogHarness::with_ids(
        "cmpn-first",
        "cmpn-second",
        "cmpn-third",
        Arc::clone(&register),
    )
    .await;

    let writer_faults = Arc::new(FaultController::new());
    let first = harness
        .fleet
        .provision_with_components(
            &harness.first,
            backend.traced_components(&harness.first, &writer_faults, &foundation_trace)?,
        )
        .await;
    harness
        .virtual_log()
        .bootstrap_with_receipt(
            first.receipt,
            first.writable.as_ref(),
            &first.bind,
            campaign_fence(0, owner_a()).encode(),
        )
        .await
        .map_err(|error| CampaignError::Scenario(format!("bootstrap: {error}")))?;

    let runtime = VerseRuntime::start(
        config(owner_a()),
        harness.virtual_log(),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .map_err(|error| CampaignError::Scenario(format!("start A: {error}")))?;

    let payloads: [&'static [u8]; 2] = [b"baseline-0", b"baseline-1"];
    for (sequence, payload) in payloads.into_iter().enumerate() {
        let sequence = sequence as u64;
        let receipt = commit_record(&runtime, sequence, payload).await?;
        emit_committed_ack(
            &ActorTrace::new(
                run.clone(),
                ActorId::new("scripture-a"),
                Arc::clone(&sink) as Arc<dyn TraceSink>,
            ),
            &format!("baseline-a-{sequence}"),
            &receipt,
            &harness.first,
        );
    }

    let final_root = membership_json(
        Arc::clone(&register),
        Arc::clone(&harness.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    Ok((sink.events(), final_root, serde_json::Value::Null))
}

async fn run_root_cas_reply_lost(
    run_id: &str,
    backend: &CampaignBackend,
) -> Result<ScenarioOutput, CampaignError> {
    let run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let foundation_trace = ActorTrace::new(
        run.clone(),
        ActorId::new("foundation"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let root_faults = Arc::new(FaultController::new());
    let register = Arc::new(FaultableConditionalRegister::new(
        backend.inner_register()?,
        Arc::clone(&root_faults),
        foundation_trace.clone(),
    )) as Arc<dyn ConditionalRegister>;
    let harness = VirtualLogHarness::with_ids(
        "cmpn-first",
        "cmpn-second",
        "cmpn-third",
        Arc::clone(&register),
    )
    .await;

    let first_faults = Arc::new(FaultController::new());
    let first = harness
        .fleet
        .provision_with_components(
            &harness.first,
            backend.traced_components(&harness.first, &first_faults, &foundation_trace)?,
        )
        .await;
    harness
        .virtual_log()
        .bootstrap_with_receipt(
            first.receipt,
            first.writable.as_ref(),
            &first.bind,
            campaign_fence(0, owner_a()).encode(),
        )
        .await
        .map_err(|error| CampaignError::Scenario(format!("bootstrap A: {error}")))?;

    let runtime_a = VerseRuntime::start(
        config(owner_a()),
        harness.virtual_log(),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .map_err(|error| CampaignError::Scenario(format!("start A: {error}")))?;
    let receipt_a = commit_record(&runtime_a, 0, b"before-cutover").await?;
    emit_committed_ack(
        &ActorTrace::new(
            run.clone(),
            ActorId::new("scripture-a"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        ),
        "root-cas-a-0",
        &receipt_a,
        &harness.first,
    );

    let second_faults = Arc::new(FaultController::new());
    let successor = harness
        .fleet
        .provision_with_components(
            &harness.second,
            backend.traced_components(&harness.second, &second_faults, &foundation_trace)?,
        )
        .await;

    root_faults.arm(ArmedFault::RootCasReplyLost);
    let (runtime_a, outcome) = runtime_a
        .drain_seal_publish(VerseHandoffRequest {
            successor,
            next_owner: campaign_owner(owner_b()),
            journal_id: journal(),
            verse_id: verse(),
        })
        .await
        .map_err(|failure| {
            CampaignError::Scenario(format!("drain_seal_publish: {}", failure.error))
        })?;
    if !matches!(outcome, CanonTransitionOutcome::FailedNeedsReconcile { .. }) {
        return Err(CampaignError::Scenario(
            "root-CAS reply loss did not leave A in the expected terminal reconciliation outcome"
                .into(),
        ));
    }
    if !runtime_a.is_terminal() {
        return Err(CampaignError::Scenario(
            "root-CAS reply loss left A non-terminal".into(),
        ));
    }
    drop(runtime_a);

    // Exact readback of the applied-but-reply-lost CAS re-establishes B.
    harness
        .virtual_log()
        .observe_membership()
        .await
        .map_err(|error| CampaignError::Scenario(format!("readback: {error}")))?;

    let runtime_b = VerseRuntime::start(
        config(owner_b()),
        harness.virtual_log(),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .map_err(|error| CampaignError::Scenario(format!("start B: {error}")))?;
    let receipt_b = commit_record(&runtime_b, 1, b"after-cutover").await?;
    emit_committed_ack(
        &ActorTrace::new(
            run.clone(),
            ActorId::new("scripture-b"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        ),
        "root-cas-b-1",
        &receipt_b,
        &harness.second,
    );

    let final_root = membership_json(
        Arc::clone(&register),
        Arc::clone(&harness.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    Ok((sink.events(), final_root, serde_json::Value::Null))
}

struct NodeBundle {
    resolver: Arc<ProcessLogletResolver>,
    foundation: Arc<HolylogJournalFoundation>,
    coordinator: AuthorityCoordinator,
}

fn build_node(
    owner: OwnerId,
    endpoint: &str,
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
) -> Result<NodeBundle, CampaignError> {
    let identity = NodeIdentity {
        owner_id: owner,
        endpoint: OwnerEndpoint::new(endpoint)
            .map_err(|error| CampaignError::Scenario(format!("endpoint: {error}")))?,
    };
    let resolver = Arc::new(ProcessLogletResolver::default());
    let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
        key(),
        identity,
        Arc::clone(&register),
        Arc::clone(&resolver),
        Arc::clone(&parts),
        Arc::clone(&claims),
        FOUNDATION_K,
    ));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
        Arc::new(DeterministicTransitionIdGenerator::new()),
        owner,
        RouteHint::new(endpoint)
            .map_err(|error| CampaignError::Scenario(format!("route: {error}")))?,
    );
    Ok(NodeBundle {
        resolver,
        foundation,
        coordinator,
    })
}

async fn run_wedged_payload(
    run_id: &str,
    backend: &CampaignBackend,
) -> Result<ScenarioOutput, CampaignError> {
    let run = RunId::new(run_id.to_owned());
    let sink = RecordingSink::new().shared();
    let foundation_trace = ActorTrace::new(
        run.clone(),
        ActorId::new("foundation"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let root_faults = Arc::new(FaultController::new());
    let register = Arc::new(FaultableConditionalRegister::new(
        backend.inner_register()?,
        root_faults,
        foundation_trace.clone(),
    )) as Arc<dyn ConditionalRegister>;
    let writer_faults = Arc::new(FaultController::new());
    let parts = backend.tracing_parts_factory(Arc::clone(&writer_faults), foundation_trace);
    let claims = backend.claims()?;

    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    )?;
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    )?;

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        key(),
        WriterTerm::new(1).map_err(|error| CampaignError::Scenario(format!("term: {error}")))?,
        config(owner_a()),
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .map_err(|error| CampaignError::Scenario(format!("bootstrap_and_serve A: {error}")))?;
    let expected = a_session.generation().clone();

    writer_faults.arm(ArmedFault::DieAfterPayload);
    let pending = a_session
        .submit(Submission {
            producer_id: producer(),
            producer_epoch: 0,
            sequence: 0,
            records: vec![Record::new(
                [],
                Bytes::from_static(b"durable-but-uncommitted"),
            )],
        })
        .await
        .map_err(|error| CampaignError::Scenario(format!("A admit: {error}")))?;
    // The payload write lands durably, then the writer wedges before completion.
    match tokio::time::timeout(Duration::from_millis(200), a_session.flush()).await {
        Err(_) => {}
        Ok(Ok(())) => {
            return Err(CampaignError::Scenario(
                "writer-dies-after-payload flush completed before the injected death".into(),
            ));
        }
        Ok(Err(error)) => {
            return Err(CampaignError::Scenario(format!(
                "writer-dies-after-payload flush failed before the injected death: {error}"
            )));
        }
    }

    // Release the simulated dead actor so the wedged receipt resolves as an
    // error. This models process loss; it never turns the write into a commit.
    writer_faults.death_gate().open();
    match tokio::time::timeout(Duration::from_secs(2), pending).await {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => {
            return Err(CampaignError::Scenario(
                "writer-dies-after-payload returned a committed receipt for A".into(),
            ));
        }
        Err(_) => {
            return Err(CampaignError::Scenario(
                "writer-dies-after-payload receipt did not resolve after simulated death".into(),
            ));
        }
    }

    let active = expected.active_loglet_id.clone();
    drop(a_session);
    a.resolver.remove(&active);

    let b_session = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        key(),
        WriterTerm::new(2).map_err(|error| CampaignError::Scenario(format!("term: {error}")))?,
        expected,
        config(owner_b()),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .map_err(|error| CampaignError::Scenario(format!("promote_and_serve B: {error}")))?;

    let successor_payload = b"committed-after-recovery";
    let receipt = commit_session(&b_session, 1, successor_payload).await?;
    emit_committed_ack(
        &ActorTrace::new(
            run,
            ActorId::new("scripture-b"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        ),
        "wedged-b-1",
        &receipt,
        &b_session.generation().active_loglet_id,
    );

    let final_root = membership_json(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    let final_authority = authority_json(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    Ok((sink.events(), final_root, final_authority))
}
