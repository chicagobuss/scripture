//! Scripture Verse node supervisor.
//!
//! Explicitly **single-Verse**: one ConditionalRegister, one config, one key.
//! Ordinary startup never bootstraps, elects, or auto-replaces. Control methods
//! serialize on an internal mutex. Listener paths hold only [`Arc<VerseRuntime>`].
//!
//! Process restart materializes Canon-referenced Loglets via [`PartsFactory::open`]
//! before any owner recovery attempt. A locally owned open active generation
//! yields [`VerseControlOutcome::RecoveryRequired`].

use std::collections::BTreeMap;
use std::sync::Arc;

use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{
    BindTag, ExclusiveClaimStore, InMemoryExclusiveClaimStore, LogletComponents,
    LogletObjectNamespaces, OpenReattachError, ProvisionAuthority, ProvisionError, ProvisionerId,
    ReadSealView, ResolvedLoglet, WritableLoglet,
};
use holylog::virtual_log::{
    ConditionalRegister, LogletId, LogletResolver, ResolveFuture, VirtualLog, VirtualLogError,
};
use scripture::{CanonFenceError, CanonOwner, OwnerEndpoint, OwnerId};
use scripture_service::{
    AbandonedProvisionCandidate, ProvisionedSuccessor, ScriptureNode, ScriptureNodeConfigError,
    VerseKey, VerseRuntime, VerseRuntimeConfig, VerseRuntimeStartError,
};
use tokio::sync::Mutex;

pub(super) fn owned_with_sequencer(owner_id: OwnerId, endpoint: OwnerEndpoint, _revision: u64) -> CanonOwner {
    CanonOwner::Owned {
        owner_id,
        endpoint,
        // This runtime remains legacy until it has a real locally-held remote
        // sequencer capability. A v2 fence must never be published merely to
        // make a pre-remote owner look dynamic.
        sequencer: None,
        writer_term: None,
    }
}

/// Stable identity of one Scripture node process for one Scripture node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeIdentity {
    /// Durable owner identity supplied by the deployment.
    pub owner_id: OwnerId,
    /// Advisory endpoint published into Canon fences for this node.
    pub endpoint: OwnerEndpoint,
}

/// Durable Loglet components for one generation (object-store backed).
#[derive(Clone)]
pub struct DurableLogletParts {
    drive: Arc<dyn LogDrive>,
    seal: Arc<dyn Seal>,
    trim: Arc<dyn TrimPoint>,
}

impl DurableLogletParts {
    /// Assembles already-namespaced drive/seal/trim handles.
    #[must_use]
    pub fn from_components(
        drive: Arc<dyn LogDrive>,
        seal: Arc<dyn Seal>,
        trim: Arc<dyn TrimPoint>,
    ) -> Self {
        Self { drive, seal, trim }
    }

    /// Shared handle to the durable data drive.
    #[must_use]
    pub fn drive(&self) -> Arc<dyn LogDrive> {
        Arc::clone(&self.drive)
    }

    /// Shared handle to the durable seal.
    #[must_use]
    pub fn seal(&self) -> Arc<dyn Seal> {
        Arc::clone(&self.seal)
    }

    /// Shared handle to the durable trim point.
    #[must_use]
    pub fn trim(&self) -> Arc<dyn TrimPoint> {
        Arc::clone(&self.trim)
    }

    /// Bundles durable parts into Holylog [`LogletComponents`] under `k`.
    #[must_use]
    pub fn components(&self, k: u64) -> LogletComponents {
        LogletComponents::new(
            Arc::clone(&self.drive),
            Arc::clone(&self.seal),
            Arc::clone(&self.trim),
            k,
        )
    }

    fn fresh_memory() -> Self {
        Self::from_components(
            Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
            Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
            Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
        )
    }
}

/// Lab-only root for in-memory claim/namespace identity (must agree with the
/// claim store shared across supervisors that reuse the same factory).
const MEMORY_PARTS_ROOT: &str = "scripture-memory";
const SHARED_MEMORY_PARTS_ROOT: &str = "scripture-shared-memory";

/// Allocates durable data/seal/trim namespaces for a Loglet id.
pub trait PartsFactory: Send + Sync {
    /// Empty namespaces suitable for fresh provision.
    fn fresh(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError>;
    /// Re-open existing durable namespaces (process restart / refuse path).
    fn open(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError>;
    /// Deterministic object namespaces for claim + provision (same root as parts).
    fn namespaces(&self, loglet_id: &LogletId)
    -> Result<LogletObjectNamespaces, PartsFactoryError>;
}

/// Failures while allocating durable Loglet parts.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct PartsFactoryError(String);

impl PartsFactoryError {
    /// Builds a parts-factory failure from a displayable cause.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Process-local in-memory parts. [`PartsFactory::open`] invents empty drives and
/// is not valid across process boundaries — use [`SharedMemoryPartsFactory`] or
/// an object-store factory for durable reopen.
#[derive(Debug, Default)]
pub struct InMemoryPartsFactory;

impl PartsFactory for InMemoryPartsFactory {
    fn fresh(&self, _loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        Ok(DurableLogletParts::fresh_memory())
    }

    fn open(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        Err(PartsFactoryError::new(format!(
            "InMemoryPartsFactory cannot reopen {loglet_id}; use SharedMemoryPartsFactory or object-store"
        )))
    }

    fn namespaces(
        &self,
        loglet_id: &LogletId,
    ) -> Result<LogletObjectNamespaces, PartsFactoryError> {
        Ok(LogletObjectNamespaces::under_root(
            MEMORY_PARTS_ROOT,
            loglet_id,
        ))
    }
}

/// In-memory parts that survive across independently constructed supervisors
/// when the factory Arc is shared (lab stand-in for object-store prefixes).
#[derive(Default)]
pub struct SharedMemoryPartsFactory {
    parts: std::sync::Mutex<BTreeMap<LogletId, DurableLogletParts>>,
}

impl std::fmt::Debug for SharedMemoryPartsFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedMemoryPartsFactory")
            .finish_non_exhaustive()
    }
}

impl PartsFactory for SharedMemoryPartsFactory {
    fn fresh(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        let parts = DurableLogletParts::fresh_memory();
        let mut map = self
            .parts
            .lock()
            .map_err(|_| PartsFactoryError::new("SharedMemoryPartsFactory lock poisoned"))?;
        if map.contains_key(loglet_id) {
            return Err(PartsFactoryError::new(format!(
                "Loglet {loglet_id} already has durable parts"
            )));
        }
        map.insert(loglet_id.clone(), parts.clone());
        Ok(parts)
    }

    fn open(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        self.parts
            .lock()
            .map_err(|_| PartsFactoryError::new("SharedMemoryPartsFactory lock poisoned"))?
            .get(loglet_id)
            .cloned()
            .ok_or_else(|| {
                PartsFactoryError::new(format!("no durable parts for Loglet {loglet_id}"))
            })
    }

    fn namespaces(
        &self,
        loglet_id: &LogletId,
    ) -> Result<LogletObjectNamespaces, PartsFactoryError> {
        Ok(LogletObjectNamespaces::under_root(
            SHARED_MEMORY_PARTS_ROOT,
            loglet_id,
        ))
    }
}

/// Shared Loglet resolver that installs capability-typed handles only.
#[derive(Default)]
pub struct ProcessLogletResolver {
    loglets: std::sync::Mutex<BTreeMap<LogletId, ResolvedLoglet>>,
}

impl ProcessLogletResolver {
    /// Installs a freshly provisioned writable Loglet.
    pub fn insert_writable(&self, id: LogletId, writable: Arc<WritableLoglet>) {
        self.loglets
            .lock()
            .expect("lock")
            .insert(id, ResolvedLoglet::Writable(writable));
    }

    /// Installs a read/seal-only historical Loglet.
    pub fn insert_read_seal(&self, id: LogletId, view: Arc<ReadSealView>) {
        self.loglets
            .lock()
            .expect("lock")
            .insert(id, ResolvedLoglet::ReadSeal(view));
    }

    /// Removes a Loglet handle (process crash simulation).
    pub fn remove(&self, id: &LogletId) -> Option<ResolvedLoglet> {
        self.loglets.lock().expect("lock").remove(id)
    }

    /// Whether `id` is currently installed.
    #[must_use]
    pub fn contains(&self, id: &LogletId) -> bool {
        self.loglets.lock().expect("lock").contains_key(id)
    }

    /// Whether `id` is installed as writable (append-capable).
    #[must_use]
    pub fn is_writable(&self, id: &LogletId) -> bool {
        matches!(
            self.loglets.lock().expect("lock").get(id),
            Some(ResolvedLoglet::Writable(_))
        )
    }
}

impl LogletResolver for ProcessLogletResolver {
    fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<ResolvedLoglet>> {
        let id = id.clone();
        Box::pin(async move { Ok(self.loglets.lock().expect("lock").get(&id).cloned()) })
    }
}

/// Local bookkeeping for the configured Verse.
struct VerseStore {
    parts: BTreeMap<LogletId, DurableLogletParts>,
    active: Option<LogletId>,
}

impl VerseStore {
    fn new() -> Self {
        Self {
            parts: BTreeMap::new(),
            active: None,
        }
    }
}

/// Startup / control outcome for the supervised Verse.
#[derive(Debug)]
pub enum VerseControlOutcome {
    /// [`VerseRuntime`] is serving.
    Serving,
    /// Runtime is standby (no actor).
    Standby,
    /// Durable membership exists but this process must not write the open generation.
    RecoveryRequired {
        /// Typed Holylog refusal.
        error: OpenReattachError,
    },
    /// Replacement CAS lost; local active was not promoted.
    ///
    /// The successor was removed from the resolver. The receipt is preserved on
    /// `candidate` for explicit operator retry or deliberate abandonment.
    ConflictNeedsInspect {
        /// Unpublished provisioned successor (receipt unconsumed).
        candidate: AbandonedProvisionCandidate,
    },
    /// Runtime startup failed for another typed reason.
    StartFailed(VerseRuntimeStartError),
}

/// Verse node supervisor for one Scripture Verse in one process.
///
/// Control methods take `&self` and serialize through the private control lock so a
/// raw-lines listener may hold [`Arc<VerseRuntime>`] clones while the operator
/// thread runs consuming handoff/replace.
pub struct VerseNodeSupervisor {
    identity: NodeIdentity,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
    parts: Arc<dyn PartsFactory>,
    authority: ProvisionAuthority,
    config: VerseRuntimeConfig,
    key: VerseKey,
    store: Mutex<VerseStore>,
    node: Mutex<Option<ScriptureNode>>,
    runtime: Mutex<Option<Arc<VerseRuntime>>>,
    /// Serializes bootstrap / start / replace / crash / handoff.
    control: Mutex<()>,
}

impl VerseNodeSupervisor {
    /// Builds a single-Verse supervisor with process-local in-memory parts and claims.
    pub fn new(
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<ProcessLogletResolver>,
        config: VerseRuntimeConfig,
    ) -> Self {
        Self::with_parts_factory(
            identity,
            register,
            resolver,
            Arc::new(InMemoryPartsFactory),
            config,
        )
    }

    /// Builds a single-Verse supervisor with an explicit durable parts factory and
    /// a process-local in-memory claim store.
    pub fn with_parts_factory(
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<ProcessLogletResolver>,
        parts: Arc<dyn PartsFactory>,
        config: VerseRuntimeConfig,
    ) -> Self {
        Self::with_parts_factory_and_claims(
            identity,
            register,
            resolver,
            parts,
            config,
            Arc::new(InMemoryExclusiveClaimStore::new()),
        )
    }

    /// Builds a supervisor with shared durable parts and a shared claim store
    /// (shared durable parts and claim store).
    pub fn with_parts_factory_and_claims(
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<ProcessLogletResolver>,
        parts: Arc<dyn PartsFactory>,
        config: VerseRuntimeConfig,
        claims: Arc<dyn ExclusiveClaimStore>,
    ) -> Self {
        let provisioner = ProvisionerId::new(format!("scripture-node-{}", identity.owner_id));
        let key = VerseKey::from_config(&config);
        Self {
            identity,
            register,
            resolver,
            parts,
            authority: ProvisionAuthority::new(claims, provisioner),
            config,
            key,
            store: Mutex::new(VerseStore::new()),
            node: Mutex::new(None),
            runtime: Mutex::new(None),
            control: Mutex::new(()),
        }
    }

    fn bind_for(loglet_id: &LogletId) -> BindTag {
        BindTag::new(format!("scripture:{loglet_id}").into_bytes())
    }

    async fn provision_and_install(
        &self,
        loglet_id: LogletId,
        k: u64,
    ) -> Result<(DurableLogletParts, ProvisionedSuccessor), SupervisorError> {
        let parts = self.parts.fresh(&loglet_id)?;
        let namespaces = self.parts.namespaces(&loglet_id)?;
        let bind = Self::bind_for(&loglet_id);
        let (receipt, writable) = self
            .authority
            .provision_fresh(
                loglet_id.clone(),
                namespaces,
                bind.clone(),
                parts.components(k),
            )
            .await?;
        let writable = Arc::new(writable);
        self.resolver
            .insert_writable(loglet_id, Arc::clone(&writable));
        Ok((
            parts,
            ProvisionedSuccessor {
                receipt,
                writable,
                bind,
            },
        ))
    }

    /// Configured node identity.
    #[must_use]
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Sole Verse key for this supervisor.
    #[must_use]
    pub fn verse_key(&self) -> VerseKey {
        self.key
    }

    /// Locally remembered active Loglet after bootstrap/replace materialization.
    pub async fn active_loglet(&self) -> Option<LogletId> {
        self.store.lock().await.active.clone()
    }

    /// VirtualLog bound to this Verse's sole register/resolver.
    #[must_use]
    pub fn virtual_log(&self) -> VirtualLog {
        VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        )
    }

    /// Borrow the started runtime for listener admission (cloneable Arc).
    pub async fn runtime(&self) -> Option<Arc<VerseRuntime>> {
        self.runtime.lock().await.clone()
    }
}

mod activation;
mod handoff;
mod inspect;
mod prelude;

/// Supervisor control-plane failures.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    /// Unknown configured Verse.
    #[error("unknown Verse key")]
    UnknownVerse {
        /// Missing key.
        key: VerseKey,
    },
    /// Config owner disagrees with the node identity.
    #[error("Verse owner does not match node identity")]
    OwnerMismatch {
        /// Configured owner.
        configured: OwnerId,
        /// Node owner.
        node: OwnerId,
    },
    /// Canon fence journal/Verse disagree with the supervisor config.
    #[error("Canon fence identity disagrees with supervisor config")]
    CanonIdentityMismatch {
        /// Fence journal.
        fence_journal: scripture::JournalId,
        /// Fence Verse.
        fence_verse: scripture::VerseId,
        /// Config journal.
        config_journal: scripture::JournalId,
        /// Config Verse.
        config_verse: scripture::VerseId,
    },
    /// No active Loglet recorded for replace/crash.
    #[error("Verse has no active Loglet in the supervisor store")]
    NoActiveLoglet {
        /// Verse key.
        key: VerseKey,
    },
    /// Local active disagrees with the register observation (another reconfigurer won).
    #[error("local active {local} disagrees with observed active {observed}")]
    StaleActive {
        /// Locally remembered active.
        local: LogletId,
        /// Register-observed active.
        observed: LogletId,
    },
    /// Listener still holds the runtime Arc during consuming handoff.
    #[error("Verse runtime is still shared; finish listener work before handoff")]
    RuntimeInUse {
        /// Busy key.
        key: VerseKey,
    },
    /// Canon revision would overflow.
    #[error("Canon revision overflow from {revision}")]
    RevisionOverflow {
        /// Current revision.
        revision: u64,
    },
    /// Duplicate / config errors from [`ScriptureNode::start`].
    #[error(transparent)]
    Config(#[from] ScriptureNodeConfigError),
    /// Canon fence decode / binding failure.
    #[error(transparent)]
    Canon(#[from] CanonFenceError),
    /// Holylog provision failure.
    #[error(transparent)]
    Provision(#[from] ProvisionError),
    /// Durable parts allocation failure.
    #[error(transparent)]
    Parts(#[from] PartsFactoryError),
    /// Holylog AtomicLog construction failure.
    #[error(transparent)]
    AtomicLog(#[from] holylog::atomic::AtomicLogError),
    /// VirtualLog / register failure.
    #[error(transparent)]
    VirtualLog(#[from] VirtualLogError),
    /// Handoff rejection.
    #[error(transparent)]
    Handoff(#[from] scripture_service::VerseHandoffError),
    /// Greenfield bootstrap refused because Canon membership already exists.
    #[error(
        "Canon already initialized; refusing another bootstrap (no provision or publish performed)"
    )]
    AlreadyInitialized,
    /// Canon authority validation failed.
    #[error(transparent)]
    CanonAuthority(#[from] scripture::CanonAuthorityError),
    /// Greenfield activation observed a non-empty durable tail.
    #[error(
        "greenfield activation requires an empty active generation, but observed tail was {tail}"
    )]
    NonEmptyTail {
        /// Observed non-zero tail.
        tail: u64,
    },
    /// Greenfield activation did not observe RecoveryRequired/MustSealAndReplace.
    #[error(
        "greenfield activation requires disposition RecoveryRequired(MustSealAndReplace), but was {disposition:?}"
    )]
    InvalidActivationDisposition {
        /// Observed refusal/disposition.
        disposition: Result<WritableLoglet, OpenReattachError>,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
    use holylog::virtual_log::{InMemoryConditionalRegister, LogletId};
    use scripture::{
        ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, SystemClock,
        VerseId, WriterId,
    };
    use scripture_service::VerseRuntimeConfig;

    use super::{
        NodeIdentity, SharedMemoryPartsFactory, SupervisorError, VerseControlOutcome,
        VerseNodeSupervisor,
    };
    use crate::ProcessLogletResolver;

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"fleet-lab-jrnl!!")
    }

    fn verse() -> VerseId {
        VerseId::from_bytes(*b"fleet-lab-verse!")
    }

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"fleet-lab-own-a!")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"fleet-lab-own-b!")
    }

    fn config(owner: OwnerId) -> VerseRuntimeConfig {
        VerseRuntimeConfig {
            journal_id: journal(),
            verse_id: verse(),
            owner_id: owner,
            cohort_id: CohortId::from_bytes(*b"fleet-lab-cohrt!"),
            writer_id: WriterId::from_bytes(*b"fleet-lab-wrtr!!"),
            policy: ChunkPolicy {
                max_chunk_bytes: 64 * 1024,
                max_record_bytes: 16 * 1024,
                max_chunk_records: 8,
                max_chunk_age: Duration::from_secs(60),
                max_buffered_bytes: 64 * 1024,
                max_inflight_chunks: 1,
                max_uncommitted_age: Duration::from_secs(60),
                recovery_scan: RecoveryBound::new(8).expect("bound"),
            },
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 16,
        }
    }

    fn identity(owner: OwnerId) -> NodeIdentity {
        NodeIdentity {
            owner_id: owner,
            endpoint: OwnerEndpoint::new("tcp://owner.lab:9000").expect("endpoint"),
        }
    }

    #[tokio::test]
    async fn bootstrap_canon_fails_closed_when_already_initialized() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let node = VerseNodeSupervisor::with_parts_factory(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
        );
        node.bootstrap_canon(LogletId::new("gen-once").expect("id"), 2)
            .await
            .expect("first");
        assert!(node.runtime().await.is_none());
        let err = node
            .bootstrap_canon(LogletId::new("gen-twice").expect("id"), 2)
            .await
            .expect_err("second");
        assert!(matches!(err, SupervisorError::AlreadyInitialized));
    }

    #[tokio::test]
    async fn bootstrap_serving_and_peer_standby_share_backend() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let key_resolver = Arc::new(ProcessLogletResolver::default());

        let node_a = VerseNodeSupervisor::with_parts_factory(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&key_resolver),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
        );
        let outcome = node_a
            .bootstrap_verse(
                LogletId::new("fleet-a0").expect("id"),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect("bootstrap");
        assert!(matches!(outcome, VerseControlOutcome::Serving));

        // Peer B: fresh resolver (process boundary), shared durable parts + register.
        let node_b = VerseNodeSupervisor::with_parts_factory(
            identity(owner_b()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_b()),
        );
        let outcome = node_b
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect("start b");
        assert!(matches!(outcome, VerseControlOutcome::Standby));
        assert!(node_b.runtime().await.expect("rt").is_standby());
    }

    #[tokio::test]
    async fn bootstrap_cas_conflict_leaves_no_local_active_candidate() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());

        let winner = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            Arc::new(InMemoryExclusiveClaimStore::new()),
        );
        winner
            .bootstrap_verse(
                LogletId::new("bootstrap-winner").expect("winner id"),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect("winner bootstrap");

        let loser_resolver = Arc::new(ProcessLogletResolver::default());
        let loser = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_b()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&loser_resolver),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_b()),
            Arc::new(InMemoryExclusiveClaimStore::new()),
        );
        let loser_id = LogletId::new("bootstrap-loser").expect("loser id");
        let error = loser
            .bootstrap_verse(
                loser_id.clone(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect_err("second bootstrap must fail closed");
        assert!(matches!(error, SupervisorError::AlreadyInitialized));
        assert_eq!(loser.active_loglet().await, None);
        assert!(!loser_resolver.contains(&loser_id));
    }

    #[tokio::test]
    async fn crash_reports_recovery_required_not_serving() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let resolver = Arc::new(ProcessLogletResolver::default());
        let node = VerseNodeSupervisor::new(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&resolver),
            config(owner_a()),
        );
        node.bootstrap_verse(
            LogletId::new("fleet-crash").expect("id"),
            SystemClock::new(),
            scripture::SystemTimer::new(),
            2,
        )
        .await
        .expect("bootstrap");
        node.crash_active_writer().await.expect("crash");
        let outcome = node
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect("restart");
        assert!(matches!(
            outcome,
            VerseControlOutcome::RecoveryRequired { .. }
        ));
    }

    #[tokio::test]
    async fn process_boundary_fresh_owner_reports_recovery_required() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());

        let node_a = VerseNodeSupervisor::with_parts_factory(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
        );
        node_a
            .bootstrap_verse(
                LogletId::new("fleet-proc-a").expect("id"),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect("bootstrap");
        // Drop process-local state; durable parts + register remain.
        drop(node_a);

        let fresh_a = VerseNodeSupervisor::with_parts_factory(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
        );
        let outcome = fresh_a
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect("fresh a");
        assert!(matches!(
            outcome,
            VerseControlOutcome::RecoveryRequired { .. }
        ));

        let fresh_b = VerseNodeSupervisor::with_parts_factory(
            identity(owner_b()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_b()),
        );
        let outcome = fresh_b
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect("fresh b");
        assert!(matches!(outcome, VerseControlOutcome::Standby));
    }

    #[tokio::test]
    async fn competing_replace_loser_is_conflict_and_not_active() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let claims: Arc<dyn ExclusiveClaimStore> = Arc::new(InMemoryExclusiveClaimStore::new());

        let boot = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            Arc::clone(&claims),
        );
        boot.bootstrap_verse(
            LogletId::new("fleet-race-0").expect("id"),
            SystemClock::new(),
            scripture::SystemTimer::new(),
            2,
        )
        .await
        .expect("bootstrap");
        drop(boot);

        let left = Arc::new(VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            Arc::clone(&claims),
        ));
        let right = Arc::new(VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            Arc::clone(&claims),
        ));
        assert!(matches!(
            left.start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
                .await
                .expect("left rr"),
            VerseControlOutcome::RecoveryRequired { .. }
        ));
        assert!(matches!(
            right
                .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
                .await
                .expect("right rr"),
            VerseControlOutcome::RecoveryRequired { .. }
        ));

        let left_task = Arc::clone(&left);
        let right_task = Arc::clone(&right);
        let (left_out, right_out) = tokio::join!(
            left_task.replace_after_lost_sequencer(
                LogletId::new("fleet-race-l").expect("id"),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            ),
            right_task.replace_after_lost_sequencer(
                LogletId::new("fleet-race-r").expect("id"),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            ),
        );
        let left_out = left_out;
        let right_out = right_out;
        let mut serving = 0usize;
        let mut conflict = 0usize;
        let mut stale = 0usize;
        for result in [&left_out, &right_out] {
            match result {
                Ok(VerseControlOutcome::Serving) => serving += 1,
                Ok(VerseControlOutcome::ConflictNeedsInspect { .. }) => conflict += 1,
                Err(SupervisorError::StaleActive { .. }) => stale += 1,
                other => panic!("unexpected replace outcome: {other:?}"),
            }
        }
        assert_eq!(serving, 1);
        assert_eq!(conflict + stale, 1);

        let winner = if matches!(left_out, Ok(VerseControlOutcome::Serving)) {
            &left
        } else {
            &right
        };
        let loser = if matches!(left_out, Ok(VerseControlOutcome::Serving)) {
            &right
        } else {
            &left
        };
        let published = winner
            .virtual_log()
            .observe_membership()
            .await
            .expect("observe")
            .state
            .active()
            .expect("active")
            .loglet_id
            .clone();
        assert_eq!(
            winner.active_loglet().await.expect("winner active"),
            published
        );
        // Loser must not have promoted an unpublished successor.
        assert_ne!(
            loser.active_loglet().await.expect("loser active"),
            published
        );
        assert!(!matches!(
            loser.runtime().await.map(|r| r.is_serving()),
            Some(true)
        ));
    }

    #[tokio::test]
    async fn second_provision_of_same_namespaces_is_refused_by_claim_store() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let claims: Arc<dyn ExclusiveClaimStore> = Arc::new(InMemoryExclusiveClaimStore::new());
        let loglet = LogletId::new("fleet-claim-once").expect("id");

        let first = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::new(super::InMemoryPartsFactory) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            Arc::clone(&claims),
        );
        first
            .bootstrap_verse(
                loglet.clone(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect("first bootstrap");

        let second = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(ProcessLogletResolver::default()),
            Arc::new(super::InMemoryPartsFactory) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            claims,
        );
        let err = second
            .bootstrap_verse(loglet, SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect_err("second provision must fail");
        assert!(matches!(err, SupervisorError::AlreadyInitialized));
    }

    #[tokio::test]
    async fn recovery_required_installs_no_writable_and_cannot_append() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let claims: Arc<dyn ExclusiveClaimStore> = Arc::new(InMemoryExclusiveClaimStore::new());
        let boot_resolver = Arc::new(ProcessLogletResolver::default());
        let boot = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&boot_resolver),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            Arc::clone(&claims),
        );
        let loglet = LogletId::new("fleet-no-writer").expect("id");
        boot.bootstrap_verse(
            loglet.clone(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
            2,
        )
        .await
        .expect("bootstrap");
        drop(boot);

        let resolver = Arc::new(ProcessLogletResolver::default());
        let fresh = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&resolver),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            claims,
        );
        let outcome = fresh
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect("restart");
        assert!(matches!(
            outcome,
            VerseControlOutcome::RecoveryRequired { .. }
        ));
        assert!(resolver.contains(&loglet));
        assert!(!resolver.is_writable(&loglet));
        let append_err = fresh
            .virtual_log()
            .append(bytes::Bytes::from_static(b"no"))
            .await
            .expect_err("append must refuse without writable");
        assert!(matches!(
            append_err,
            holylog::virtual_log::VirtualLogError::NotWritable { .. }
        ));
    }

    #[tokio::test]
    async fn replace_makes_predecessor_read_seal_only_and_successor_writable() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let claims: Arc<dyn ExclusiveClaimStore> = Arc::new(InMemoryExclusiveClaimStore::new());
        let resolver = Arc::new(ProcessLogletResolver::default());
        let node = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&resolver),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            claims,
        );
        let first = LogletId::new("fleet-pred").expect("id");
        let second = LogletId::new("fleet-succ").expect("id");
        node.bootstrap_verse(
            first.clone(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
            2,
        )
        .await
        .expect("bootstrap");
        node.crash_active_writer().await.expect("crash");
        assert!(matches!(
            node.start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
                .await
                .expect("rr"),
            VerseControlOutcome::RecoveryRequired { .. }
        ));
        let outcome = node
            .replace_after_lost_sequencer(
                second.clone(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect("replace");
        assert!(matches!(outcome, VerseControlOutcome::Serving));
        assert!(!resolver.is_writable(&first));
        assert!(resolver.is_writable(&second));
        assert_eq!(node.active_loglet().await.expect("active"), second);
    }

    #[tokio::test]
    async fn activate_empty_open_generation_refuses_when_serving() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let claims: Arc<dyn ExclusiveClaimStore> = Arc::new(InMemoryExclusiveClaimStore::new());
        let resolver = Arc::new(ProcessLogletResolver::default());
        let node = VerseNodeSupervisor::with_parts_factory_and_claims(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&resolver),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
            claims,
        );
        let first = LogletId::new("activation-serving-a").expect("id");
        let second = LogletId::new("activation-serving-b").expect("id");
        assert!(matches!(
            node.bootstrap_verse(
                first.clone(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect("bootstrap"),
            VerseControlOutcome::Serving
        ));

        let error = node
            .activate_empty_open_generation(
                second.clone(),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect_err("serving runtime must refuse activation");
        assert!(matches!(error, SupervisorError::RuntimeInUse { .. }));
        assert_eq!(node.active_loglet().await.expect("active"), first);
        assert!(resolver.is_writable(&first));
        assert!(!resolver.contains(&second));
    }
}
