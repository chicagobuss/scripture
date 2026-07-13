//! Fleet-lab Verse node supervisor.
//!
//! Composes stable node identity, Verse configurations, Holylog provision
//! boundaries, and [`ScriptureNode`] lifecycle. Ordinary startup never
//! bootstraps, elects, or auto-replaces a wedged Verse — those are explicit
//! operator/test commands.
//!
//! Administrative handoff uses [`VerseRuntime::drain_seal_publish`] (consuming).
//! The listener path holds only [`Arc<VerseRuntime>`] for route/admit; it cannot
//! unwrap a mutable [`scripture_service::ChunkJournalService`].

use std::collections::BTreeMap;
use std::sync::Arc;

use holylog::atomic::AtomicLog;
use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{
    OpenReattachError, ProvisionError, provision_fresh_writable, refuse_open_writable_reattach,
    resolve_read_seal,
};
use holylog::virtual_log::{
    ConditionalRegister, GenerationDescriptor, LogletId, LogletResolver, ResolveFuture, VirtualLog,
};
use scripture::{CanonFence, CanonOwner, Clock, OwnerEndpoint, OwnerId, Timer};
use scripture_service::{
    ScriptureNode, ScriptureNodeConfigError, ScriptureNodeStart, VerseHandoffRequest, VerseKey,
    VerseRuntime, VerseRuntimeConfig, VerseRuntimeStartError,
};
use tokio::sync::Mutex;

/// Stable identity of one Scripture node process in the fleet lab.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeIdentity {
    /// Durable owner identity supplied by the deployment.
    pub owner_id: OwnerId,
    /// Advisory endpoint published into Canon fences for this node.
    pub endpoint: OwnerEndpoint,
}

/// Durable Loglet components for one generation (fleet-lab).
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

    fn fresh_memory() -> Self {
        Self::from_components(
            Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
            Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
            Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
        )
    }
}

/// Allocates durable data/seal/trim namespaces for a Loglet id.
pub trait PartsFactory: Send + Sync {
    /// Empty namespaces suitable for [`provision_fresh_writable`].
    fn fresh(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError>;
    /// Re-open existing durable namespaces (process restart / refuse path).
    fn open(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError>;
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

/// Process-local in-memory parts. Cross-process reopen is not durable.
#[derive(Debug, Default)]
pub struct InMemoryPartsFactory;

impl PartsFactory for InMemoryPartsFactory {
    fn fresh(&self, _loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        Ok(DurableLogletParts::fresh_memory())
    }

    fn open(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        self.fresh(loglet_id)
    }
}

/// Shared Loglet resolver that can install provisioned AtomicLogs.
#[derive(Default)]
pub struct FleetLabResolver {
    loglets: std::sync::Mutex<BTreeMap<LogletId, Arc<AtomicLog>>>,
}

impl FleetLabResolver {
    /// Installs a resolved AtomicLog under `id`.
    pub fn insert(&self, id: LogletId, log: Arc<AtomicLog>) {
        self.loglets.lock().expect("lock").insert(id, log);
    }

    /// Removes a Loglet handle (process crash simulation).
    pub fn remove(&self, id: &LogletId) -> Option<Arc<AtomicLog>> {
        self.loglets.lock().expect("lock").remove(id)
    }
}

impl LogletResolver for FleetLabResolver {
    fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<Arc<AtomicLog>>> {
        let id = id.clone();
        Box::pin(async move { Ok(self.loglets.lock().expect("lock").get(&id).cloned()) })
    }
}

/// Per-Verse durable bookkeeping for the in-memory fleet lab.
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

/// Startup / control outcome for one Verse under the supervisor.
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
    /// Runtime startup failed for another typed reason.
    StartFailed(VerseRuntimeStartError),
}

/// Fleet-lab supervisor for one Scripture node process.
///
/// Control methods take `&self` and serialize through an internal mutex so a
/// raw-lines listener may hold [`Arc<VerseRuntime>`] clones while the operator
/// thread runs consuming handoff/replace on the selected Verse.
pub struct VerseNodeSupervisor {
    identity: NodeIdentity,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<FleetLabResolver>,
    parts: Arc<dyn PartsFactory>,
    configs: BTreeMap<VerseKey, VerseRuntimeConfig>,
    stores: Mutex<BTreeMap<VerseKey, VerseStore>>,
    node: Mutex<Option<ScriptureNode>>,
    runtimes: Mutex<BTreeMap<VerseKey, Arc<VerseRuntime>>>,
}

impl VerseNodeSupervisor {
    /// Builds a supervisor over a shared register/resolver. Does not start Verses.
    pub fn new(
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<FleetLabResolver>,
        configs: Vec<VerseRuntimeConfig>,
    ) -> Result<Self, ScriptureNodeConfigError> {
        Self::with_parts_factory(
            identity,
            register,
            resolver,
            Arc::new(InMemoryPartsFactory),
            configs,
        )
    }

    /// Builds a supervisor with an explicit durable parts factory (object-store lab).
    pub fn with_parts_factory(
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<FleetLabResolver>,
        parts: Arc<dyn PartsFactory>,
        configs: Vec<VerseRuntimeConfig>,
    ) -> Result<Self, ScriptureNodeConfigError> {
        let mut map = BTreeMap::new();
        for config in configs {
            let key = VerseKey::from_config(&config);
            if map.insert(key, config).is_some() {
                return Err(ScriptureNodeConfigError::DuplicateVerse { key });
            }
        }
        Ok(Self {
            identity,
            register,
            resolver,
            parts,
            configs: map,
            stores: Mutex::new(BTreeMap::new()),
            node: Mutex::new(None),
            runtimes: Mutex::new(BTreeMap::new()),
        })
    }

    /// Configured node identity.
    #[must_use]
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Shared VirtualLog for one Verse key (same register/resolver for all Verses
    /// in this lab composition).
    #[must_use]
    pub fn virtual_log(&self) -> VirtualLog {
        VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        )
    }

    /// Explicit bootstrap of a brand-new Verse: provision empty Loglet, publish
    /// Canon naming this node, then start the runtime.
    pub async fn bootstrap_verse<C, T>(
        &self,
        key: VerseKey,
        loglet_id: LogletId,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let config = self
            .configs
            .get(&key)
            .ok_or(SupervisorError::UnknownVerse { key })?
            .clone();
        if config.owner_id != self.identity.owner_id {
            return Err(SupervisorError::OwnerMismatch {
                configured: config.owner_id,
                node: self.identity.owner_id,
            });
        }

        let parts = self.parts.fresh(&loglet_id)?;
        let fresh = provision_fresh_writable(
            GenerationDescriptor {
                loglet_id: loglet_id.clone(),
                start: 0,
            },
            Arc::clone(&parts.drive),
            Arc::clone(&parts.seal),
            Arc::clone(&parts.trim),
            k,
        )
        .await?;
        self.resolver
            .insert(loglet_id.clone(), Arc::new(fresh.into_atomic_log()));
        {
            let mut stores = self.stores.lock().await;
            let store = stores.entry(key).or_insert_with(VerseStore::new);
            store.parts.insert(loglet_id.clone(), parts);
            store.active = Some(loglet_id.clone());
        }

        let fence = CanonFence::new(
            0,
            config.journal_id,
            config.verse_id,
            CanonOwner::Owned {
                owner_id: self.identity.owner_id,
                endpoint: self.identity.endpoint.clone(),
            },
        );
        self.virtual_log()
            .bootstrap_with_application_fence(loglet_id, fence.encode())
            .await?;

        self.start_verse(key, clock, timer).await
    }

    /// Starts configured Verses from existing durable Canon evidence.
    ///
    /// Does not provision or replace. Open active generations that this process
    /// cannot write are reported as [`VerseControlOutcome::RecoveryRequired`].
    pub async fn start_configured<C, T>(
        &self,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<BTreeMap<VerseKey, VerseControlOutcome>, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let mut outcomes = BTreeMap::new();
        for key in self.configs.keys().copied() {
            outcomes.insert(
                key,
                self.start_or_require_recovery(key, clock.clone(), timer.clone(), k)
                    .await?,
            );
        }
        Ok(outcomes)
    }

    async fn start_or_require_recovery<C, T>(
        &self,
        key: VerseKey,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let (active, parts) = {
            let stores = self.stores.lock().await;
            let Some(store) = stores.get(&key) else {
                // No local durable parts yet — attempt runtime start from register alone
                // (standby / error paths). Serving requires prior bootstrap/replace.
                return self.start_verse(key, clock, timer).await;
            };
            let Some(active) = store.active.clone() else {
                return self.start_verse(key, clock, timer).await;
            };
            let Some(parts) = store.parts.get(&active).cloned() else {
                return self.start_verse(key, clock, timer).await;
            };
            (active, parts)
        };

        match refuse_open_writable_reattach(
            active,
            Arc::clone(&parts.drive),
            Arc::clone(&parts.seal),
            Arc::clone(&parts.trim),
            k,
        )
        .await
        {
            Ok(_) => unreachable!("refuse_open_writable_reattach never returns Ok"),
            Err(error) => Ok(VerseControlOutcome::RecoveryRequired { error }),
        }
    }

    /// Seals a lost-sequencer active Verse and provisions a fresh successor owned
    /// by this node. Explicit operator action only.
    pub async fn replace_after_lost_sequencer<C, T>(
        &self,
        key: VerseKey,
        successor: LogletId,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let config = self
            .configs
            .get(&key)
            .ok_or(SupervisorError::UnknownVerse { key })?
            .clone();
        let (active, parts) = {
            let stores = self.stores.lock().await;
            let store = stores
                .get(&key)
                .ok_or(SupervisorError::UnknownVerse { key })?;
            let active = store
                .active
                .clone()
                .ok_or(SupervisorError::NoActiveLoglet { key })?;
            let parts = store
                .parts
                .get(&active)
                .cloned()
                .ok_or(SupervisorError::NoActiveLoglet { key })?;
            (active, parts)
        };

        let historical = resolve_read_seal(
            Arc::clone(&parts.drive),
            Arc::clone(&parts.seal),
            Arc::clone(&parts.trim),
            k,
        )
        .await?;
        if !matches!(
            historical.check_tail().await?.seal_status,
            holylog::atomic::SealStatus::Sealed
        ) {
            historical.seal().await?;
        }
        let sealed_tail = historical.check_tail().await?.tail;
        let sealed_view = Arc::new(
            AtomicLog::builder(Arc::clone(&parts.drive), k)
                .seal(Arc::clone(&parts.seal))
                .trim(Arc::clone(&parts.trim))
                .build()?,
        );
        self.resolver.insert(active, sealed_view);

        let next_parts = self.parts.fresh(&successor)?;
        let fresh = provision_fresh_writable(
            GenerationDescriptor {
                loglet_id: successor.clone(),
                start: sealed_tail,
            },
            Arc::clone(&next_parts.drive),
            Arc::clone(&next_parts.seal),
            Arc::clone(&next_parts.trim),
            k,
        )
        .await?;
        self.resolver
            .insert(successor.clone(), Arc::new(fresh.into_atomic_log()));
        {
            let mut stores = self.stores.lock().await;
            let store = stores.entry(key).or_insert_with(VerseStore::new);
            store.parts.insert(successor.clone(), next_parts);
            store.active = Some(successor.clone());
        }

        let observed = self.virtual_log().observe_membership().await?;
        let next_revision =
            observed
                .state
                .revision
                .checked_add(1)
                .ok_or(SupervisorError::RevisionOverflow {
                    revision: observed.state.revision,
                })?;
        let fence = CanonFence::new(
            next_revision,
            config.journal_id,
            config.verse_id,
            CanonOwner::Owned {
                owner_id: self.identity.owner_id,
                endpoint: self.identity.endpoint.clone(),
            },
        );
        self.virtual_log()
            .reconfigure_from_observation(&observed, successor, fence.encode())
            .await?;

        self.start_verse(key, clock, timer).await
    }

    /// Simulates losing in-process writer handles for the active Loglet.
    pub async fn crash_active_writer(&self, key: VerseKey) -> Result<(), SupervisorError> {
        let active = {
            let stores = self.stores.lock().await;
            stores
                .get(&key)
                .and_then(|store| store.active.clone())
                .ok_or(SupervisorError::NoActiveLoglet { key })?
        };
        self.resolver.remove(&active);
        let mut runtimes = self.runtimes.lock().await;
        runtimes.remove(&key);
        let mut node = self.node.lock().await;
        *node = None;
        Ok(())
    }

    /// Borrow the started runtime for listener admission (cloneable Arc).
    pub async fn runtime(&self, key: VerseKey) -> Option<Arc<VerseRuntime>> {
        self.runtimes.lock().await.get(&key).cloned()
    }

    /// Consuming fenced handoff for a serving Verse.
    pub async fn drain_seal_publish(
        &self,
        key: VerseKey,
        request: VerseHandoffRequest,
    ) -> Result<scripture_service::CanonTransitionOutcome, SupervisorError> {
        let mut runtimes = self.runtimes.lock().await;
        let runtime = runtimes
            .remove(&key)
            .ok_or(SupervisorError::UnknownVerse { key })?;
        // Exclusive consume: drop Arc so into_inner can succeed.
        let runtime =
            Arc::try_unwrap(runtime).map_err(|_| SupervisorError::RuntimeInUse { key })?;
        match runtime.drain_seal_publish(request).await {
            Ok((runtime, outcome)) => {
                runtimes.insert(key, Arc::new(runtime));
                Ok(outcome)
            }
            Err(failure) => {
                runtimes.insert(
                    VerseKey::new(failure.runtime.journal_id(), failure.runtime.verse_id()),
                    Arc::new(failure.runtime),
                );
                Err(SupervisorError::Handoff(failure.error))
            }
        }
    }

    async fn start_verse<C, T>(
        &self,
        key: VerseKey,
        clock: C,
        timer: T,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let config = self
            .configs
            .get(&key)
            .ok_or(SupervisorError::UnknownVerse { key })?
            .clone();
        let started =
            ScriptureNode::start(vec![config], |_| self.virtual_log(), clock, timer).await?;
        self.install_start(key, started).await
    }

    async fn install_start(
        &self,
        key: VerseKey,
        started: ScriptureNodeStart,
    ) -> Result<VerseControlOutcome, SupervisorError> {
        if let Some(error) = started.failures.into_values().next() {
            return Ok(VerseControlOutcome::StartFailed(error));
        }
        let mut runtimes_map = started.runtimes;
        let Some(runtime) = runtimes_map.remove(&key) else {
            return Err(SupervisorError::UnknownVerse { key });
        };
        let serving = runtime.is_serving();
        let standby = runtime.is_standby();
        self.runtimes.lock().await.insert(key, Arc::new(runtime));
        *self.node.lock().await = Some(ScriptureNode::from_started(runtimes_map));
        let _ = standby;
        Ok(if serving {
            VerseControlOutcome::Serving
        } else {
            VerseControlOutcome::Standby
        })
    }
}

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
    /// No active Loglet recorded for replace/crash.
    #[error("Verse has no active Loglet in the supervisor store")]
    NoActiveLoglet {
        /// Verse key.
        key: VerseKey,
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
    VirtualLog(#[from] holylog::virtual_log::VirtualLogError),
    /// Handoff rejection.
    #[error(transparent)]
    Handoff(#[from] scripture_service::VerseHandoffError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use holylog::virtual_log::{InMemoryConditionalRegister, LogletId};
    use scripture::{
        ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, SystemClock,
        VerseId, WriterId,
    };
    use scripture_service::VerseRuntimeConfig;

    use super::{NodeIdentity, VerseControlOutcome, VerseNodeSupervisor};
    use crate::FleetLabResolver;
    use scripture_service::VerseKey;

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
    async fn bootstrap_serving_and_peer_standby_share_backend() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let resolver = Arc::new(FleetLabResolver::default());
        let key = VerseKey::new(journal(), verse());

        let node_a = VerseNodeSupervisor::new(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&resolver),
            vec![config(owner_a())],
        )
        .expect("a");
        let outcome = node_a
            .bootstrap_verse(
                key,
                LogletId::new("fleet-a0").expect("id"),
                SystemClock::new(),
                scripture::SystemTimer::new(),
                2,
            )
            .await
            .expect("bootstrap");
        assert!(matches!(outcome, VerseControlOutcome::Serving));

        let node_b = VerseNodeSupervisor::new(
            identity(owner_b()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&resolver),
            vec![config(owner_b())],
        )
        .expect("b");
        let outcomes = node_b
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect("start b");
        assert!(matches!(
            outcomes.get(&key),
            Some(VerseControlOutcome::Standby)
        ));
        assert!(node_b.runtime(key).await.expect("rt").is_standby());
    }

    #[tokio::test]
    async fn crash_reports_recovery_required_not_serving() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let resolver = Arc::new(FleetLabResolver::default());
        let key = VerseKey::new(journal(), verse());
        let node = VerseNodeSupervisor::new(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::clone(&resolver),
            vec![config(owner_a())],
        )
        .expect("node");
        node.bootstrap_verse(
            key,
            LogletId::new("fleet-crash").expect("id"),
            SystemClock::new(),
            scripture::SystemTimer::new(),
            2,
        )
        .await
        .expect("bootstrap");
        node.crash_active_writer(key).await.expect("crash");
        let outcomes = node
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect("restart");
        assert!(matches!(
            outcomes.get(&key),
            Some(VerseControlOutcome::RecoveryRequired { .. })
        ));
    }
}
