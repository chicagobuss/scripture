//! Fleet-lab Verse node supervisor.
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

use holylog::atomic::AtomicLog;
use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{
    OpenReattachError, ProvisionError, provision_fresh_writable, refuse_open_writable_reattach,
    resolve_read_seal,
};
use holylog::virtual_log::{
    ConditionalRegister, GenerationDescriptor, LogletId, LogletResolver, Reconfiguration,
    ResolveFuture, VirtualLog, VirtualLogError,
};
use scripture::{CanonFence, CanonFenceError, CanonOwner, Clock, OwnerEndpoint, OwnerId, Timer};
use scripture_service::{
    ScriptureNode, ScriptureNodeConfigError, ScriptureNodeStart, VerseHandoffRequest, VerseKey,
    VerseRuntime, VerseRuntimeConfig, VerseRuntimeStartError,
};
use tokio::sync::Mutex;

fn owned_with_sequencer(owner_id: OwnerId, endpoint: OwnerEndpoint, _revision: u64) -> CanonOwner {
    CanonOwner::Owned {
        owner_id,
        endpoint,
        // The fleet lab remains legacy until it has a real locally-held remote
        // sequencer capability. A v2 fence must never be published merely to
        // make a pre-remote owner look dynamic.
        sequencer: None,
    }
}

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

    /// Whether `id` is currently installed.
    #[must_use]
    pub fn contains(&self, id: &LogletId) -> bool {
        self.loglets.lock().expect("lock").contains_key(id)
    }
}

impl LogletResolver for FleetLabResolver {
    fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<Arc<AtomicLog>>> {
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
    ConflictNeedsInspect,
    /// Runtime startup failed for another typed reason.
    StartFailed(VerseRuntimeStartError),
}

/// Fleet-lab supervisor for one Scripture Verse in one process.
///
/// Control methods take `&self` and serialize through the private control lock so a
/// raw-lines listener may hold [`Arc<VerseRuntime>`] clones while the operator
/// thread runs consuming handoff/replace.
pub struct VerseNodeSupervisor {
    identity: NodeIdentity,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<FleetLabResolver>,
    parts: Arc<dyn PartsFactory>,
    config: VerseRuntimeConfig,
    key: VerseKey,
    store: Mutex<VerseStore>,
    node: Mutex<Option<ScriptureNode>>,
    runtime: Mutex<Option<Arc<VerseRuntime>>>,
    /// Serializes bootstrap / start / replace / crash / handoff.
    control: Mutex<()>,
}

impl VerseNodeSupervisor {
    /// Builds a single-Verse supervisor with process-local in-memory parts.
    pub fn new(
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<FleetLabResolver>,
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

    /// Builds a single-Verse supervisor with an explicit durable parts factory.
    pub fn with_parts_factory(
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<FleetLabResolver>,
        parts: Arc<dyn PartsFactory>,
        config: VerseRuntimeConfig,
    ) -> Self {
        let key = VerseKey::from_config(&config);
        Self {
            identity,
            register,
            resolver,
            parts,
            config,
            key,
            store: Mutex::new(VerseStore::new()),
            node: Mutex::new(None),
            runtime: Mutex::new(None),
            control: Mutex::new(()),
        }
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

    /// Explicit bootstrap of a brand-new Verse: provision empty Loglet, publish
    /// Canon naming this node, then start the runtime.
    pub async fn bootstrap_verse<C, T>(
        &self,
        loglet_id: LogletId,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let _control = self.control.lock().await;
        if self.config.owner_id != self.identity.owner_id {
            return Err(SupervisorError::OwnerMismatch {
                configured: self.config.owner_id,
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
            let mut store = self.store.lock().await;
            store.parts.insert(loglet_id.clone(), parts);
            store.active = Some(loglet_id.clone());
        }

        let fence = CanonFence::new(
            0,
            self.config.journal_id,
            self.config.verse_id,
            owned_with_sequencer(self.identity.owner_id, self.identity.endpoint.clone(), 0),
        );
        self.virtual_log()
            .bootstrap_with_application_fence(loglet_id, fence.encode())
            .await?;

        self.start_verse_locked(clock, timer).await
    }

    /// Starts from existing durable Canon evidence.
    ///
    /// Does not provision or replace. Materializes every Canon-referenced
    /// generation through [`PartsFactory::open`]. A locally owned open active
    /// generation returns [`VerseControlOutcome::RecoveryRequired`] before any
    /// owner-recovery attempt.
    pub async fn start_configured<C, T>(
        &self,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let _control = self.control.lock().await;
        self.start_or_require_recovery_locked(clock, timer, k).await
    }

    async fn start_or_require_recovery_locked<C, T>(
        &self,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        // Same-process crash path: VerseStore still holds durable parts.
        let local = {
            let store = self.store.lock().await;
            match (
                &store.active,
                store.active.as_ref().and_then(|id| store.parts.get(id)),
            ) {
                (Some(active), Some(parts)) => Some((active.clone(), parts.clone())),
                _ => None,
            }
        };
        if let Some((active, parts)) = local {
            return self.refuse_writable(active, &parts, k).await;
        }

        match self.virtual_log().observe_membership().await {
            Err(VirtualLogError::Uninitialized) => self.start_verse_locked(clock, timer).await,
            Err(error) => Err(SupervisorError::VirtualLog(error)),
            Ok(observed) => {
                let fence = CanonFence::from_virtual_log_state(&observed.state)?;
                if fence.journal_id != self.config.journal_id
                    || fence.verse_id != self.config.verse_id
                {
                    return Err(SupervisorError::CanonIdentityMismatch {
                        fence_journal: fence.journal_id,
                        fence_verse: fence.verse_id,
                        config_journal: self.config.journal_id,
                        config_verse: self.config.verse_id,
                    });
                }

                let generations = &observed.state.generations;
                if generations.is_empty() {
                    return Err(SupervisorError::VirtualLog(
                        VirtualLogError::EmptyMembership,
                    ));
                }

                let mut store = VerseStore::new();
                let last = generations.len() - 1;
                for (index, generation) in generations.iter().enumerate() {
                    let parts = self.parts.open(&generation.loglet_id)?;
                    let view = resolve_read_seal(
                        Arc::clone(&parts.drive),
                        Arc::clone(&parts.seal),
                        Arc::clone(&parts.trim),
                        k,
                    )
                    .await?;
                    self.resolver.insert(
                        generation.loglet_id.clone(),
                        Arc::new(view.into_atomic_log()),
                    );
                    store.parts.insert(generation.loglet_id.clone(), parts);
                    if index == last {
                        store.active = Some(generation.loglet_id.clone());
                    }
                }
                *self.store.lock().await = store;

                let active = generations[last].loglet_id.clone();
                let parts = self
                    .store
                    .lock()
                    .await
                    .parts
                    .get(&active)
                    .cloned()
                    .ok_or(SupervisorError::NoActiveLoglet { key: self.key })?;

                let locally_owned = matches!(
                    &fence.owner,
                    CanonOwner::Owned { owner_id, .. } if *owner_id == self.identity.owner_id
                );
                if locally_owned {
                    // Read/seal views are installed; refuse writable recovery.
                    return self.refuse_writable(active, &parts, k).await;
                }

                self.start_verse_locked(clock, timer).await
            }
        }
    }

    async fn refuse_writable(
        &self,
        active: LogletId,
        parts: &DurableLogletParts,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError> {
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
    /// by this node. Commits local active state only on [`Reconfiguration::Applied`].
    pub async fn replace_after_lost_sequencer<C, T>(
        &self,
        successor: LogletId,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let _control = self.control.lock().await;
        let (active, parts) = {
            let store = self.store.lock().await;
            let active = store
                .active
                .clone()
                .ok_or(SupervisorError::NoActiveLoglet { key: self.key })?;
            let parts = store
                .parts
                .get(&active)
                .cloned()
                .ok_or(SupervisorError::NoActiveLoglet { key: self.key })?;
            (active, parts)
        };

        // Observe before mutating. If another reconfigurer already moved the
        // active generation, do not seal/provision against a stale local active.
        let observed = self.virtual_log().observe_membership().await?;
        let observed_active = observed
            .state
            .active()
            .ok_or(VirtualLogError::EmptyMembership)?
            .loglet_id
            .clone();
        if observed_active != active {
            return Ok(VerseControlOutcome::ConflictNeedsInspect);
        }

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
        let sealed_view = Arc::new(historical.into_atomic_log());
        self.resolver.insert(active.clone(), sealed_view);

        let next_parts = self.parts.fresh(&successor)?;
        let fresh = provision_fresh_writable(
            GenerationDescriptor {
                loglet_id: successor.clone(),
                start: 0,
            },
            Arc::clone(&next_parts.drive),
            Arc::clone(&next_parts.seal),
            Arc::clone(&next_parts.trim),
            k,
        )
        .await?;
        self.resolver
            .insert(successor.clone(), Arc::new(fresh.into_atomic_log()));

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
            self.config.journal_id,
            self.config.verse_id,
            owned_with_sequencer(
                self.identity.owner_id,
                self.identity.endpoint.clone(),
                next_revision,
            ),
        );
        let outcome = self
            .virtual_log()
            .reconfigure_from_observation(&observed, successor.clone(), fence.encode())
            .await?;

        match outcome {
            Reconfiguration::Applied { .. } => {
                let mut store = self.store.lock().await;
                store.parts.insert(successor.clone(), next_parts);
                store.active = Some(successor);
                self.start_verse_locked(clock, timer).await
            }
            Reconfiguration::Conflict => {
                self.resolver.remove(&successor);
                Ok(VerseControlOutcome::ConflictNeedsInspect)
            }
        }
    }

    /// Simulates losing in-process writer handles for the active Loglet.
    pub async fn crash_active_writer(&self) -> Result<(), SupervisorError> {
        let _control = self.control.lock().await;
        let active = self
            .store
            .lock()
            .await
            .active
            .clone()
            .ok_or(SupervisorError::NoActiveLoglet { key: self.key })?;
        self.resolver.remove(&active);
        *self.runtime.lock().await = None;
        *self.node.lock().await = None;
        Ok(())
    }

    /// Drops local VerseStore and runtime while retaining shared durable parts
    /// (process-boundary simulation when using [`SharedMemoryPartsFactory`]).
    pub async fn drop_process_local_state(&self) -> Result<(), SupervisorError> {
        let _control = self.control.lock().await;
        if let Some(active) = self.store.lock().await.active.clone() {
            self.resolver.remove(&active);
        }
        // Clear all resolver entries this process knew.
        // Historical gens may remain if shared resolver — clear known parts keys.
        let ids: Vec<LogletId> = self.store.lock().await.parts.keys().cloned().collect();
        for id in ids {
            self.resolver.remove(&id);
        }
        *self.store.lock().await = VerseStore::new();
        *self.runtime.lock().await = None;
        *self.node.lock().await = None;
        Ok(())
    }

    /// Borrow the started runtime for listener admission (cloneable Arc).
    pub async fn runtime(&self) -> Option<Arc<VerseRuntime>> {
        self.runtime.lock().await.clone()
    }

    /// Consuming fenced handoff for a serving Verse.
    pub async fn drain_seal_publish(
        &self,
        request: VerseHandoffRequest,
    ) -> Result<scripture_service::CanonTransitionOutcome, SupervisorError> {
        let _control = self.control.lock().await;
        let runtime = self
            .runtime
            .lock()
            .await
            .take()
            .ok_or(SupervisorError::UnknownVerse { key: self.key })?;
        let runtime = Arc::try_unwrap(runtime)
            .map_err(|_| SupervisorError::RuntimeInUse { key: self.key })?;
        match runtime.drain_seal_publish(request).await {
            Ok((runtime, outcome)) => {
                *self.runtime.lock().await = Some(Arc::new(runtime));
                Ok(outcome)
            }
            Err(failure) => {
                *self.runtime.lock().await = Some(Arc::new(failure.runtime));
                Err(SupervisorError::Handoff(failure.error))
            }
        }
    }

    async fn start_verse_locked<C, T>(
        &self,
        clock: C,
        timer: T,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let started = ScriptureNode::start(
            vec![self.config.clone()],
            |_| self.virtual_log(),
            clock,
            timer,
        )
        .await?;
        self.install_start(started).await
    }

    async fn install_start(
        &self,
        started: ScriptureNodeStart,
    ) -> Result<VerseControlOutcome, SupervisorError> {
        if let Some(error) = started.failures.into_values().next() {
            return Ok(VerseControlOutcome::StartFailed(error));
        }
        let mut runtimes_map = started.runtimes;
        let Some(runtime) = runtimes_map.remove(&self.key) else {
            return Err(SupervisorError::UnknownVerse { key: self.key });
        };
        let serving = runtime.is_serving();
        *self.runtime.lock().await = Some(Arc::new(runtime));
        *self.node.lock().await = Some(ScriptureNode::from_started(runtimes_map));
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

    use super::{NodeIdentity, SharedMemoryPartsFactory, VerseControlOutcome, VerseNodeSupervisor};
    use crate::FleetLabResolver;

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
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let key_resolver = Arc::new(FleetLabResolver::default());

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
            Arc::new(FleetLabResolver::default()),
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
    async fn crash_reports_recovery_required_not_serving() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let resolver = Arc::new(FleetLabResolver::default());
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
            Arc::new(FleetLabResolver::default()),
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
            Arc::new(FleetLabResolver::default()),
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
            Arc::new(FleetLabResolver::default()),
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

        let boot = VerseNodeSupervisor::with_parts_factory(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(FleetLabResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
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

        let left = Arc::new(VerseNodeSupervisor::with_parts_factory(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(FleetLabResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
        ));
        let right = Arc::new(VerseNodeSupervisor::with_parts_factory(
            identity(owner_a()),
            Arc::clone(&register) as Arc<dyn holylog::virtual_log::ConditionalRegister>,
            Arc::new(FleetLabResolver::default()),
            Arc::clone(&parts) as Arc<dyn super::PartsFactory>,
            config(owner_a()),
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
        let left_out = left_out.expect("left replace");
        let right_out = right_out.expect("right replace");
        let outcomes = [&left_out, &right_out];
        assert!(
            outcomes
                .iter()
                .any(|o| matches!(o, VerseControlOutcome::Serving))
        );
        assert!(
            outcomes
                .iter()
                .any(|o| matches!(o, VerseControlOutcome::ConflictNeedsInspect))
        );

        let winner = if matches!(left_out, VerseControlOutcome::Serving) {
            &left
        } else {
            &right
        };
        let loser = if matches!(left_out, VerseControlOutcome::Serving) {
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
}
