//! Production Holylog adapter for [`JournalFoundationTransition`].
//!
//! Extracts seal → provision → witnessed VirtualLog CAS mechanics used by
//! [`crate::VerseNodeSupervisor`], but couples them to Serving Authority via
//! v3 Canon fences (candidate + [`WriterTerm`]) and exact
//! [`FoundationPrecondition`] / [`JournalGenerationRef`] matching.
//!
//! Does not start a Verse runtime or open ingress. Inspect/classify after
//! process restart uses durable open + read/seal handles only — never writable
//! reattach of an open generation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use holylog::provision::{
    BindTag, ExclusiveClaimStore, ProvisionAuthority, ProvisionerId, resolve_read_seal,
};
use holylog::virtual_log::{
    ConditionalRegister, LogletId, LogletResolver, ReceiptReconfiguration, VersionedState,
    VirtualLog, VirtualLogError, VirtualLogState,
};
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, FoundationPrecondition, JournalGenerationRef,
    ServingAuthorityRecord, ServingPublication, TransitionIntent, WriterAuthority, WriterTerm,
};
use scripture::{OwnerEndpoint, OwnerId};
use scripture_service::{
    FoundationTransitionError, JournalFoundationTransition, TransitionClassification,
};
use tokio::sync::Mutex;

use crate::node::{DurableLogletParts, NodeIdentity, PartsFactory, ProcessLogletResolver};

/// Membership identity for Expected matching: fence-only revision bumps must not
/// invalidate a durable Transitioning intent's predecessor binding.
fn same_active_membership(left: &JournalGenerationRef, right: &JournalGenerationRef) -> bool {
    left.active_loglet_id == right.active_loglet_id && left.active_start == right.active_start
}

/// Policy for allocating the next exclusive Loglet ID during a Foundation transition.
pub trait FreshLogletIdPolicy: Send + Sync {
    /// Deterministic, namespace-safe successor Loglet identifier.
    fn next_loglet_id(
        &self,
        next_revision: u64,
        next_term: WriterTerm,
        candidate: OwnerId,
        attempt: u32,
    ) -> Result<LogletId, FoundationTransitionError>;
}

/// Default policy: `g{revision}-t{term}-{owner-hex}-a{attempt}`.
///
/// Uses the full owner identity (not a short prefix) so concurrent Verses that
/// share a [`PartsFactory`] do not collide on Loglet namespaces when owners
/// share a common ASCII prefix (e.g. `fleet-own-*`).
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultFreshLogletIdPolicy;

impl FreshLogletIdPolicy for DefaultFreshLogletIdPolicy {
    fn next_loglet_id(
        &self,
        next_revision: u64,
        next_term: WriterTerm,
        candidate: OwnerId,
        attempt: u32,
    ) -> Result<LogletId, FoundationTransitionError> {
        let hex: String = candidate
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let raw = format!("g{next_revision}-t{}-{hex}-a{attempt}", next_term.get());
        LogletId::new(raw).map_err(|error| FoundationTransitionError::Unavailable(Box::new(error)))
    }
}

/// Durable replacement boundaries reached after the root intent exists.
///
/// This is an observability/test seam, not an additional authority source. An
/// interruption at any checkpoint leaves the root `Transitioning` and requires
/// forward recovery; it can never restore predecessor Serving authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoundationTransitionCheckpoint {
    /// The predecessor seal is durable, before its authoritative tail is read.
    PredecessorSealed,
    /// The sealed predecessor's authoritative tail was read, before provision.
    SealedTailObserved,
    /// A successor and its single-use receipt exist, before the root CAS.
    SuccessorProvisioned,
}

/// Observes an internal Foundation replacement boundary.
pub trait FoundationTransitionObserver: Send + Sync {
    /// Returning an error models interruption of the current process.
    fn checkpoint(
        &self,
        checkpoint: FoundationTransitionCheckpoint,
    ) -> Result<(), FoundationTransitionError>;
}

/// Production observer with no side effects.
#[derive(Debug, Default)]
pub struct NoopFoundationTransitionObserver;

impl FoundationTransitionObserver for NoopFoundationTransitionObserver {
    fn checkpoint(
        &self,
        _checkpoint: FoundationTransitionCheckpoint,
    ) -> Result<(), FoundationTransitionError> {
        Ok(())
    }
}

/// Builds a Serving-only publication for the local candidate (route = advertise).
pub(crate) fn serving_publication_for(
    key: AuthorityKey,
    owner_id: OwnerId,
    endpoint: &OwnerEndpoint,
    writer_term: WriterTerm,
    generation_ref: JournalGenerationRef,
) -> Result<ServingPublication, FoundationTransitionError> {
    let route_hint = scripture::RouteHint::new(endpoint.as_str()).map_err(|error| {
        FoundationTransitionError::Unavailable(Box::new(std::io::Error::other(error.to_string())))
    })?;
    ServingPublication::new(
        key,
        WriterAuthority {
            owner_id,
            writer_term,
            generation_ref,
        },
        route_hint,
    )
    .map_err(|error| {
        FoundationTransitionError::Unavailable(Box::new(std::io::Error::other(error.to_string())))
    })
}

/// Owned v3 Canon owner binding carrying an explicit WriterTerm (legacy helper).
#[must_use]
pub fn owned_with_writer_term(
    owner_id: OwnerId,
    endpoint: OwnerEndpoint,
    writer_term: WriterTerm,
) -> scripture::CanonOwner {
    scripture::CanonOwner::Owned {
        owner_id,
        endpoint,
        sequencer: None,
        writer_term: Some(writer_term),
    }
}

/// Concrete Holylog [`JournalFoundationTransition`] adapter.
pub struct HolylogJournalFoundation {
    key: AuthorityKey,
    identity: NodeIdentity,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
    parts: Arc<dyn PartsFactory>,
    authority: ProvisionAuthority,
    loglet_ids: Arc<dyn FreshLogletIdPolicy>,
    observer: Arc<dyn FoundationTransitionObserver>,
    k: u64,
    control: Mutex<()>,
}

impl std::fmt::Debug for HolylogJournalFoundation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HolylogJournalFoundation")
            .field("key", &self.key)
            .field("identity", &self.identity)
            .field("k", &self.k)
            .finish_non_exhaustive()
    }
}

impl HolylogJournalFoundation {
    /// Builds a Foundation adapter from shared Holylog durable seams.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: AuthorityKey,
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<ProcessLogletResolver>,
        parts: Arc<dyn PartsFactory>,
        claims: Arc<dyn ExclusiveClaimStore>,
        loglet_ids: Arc<dyn FreshLogletIdPolicy>,
        k: u64,
    ) -> Self {
        Self::new_with_transition_observer(
            key,
            identity,
            register,
            resolver,
            parts,
            claims,
            loglet_ids,
            Arc::new(NoopFoundationTransitionObserver),
            k,
        )
    }

    /// Builds a Foundation adapter with an explicit internal transition observer.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_transition_observer(
        key: AuthorityKey,
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<ProcessLogletResolver>,
        parts: Arc<dyn PartsFactory>,
        claims: Arc<dyn ExclusiveClaimStore>,
        loglet_ids: Arc<dyn FreshLogletIdPolicy>,
        observer: Arc<dyn FoundationTransitionObserver>,
        k: u64,
    ) -> Self {
        let provisioner = ProvisionerId::new(format!("scripture-ha-{}", identity.owner_id));
        Self {
            key,
            identity,
            register,
            resolver,
            parts,
            authority: ProvisionAuthority::new(claims, provisioner),
            loglet_ids,
            observer,
            k,
            control: Mutex::new(()),
        }
    }

    /// Convenience constructor using [`DefaultFreshLogletIdPolicy`].
    #[must_use]
    pub fn with_default_loglet_ids(
        key: AuthorityKey,
        identity: NodeIdentity,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<ProcessLogletResolver>,
        parts: Arc<dyn PartsFactory>,
        claims: Arc<dyn ExclusiveClaimStore>,
        k: u64,
    ) -> Self {
        Self::new(
            key,
            identity,
            register,
            resolver,
            parts,
            claims,
            Arc::new(DefaultFreshLogletIdPolicy),
            k,
        )
    }

    /// Configured authority key.
    #[must_use]
    pub fn key(&self) -> AuthorityKey {
        self.key
    }

    /// Local candidate identity used for v3 Canon fences.
    #[must_use]
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    fn virtual_log(&self) -> VirtualLog {
        VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        )
    }

    fn bind_for(loglet_id: &LogletId) -> BindTag {
        BindTag::new(format!("scripture-ha:{loglet_id}").into_bytes())
    }

    fn map_unavailable(
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> FoundationTransitionError {
        FoundationTransitionError::Unavailable(Box::new(error))
    }

    fn map_indeterminate(
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> FoundationTransitionError {
        FoundationTransitionError::Indeterminate(Box::new(error))
    }

    fn generation_ref_from_state(
        state: &VirtualLogState,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        JournalGenerationRef::from_virtual_log_state(state)
            .map_err(|error| Self::map_unavailable(std::io::Error::other(error.to_string())))
    }

    fn require_configured_key(&self, key: AuthorityKey) -> Result<(), FoundationTransitionError> {
        if key != self.key {
            return Err(FoundationTransitionError::Conflict {
                message: format!(
                    "Foundation key mismatch: adapter {:?}/{:?} vs request {:?}/{:?}",
                    self.key.journal_id, self.key.verse_id, key.journal_id, key.verse_id
                ),
            });
        }
        Ok(())
    }

    fn require_local_candidate(
        &self,
        target_owner_id: OwnerId,
    ) -> Result<(), FoundationTransitionError> {
        if target_owner_id != self.identity.owner_id {
            return Err(FoundationTransitionError::Conflict {
                message: "Foundation adapter refuses foreign candidate owner".into(),
            });
        }
        Ok(())
    }

    async fn observe_live(
        &self,
    ) -> Result<Option<(VersionedState, JournalGenerationRef)>, FoundationTransitionError> {
        match self.virtual_log().observe_membership().await {
            Err(VirtualLogError::Uninitialized) => Ok(None),
            Err(error) => Err(Self::map_unavailable(error)),
            Ok(observed) => {
                if observed.state.generations.is_empty() {
                    return Err(FoundationTransitionError::Conflict {
                        message: "VirtualLog membership is present but has no generations".into(),
                    });
                }
                // One-record: authority lives in application_fence. Accept Serving or
                // Transitioning records that name this key; reject foreign keys.
                if let Ok(record) = ServingAuthorityRecord::decode_application_fence(
                    &observed.state.application_fence,
                ) && record.key != self.key
                {
                    return Err(FoundationTransitionError::Conflict {
                        message: "root authority fence journal/verse disagree with AuthorityKey"
                            .into(),
                    });
                }
                let generation = Self::generation_ref_from_state(&observed.state)?;
                Ok(Some((observed, generation)))
            }
        }
    }

    /// Installs read/seal views for every generation in membership.
    ///
    /// A promote on a fresh process resolver (after crash simulation removes
    /// only the active writable) must still resolve historical generations:
    /// VirtualLog `read_next` / catch-up routes by chain position and will
    /// surface [`VirtualLogError::MissingLoglet`] for any gen absent from the
    /// resolver. Mirrors supervisor restart materialization in `inspect`.
    ///
    /// The active predecessor is sealed if still open so
    /// [`VirtualLog::reconfigure_with_receipt`] can observe an authoritative
    /// sealed tail.
    async fn materialize_membership_for_cutover(
        &self,
        state: &VirtualLogState,
    ) -> Result<(), FoundationTransitionError> {
        let active_id = state
            .active()
            .ok_or_else(|| FoundationTransitionError::Conflict {
                message: "no active generation to seal".into(),
            })?
            .loglet_id
            .clone();

        for generation in &state.generations {
            let is_active = generation.loglet_id == active_id;
            if !is_active && self.resolver.contains(&generation.loglet_id) {
                continue;
            }
            let parts = self
                .parts
                .open(&generation.loglet_id)
                .map_err(|error| FoundationTransitionError::Unavailable(Box::new(error)))?;
            let historical = resolve_read_seal(parts.components(self.k))
                .await
                .map_err(Self::map_unavailable)?;
            if is_active
                && !historical
                    .observe_durable()
                    .await
                    .map_err(Self::map_unavailable)?
                    .sealed()
            {
                historical.seal().await.map_err(Self::map_unavailable)?;
            }
            self.resolver
                .insert_read_seal(generation.loglet_id.clone(), Arc::new(historical));
        }
        Ok(())
    }

    async fn provision_successor_uninstalled(
        &self,
        next_revision: u64,
        next_term: WriterTerm,
    ) -> Result<
        (
            LogletId,
            DurableLogletParts,
            holylog::provision::FreshWritableProvisionReceipt,
            Arc<holylog::provision::WritableLoglet>,
            BindTag,
        ),
        FoundationTransitionError,
    > {
        // A crash after fresh provision loses the linear, single-use receipt.
        // It cannot be reconstructed, so recovery must abandon that unreachable
        // candidate and try a distinct suffix. The bounded loop is fail-closed.
        const MAX_CANDIDATE_ATTEMPTS: u32 = 8;
        let mut last_error = None;
        for attempt in 0..MAX_CANDIDATE_ATTEMPTS {
            let successor = self.loglet_ids.next_loglet_id(
                next_revision,
                next_term,
                self.identity.owner_id,
                attempt,
            )?;
            let parts = match self.parts.fresh(&successor) {
                Ok(parts) => parts,
                Err(error) => {
                    // A retained candidate after a process crash is expected to
                    // collide here for in-memory/test factories. Moving to a new
                    // namespace is safer than attempting to forge its receipt.
                    last_error = Some(error.to_string());
                    continue;
                }
            };
            let namespaces = self
                .parts
                .namespaces(&successor)
                .map_err(|error| FoundationTransitionError::Unavailable(Box::new(error)))?;
            let bind = Self::bind_for(&successor);
            match self
                .authority
                .provision_fresh(
                    successor.clone(),
                    namespaces,
                    bind.clone(),
                    parts.components(self.k),
                )
                .await
            {
                Ok((receipt, writable)) => {
                    return Ok((successor, parts, receipt, Arc::new(writable), bind));
                }
                Err(holylog::provision::ProvisionError::NamespaceAlreadyClaimed { .. }) => {
                    last_error = Some("candidate namespace already claimed".into());
                }
                Err(error) => return Err(Self::map_unavailable(error)),
            }
        }
        Err(FoundationTransitionError::Unavailable(Box::new(
            std::io::Error::other(format!(
                "could not provision a fresh successor after {MAX_CANDIDATE_ATTEMPTS} attempts: {}",
                last_error.unwrap_or_else(|| "no candidate error".into())
            )),
        )))
    }

    async fn drive_locked(
        &self,
        key: AuthorityKey,
        publication: ServingPublication,
        precondition: FoundationPrecondition,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        self.require_configured_key(key)?;
        self.require_local_candidate(publication.authority().owner_id)?;
        if publication.key() != key {
            return Err(FoundationTransitionError::Conflict {
                message: "ServingPublication key disagrees with Foundation key".into(),
            });
        }

        let live = self.observe_live().await?;
        match (&precondition, &live) {
            (FoundationPrecondition::Empty, Some(_)) => {
                return Err(FoundationTransitionError::Conflict {
                    message: "precondition Empty but VirtualLog register is present".into(),
                });
            }
            (FoundationPrecondition::Empty, None) => {}
            (FoundationPrecondition::Expected(_), None) => {
                return Err(FoundationTransitionError::Conflict {
                    message: "precondition Expected but VirtualLog register is absent".into(),
                });
            }
            (FoundationPrecondition::Expected(expected), Some((_, generation))) => {
                if !same_active_membership(generation, expected) {
                    return Err(FoundationTransitionError::Conflict {
                        message: "precondition Expected generation does not match live Foundation"
                            .into(),
                    });
                }
            }
        }

        match precondition {
            FoundationPrecondition::Empty => self.bootstrap_empty(publication).await,
            FoundationPrecondition::Expected(_) => {
                let (observed, _) = live.expect("checked");
                self.replace_expected(observed, publication).await
            }
        }
    }

    async fn bootstrap_empty(
        &self,
        publication: ServingPublication,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        let next_term = publication.authority().writer_term;
        let (successor, _parts, receipt, writable, bind) =
            self.provision_successor_uninstalled(0, next_term).await?;

        // Build Serving fence with the generation binding we are about to publish.
        let generation_ref = JournalGenerationRef::from_active_generation(0, successor.clone(), 0);
        let publish = serving_publication_for(
            self.key,
            publication.authority().owner_id,
            &self.identity.endpoint,
            next_term,
            generation_ref,
        )?;
        let fence = publish
            .encode_application_fence()
            .map_err(|error| Self::map_unavailable(std::io::Error::other(error.to_string())))?;

        match self
            .virtual_log()
            .bootstrap_with_receipt(receipt, writable.as_ref(), &bind, fence)
            .await
        {
            Ok(()) => {
                self.resolver
                    .insert_writable(successor.clone(), Arc::clone(&writable));
            }
            Err(error) => {
                return Err(Self::map_indeterminate(error));
            }
        }

        let (_observed, generation) = self.observe_live().await?.ok_or_else(|| {
            FoundationTransitionError::Indeterminate(Box::new(std::io::Error::other(
                "bootstrap Applied but VirtualLog observe reports uninitialized",
            )))
        })?;
        Ok(generation)
    }

    async fn replace_expected(
        &self,
        observed: VersionedState,
        publication: ServingPublication,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        let next_term = publication.authority().writer_term;
        let predecessor = observed
            .state
            .active()
            .ok_or_else(|| FoundationTransitionError::Conflict {
                message: "no active generation to seal".into(),
            })?
            .loglet_id
            .clone();

        self.materialize_membership_for_cutover(&observed.state)
            .await?;
        self.observer
            .checkpoint(FoundationTransitionCheckpoint::PredecessorSealed)?;

        // Successor start must match Holylog's reconfigure boundary:
        // `predecessor.start + sealed_local_tail`. Embedding only the local
        // tail breaks the second+ cutover when predecessor.start > 0
        // (ContenderConflict in require_local_serving).
        let predecessor_start = observed
            .state
            .active()
            .ok_or_else(|| FoundationTransitionError::Conflict {
                message: "no active generation to seal".into(),
            })?
            .start;
        let next_revision = observed.state.revision.checked_add(1).ok_or_else(|| {
            FoundationTransitionError::Unavailable(Box::new(std::io::Error::other(
                "VirtualLog revision overflow",
            )))
        })?;
        let local_tail = self.sealed_predecessor_start(&predecessor).await?;
        let start = predecessor_start.checked_add(local_tail).ok_or_else(|| {
            FoundationTransitionError::Unavailable(Box::new(std::io::Error::other(
                "VirtualLog address space exhausted computing successor start",
            )))
        })?;
        self.observer
            .checkpoint(FoundationTransitionCheckpoint::SealedTailObserved)?;
        let (successor, _parts, receipt, writable, bind) = self
            .provision_successor_uninstalled(next_revision, next_term)
            .await?;
        self.observer
            .checkpoint(FoundationTransitionCheckpoint::SuccessorProvisioned)?;
        let generation_ref =
            JournalGenerationRef::from_active_generation(next_revision, successor.clone(), start);
        let publish = serving_publication_for(
            self.key,
            publication.authority().owner_id,
            &self.identity.endpoint,
            next_term,
            generation_ref,
        )?;
        let fence = publish
            .encode_application_fence()
            .map_err(|error| Self::map_unavailable(std::io::Error::other(error.to_string())))?;

        let outcome = match self
            .virtual_log()
            .reconfigure_with_receipt(&observed, receipt, writable.as_ref(), &bind, fence)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(Self::map_indeterminate(error));
            }
        };

        match outcome {
            ReceiptReconfiguration::Applied { .. } => {
                self.resolver
                    .insert_writable(successor.clone(), Arc::clone(&writable));
            }
            ReceiptReconfiguration::Conflict { .. } => {
                return Err(FoundationTransitionError::Conflict {
                    message: "VirtualLog reconfigure CAS conflict; candidate retained for inspect"
                        .into(),
                });
            }
        }

        let (_observed, generation) = self.observe_live().await?.ok_or_else(|| {
            FoundationTransitionError::Indeterminate(Box::new(std::io::Error::other(
                "reconfigure Applied but VirtualLog observe reports uninitialized",
            )))
        })?;
        Ok(generation)
    }

    async fn sealed_predecessor_start(
        &self,
        predecessor: &LogletId,
    ) -> Result<u64, FoundationTransitionError> {
        let parts = self
            .parts
            .open(predecessor)
            .map_err(|error| FoundationTransitionError::Unavailable(Box::new(error)))?;
        let historical = resolve_read_seal(parts.components(self.k))
            .await
            .map_err(Self::map_unavailable)?;
        let durable = historical
            .observe_durable()
            .await
            .map_err(Self::map_unavailable)?;
        Ok(durable.non_contiguous_tail())
    }

    async fn classify_locked(
        &self,
        key: AuthorityKey,
        intent: &TransitionIntent,
    ) -> Result<TransitionClassification, FoundationTransitionError> {
        self.require_configured_key(key)?;

        let live = self.observe_live().await?;
        let matches_precondition = match &intent.precondition {
            FoundationPrecondition::Empty => live.is_none(),
            FoundationPrecondition::Expected(expected) => live
                .as_ref()
                .is_some_and(|(_, generation)| same_active_membership(generation, expected)),
        };
        if matches_precondition {
            return Ok(TransitionClassification::StillPredecessor);
        }

        let Some((observed, generation)) = live else {
            return Ok(TransitionClassification::Divergent {
                observed_generation: None,
            });
        };

        // Empty → initial Foundation may publish at revision zero. A successful
        // empty transition is an intended successor whenever the live fence
        // matches the candidate/term; do not treat rev==0 as divergent.
        // Expected: fence-only revision bumps keep the same membership and are
        // StillPredecessor above; a real cutover changes active loglet/start.
        let is_successor = match &intent.precondition {
            FoundationPrecondition::Empty => true,
            FoundationPrecondition::Expected(expected) => {
                !same_active_membership(&generation, expected)
                    && generation.virtual_log_revision > expected.virtual_log_revision
            }
        };
        if !is_successor {
            return Ok(TransitionClassification::Divergent {
                observed_generation: Some(generation),
            });
        }

        let fence_record =
            ServingAuthorityRecord::decode_application_fence(&observed.state.application_fence)
                .map_err(|error| Self::map_unavailable(std::io::Error::other(error.to_string())))?;
        if fence_record.key != key {
            return Ok(TransitionClassification::Divergent {
                observed_generation: Some(generation),
            });
        }
        match fence_record.state {
            AuthorityState::Serving { authority, .. }
                if authority.owner_id == intent.candidate_owner_id
                    && authority.writer_term == intent.next_writer_term =>
            {
                Ok(TransitionClassification::IntendedSuccessor { generation })
            }
            _ => Ok(TransitionClassification::Divergent {
                observed_generation: Some(generation),
            }),
        }
    }

    /// One-shot brand-new authority-domain Foundation bootstrap with Serving fence.
    ///
    /// Publishes membership + Serving in one root CAS. No separate authority store.
    pub async fn bootstrap_foundation_serving(
        &self,
        initial_term: WriterTerm,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        let publication = serving_publication_for(
            self.key,
            self.identity.owner_id,
            &self.identity.endpoint,
            initial_term,
            JournalGenerationRef::from_active_generation(
                0,
                LogletId::new("bootstrap-placeholder").map_err(|error| {
                    Self::map_unavailable(std::io::Error::other(error.to_string()))
                })?,
                0,
            ),
        )?;
        let _guard = self.control.lock().await;
        self.drive_locked(self.key, publication, FoundationPrecondition::Empty)
            .await
    }

    /// Deprecated name retained for call-site migration; prefer
    /// [`Self::bootstrap_foundation_serving`].
    pub async fn bootstrap_foundation_v3(
        &self,
        initial_term: WriterTerm,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        self.bootstrap_foundation_serving(initial_term).await
    }
}

impl JournalFoundationTransition for HolylogJournalFoundation {
    fn drive_foundation_transition(
        &self,
        key: AuthorityKey,
        publication: ServingPublication,
        precondition: FoundationPrecondition,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<JournalGenerationRef, FoundationTransitionError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let _guard = self.control.lock().await;
            self.drive_locked(key, publication, precondition).await
        })
    }

    fn classify_transition(
        &self,
        key: AuthorityKey,
        intent: &TransitionIntent,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<TransitionClassification, FoundationTransitionError>>
                + Send
                + '_,
        >,
    > {
        let intent = intent.clone();
        Box::pin(async move {
            let _guard = self.control.lock().await;
            self.classify_locked(key, &intent).await
        })
    }
}
