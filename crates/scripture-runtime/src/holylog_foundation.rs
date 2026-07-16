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
use scripture::canon::{CanonFence, CanonOwner};
use scripture::serving_authority::{
    AuthorityKey, FoundationPrecondition, JournalGenerationRef, TransitionIntent, WriterTerm,
};
use scripture::{OwnerEndpoint, OwnerId};
use scripture_service::{
    FoundationTransitionError, JournalFoundationTransition, TransitionClassification,
};
use tokio::sync::Mutex;

use crate::node::{DurableLogletParts, NodeIdentity, PartsFactory, ProcessLogletResolver};

/// Policy for allocating the next exclusive Loglet ID during a Foundation transition.
pub trait FreshLogletIdPolicy: Send + Sync {
    /// Deterministic, namespace-safe successor Loglet identifier.
    fn next_loglet_id(
        &self,
        next_revision: u64,
        next_term: WriterTerm,
        candidate: OwnerId,
    ) -> Result<LogletId, FoundationTransitionError>;
}

/// Default policy: `g{revision}-t{term}-{owner-hex-prefix}`.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultFreshLogletIdPolicy;

impl FreshLogletIdPolicy for DefaultFreshLogletIdPolicy {
    fn next_loglet_id(
        &self,
        next_revision: u64,
        next_term: WriterTerm,
        candidate: OwnerId,
    ) -> Result<LogletId, FoundationTransitionError> {
        let hex: String = candidate
            .as_bytes()
            .iter()
            .take(4)
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let raw = format!("g{next_revision}-t{}-{hex}", next_term.get());
        LogletId::new(raw).map_err(|error| FoundationTransitionError::Unavailable(Box::new(error)))
    }
}

/// Owned v3 Canon owner binding carrying an explicit WriterTerm.
#[must_use]
pub fn owned_with_writer_term(
    owner_id: OwnerId,
    endpoint: OwnerEndpoint,
    writer_term: WriterTerm,
) -> CanonOwner {
    CanonOwner::Owned {
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
        let provisioner = ProvisionerId::new(format!("scripture-ha-{}", identity.owner_id));
        Self {
            key,
            identity,
            register,
            resolver,
            parts,
            authority: ProvisionAuthority::new(claims, provisioner),
            loglet_ids,
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
                let fence =
                    CanonFence::decode(&observed.state.application_fence).map_err(|error| {
                        Self::map_unavailable(std::io::Error::other(error.to_string()))
                    })?;
                if fence.journal_id != self.key.journal_id || fence.verse_id != self.key.verse_id {
                    return Err(FoundationTransitionError::Conflict {
                        message: "Canon fence journal/verse disagree with AuthorityKey".into(),
                    });
                }
                let generation = Self::generation_ref_from_state(&observed.state)?;
                Ok(Some((observed, generation)))
            }
        }
    }

    async fn ensure_predecessor_sealed(
        &self,
        predecessor: &LogletId,
    ) -> Result<(), FoundationTransitionError> {
        let parts = self
            .parts
            .open(predecessor)
            .map_err(|error| FoundationTransitionError::Unavailable(Box::new(error)))?;
        let historical = resolve_read_seal(parts.components(self.k))
            .await
            .map_err(Self::map_unavailable)?;
        if !historical
            .observe_durable()
            .await
            .map_err(Self::map_unavailable)?
            .sealed()
        {
            historical.seal().await.map_err(Self::map_unavailable)?;
        }
        self.resolver
            .insert_read_seal(predecessor.clone(), Arc::new(historical));
        Ok(())
    }

    async fn provision_successor_uninstalled(
        &self,
        successor: &LogletId,
    ) -> Result<
        (
            DurableLogletParts,
            holylog::provision::FreshWritableProvisionReceipt,
            Arc<holylog::provision::WritableLoglet>,
            BindTag,
        ),
        FoundationTransitionError,
    > {
        let parts = self
            .parts
            .fresh(successor)
            .map_err(|error| FoundationTransitionError::Unavailable(Box::new(error)))?;
        let namespaces = self
            .parts
            .namespaces(successor)
            .map_err(|error| FoundationTransitionError::Unavailable(Box::new(error)))?;
        let bind = Self::bind_for(successor);
        let (receipt, writable) = self
            .authority
            .provision_fresh(
                successor.clone(),
                namespaces,
                bind.clone(),
                parts.components(self.k),
            )
            .await
            .map_err(Self::map_unavailable)?;
        Ok((parts, receipt, Arc::new(writable), bind))
    }

    async fn drive_locked(
        &self,
        key: AuthorityKey,
        target_owner_id: OwnerId,
        next_term: WriterTerm,
        precondition: FoundationPrecondition,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        self.require_configured_key(key)?;
        self.require_local_candidate(target_owner_id)?;

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
                if generation != expected {
                    return Err(FoundationTransitionError::Conflict {
                        message: "precondition Expected generation does not match live Foundation"
                            .into(),
                    });
                }
            }
        }

        match precondition {
            FoundationPrecondition::Empty => self.bootstrap_empty(next_term).await,
            FoundationPrecondition::Expected(_) => {
                let (observed, _) = live.expect("checked");
                self.replace_expected(observed, next_term).await
            }
        }
    }

    async fn bootstrap_empty(
        &self,
        next_term: WriterTerm,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        let successor = self
            .loglet_ids
            .next_loglet_id(0, next_term, self.identity.owner_id)?;
        let (_parts, receipt, writable, bind) =
            self.provision_successor_uninstalled(&successor).await?;
        let fence = CanonFence::new(
            0,
            self.key.journal_id,
            self.key.verse_id,
            owned_with_writer_term(
                self.identity.owner_id,
                self.identity.endpoint.clone(),
                next_term,
            ),
        );
        match self
            .virtual_log()
            .bootstrap_with_receipt(receipt, writable.as_ref(), &bind, fence.encode())
            .await
        {
            Ok(()) => {
                // Install writable only after Applied/bootstrap success so a
                // crash mid-path cannot expose an unpublished soft sequencer.
                self.resolver
                    .insert_writable(successor.clone(), Arc::clone(&writable));
            }
            Err(error) => {
                // Bootstrap moved the receipt; Holylog does not hand it back.
                return Err(Self::map_indeterminate(error));
            }
        }

        // Fresh observe — never predict revision/digest.
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
        next_term: WriterTerm,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        let predecessor = observed
            .state
            .active()
            .ok_or_else(|| FoundationTransitionError::Conflict {
                message: "no active generation to seal".into(),
            })?
            .loglet_id
            .clone();

        self.ensure_predecessor_sealed(&predecessor).await?;

        let next_revision = observed.state.revision.checked_add(1).ok_or_else(|| {
            FoundationTransitionError::Unavailable(Box::new(std::io::Error::other(
                "VirtualLog revision overflow",
            )))
        })?;
        let successor =
            self.loglet_ids
                .next_loglet_id(next_revision, next_term, self.identity.owner_id)?;
        let (_parts, receipt, writable, bind) =
            self.provision_successor_uninstalled(&successor).await?;

        let fence = CanonFence::new(
            next_revision,
            self.key.journal_id,
            self.key.verse_id,
            owned_with_writer_term(
                self.identity.owner_id,
                self.identity.endpoint.clone(),
                next_term,
            ),
        );

        let outcome = match self
            .virtual_log()
            .reconfigure_with_receipt(&observed, receipt, writable.as_ref(), &bind, fence.encode())
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                // Receipt was moved into Holylog; outcome is indeterminate.
                return Err(Self::map_indeterminate(error));
            }
        };

        match outcome {
            ReceiptReconfiguration::Applied { .. } => {
                self.resolver
                    .insert_writable(successor.clone(), Arc::clone(&writable));
            }
            ReceiptReconfiguration::Conflict { .. } => {
                // Receipt retained by Holylog conflict path; do not install writable.
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
                .is_some_and(|(_, generation)| generation == expected),
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
        let is_successor = match &intent.precondition {
            FoundationPrecondition::Empty => true,
            FoundationPrecondition::Expected(expected) => {
                generation.virtual_log_revision > expected.virtual_log_revision
            }
        };
        if !is_successor {
            return Ok(TransitionClassification::Divergent {
                observed_generation: Some(generation),
            });
        }

        let fence = CanonFence::decode(&observed.state.application_fence)
            .map_err(|error| Self::map_unavailable(std::io::Error::other(error.to_string())))?;
        if fence.journal_id != key.journal_id || fence.verse_id != key.verse_id {
            return Ok(TransitionClassification::Divergent {
                observed_generation: Some(generation),
            });
        }
        match fence.owner {
            CanonOwner::Owned {
                owner_id,
                writer_term: Some(term),
                ..
            } if owner_id == intent.candidate_owner_id && term == intent.next_writer_term => {
                Ok(TransitionClassification::IntendedSuccessor { generation })
            }
            _ => Ok(TransitionClassification::Divergent {
                observed_generation: Some(generation),
            }),
        }
    }

    /// One-shot brand-new authority-domain Foundation bootstrap (v3 fence).
    ///
    /// Publishes Canon only. Callers must separately establish the matching
    /// Serving Authority record; no runtime is started here.
    pub async fn bootstrap_foundation_v3(
        &self,
        initial_term: WriterTerm,
    ) -> Result<JournalGenerationRef, FoundationTransitionError> {
        let _guard = self.control.lock().await;
        self.drive_locked(
            self.key,
            self.identity.owner_id,
            initial_term,
            FoundationPrecondition::Empty,
        )
        .await
    }
}

impl JournalFoundationTransition for HolylogJournalFoundation {
    fn drive_foundation_transition(
        &self,
        key: AuthorityKey,
        target_owner_id: OwnerId,
        next_term: WriterTerm,
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
            self.drive_locked(key, target_owner_id, next_term, precondition)
                .await
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
