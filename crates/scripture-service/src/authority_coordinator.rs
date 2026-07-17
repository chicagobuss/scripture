//! One-record `AuthorityCoordinator`: VirtualLog root fence is the only durable authority.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use holylog::virtual_log::{
    ConditionalRegister, FenceUpdate, LogletResolver, VirtualLog, VirtualLogError,
};
use scripture::canon::OwnerId;
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, FoundationPrecondition, JournalGenerationRef, RouteHint,
    ServingAuthorityRecord, TransitionId, TransitionIntent, TransitionKind, WriterAuthority,
    WriterTerm,
};

pub type CoordinatorFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, CoordinatorError>> + Send + 'a>>;

/// Generator trait for secure, collision-resistant transition IDs.
pub trait TransitionIdGenerator: Send + Sync {
    /// Generates a globally unique, collision-resistant TransitionId.
    fn generate(&self) -> Result<TransitionId, TransitionIdGenerationError>;
}

/// Failure to obtain a transition identity before any authority state changes.
#[derive(Debug, thiserror::Error)]
pub enum TransitionIdGenerationError {
    /// The operating system could not provide cryptographically secure randomness.
    #[error("secure transition-id entropy is unavailable: {message}")]
    Entropy {
        /// Platform-specific entropy failure detail.
        message: String,
    },
}

/// A cryptographically secure, collision-resistant TransitionId generator.
#[derive(Debug, Default)]
pub struct SecureTransitionIdGenerator;

impl SecureTransitionIdGenerator {
    /// Constructs a SecureTransitionIdGenerator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TransitionIdGenerator for SecureTransitionIdGenerator {
    fn generate(&self) -> Result<TransitionId, TransitionIdGenerationError> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| TransitionIdGenerationError::Entropy {
            message: error.to_string(),
        })?;
        Ok(TransitionId::from_bytes(bytes))
    }
}

/// A deterministic TransitionId generator for testing.
#[derive(Debug, Default)]
pub struct DeterministicTransitionIdGenerator {
    counter: AtomicU64,
}

impl DeterministicTransitionIdGenerator {
    /// Constructs a DeterministicTransitionIdGenerator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }
}

impl TransitionIdGenerator for DeterministicTransitionIdGenerator {
    fn generate(&self) -> Result<TransitionId, TransitionIdGenerationError> {
        let seq = self.counter.fetch_add(1, Ordering::SeqCst);
        let mut bytes = [0_u8; 16];
        bytes[0..8].copy_from_slice(&seq.to_be_bytes());
        Ok(TransitionId::from_bytes(bytes))
    }
}

/// Local facts required before this process may publish a Serving route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalServingEligibility {
    /// Whether this process holds the lawful writable capability.
    pub is_writable: bool,
    /// Whether the local writable capability has been sealed.
    pub is_sealed: bool,
}

impl LocalServingEligibility {
    /// Returns true only when the local runtime can lawfully begin serving.
    #[must_use]
    pub const fn permits_serving(self) -> bool {
        self.is_writable && !self.is_sealed
    }
}

/// Typed outcomes from driving the Journal Foundation transition.
#[derive(Debug, thiserror::Error)]
pub enum FoundationTransitionError {
    /// The Foundation transition collided with a concurrent change or could not be established.
    #[error("Foundation transition conflict: {message}")]
    Conflict { message: String },
    /// The database or network is transiently or permanently unavailable.
    #[error("Foundation transition unavailable: {0}")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The outcome is unknown (e.g. timeout during publish); the transition may have completed.
    #[error("Foundation transition outcome indeterminate: {0}")]
    Indeterminate(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Classification of a Foundation transition relative to a transition intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionClassification {
    /// The foundation matches the intent's precondition exactly; the transition did not publish.
    StillPredecessor,
    /// The foundation matches the intent's target candidate and term.
    IntendedSuccessor {
        /// Extracted successor generation reference.
        generation: JournalGenerationRef,
    },
    /// The foundation contains a divergent generation.
    Divergent {
        /// Extracted divergent generation reference.
        observed_generation: Option<JournalGenerationRef>,
    },
}

/// Fault ports for driving the Journal Foundation during cutover.
pub trait JournalFoundationTransition: Send + Sync {
    /// Drives seal → provision → one root CAS of membership + typed Serving fence.
    ///
    /// `publication` is Serving-only — Transitioning cannot be published here.
    fn drive_foundation_transition(
        &self,
        key: AuthorityKey,
        publication: scripture::ServingPublication,
        precondition: FoundationPrecondition,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<JournalGenerationRef, FoundationTransitionError>>
                + Send
                + '_,
        >,
    >;

    /// Freshly observes the live Foundation and classifies its state against the intent.
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
    >;
}

/// Errors exposed by the `AuthorityCoordinator` promotion and recovery loop.
#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    /// The VirtualLog root register could not be observed or updated.
    #[error("VirtualLog root error: {0}")]
    Root(#[from] VirtualLogError),

    /// A secure transition identity could not be created before starting a transition.
    #[error(transparent)]
    TransitionId(#[from] TransitionIdGenerationError),

    /// The Journal Foundation transition failed clearly.
    #[error("Journal Foundation transition failed: {0}")]
    FoundationFailed(#[source] FoundationTransitionError),

    /// Re-observation after serving transition showed a different coordinator already won.
    #[error("another coordinator won the transition concurrently")]
    ContenderConflict,

    /// Transitioning state is locked with a different transition ID or conflicting details.
    #[error("transition is locked or in conflict: {message}")]
    Conflict {
        /// Conflict details.
        message: String,
    },

    /// Local node is sealed or lacks writable capability.
    #[error("local runtime lacks writable capability or is sealed")]
    Unwritable,

    /// Validation of facts or eligibility failed.
    #[error("eligibility or facts validation failed: {message}")]
    InvalidInput {
        /// Validation details.
        message: String,
    },
}

/// Observed root authority decoded from the VirtualLog application fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedRootAuthority {
    /// Register is uninitialized (no membership).
    Uninitialized,
    /// Membership exists but fence bytes are empty / undecodable as Scripture authority.
    AbsentOrMalformed {
        /// Optional decode failure detail.
        message: Option<String>,
    },
    /// Decoded Scripture authority record from the root fence.
    Record(Box<ServingAuthorityRecord>),
}

/// Transport-neutral coordinator that orchestrates one-record Serving Authority.
pub struct AuthorityCoordinator {
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<dyn LogletResolver>,
    foundation: Arc<dyn JournalFoundationTransition>,
    id_generator: Arc<dyn TransitionIdGenerator>,
    local_owner_id: OwnerId,
    route_hint: RouteHint,
}

impl std::fmt::Debug for AuthorityCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorityCoordinator")
            .field("local_owner_id", &self.local_owner_id)
            .field("route_hint", &self.route_hint)
            .finish_non_exhaustive()
    }
}

impl AuthorityCoordinator {
    /// Constructs an AuthorityCoordinator bound to one VirtualLog root register.
    #[must_use]
    pub fn new(
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<dyn LogletResolver>,
        foundation: Arc<dyn JournalFoundationTransition>,
        id_generator: Arc<dyn TransitionIdGenerator>,
        local_owner_id: OwnerId,
        route_hint: RouteHint,
    ) -> Self {
        Self {
            register,
            resolver,
            foundation,
            id_generator,
            local_owner_id,
            route_hint,
        }
    }

    fn virtual_log(&self) -> VirtualLog {
        VirtualLog::new(Arc::clone(&self.register), Arc::clone(&self.resolver))
    }

    /// Freshly observes and decodes the root application fence.
    pub async fn observe_root_authority(&self) -> Result<ObservedRootAuthority, CoordinatorError> {
        match self.virtual_log().observe_membership().await {
            Err(VirtualLogError::Uninitialized) => Ok(ObservedRootAuthority::Uninitialized),
            Err(error) => Err(CoordinatorError::Root(error)),
            Ok(observed) => {
                if observed.state.application_fence.as_bytes().is_empty() {
                    return Ok(ObservedRootAuthority::AbsentOrMalformed { message: None });
                }
                match ServingAuthorityRecord::decode_application_fence(
                    &observed.state.application_fence,
                ) {
                    Ok(record) => Ok(ObservedRootAuthority::Record(Box::new(record))),
                    Err(error) => Ok(ObservedRootAuthority::AbsentOrMalformed {
                        message: Some(error.to_string()),
                    }),
                }
            }
        }
    }

    /// Drives the operator-requested recovery-promotion flow.
    ///
    /// Empty: foundation bootstrap publishes membership + Serving in one root CAS.
    /// Expected: intent fence CAS (Transitioning) → foundation seal/provision/Serving CAS.
    pub async fn promote(
        &self,
        key: AuthorityKey,
        candidate_term: WriterTerm,
        precondition: FoundationPrecondition,
        eligibility: LocalServingEligibility,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        if !eligibility.permits_serving() {
            return Err(CoordinatorError::Unwritable);
        }

        match &precondition {
            FoundationPrecondition::Empty => {
                self.promote_empty(key, candidate_term, eligibility).await
            }
            FoundationPrecondition::Expected(_) => {
                self.promote_expected(key, candidate_term, precondition, eligibility)
                    .await
            }
        }
    }

    async fn promote_empty(
        &self,
        key: AuthorityKey,
        candidate_term: WriterTerm,
        _eligibility: LocalServingEligibility,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        match self.observe_root_authority().await? {
            ObservedRootAuthority::Uninitialized => {}
            ObservedRootAuthority::Record(record) => {
                return Err(CoordinatorError::InvalidInput {
                    message: format!(
                        "Empty precondition requires uninitialized root; observed {:?}",
                        state_tag(&record.state)
                    ),
                });
            }
            ObservedRootAuthority::AbsentOrMalformed { message } => {
                return Err(CoordinatorError::InvalidInput {
                    message: format!(
                        "Empty precondition requires uninitialized root; fence present ({})",
                        message.unwrap_or_else(|| "undecodable".into())
                    ),
                });
            }
        }

        let publication = self.provisional_publication(key, candidate_term)?;
        let generation = self
            .foundation
            .drive_foundation_transition(key, publication, FoundationPrecondition::Empty)
            .await
            .map_err(CoordinatorError::FoundationFailed)?;

        self.require_local_serving(key, candidate_term, &generation)
            .await
    }

    async fn promote_expected(
        &self,
        key: AuthorityKey,
        candidate_term: WriterTerm,
        precondition: FoundationPrecondition,
        _eligibility: LocalServingEligibility,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        let FoundationPrecondition::Expected(expected_ref) = &precondition else {
            unreachable!("promote_expected only for Expected");
        };

        let virtual_log = self.virtual_log();
        let observed = virtual_log.observe_membership().await?;

        let current =
            ServingAuthorityRecord::decode_application_fence(&observed.state.application_fence)
                .map_err(|error| CoordinatorError::InvalidInput {
                    message: format!("root fence is not a Scripture authority record: {error}"),
                })?;

        if current.key != key {
            return Err(CoordinatorError::InvalidInput {
                message: "root authority key does not match promote key".into(),
            });
        }

        match &current.state {
            AuthorityState::Serving { authority, .. } => {
                if authority.writer_term.get() >= candidate_term.get() {
                    return Err(CoordinatorError::InvalidInput {
                        message: format!(
                            "candidate term {candidate_term} is not strictly greater than serving term {}",
                            authority.writer_term
                        ),
                    });
                }
                if &authority.generation_ref != expected_ref {
                    return Err(CoordinatorError::InvalidInput {
                        message:
                            "current serving generation ref does not match expected precondition"
                                .into(),
                    });
                }
            }
            AuthorityState::Transitioning { intent }
                if intent.candidate_owner_id == self.local_owner_id
                    && intent.next_writer_term == candidate_term
                    && intent.precondition == precondition =>
            {
                // Durable intent already ours — continue forward-only from foundation.
                return self
                    .complete_after_intent(key, candidate_term, precondition, intent.clone())
                    .await;
            }
            AuthorityState::Transitioning { .. } => {
                return Err(CoordinatorError::Conflict {
                    message: "a live transition is already in progress".into(),
                });
            }
            AuthorityState::Unassigned | AuthorityState::ReconciliationRequired { .. } => {
                return Err(CoordinatorError::Conflict {
                    message: format!(
                        "root fence is {:?}; promote requires Serving or our Transitioning",
                        state_tag(&current.state)
                    ),
                });
            }
        }

        let intent = TransitionIntent {
            transition_id: self.id_generator.generate()?,
            kind: TransitionKind::RecoveryPromotion,
            precondition: precondition.clone(),
            candidate_owner_id: self.local_owner_id,
            next_writer_term: candidate_term,
        };
        let transitioning = ServingAuthorityRecord::new(
            key,
            AuthorityState::Transitioning {
                intent: intent.clone(),
            },
        );
        let fence = transitioning.encode_application_fence().map_err(|error| {
            CoordinatorError::InvalidInput {
                message: error.to_string(),
            }
        })?;

        match virtual_log
            .update_application_fence(&observed, fence)
            .await?
        {
            FenceUpdate::Applied { .. } => {}
            FenceUpdate::Conflict => {
                // Reply-loss / race: resolve by fresh root read only.
                return self
                    .resolve_intent_after_uncertainty(key, candidate_term, &precondition, &intent)
                    .await;
            }
        }

        // Confirm our intent is durable (covers Applied + reply-loss that still wrote).
        self.confirm_durable_intent(key, &intent).await?;

        self.complete_after_intent(key, candidate_term, precondition, intent)
            .await
    }

    async fn confirm_durable_intent(
        &self,
        key: AuthorityKey,
        intent: &TransitionIntent,
    ) -> Result<(), CoordinatorError> {
        match self.observe_root_authority().await? {
            ObservedRootAuthority::Record(record)
                if record.key == key
                    && matches!(
                        &record.state,
                        AuthorityState::Transitioning { intent: observed }
                            if observed == intent
                    ) =>
            {
                Ok(())
            }
            ObservedRootAuthority::Record(record)
                if matches!(
                    &record.state,
                    AuthorityState::Serving { authority, .. }
                        if authority.owner_id == intent.candidate_owner_id
                            && authority.writer_term == intent.next_writer_term
                ) =>
            {
                // Foundation raced ahead (or prior complete); treat as success path later.
                Ok(())
            }
            other => Err(CoordinatorError::Conflict {
                message: format!("intent CAS did not establish planned Transitioning: {other:?}"),
            }),
        }
    }

    async fn resolve_intent_after_uncertainty(
        &self,
        key: AuthorityKey,
        candidate_term: WriterTerm,
        precondition: &FoundationPrecondition,
        intent: &TransitionIntent,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        match self.observe_root_authority().await? {
            ObservedRootAuthority::Record(record)
                if record.key == key
                    && matches!(
                        &record.state,
                        AuthorityState::Transitioning { intent: observed }
                            if observed == intent
                    ) =>
            {
                self.complete_after_intent(
                    key,
                    candidate_term,
                    precondition.clone(),
                    intent.clone(),
                )
                .await
            }
            ObservedRootAuthority::Record(record)
                if matches!(
                    &record.state,
                    AuthorityState::Serving { authority, .. }
                        if authority.owner_id == self.local_owner_id
                            && authority.writer_term == candidate_term
                ) =>
            {
                Ok(*record)
            }
            _ => Err(CoordinatorError::Conflict {
                message: "CAS to Transitioning conflicted; fresh root is not our intent".into(),
            }),
        }
    }

    async fn complete_after_intent(
        &self,
        key: AuthorityKey,
        candidate_term: WriterTerm,
        precondition: FoundationPrecondition,
        intent: TransitionIntent,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        // If Serving already installed for this intent, return it (idempotent resume).
        if let ObservedRootAuthority::Record(record) = self.observe_root_authority().await?
            && let AuthorityState::Serving { authority, .. } = &record.state
            && authority.owner_id == intent.candidate_owner_id
            && authority.writer_term == intent.next_writer_term
        {
            return Ok(*record);
        }

        let publication = self.provisional_publication(key, candidate_term)?;
        let foundation_res = self
            .foundation
            .drive_foundation_transition(key, publication, precondition)
            .await;

        let generation = match foundation_res {
            Ok(generation) => generation,
            Err(error) => {
                // Forward-only: leave Transitioning on the root; do not restore predecessor.
                return Err(CoordinatorError::FoundationFailed(error));
            }
        };

        self.require_local_serving(key, candidate_term, &generation)
            .await
    }

    fn provisional_publication(
        &self,
        key: AuthorityKey,
        candidate_term: WriterTerm,
    ) -> Result<scripture::ServingPublication, CoordinatorError> {
        let provisional = JournalGenerationRef::from_active_generation(
            0,
            holylog::virtual_log::LogletId::new("provisional").map_err(|error| {
                CoordinatorError::InvalidInput {
                    message: error.to_string(),
                }
            })?,
            0,
        );
        scripture::ServingPublication::new(
            key,
            WriterAuthority {
                owner_id: self.local_owner_id,
                writer_term: candidate_term,
                generation_ref: provisional,
            },
            self.route_hint.clone(),
        )
        .map_err(|error| CoordinatorError::InvalidInput {
            message: error.to_string(),
        })
    }

    async fn require_local_serving(
        &self,
        key: AuthorityKey,
        candidate_term: WriterTerm,
        generation: &JournalGenerationRef,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        match self.observe_root_authority().await? {
            ObservedRootAuthority::Record(record)
                if record.key == key
                    && matches!(
                        &record.state,
                        AuthorityState::Serving { authority, .. }
                            if authority.owner_id == self.local_owner_id
                                && authority.writer_term == candidate_term
                                && &authority.generation_ref == generation
                    ) =>
            {
                Ok(*record)
            }
            ObservedRootAuthority::Record(record)
                if matches!(&record.state, AuthorityState::Serving { .. }) =>
            {
                Err(CoordinatorError::ContenderConflict)
            }
            other => Err(CoordinatorError::Conflict {
                message: format!(
                    "foundation Applied but root is not local Serving for generation {generation:?}: {other:?}"
                ),
            }),
        }
    }

    /// Completes a durable Transitioning intent by inspecting Foundation and finishing forward-only.
    pub async fn reconcile(
        &self,
        key: AuthorityKey,
        eligibility: LocalServingEligibility,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        let record = match self.observe_root_authority().await? {
            ObservedRootAuthority::Record(record) if record.key == key => *record,
            ObservedRootAuthority::Uninitialized => {
                return Err(CoordinatorError::InvalidInput {
                    message: "reconcile failed: root uninitialized".into(),
                });
            }
            other => {
                return Err(CoordinatorError::InvalidInput {
                    message: format!("reconcile failed: no authority record on root ({other:?})"),
                });
            }
        };

        let intent = match &record.state {
            AuthorityState::Serving { .. } => return Ok(record),
            AuthorityState::Transitioning { intent } => intent.clone(),
            AuthorityState::Unassigned | AuthorityState::ReconciliationRequired { .. } => {
                return Err(CoordinatorError::Conflict {
                    message: format!(
                        "reconcile requires Transitioning or Serving; observed {:?}",
                        state_tag(&record.state)
                    ),
                });
            }
        };

        if self.local_owner_id != intent.candidate_owner_id {
            return Err(CoordinatorError::Conflict {
                message: "non-candidate cannot complete another owner's transition".into(),
            });
        }
        if !eligibility.permits_serving() {
            return Err(CoordinatorError::Unwritable);
        }

        let classification = self
            .foundation
            .classify_transition(key, &intent)
            .await
            .map_err(CoordinatorError::FoundationFailed)?;

        match classification {
            TransitionClassification::IntendedSuccessor { generation } => {
                self.require_local_serving(key, intent.next_writer_term, &generation)
                    .await
            }
            TransitionClassification::StillPredecessor => {
                // Forward-only: never restore predecessor Serving; complete replacement.
                self.complete_after_intent(
                    key,
                    intent.next_writer_term,
                    intent.precondition.clone(),
                    intent,
                )
                .await
            }
            TransitionClassification::Divergent { .. } => Err(CoordinatorError::Conflict {
                message:
                    "foundation diverged from durable intent; remain fail-closed Transitioning"
                        .into(),
            }),
        }
    }
}

fn state_tag(state: &AuthorityState) -> &'static str {
    match state {
        AuthorityState::Unassigned => "Unassigned",
        AuthorityState::Transitioning { .. } => "Transitioning",
        AuthorityState::Serving { .. } => "Serving",
        AuthorityState::ReconciliationRequired { .. } => "ReconciliationRequired",
    }
}
