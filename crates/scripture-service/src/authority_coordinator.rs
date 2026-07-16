//! Asynchronous transport-neutral `AuthorityCoordinator` and recovery model.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use scripture::canon::OwnerId;
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, FoundationPrecondition, JournalGenerationRef, RouteHint,
    ServingAuthorityRecord, TransitionId, TransitionIntent, TransitionKind, WriterAuthority,
    WriterTerm,
};

use crate::serving_authority_store::{
    CasOutcome, ServingAuthorityStore, ServingAuthorityStoreError,
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
/// Uses the platform's secure operating-system randomness source.
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
    /// Drives the transition on the Journal Foundation:
    /// - seals the current active generation loglet
    /// - provisions the new loglet
    /// - publishes the new VirtualLog state containing the matching v3 CanonFence.
    ///
    /// # Safety and Precondition
    ///
    /// The transition MUST atomically validate that the current Foundation state matches `precondition`
    /// before publishing the successor generation.
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
    /// The ServingAuthorityStore returned a CAS conflict or read failure.
    #[error("ServingAuthorityStore error: {0}")]
    Store(#[from] ServingAuthorityStoreError),

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

/// Transport-neutral coordinator that orchestrates the Serving Authority lifecycle.
pub struct AuthorityCoordinator {
    store: Arc<dyn ServingAuthorityStore>,
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
    /// Constructs an AuthorityCoordinator.
    #[must_use]
    pub fn new(
        store: Arc<dyn ServingAuthorityStore>,
        foundation: Arc<dyn JournalFoundationTransition>,
        id_generator: Arc<dyn TransitionIdGenerator>,
        local_owner_id: OwnerId,
        route_hint: RouteHint,
    ) -> Self {
        Self {
            store,
            foundation,
            id_generator,
            local_owner_id,
            route_hint,
        }
    }

    /// Drives the operator-requested recovery-promotion flow.
    pub async fn promote(
        &self,
        key: AuthorityKey,
        candidate_term: WriterTerm,
        precondition: FoundationPrecondition,
        eligibility: LocalServingEligibility,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        // Validation: Local writable/sealed check
        if !eligibility.permits_serving() {
            return Err(CoordinatorError::Unwritable);
        }

        // 1. Observe Serving Authority.
        let snapshot_opt = self.store.observe(key).await?;

        // 2. Validate current effective authority, expected JournalGenerationRef, and next WriterTerm.
        let (expected_version, next_state) = match &snapshot_opt {
            None => {
                let intent = TransitionIntent {
                    transition_id: self.id_generator.generate()?,
                    kind: TransitionKind::RecoveryPromotion,
                    precondition: precondition.clone(),
                    candidate_owner_id: self.local_owner_id,
                    next_writer_term: candidate_term,
                };
                let next_state = AuthorityState::Transitioning { intent };
                (None, next_state)
            }
            Some(snapshot) => {
                match &snapshot.record.state {
                    AuthorityState::Serving { authority, .. } => {
                        if authority.writer_term.get() >= candidate_term.get() {
                            return Err(CoordinatorError::InvalidInput {
                                message: format!(
                                    "candidate term {candidate_term} is not strictly greater than serving term {}",
                                    authority.writer_term
                                ),
                            });
                        }
                        match &precondition {
                            FoundationPrecondition::Empty => {
                                return Err(CoordinatorError::InvalidInput {
                                    message: "cannot use Empty precondition when replacing an active Serving state".to_string(),
                                });
                            }
                            FoundationPrecondition::Expected(expected_ref) => {
                                if &authority.generation_ref != expected_ref {
                                    return Err(CoordinatorError::InvalidInput {
                                        message: "current serving generation ref does not match expected precondition facts".to_string(),
                                    });
                                }
                            }
                        }
                    }
                    AuthorityState::Transitioning { .. } => {
                        return Err(CoordinatorError::Conflict {
                            message: "a live transition is already in progress".to_string(),
                        });
                    }
                    AuthorityState::ReconciliationRequired { .. } => {
                        return Err(CoordinatorError::Conflict {
                            message: "record is in ReconciliationRequired state and requires manual reconciliation".to_string(),
                        });
                    }
                    AuthorityState::Unassigned => {}
                }

                let intent = TransitionIntent {
                    transition_id: self.id_generator.generate()?,
                    kind: TransitionKind::RecoveryPromotion,
                    precondition: precondition.clone(),
                    candidate_owner_id: self.local_owner_id,
                    next_writer_term: candidate_term,
                };
                let next_state = AuthorityState::Transitioning { intent };
                (Some(snapshot.version.clone()), next_state)
            }
        };

        let active_intent = match &next_state {
            AuthorityState::Transitioning { intent } => intent.clone(),
            _ => unreachable!(),
        };

        // 3. CAS the authority record to Transitioning. This halts client appends.
        let next_record = ServingAuthorityRecord::new(key, next_state.clone());
        let cas_trans_res = self
            .store
            .compare_and_swap(key, expected_version, next_record.clone())
            .await;

        let trans_snapshot_version = match cas_trans_res {
            Ok(CasOutcome::Applied) => {
                let snap =
                    self.store
                        .observe(key)
                        .await?
                        .ok_or_else(|| CoordinatorError::Conflict {
                            message: "transitioning record vanished".to_string(),
                        })?;

                // Safety Reread check: Verify that the TransitionIntent is exactly ours!
                if let AuthorityState::Transitioning { intent } = &snap.record.state {
                    if *intent != active_intent {
                        return Err(CoordinatorError::Conflict {
                            message: "concurrent transition overrode intent immediately after CAS"
                                .to_string(),
                        });
                    }
                } else {
                    return Err(CoordinatorError::Conflict {
                        message: "concurrent update overrode intent immediately after CAS"
                            .to_string(),
                    });
                }

                snap.version
            }
            Ok(CasOutcome::Conflict) => {
                return Err(CoordinatorError::Conflict {
                    message: "CAS to Transitioning failed: concurrent update".to_string(),
                });
            }
            Err(ServingAuthorityStoreError::Indeterminate(e)) => {
                // Perform a linearizable read check
                let snap = self.store.observe(key).await?;
                if let Some(s) = snap {
                    if let AuthorityState::Transitioning { intent } = &s.record.state {
                        if *intent == active_intent {
                            s.version
                        } else {
                            return Err(CoordinatorError::Conflict {
                                message: format!(
                                    "indeterminate CAS write did not establish planned transition: {e}"
                                ),
                            });
                        }
                    } else {
                        return Err(CoordinatorError::Conflict {
                            message: format!(
                                "indeterminate CAS write did not establish planned transition: {e}"
                            ),
                        });
                    }
                } else {
                    return Err(CoordinatorError::Conflict {
                        message: format!(
                            "indeterminate CAS write did not establish planned transition: {e}"
                        ),
                    });
                }
            }
            Err(e) => return Err(CoordinatorError::Store(e)),
        };

        // 4. Drive the existing lawful Journal Foundation transition
        let foundation_res = self
            .foundation
            .drive_foundation_transition(
                key,
                self.local_owner_id,
                candidate_term,
                precondition.clone(),
            )
            .await;

        let successor_gen_ref = match foundation_res {
            Ok(gen_ref) => gen_ref,
            Err(e) => {
                let rec_state = AuthorityState::ReconciliationRequired {
                    intent: active_intent.clone(),
                    observed_generation: None,
                };
                let rec_record = ServingAuthorityRecord::new(key, rec_state);
                let _ = self
                    .store
                    .compare_and_swap(key, Some(trans_snapshot_version), rec_record)
                    .await;
                return Err(CoordinatorError::FoundationFailed(e));
            }
        };

        // 5. Build Serving state
        let auth = WriterAuthority {
            owner_id: self.local_owner_id,
            writer_term: candidate_term,
            generation_ref: successor_gen_ref.clone(),
        };
        let serving_state = AuthorityState::Serving {
            authority: auth,
            route_hint: self.route_hint.clone(),
        };
        let final_record = ServingAuthorityRecord::new(key, serving_state);

        // 6. CAS authority record to Serving
        let cas_serving_res = self
            .store
            .compare_and_swap(key, Some(trans_snapshot_version), final_record.clone())
            .await;

        match cas_serving_res {
            Ok(CasOutcome::Applied) => {}
            Ok(CasOutcome::Conflict) => {
                return Err(CoordinatorError::Conflict {
                    message: "CAS to Serving failed: concurrent reconfigurer won".to_string(),
                });
            }
            Err(ServingAuthorityStoreError::Indeterminate(e)) => {
                let snap = self.store.observe(key).await?;
                if let Some(s) = snap {
                    if s.record == final_record {
                        // Succeeded!
                    } else {
                        return Err(CoordinatorError::Conflict {
                            message: format!(
                                "indeterminate CAS write did not establish Serving state: {e}"
                            ),
                        });
                    }
                } else {
                    return Err(CoordinatorError::Conflict {
                        message: format!(
                            "indeterminate CAS write did not establish Serving state: {e}"
                        ),
                    });
                }
            }
            Err(e) => return Err(CoordinatorError::Store(e)),
        }

        // 7. Require a final re-observation before we are officially serving
        let final_snapshot =
            self.store
                .observe(key)
                .await?
                .ok_or_else(|| CoordinatorError::Conflict {
                    message: "serving record vanished".to_string(),
                })?;

        if final_snapshot.record != final_record {
            return Err(CoordinatorError::ContenderConflict);
        }

        Ok(final_snapshot.record)
    }

    /// Reconciles an in-progress or failed transition by inspecting the durable
    /// Foundation state and applying the correct fail-closed resolution.
    pub async fn reconcile(
        &self,
        key: AuthorityKey,
        eligibility: LocalServingEligibility,
    ) -> Result<ServingAuthorityRecord, CoordinatorError> {
        let snap =
            self.store
                .observe(key)
                .await?
                .ok_or_else(|| CoordinatorError::InvalidInput {
                    message: "reconcile failed: no record exists".to_string(),
                })?;

        let (intent, observed_ref_opt) = match &snap.record.state {
            AuthorityState::Transitioning { intent } => (intent.clone(), None),
            AuthorityState::ReconciliationRequired {
                intent,
                observed_generation,
            } => (intent.clone(), observed_generation.clone()),
            _ => {
                return Ok(snap.record);
            }
        };

        // 2. Query live classified Foundation state
        let classification = match self.foundation.classify_transition(key, &intent).await {
            Ok(c) => c,
            Err(e) => {
                let rec_state = AuthorityState::ReconciliationRequired {
                    intent: intent.clone(),
                    observed_generation: observed_ref_opt,
                };
                let rec_record = ServingAuthorityRecord::new(key, rec_state);
                let _ = self
                    .store
                    .compare_and_swap(key, Some(snap.version), rec_record)
                    .await;
                return Err(CoordinatorError::FoundationFailed(e));
            }
        };

        let next_record = match classification {
            TransitionClassification::StillPredecessor => {
                ServingAuthorityRecord::new(key, AuthorityState::Unassigned)
            }
            TransitionClassification::IntendedSuccessor { generation } => {
                // Safety Guard: "Do not use the reconciling node's route for another owner."
                // Only the actual candidate node is allowed to transition to Serving!
                if self.local_owner_id == intent.candidate_owner_id && eligibility.permits_serving()
                {
                    let auth = WriterAuthority {
                        owner_id: intent.candidate_owner_id,
                        writer_term: intent.next_writer_term,
                        generation_ref: generation,
                    };
                    ServingAuthorityRecord::new(
                        key,
                        AuthorityState::Serving {
                            authority: auth,
                            route_hint: self.route_hint.clone(),
                        },
                    )
                } else {
                    // A non-candidate or an ineligible candidate persists the observed generation
                    // evidence but may not publish a client route.
                    ServingAuthorityRecord::new(
                        key,
                        AuthorityState::ReconciliationRequired {
                            intent: intent.clone(),
                            observed_generation: Some(generation),
                        },
                    )
                }
            }
            TransitionClassification::Divergent {
                observed_generation,
            } => ServingAuthorityRecord::new(
                key,
                AuthorityState::ReconciliationRequired {
                    intent: intent.clone(),
                    observed_generation,
                },
            ),
        };

        let cas_res = self
            .store
            .compare_and_swap(key, Some(snap.version), next_record.clone())
            .await;

        match cas_res {
            Ok(CasOutcome::Applied) => Ok(next_record),
            Ok(CasOutcome::Conflict) => {
                let final_snap =
                    self.store
                        .observe(key)
                        .await?
                        .ok_or_else(|| CoordinatorError::Conflict {
                            message: "record vanished during reconciliation CAS".to_string(),
                        })?;
                Ok(final_snap.record)
            }
            Err(ServingAuthorityStoreError::Indeterminate(e)) => {
                // Indeterminate CAS resolution on reconciliation
                let snap2 = self.store.observe(key).await?;
                if let Some(s) = snap2 {
                    if s.record == next_record {
                        Ok(next_record)
                    } else {
                        Err(CoordinatorError::Store(
                            ServingAuthorityStoreError::Indeterminate(e),
                        ))
                    }
                } else {
                    Err(CoordinatorError::Store(
                        ServingAuthorityStoreError::Indeterminate(e),
                    ))
                }
            }
            Err(e) => Err(CoordinatorError::Store(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryServingAuthorityStore;
    use holylog::virtual_log::LogletId;
    use scripture::canon::{CanonFence, CanonOwner, OwnerEndpoint, VerseId};
    use scripture::model::JournalId;
    use std::sync::Mutex;

    fn journal() -> JournalId {
        JournalId::from_bytes(*b"canon-journal-id")
    }

    fn verse() -> VerseId {
        VerseId::from_bytes(*b"canon-line-id!!!")
    }

    fn owner() -> OwnerId {
        OwnerId::from_bytes(*b"canon-owner-id!!")
    }

    fn route_hint() -> RouteHint {
        RouteHint::new("tcp://scripture-hint.internal:9000").expect("route hint")
    }

    const ELIGIBLE: LocalServingEligibility = LocalServingEligibility {
        is_writable: true,
        is_sealed: false,
    };

    struct MockState {
        current_ref: Option<JournalGenerationRef>,
        // Track the current publisher details statefully
        current_owner: Option<OwnerId>,
        current_term: Option<WriterTerm>,
    }

    struct StatefulMockFoundation {
        state: Mutex<MockState>,
        should_fail: Mutex<Option<FoundationTransitionError>>,
    }

    impl StatefulMockFoundation {
        fn new_empty() -> Self {
            Self {
                state: Mutex::new(MockState {
                    current_ref: None,
                    current_owner: None,
                    current_term: None,
                }),
                should_fail: Mutex::new(None),
            }
        }

        #[allow(dead_code)]
        fn set_fail(&self, err: Option<FoundationTransitionError>) {
            *self.should_fail.lock().expect("lock") = err;
        }

        // Test helper to simulate external concurrent foundation transition (e.g. Actor B terms in)
        fn simulate_external_transition(&self, next_owner: OwnerId, next_term: WriterTerm) {
            let mut guard = self.state.lock().expect("lock");
            let next_rev = guard
                .current_ref
                .as_ref()
                .map_or(1, |current| current.virtual_log_revision + 1);
            let next_start = guard
                .current_ref
                .as_ref()
                .map_or(0, |current| current.active_start + 100);
            let loglet_str = format!("external-gen-{}", next_rev);

            let owned = CanonOwner::Owned {
                owner_id: next_owner,
                endpoint: OwnerEndpoint::new("tcp://sequencer.internal:9000")
                    .expect("valid endpoint"),
                sequencer: None,
                writer_term: Some(next_term),
            };
            let fence = CanonFence::new(next_rev, journal(), verse(), owned);
            let encoded = fence.encode();
            let digest: [u8; 32] = blake3::hash(encoded.as_bytes()).into();

            guard.current_ref = Some(JournalGenerationRef {
                virtual_log_revision: next_rev,
                active_loglet_id: LogletId::new(loglet_str).expect("valid loglet"),
                active_start: next_start,
                canon_fence_digest: digest,
            });
            guard.current_owner = Some(next_owner);
            guard.current_term = Some(next_term);
        }
    }

    impl JournalFoundationTransition for StatefulMockFoundation {
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
            if let Some(err) = &*self.should_fail.lock().expect("lock") {
                let e_clone = match err {
                    FoundationTransitionError::Conflict { message } => {
                        FoundationTransitionError::Conflict {
                            message: message.clone(),
                        }
                    }
                    FoundationTransitionError::Unavailable(_) => {
                        FoundationTransitionError::Unavailable(Box::new(std::io::Error::other(
                            "mock error",
                        )))
                    }
                    FoundationTransitionError::Indeterminate(_) => {
                        FoundationTransitionError::Indeterminate(Box::new(std::io::Error::other(
                            "mock error",
                        )))
                    }
                };
                return Box::pin(async move { Err(e_clone) });
            }

            let mut guard = self.state.lock().expect("lock");

            match &precondition {
                FoundationPrecondition::Empty => {
                    if guard.current_ref.is_some() {
                        return Box::pin(async move {
                            Err(FoundationTransitionError::Conflict {
                                message: "precondition mismatch: expected empty".to_string(),
                            })
                        });
                    }
                }
                FoundationPrecondition::Expected(expected) => {
                    if guard.current_ref.as_ref() != Some(expected) {
                        return Box::pin(async move {
                            Err(FoundationTransitionError::Conflict {
                                message: "precondition mismatch".to_string(),
                            })
                        });
                    }
                }
            }

            let next_rev = guard
                .current_ref
                .as_ref()
                .map_or(1, |current| current.virtual_log_revision + 1);
            let next_start = guard
                .current_ref
                .as_ref()
                .map_or(0, |current| current.active_start + 100);
            let loglet_str = format!("loglet-gen-{}", next_rev);

            let owned = CanonOwner::Owned {
                owner_id: target_owner_id,
                endpoint: OwnerEndpoint::new("tcp://sequencer.internal:9000")
                    .expect("valid endpoint"),
                sequencer: None,
                writer_term: Some(next_term),
            };
            let fence = CanonFence::new(next_rev, key.journal_id, key.verse_id, owned);
            let encoded = fence.encode();
            let digest: [u8; 32] = blake3::hash(encoded.as_bytes()).into();

            let next_ref = JournalGenerationRef {
                virtual_log_revision: next_rev,
                active_loglet_id: LogletId::new(loglet_str).expect("valid loglet"),
                active_start: next_start,
                canon_fence_digest: digest,
            };

            guard.current_ref = Some(next_ref.clone());
            guard.current_owner = Some(target_owner_id);
            guard.current_term = Some(next_term);

            Box::pin(async move { Ok(next_ref) })
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
            let guard = self.state.lock().expect("lock");
            let live_ref = guard.current_ref.clone();

            // 1. Verify exact Precondition matches
            let matches_precondition = match &intent.precondition {
                FoundationPrecondition::Empty => live_ref.is_none(),
                FoundationPrecondition::Expected(expected_ref) => {
                    live_ref.as_ref() == Some(expected_ref)
                }
            };

            if matches_precondition {
                return Box::pin(async move { Ok(TransitionClassification::StillPredecessor) });
            }

            let Some(live_ref) = live_ref else {
                return Box::pin(async move {
                    Ok(TransitionClassification::Divergent {
                        observed_generation: None,
                    })
                });
            };

            // 2. Verify Successor checks exact owner, term, key, and successor relation (strictly greater revision)
            let prev_rev = match &intent.precondition {
                FoundationPrecondition::Empty => 0,
                FoundationPrecondition::Expected(expected_ref) => expected_ref.virtual_log_revision,
            };

            if live_ref.virtual_log_revision > prev_rev {
                // Successor matches candidate owner & next writer term exactly
                if guard.current_owner == Some(intent.candidate_owner_id)
                    && guard.current_term == Some(intent.next_writer_term)
                    && key
                        == (AuthorityKey {
                            journal_id: journal(),
                            verse_id: verse(),
                        })
                {
                    return Box::pin(async move {
                        Ok(TransitionClassification::IntendedSuccessor {
                            generation: live_ref,
                        })
                    });
                }
            }

            // 3. Otherwise, Divergent
            Box::pin(async move {
                Ok(TransitionClassification::Divergent {
                    observed_generation: Some(live_ref),
                })
            })
        }
    }

    #[tokio::test]
    async fn test_authority_coordinator_promotion_success() {
        let store = Arc::new(InMemoryServingAuthorityStore::new());
        let foundation = Arc::new(StatefulMockFoundation::new_empty());
        let id_gen = Arc::new(DeterministicTransitionIdGenerator::new());

        let coordinator = AuthorityCoordinator::new(
            Arc::clone(&store) as Arc<dyn ServingAuthorityStore>,
            Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
            Arc::clone(&id_gen) as Arc<dyn TransitionIdGenerator>,
            owner(),
            route_hint(),
        );

        let key = AuthorityKey {
            journal_id: journal(),
            verse_id: verse(),
        };

        let term = WriterTerm::new(1).expect("valid term");

        // 1. Promote Empty bootstrap
        let record = coordinator
            .promote(key, term, FoundationPrecondition::Empty, ELIGIBLE)
            .await
            .expect("promote success");

        let AuthorityState::Serving { authority, .. } = &record.state else {
            panic!("expected Serving state");
        };

        assert_eq!(authority.owner_id, owner());
        assert_eq!(authority.writer_term, term);
        assert_eq!(authority.generation_ref.virtual_log_revision, 1);
    }

    #[tokio::test]
    async fn test_reconcile_recounts_divergent_owner_or_term_reconciliation_required() {
        let store = Arc::new(InMemoryServingAuthorityStore::new());
        let foundation = Arc::new(StatefulMockFoundation::new_empty());
        let id_gen = Arc::new(DeterministicTransitionIdGenerator::new());

        let coordinator = AuthorityCoordinator::new(
            Arc::clone(&store) as Arc<dyn ServingAuthorityStore>,
            Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
            Arc::clone(&id_gen) as Arc<dyn TransitionIdGenerator>,
            owner(),
            route_hint(),
        );

        let key = AuthorityKey {
            journal_id: journal(),
            verse_id: verse(),
        };

        let term = WriterTerm::new(1).expect("valid");

        // Seed a pending Transitioning record
        let intent = TransitionIntent {
            transition_id: id_gen.generate().expect("valid"),
            kind: TransitionKind::RecoveryPromotion,
            precondition: FoundationPrecondition::Empty,
            candidate_owner_id: owner(),
            next_writer_term: term,
        };
        let rec = ServingAuthorityRecord::new(
            key,
            AuthorityState::Transitioning {
                intent: intent.clone(),
            },
        );
        store
            .compare_and_swap(key, None, rec)
            .await
            .expect("seeded");

        // Simulate a divergent external actor winning the foundation (different owner/term)
        let other_owner = OwnerId::from_bytes(*b"other-owner-id!!");
        let other_term = WriterTerm::new(10).expect("valid");
        foundation.simulate_external_transition(other_owner, other_term);

        // Reconcile must safely classify it as Divergent and CAS to ReconciliationRequired containing the observed divergent ref!
        let reconciled = coordinator
            .reconcile(key, ELIGIBLE)
            .await
            .expect("reconcile succeeds");
        let AuthorityState::ReconciliationRequired {
            observed_generation: Some(obs),
            ..
        } = reconciled.state
        else {
            panic!("Expected ReconciliationRequired with observed generation evidence");
        };

        assert_eq!(obs.virtual_log_revision, 1);
        assert_eq!(obs.active_loglet_id.as_str(), "external-gen-1");
    }

    #[tokio::test]
    async fn test_reconcile_from_non_candidate_cannot_advertise_its_route() {
        let store = Arc::new(InMemoryServingAuthorityStore::new());
        let foundation = Arc::new(StatefulMockFoundation::new_empty());
        let id_gen = Arc::new(DeterministicTransitionIdGenerator::new());

        let key = AuthorityKey {
            journal_id: journal(),
            verse_id: verse(),
        };

        let term = WriterTerm::new(1).expect("valid");

        // Transition intent targets "owner()"
        let intent = TransitionIntent {
            transition_id: id_gen.generate().expect("valid"),
            kind: TransitionKind::RecoveryPromotion,
            precondition: FoundationPrecondition::Empty,
            candidate_owner_id: owner(), // candidate is owner A
            next_writer_term: term,
        };
        let rec = ServingAuthorityRecord::new(
            key,
            AuthorityState::Transitioning {
                intent: intent.clone(),
            },
        );
        store
            .compare_and_swap(key, None, rec)
            .await
            .expect("seeded");

        // Foundation successfully transitioned to owner A
        foundation
            .drive_foundation_transition(key, owner(), term, FoundationPrecondition::Empty)
            .await
            .expect("drive success");

        // Coordinator is owner B (non-candidate reconciling the record)
        let other_owner = OwnerId::from_bytes(*b"other-owner-id!!");
        let coord_b = AuthorityCoordinator::new(
            Arc::clone(&store) as Arc<dyn ServingAuthorityStore>,
            Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
            Arc::clone(&id_gen) as Arc<dyn TransitionIdGenerator>,
            other_owner,
            route_hint(),
        );

        // Non-candidate B reconciles: must PERSIST divergence/successor evidence but must NOT transition to Serving
        let reconciled = coord_b
            .reconcile(key, ELIGIBLE)
            .await
            .expect("reconcile success");
        let AuthorityState::ReconciliationRequired {
            observed_generation: Some(obs),
            ..
        } = reconciled.state
        else {
            panic!(
                "Expected ReconciliationRequired for non-candidate instead of Serving route publication"
            );
        };
        assert_eq!(obs.virtual_log_revision, 1);
    }

    #[tokio::test]
    async fn test_reconcile_from_sealed_candidate_cannot_advertise_route() {
        let store = Arc::new(InMemoryServingAuthorityStore::new());
        let foundation = Arc::new(StatefulMockFoundation::new_empty());
        let id_gen = Arc::new(DeterministicTransitionIdGenerator::new());
        let key = AuthorityKey {
            journal_id: journal(),
            verse_id: verse(),
        };
        let term = WriterTerm::new(1).expect("valid");
        let intent = TransitionIntent {
            transition_id: id_gen.generate().expect("valid"),
            kind: TransitionKind::RecoveryPromotion,
            precondition: FoundationPrecondition::Empty,
            candidate_owner_id: owner(),
            next_writer_term: term,
        };
        store
            .compare_and_swap(
                key,
                None,
                ServingAuthorityRecord::new(
                    key,
                    AuthorityState::Transitioning {
                        intent: intent.clone(),
                    },
                ),
            )
            .await
            .expect("seeded");
        foundation
            .drive_foundation_transition(key, owner(), term, FoundationPrecondition::Empty)
            .await
            .expect("foundation successor");

        let coordinator = AuthorityCoordinator::new(
            Arc::clone(&store) as Arc<dyn ServingAuthorityStore>,
            Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
            Arc::clone(&id_gen) as Arc<dyn TransitionIdGenerator>,
            owner(),
            route_hint(),
        );
        let sealed = LocalServingEligibility {
            is_writable: true,
            is_sealed: true,
        };

        let reconciled = coordinator
            .reconcile(key, sealed)
            .await
            .expect("reconcile succeeds without serving");
        assert!(matches!(
            reconciled.state,
            AuthorityState::ReconciliationRequired {
                observed_generation: Some(_),
                ..
            }
        ));
    }
}
