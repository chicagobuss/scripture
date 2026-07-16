//! Durable authority-domain bootstrap via [`AuthorityCoordinator`].
//!
//! Protocol: CAS `Transitioning { Empty, … }` → publish v3 Foundation → CAS
//! `Serving`. Crash between steps leaves a durable Transitioning intent that
//! [`AuthorityCoordinator::reconcile`] can classify. Ordinary `scripture serve`
//! never bootstraps.

use scripture::serving_authority::{
    AuthorityKey, FoundationPrecondition, ServingAuthorityRecord, WriterTerm,
};
use scripture_service::{AuthorityCoordinator, CoordinatorError, LocalServingEligibility};

/// Bootstraps a brand-new authority domain through the durable Transitioning protocol.
pub async fn bootstrap_authority_domain(
    coordinator: &AuthorityCoordinator,
    key: AuthorityKey,
    initial_term: WriterTerm,
) -> Result<ServingAuthorityRecord, CoordinatorError> {
    coordinator
        .promote(
            key,
            initial_term,
            FoundationPrecondition::Empty,
            LocalServingEligibility {
                // Candidate will hold the freshly provisioned writable after Foundation Applied.
                is_writable: true,
                is_sealed: false,
            },
        )
        .await
}
