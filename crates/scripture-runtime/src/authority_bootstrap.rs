//! Durable authority-domain bootstrap via [`AuthorityCoordinator`].
//!
//! Empty path: foundation publishes membership + Serving in one root CAS.
//! Ordinary `scripture serve` never bootstraps.

use scripture::serving_authority::{
    AuthorityKey, FoundationPrecondition, ServingAuthorityRecord, WriterTerm,
};
use scripture_service::{AuthorityCoordinator, CoordinatorError, LocalServingEligibility};

/// Bootstraps a brand-new authority domain through one-record Empty→Serving.
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
