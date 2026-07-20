//! Serving Authority admission gate for the product runtime.
//!
//! A process may install writable ingress / return committed acknowledgements
//! only when the fresh VirtualLog root fence decodes to Serving and
//! [`ServingAuthorityRecord::is_effective_writer`] holds exactly.
//! Canon naming the local owner is never sufficient on its own.

use std::sync::Arc;

use holylog::virtual_log::{ConditionalRegister, LogletResolver, VirtualLog, VirtualLogError};
use scripture::OwnerId;
use scripture::root_authority::{RootAuthority, observe_root_authority};
use scripture::serving_authority::{AuthorityKey, AuthorityState, ServingAuthorityRecord};

/// Outcome of an authority-gated admission decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityGateDecision {
    /// Local owner holds exact effective writer authority.
    EffectiveWriter {
        /// Matching Serving Authority record.
        record: ServingAuthorityRecord,
    },
    /// Admission must be refused.
    Denied {
        /// Operator-useful refusal reason.
        reason: AuthorityGateDenial,
    },
}

/// Why the gate refused admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityGateDenial {
    /// Root fence is absent or empty.
    AuthorityAbsent,
    /// Root fence bytes could not be decoded as Scripture authority.
    AuthorityMalformed {
        /// Displayable cause.
        message: String,
    },
    /// VirtualLog / Canon could not be observed.
    FoundationUnavailable {
        /// Displayable cause.
        message: String,
    },
    /// Foundation register has not been published.
    FoundationUninitialized,
    /// Authority exists but is not Serving for this local/writable/generation set.
    NotEffectiveWriter {
        /// Observed authority state tag for logs.
        state: &'static str,
    },
}

/// Evaluates whether the local process may serve committed producer work.
///
/// One-record rule: authority is read only from the fresh VirtualLog root fence.
pub async fn evaluate_authority_gate(
    key: AuthorityKey,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<dyn LogletResolver>,
    local_owner_id: OwnerId,
    is_writable: bool,
    is_sealed: bool,
) -> AuthorityGateDecision {
    let virtual_log = VirtualLog::new(register, resolver);
    let observed = match virtual_log.observe_membership().await {
        Ok(observed) => observed,
        Err(VirtualLogError::Uninitialized) => {
            return AuthorityGateDecision::Denied {
                reason: AuthorityGateDenial::FoundationUninitialized,
            };
        }
        Err(error) => {
            return AuthorityGateDecision::Denied {
                reason: AuthorityGateDenial::FoundationUnavailable {
                    message: error.to_string(),
                },
            };
        }
    };

    let record = match observe_root_authority(&observed.state) {
        RootAuthority::Uninitialized => {
            return AuthorityGateDecision::Denied {
                reason: AuthorityGateDenial::FoundationUninitialized,
            };
        }
        RootAuthority::AbsentOrMalformed { message } => {
            return AuthorityGateDecision::Denied {
                reason: match message {
                    None => AuthorityGateDenial::AuthorityAbsent,
                    Some(message) => AuthorityGateDenial::AuthorityMalformed { message },
                },
            };
        }
        RootAuthority::Record(record) => *record,
    };

    if record.key != key {
        return AuthorityGateDecision::Denied {
            reason: AuthorityGateDenial::AuthorityMalformed {
                message: "root fence AuthorityKey does not match gate key".into(),
            },
        };
    }

    if record.is_effective_writer(&observed.state, local_owner_id, is_writable, is_sealed) {
        AuthorityGateDecision::EffectiveWriter { record }
    } else {
        AuthorityGateDecision::Denied {
            reason: AuthorityGateDenial::NotEffectiveWriter {
                state: match record.state {
                    AuthorityState::Unassigned => "Unassigned",
                    AuthorityState::Transitioning { .. } => "Transitioning",
                    AuthorityState::Serving { .. } => "Serving",
                    AuthorityState::ReconciliationRequired { .. } => "ReconciliationRequired",
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use holylog::provision::InMemoryExclusiveClaimStore;
    use holylog::virtual_log::{ConditionalRegister, InMemoryConditionalRegister};
    use scripture::serving_authority::{AuthorityKey, WriterTerm};
    use scripture::{JournalId, OwnerEndpoint, OwnerId, VerseId};

    use crate::holylog_foundation::HolylogJournalFoundation;
    use crate::node::{NodeIdentity, ProcessLogletResolver, SharedMemoryPartsFactory};
    use crate::{AuthorityGateDecision, AuthorityGateDenial, evaluate_authority_gate};

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"gate-owner-aaa!!")
    }

    fn key() -> AuthorityKey {
        AuthorityKey {
            journal_id: JournalId::from_bytes(*b"gate-journal!!!!"),
            verse_id: VerseId::from_bytes(*b"gate-verse!!!!!!"),
        }
    }

    #[tokio::test]
    async fn gate_denies_absent_authority_even_with_foundation() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let resolver = Arc::new(ProcessLogletResolver::default());
        // Empty register: no bootstrap → uninitialized.
        let decision = evaluate_authority_gate(
            key(),
            Arc::clone(&register) as Arc<dyn ConditionalRegister>,
            Arc::clone(&resolver) as Arc<dyn holylog::virtual_log::LogletResolver>,
            owner_a(),
            true,
            false,
        )
        .await;
        assert!(matches!(
            decision,
            AuthorityGateDecision::Denied {
                reason: AuthorityGateDenial::FoundationUninitialized
            }
        ));
    }

    #[tokio::test]
    async fn gate_allows_only_matching_serving_record() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let resolver = Arc::new(ProcessLogletResolver::default());
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let claims = Arc::new(InMemoryExclusiveClaimStore::new());
        let foundation = HolylogJournalFoundation::with_default_loglet_ids(
            key(),
            NodeIdentity {
                owner_id: owner_a(),
                endpoint: OwnerEndpoint::new("tcp://owner-a:9000").expect("ep"),
            },
            Arc::clone(&register) as Arc<dyn ConditionalRegister>,
            Arc::clone(&resolver),
            Arc::clone(&parts) as Arc<dyn crate::PartsFactory>,
            Arc::clone(&claims) as Arc<dyn holylog::provision::ExclusiveClaimStore>,
            2,
        );
        let term = WriterTerm::new(1).expect("term");
        let _generation = foundation
            .bootstrap_foundation_serving(term)
            .await
            .expect("bootstrap foundation publishes Serving fence");

        let ok = evaluate_authority_gate(
            key(),
            Arc::clone(&register) as Arc<dyn ConditionalRegister>,
            Arc::clone(&resolver) as Arc<dyn holylog::virtual_log::LogletResolver>,
            owner_a(),
            true,
            false,
        )
        .await;
        assert!(matches!(ok, AuthorityGateDecision::EffectiveWriter { .. }));

        let sealed = evaluate_authority_gate(
            key(),
            Arc::clone(&register) as Arc<dyn ConditionalRegister>,
            Arc::clone(&resolver) as Arc<dyn holylog::virtual_log::LogletResolver>,
            owner_a(),
            true,
            true,
        )
        .await;
        assert!(matches!(
            sealed,
            AuthorityGateDecision::Denied {
                reason: AuthorityGateDenial::NotEffectiveWriter { .. }
            }
        ));
    }
}
