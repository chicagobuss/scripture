//! Root authority projection — one durable source of truth.
//!
//! Scripture HA uses the VirtualLog application fence as the sole durable
//! authority record (SCAR). Legacy Canon-fence bytes are projected for local
//! recovery only when the root is not SCAR-encoded.

use holylog::virtual_log::VirtualLogState;

use crate::canon::{CanonFence, CanonFenceError};
use crate::serving_authority::{AuthorityState, ServingAuthorityError, ServingAuthorityRecord};

/// Observed root authority at the VirtualLog boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootAuthority {
    /// Register has no membership yet.
    Uninitialized,
    /// Membership exists but carries no decodable Scripture authority.
    AbsentOrMalformed {
        /// Optional decode detail.
        message: Option<String>,
    },
    /// Decoded SCAR Serving Authority record.
    Record(Box<ServingAuthorityRecord>),
}

/// Freshly decodes the root application fence.
pub fn observe_root_authority(state: &VirtualLogState) -> RootAuthority {
    if state.application_fence.as_bytes().is_empty() {
        return RootAuthority::AbsentOrMalformed { message: None };
    }
    if !state.application_fence.as_bytes().starts_with(b"SCAR") {
        return RootAuthority::AbsentOrMalformed {
            message: Some("root fence is not SCAR-encoded Serving Authority".into()),
        };
    }
    match ServingAuthorityRecord::decode_application_fence(&state.application_fence) {
        Ok(record) => RootAuthority::Record(Box::new(record)),
        Err(error) => RootAuthority::AbsentOrMalformed {
            message: Some(error.to_string()),
        },
    }
}

/// Whether the root record authorizes append admission for this process.
#[must_use]
pub fn root_permits_append(record: &ServingAuthorityRecord, local_owner: crate::canon::OwnerId) -> bool {
    matches!(
        &record.state,
        AuthorityState::Serving { authority, .. } if authority.owner_id == local_owner
    )
}

/// Projects a Canon fence for local recovery/startup from the root record.
pub fn project_canon_fence(state: &VirtualLogState) -> Result<CanonFence, CanonFenceError> {
    CanonFence::from_virtual_log_state(state)
}

/// Maps Serving Authority decode failures for projection callers.
pub fn scar_to_fence_error(error: ServingAuthorityError) -> CanonFenceError {
    use crate::serving_authority::ServingAuthorityError;
    match error {
        ServingAuthorityError::BadMagic => CanonFenceError::BadMagic,
        ServingAuthorityError::Truncated | ServingAuthorityError::TrailingBytes => {
            CanonFenceError::Truncated
        }
        ServingAuthorityError::UnsupportedVersion { actual, .. } => {
            CanonFenceError::UnsupportedVersion {
                version: u8::try_from(actual).unwrap_or(u8::MAX),
            }
        }
        ServingAuthorityError::ControlCharacterInText
        | ServingAuthorityError::StringTooLong { .. } => CanonFenceError::EmptyEndpoint,
        ServingAuthorityError::InvalidWriterTerm => CanonFenceError::InvalidWriterTerm,
        ServingAuthorityError::UnknownTag { tag } => CanonFenceError::UnknownOwnerTag { tag },
        ServingAuthorityError::InvalidLogletId { .. }
        | ServingAuthorityError::MalformedState { .. } => CanonFenceError::BadMagic,
    }
}

#[cfg(test)]
mod tests {
    use holylog::virtual_log::{ApplicationFence, LogletId, VirtualLogState};

    use crate::canon::{CanonFence, CanonOwner, OwnerId, VerseId};
    use crate::model::JournalId;
    use crate::serving_authority::{
        AuthorityKey, AuthorityState, JournalGenerationRef, RouteHint, ServingAuthorityRecord,
        WriterAuthority, WriterTerm,
    };

    use super::observe_root_authority;

    #[test]
    fn observes_scar_root_record() {
        let key = AuthorityKey {
            journal_id: JournalId::from_bytes(*b"journal-test!!!!"),
            verse_id: VerseId::from_bytes(*b"verse-test!!!!!!"),
        };
        let generation = JournalGenerationRef::from_active_generation(
            1,
            LogletId::new("g1").expect("loglet"),
            0,
        );
        let record = ServingAuthorityRecord::new(
            key,
            AuthorityState::Serving {
                authority: WriterAuthority {
                    owner_id: OwnerId::from_bytes(*b"owner-test!!!!!!"),
                    writer_term: WriterTerm::new(1).expect("term"),
                    generation_ref: generation,
                },
                route_hint: RouteHint::new("127.0.0.1:9000").expect("route"),
            },
        );
        let fence = record.encode_application_fence().expect("encode");
        let state = VirtualLogState {
            revision: 1,
            generations: vec![],
            application_fence: ApplicationFence::new(fence.as_bytes().to_vec()),
        };
        assert!(matches!(
            observe_root_authority(&state),
            super::RootAuthority::Record(_)
        ));
    }

    #[test]
    fn legacy_canon_fence_projects_as_absent_root() {
        let fence = CanonFence::new(
            1,
            JournalId::from_bytes(*b"journal-test!!!!"),
            VerseId::from_bytes(*b"verse-test!!!!!!"),
            CanonOwner::Unowned,
        );
        let state = VirtualLogState {
            revision: 1,
            generations: vec![],
            application_fence: fence.encode(),
        };
        assert!(matches!(
            observe_root_authority(&state),
            super::RootAuthority::AbsentOrMalformed { .. }
        ));
    }
}
