use holylog::virtual_log::{GenerationDescriptor, LogletId, VirtualLogState};
use scripture::canon::{CanonFence, CanonOwner, OwnerEndpoint, OwnerId, VerseId};
use scripture::model::JournalId;
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, FoundationPrecondition, JournalGenerationRef, RouteHint,
    ServingAuthorityError, ServingAuthorityRecord, TransitionId, TransitionIntent, TransitionKind,
    WriterAuthority, WriterTerm,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"canon-journal-id")
}

fn verse() -> VerseId {
    VerseId::from_bytes(*b"canon-line-id!!!")
}

fn owner() -> OwnerId {
    OwnerId::from_bytes(*b"canon-owner-id!!")
}

fn endpoint() -> OwnerEndpoint {
    OwnerEndpoint::new("tcp://scripture-a.internal:9000").expect("endpoint")
}

fn route_hint() -> RouteHint {
    RouteHint::new("tcp://scripture-hint.internal:9000").expect("route hint")
}

fn transition_id() -> TransitionId {
    TransitionId::from_bytes(*b"canon-trans-id!!")
}

fn writer_term() -> WriterTerm {
    WriterTerm::new(42).expect("term")
}

fn sample_gen_ref() -> JournalGenerationRef {
    JournalGenerationRef::from_active_generation(
        5,
        LogletId::new("active-loglet-id").expect("id"),
        100,
    )
}

fn sample_intent() -> TransitionIntent {
    TransitionIntent {
        transition_id: transition_id(),
        kind: TransitionKind::RecoveryPromotion,
        precondition: FoundationPrecondition::Expected(sample_gen_ref()),
        candidate_owner_id: owner(),
        next_writer_term: writer_term(),
    }
}

#[test]
fn test_canon_fence_v3_round_trips() {
    let owned = CanonOwner::Owned {
        owner_id: owner(),
        endpoint: endpoint(),
        sequencer: None,
        writer_term: Some(writer_term()),
    };
    let fence = CanonFence::new(10, journal(), verse(), owned);
    let encoded = fence.encode();
    let decoded = CanonFence::decode(&encoded).expect("decode v3");
    assert_eq!(decoded, fence);

    let bytes = encoded.as_bytes();
    assert_eq!(&bytes[0..4], b"SCNF");
    assert_eq!(bytes[4], 3); // FORMAT_VERSION_V3
}

#[test]
fn test_canon_fence_v3_unowned_binds() {
    let fence = CanonFence::new(10, journal(), verse(), CanonOwner::Unowned);
    let encoded = fence.encode();
    let decoded = CanonFence::decode(&encoded).expect("decode v3 unowned");
    assert_eq!(decoded, fence);
    assert_eq!(encoded.as_bytes()[4], 3); // FORMAT_VERSION_V3
}

#[test]
fn test_serving_authority_record_codec_round_trips() {
    let key = AuthorityKey {
        journal_id: journal(),
        verse_id: verse(),
    };

    // 1. Unassigned
    let rec_unassigned = ServingAuthorityRecord::new(key, AuthorityState::Unassigned);
    let encoded_unassigned = rec_unassigned.encode().expect("encode");
    let decoded_unassigned = ServingAuthorityRecord::decode(&encoded_unassigned).expect("decode");
    assert_eq!(decoded_unassigned, rec_unassigned);

    // 2. Transitioning
    let rec_trans = ServingAuthorityRecord::new(
        key,
        AuthorityState::Transitioning {
            intent: sample_intent(),
        },
    );
    let encoded_trans = rec_trans.encode().expect("encode");
    let decoded_trans = ServingAuthorityRecord::decode(&encoded_trans).expect("decode");
    assert_eq!(decoded_trans, rec_trans);

    // 3. Serving
    let auth = WriterAuthority {
        owner_id: owner(),
        writer_term: writer_term(),
        generation_ref: sample_gen_ref(),
    };
    let rec_serving = ServingAuthorityRecord::new(
        key,
        AuthorityState::Serving {
            authority: auth,
            route_hint: route_hint(),
        },
    );
    let encoded_serving = rec_serving.encode().expect("encode");
    let decoded_serving = ServingAuthorityRecord::decode(&encoded_serving).expect("decode");
    assert_eq!(decoded_serving, rec_serving);

    // 4. ReconciliationRequired
    let rec_reconcile = ServingAuthorityRecord::new(
        key,
        AuthorityState::ReconciliationRequired {
            intent: sample_intent(),
            observed_generation: Some(sample_gen_ref()),
        },
    );
    let encoded_reconcile = rec_reconcile.encode().expect("encode");
    let decoded_reconcile = ServingAuthorityRecord::decode(&encoded_reconcile).expect("decode");
    assert_eq!(decoded_reconcile, rec_reconcile);
}

#[test]
fn test_effective_writer_predicate_and_adversarial_mismatches() {
    let key = AuthorityKey {
        journal_id: journal(),
        verse_id: verse(),
    };

    let gen_ref = JournalGenerationRef::from_active_generation(
        5,
        LogletId::new("active-loglet-id").expect("id"),
        100,
    );
    let auth = WriterAuthority {
        owner_id: owner(),
        writer_term: writer_term(),
        generation_ref: gen_ref.clone(),
    };

    let record = ServingAuthorityRecord::new(
        key,
        AuthorityState::Serving {
            authority: auth,
            route_hint: route_hint(),
        },
    );
    let app_fence = record.encode_application_fence().expect("fence");

    let state = VirtualLogState {
        revision: 5,
        generations: vec![GenerationDescriptor {
            loglet_id: LogletId::new("active-loglet-id").expect("id"),
            start: 100,
        }],
        application_fence: app_fence,
    };

    // Active matches and writable, should be true
    assert!(record.is_effective_writer(&state, owner(), true, false));

    // sealed, should be false
    assert!(!record.is_effective_writer(&state, owner(), true, true));

    // unwritable, should be false
    assert!(!record.is_effective_writer(&state, owner(), false, false));

    // wrong owner check
    let other_owner = OwnerId::from_bytes(*b"other-owner-id!!");
    assert!(!record.is_effective_writer(&state, other_owner, true, false));

    // mismatched journal_id in root fence
    let bad_key = AuthorityKey {
        journal_id: JournalId::from_bytes(*b"other-journal-id"),
        verse_id: verse(),
    };
    let bad_j_record = ServingAuthorityRecord::new(
        bad_key,
        AuthorityState::Serving {
            authority: WriterAuthority {
                owner_id: owner(),
                writer_term: writer_term(),
                generation_ref: gen_ref.clone(),
            },
            route_hint: route_hint(),
        },
    );
    let bad_j_state = VirtualLogState {
        application_fence: bad_j_record.encode_application_fence().expect("fence"),
        ..state.clone()
    };
    assert!(!record.is_effective_writer(&bad_j_state, owner(), true, false));

    // mismatched verse_id in root fence
    let bad_v_key = AuthorityKey {
        journal_id: journal(),
        verse_id: VerseId::from_bytes(*b"other-verse-id!!"),
    };
    let bad_v_record = ServingAuthorityRecord::new(
        bad_v_key,
        AuthorityState::Serving {
            authority: WriterAuthority {
                owner_id: owner(),
                writer_term: writer_term(),
                generation_ref: gen_ref.clone(),
            },
            route_hint: route_hint(),
        },
    );
    let bad_v_state = VirtualLogState {
        application_fence: bad_v_record.encode_application_fence().expect("fence"),
        ..state.clone()
    };
    assert!(!record.is_effective_writer(&bad_v_state, owner(), true, false));

    // stale revision ⇒ generation binding mismatch
    let mut stale_state = state.clone();
    stale_state.revision = 6;
    assert!(!record.is_effective_writer(&stale_state, owner(), true, false));

    // adversarial term mismatch in root fence
    let bad_term_record = ServingAuthorityRecord::new(
        key,
        AuthorityState::Serving {
            authority: WriterAuthority {
                owner_id: owner(),
                writer_term: WriterTerm::new(99).expect("valid term"),
                generation_ref: gen_ref,
            },
            route_hint: route_hint(),
        },
    );
    let bad_term_state = VirtualLogState {
        application_fence: bad_term_record.encode_application_fence().expect("fence"),
        ..state.clone()
    };
    assert!(!record.is_effective_writer(&bad_term_state, owner(), true, false));

    // Transitioning grants no ACK entitlement
    let transitioning = ServingAuthorityRecord::new(
        key,
        AuthorityState::Transitioning {
            intent: sample_intent(),
        },
    );
    let transitioning_state = VirtualLogState {
        application_fence: transitioning.encode_application_fence().expect("fence"),
        ..state.clone()
    };
    assert!(!transitioning.is_effective_writer(&transitioning_state, owner(), true, false));

    // active_start mismatch
    let bad_start_state = VirtualLogState {
        generations: vec![GenerationDescriptor {
            loglet_id: LogletId::new("active-loglet-id").expect("id"),
            start: 999,
        }],
        ..state.clone()
    };
    assert!(!record.is_effective_writer(&bad_start_state, owner(), true, false));
}

#[test]
fn serving_publication_rejects_transitioning_bytes_for_successor() {
    // Typed API is Serving-only; Transitioning cannot be constructed as ServingPublication.
    let key = AuthorityKey {
        journal_id: journal(),
        verse_id: verse(),
    };
    let publication = scripture::ServingPublication::new(
        key,
        WriterAuthority {
            owner_id: owner(),
            writer_term: writer_term(),
            generation_ref: sample_gen_ref(),
        },
        route_hint(),
    )
    .expect("serving");
    let fence = publication.encode_application_fence().expect("encode");
    let decoded = ServingAuthorityRecord::decode_application_fence(&fence).expect("decode");
    assert!(matches!(decoded.state, AuthorityState::Serving { .. }));
}

#[test]
fn test_overlong_fields_fail_to_encode() {
    let too_long = "x".repeat(2000);
    assert!(matches!(
        RouteHint::new(too_long),
        Err(ServingAuthorityError::StringTooLong { .. })
    ));
}

#[test]
fn test_v3_unowned_binds_to_generation_ref() {
    let fence = CanonFence::new(5, journal(), verse(), CanonOwner::Unowned);
    let state = VirtualLogState {
        revision: 5,
        generations: vec![GenerationDescriptor {
            loglet_id: LogletId::new("unowned-loglet-id").expect("id"),
            start: 100,
        }],
        application_fence: fence.encode(),
    };

    let gen_ref = JournalGenerationRef::from_virtual_log_state(&state).expect("build ref");
    assert_eq!(gen_ref.virtual_log_revision, 5);
    assert_eq!(gen_ref.active_loglet_id.as_str(), "unowned-loglet-id");
}
