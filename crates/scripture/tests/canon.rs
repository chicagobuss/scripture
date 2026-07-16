use holylog::remote_sequencer::SequencerEpoch;
use holylog::virtual_log::{ApplicationFence, GenerationDescriptor, LogletId, VirtualLogState};
use scripture::{
    CanonFence, CanonFenceError, CanonOwner, JournalId, OwnedSequencerBinding, OwnerEndpoint,
    OwnerId, VerseId,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"canon-journal-id")
}

fn verse() -> VerseId {
    VerseId::from_bytes(*b"canon-line-id!!!")
}

fn endpoint() -> OwnerEndpoint {
    OwnerEndpoint::new("tcp://scripture-a.internal:9000").expect("endpoint")
}

/// Explicit legacy v1 Owned (no remote sequencer binding).
fn owner_v1_legacy() -> CanonOwner {
    CanonOwner::Owned {
        owner_id: OwnerId::from_bytes(*b"canon-owner-id!!"),
        endpoint: endpoint(),
        sequencer: None,
        writer_term: None,
    }
}

fn owner_v2(epoch: SequencerEpoch, sequencer_endpoint: OwnerEndpoint) -> CanonOwner {
    CanonOwner::Owned {
        owner_id: OwnerId::from_bytes(*b"canon-owner-id!!"),
        endpoint: endpoint(),
        sequencer: Some(OwnedSequencerBinding {
            epoch,
            sequencer_endpoint,
        }),
        writer_term: None,
    }
}

#[test]
fn canon_fence_v1_round_trips_to_deterministic_opaque_bytes() {
    let fence = CanonFence::new(7, journal(), verse(), owner_v1_legacy());
    let encoded = fence.encode();
    assert_eq!(encoded, fence.encode());
    assert_eq!(CanonFence::decode(&encoded).expect("decode"), fence);
    assert!(!fence.allows_remote_sequencer());
}

#[test]
fn canon_fence_v1_known_bytes_remain_stable_across_verse_rename() {
    // Golden vector pinned before the Line→Verse source rename. Field order is
    // positional: magic, version, revision, journal_id, verse_id (former
    // line_id slot), owner tag, owner_id, endpoint length, endpoint bytes.
    let fence = CanonFence::new(7, journal(), verse(), owner_v1_legacy());
    let encoded = fence.encode();
    let bytes = encoded.as_bytes();
    assert_eq!(&bytes[0..4], b"SCNF");
    assert_eq!(bytes[4], 1);
    assert_eq!(&bytes[5..13], &7u64.to_be_bytes());
    assert_eq!(&bytes[13..29], journal().as_bytes());
    assert_eq!(&bytes[29..45], verse().as_bytes());
    assert_eq!(bytes[45], 1); // Owned
    assert_eq!(
        &bytes[46..62],
        OwnerId::from_bytes(*b"canon-owner-id!!").as_bytes()
    );
    let endpoint_bytes = b"tcp://scripture-a.internal:9000";
    assert_eq!(
        u16::from_be_bytes([bytes[62], bytes[63]]) as usize,
        endpoint_bytes.len()
    );
    assert_eq!(&bytes[64..64 + endpoint_bytes.len()], endpoint_bytes);
    assert_eq!(CanonFence::decode(&encoded).expect("decode"), fence);
    assert!(!fence.allows_remote_sequencer());
}

#[test]
fn canon_fence_v2_round_trips_with_equal_endpoints() {
    let ep = endpoint();
    let fence = CanonFence::new(
        11,
        journal(),
        verse(),
        owner_v2(SequencerEpoch::test(3), ep.clone()),
    );
    let encoded = fence.encode();
    assert_eq!(encoded, fence.encode());
    assert_eq!(CanonFence::decode(&encoded).expect("decode"), fence);
    assert!(fence.allows_remote_sequencer());
    assert_eq!(encoded.as_bytes()[4], 2);
}

#[test]
fn canon_fence_v2_round_trips_with_distinct_sequencer_endpoint() {
    let sequencer_endpoint =
        OwnerEndpoint::new("tcp://sequencer.internal:9100").expect("sequencer endpoint");
    let fence = CanonFence::new(
        12,
        journal(),
        verse(),
        owner_v2(SequencerEpoch::test(4), sequencer_endpoint.clone()),
    );
    let encoded = fence.encode();
    assert_eq!(CanonFence::decode(&encoded).expect("decode"), fence);
    assert!(fence.allows_remote_sequencer());
}

#[test]
fn canon_fence_v2_known_bytes_remain_stable() {
    let ep = endpoint();
    let epoch = SequencerEpoch::test(5);
    let fence = CanonFence::new(13, journal(), verse(), owner_v2(epoch, ep.clone()));
    let encoded = fence.encode();
    let bytes = encoded.as_bytes();
    assert_eq!(&bytes[0..4], b"SCNF");
    assert_eq!(bytes[4], 2);
    assert_eq!(&bytes[5..13], &13u64.to_be_bytes());
    let owner_endpoint = b"tcp://scripture-a.internal:9000";
    let owner_start = 64;
    assert_eq!(
        &bytes[owner_start..owner_start + owner_endpoint.len()],
        owner_endpoint
    );
    let epoch_start = owner_start + owner_endpoint.len();
    assert_eq!(&bytes[epoch_start..epoch_start + 16], &epoch.as_bytes());
    let seq_len_start = epoch_start + 16;
    assert_eq!(
        u16::from_be_bytes([bytes[seq_len_start], bytes[seq_len_start + 1]]) as usize,
        owner_endpoint.len()
    );
    assert_eq!(CanonFence::decode(&encoded).expect("decode"), fence);
}

#[test]
fn legacy_v1_owned_cannot_allow_remote_sequencer() {
    let fence = CanonFence::new(1, journal(), verse(), owner_v1_legacy());
    assert!(!fence.allows_remote_sequencer());
}

#[test]
fn unowned_is_an_explicit_recovery_or_drain_state() {
    let fence = CanonFence::new(9, journal(), verse(), CanonOwner::Unowned);
    assert_eq!(CanonFence::decode(&fence.encode()).expect("decode"), fence);
    assert!(!fence.allows_remote_sequencer());
}

#[test]
fn canonical_decoder_rejects_bad_magic_truncation_and_trailing_bytes() {
    let encoded = CanonFence::new(1, journal(), verse(), owner_v1_legacy()).encode();

    let mut bad_magic = encoded.as_bytes().to_vec();
    bad_magic[0] ^= 1;
    assert_eq!(
        CanonFence::decode(&ApplicationFence::new(bad_magic)),
        Err(CanonFenceError::BadMagic)
    );

    assert_eq!(
        CanonFence::decode(&ApplicationFence::new(encoded.as_bytes()[..12].to_vec())),
        Err(CanonFenceError::Truncated)
    );

    let mut trailing = encoded.as_bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        CanonFence::decode(&ApplicationFence::new(trailing)),
        Err(CanonFenceError::TrailingBytes)
    );
}

#[test]
fn fence_must_name_the_enclosing_virtual_log_revision() {
    let state = VirtualLogState {
        revision: 4,
        generations: vec![GenerationDescriptor {
            loglet_id: LogletId::new("canon-state-loglet").expect("loglet"),
            start: 0,
        }],
        application_fence: CanonFence::new(3, journal(), verse(), owner_v1_legacy()).encode(),
    };
    assert_eq!(
        CanonFence::from_virtual_log_state(&state),
        Err(CanonFenceError::RevisionMismatch {
            fence_revision: 3,
            state_revision: 4,
        })
    );
}

#[test]
fn endpoints_stay_compact_and_log_safe() {
    assert!(matches!(
        OwnerEndpoint::new(""),
        Err(CanonFenceError::EmptyEndpoint)
    ));
    assert!(matches!(
        OwnerEndpoint::new("tcp://bad\nendpoint"),
        Err(CanonFenceError::ControlCharacterInEndpoint)
    ));
    assert!(matches!(
        OwnerEndpoint::new("x".repeat(1025)),
        Err(CanonFenceError::EndpointTooLong { .. })
    ));
}

#[test]
fn observe_rejects_malformed_application_fence_bytes() {
    use futures::executor::block_on;
    use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
    use holylog::drive::LogDrive;
    use holylog::memory::InMemoryLogDrive;
    use holylog::provision::{
        BindTag, InMemoryExclusiveClaimStore, LogletComponents, LogletObjectNamespaces,
        ProvisionAuthority, ProvisionerId, ResolvedLoglet,
    };
    use holylog::virtual_log::{
        ApplicationFence, ConditionalRegister, InMemoryConditionalRegister, LogletId,
        LogletResolver, ResolveFuture, VirtualLog,
    };
    use scripture::{CanonAuthorityError, observe_canon_authority};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Resolver {
        loglets: Mutex<BTreeMap<LogletId, ResolvedLoglet>>,
    }
    impl LogletResolver for Resolver {
        fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<ResolvedLoglet>> {
            let id = id.clone();
            Box::pin(async move { Ok(self.loglets.lock().expect("lock").get(&id).cloned()) })
        }
    }

    block_on(async {
        let resolver = Arc::new(Resolver::default());
        let id = LogletId::new("malformed-fence-loglet").expect("id");
        let bind = BindTag::new(id.as_str().as_bytes().to_vec());
        let authority = ProvisionAuthority::new(
            Arc::new(InMemoryExclusiveClaimStore::new()),
            ProvisionerId::new("canon-malformed"),
        );
        let (receipt, writable) = authority
            .provision_fresh(
                id.clone(),
                LogletObjectNamespaces::under_root("scripture-canon-tests", &id),
                bind.clone(),
                LogletComponents::new(
                    Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
                    Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
                    Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
                    0,
                ),
            )
            .await
            .expect("provision");
        let writable = Arc::new(writable);
        resolver
            .loglets
            .lock()
            .expect("lock")
            .insert(id.clone(), ResolvedLoglet::Writable(Arc::clone(&writable)));
        let log = VirtualLog::new(
            Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
            Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        );
        log.bootstrap_with_receipt(
            receipt,
            writable.as_ref(),
            &bind,
            ApplicationFence::new(b"not-a-canon-fence".to_vec()),
        )
        .await
        .expect("bootstrap");
        assert!(matches!(
            observe_canon_authority(
                &log,
                journal(),
                verse(),
                OwnerId::from_bytes(*b"canon-owner-id!!")
            )
            .await,
            Err(CanonAuthorityError::Fence(CanonFenceError::BadMagic))
        ));
    });
}

#[test]
fn witnessed_authority_rejects_mismatched_fence_and_observation() {
    use holylog::virtual_log::{CompareToken, VersionedState};
    use scripture::{CanonAuthorityError, WitnessedCanonAuthority};

    let observed_fence = CanonFence::new(1, journal(), verse(), owner_v1_legacy());
    let claimed_fence = CanonFence::new(
        1,
        journal(),
        verse(),
        CanonOwner::Owned {
            owner_id: OwnerId::from_bytes(*b"other-owner-id!!"),
            endpoint: OwnerEndpoint::new("tcp://other.internal:9000").expect("endpoint"),
            sequencer: None,
            writer_term: None,
        },
    );
    let authority = WitnessedCanonAuthority::from_parts_for_test(
        VersionedState {
            token: CompareToken::from_revision(1),
            state: VirtualLogState {
                revision: 1,
                generations: vec![GenerationDescriptor {
                    loglet_id: LogletId::new("witness-check").expect("id"),
                    start: 0,
                }],
                application_fence: observed_fence.encode(),
            },
        },
        claimed_fence,
    );
    assert!(matches!(
        authority.validate(),
        Err(CanonAuthorityError::InconsistentWitness)
    ));
}
