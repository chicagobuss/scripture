use holylog::virtual_log::{ApplicationFence, GenerationDescriptor, LogletId, VirtualLogState};
use scripture::{
    CanonFence, CanonFenceError, CanonOwner, JournalId, OwnerEndpoint, OwnerId, VerseId,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"canon-journal-id")
}

fn verse() -> VerseId {
    VerseId::from_bytes(*b"canon-line-id!!!")
}

fn owner() -> CanonOwner {
    CanonOwner::Owned {
        owner_id: OwnerId::from_bytes(*b"canon-owner-id!!"),
        endpoint: OwnerEndpoint::new("tcp://scripture-a.internal:9000").expect("endpoint"),
    }
}

#[test]
fn canon_fence_round_trips_to_deterministic_opaque_bytes() {
    let fence = CanonFence::new(7, journal(), verse(), owner());
    let encoded = fence.encode();
    assert_eq!(encoded, fence.encode());
    assert_eq!(CanonFence::decode(&encoded).expect("decode"), fence);
}

#[test]
fn canon_fence_known_bytes_remain_stable_across_verse_rename() {
    // Golden vector pinned before the Line→Verse source rename. Field order is
    // positional: magic, version, revision, journal_id, verse_id (former
    // line_id slot), owner tag, owner_id, endpoint length, endpoint bytes.
    let fence = CanonFence::new(7, journal(), verse(), owner());
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
    let endpoint = b"tcp://scripture-a.internal:9000";
    assert_eq!(
        u16::from_be_bytes([bytes[62], bytes[63]]) as usize,
        endpoint.len()
    );
    assert_eq!(&bytes[64..64 + endpoint.len()], endpoint);
    assert_eq!(CanonFence::decode(&encoded).expect("decode"), fence);
}

#[test]
fn unowned_is_an_explicit_recovery_or_drain_state() {
    let fence = CanonFence::new(9, journal(), verse(), CanonOwner::Unowned);
    assert_eq!(CanonFence::decode(&fence.encode()).expect("decode"), fence);
}

#[test]
fn canonical_decoder_rejects_bad_magic_truncation_and_trailing_bytes() {
    let encoded = CanonFence::new(1, journal(), verse(), owner()).encode();

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
        application_fence: CanonFence::new(3, journal(), verse(), owner()).encode(),
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
    use holylog::atomic::AtomicLog;
    use holylog::memory::InMemoryLogDrive;
    use holylog::virtual_log::{
        ConditionalRegister, InMemoryConditionalRegister, LogletResolver, ResolveFuture, VirtualLog,
    };
    use scripture::{CanonAuthorityError, observe_canon_authority};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Resolver {
        loglets: Mutex<BTreeMap<LogletId, Arc<AtomicLog>>>,
    }
    impl LogletResolver for Resolver {
        fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<Arc<AtomicLog>>> {
            let id = id.clone();
            Box::pin(async move { Ok(self.loglets.lock().expect("lock").get(&id).cloned()) })
        }
    }

    block_on(async {
        let resolver = Arc::new(Resolver::default());
        let id = LogletId::new("malformed-fence-loglet").expect("id");
        resolver.loglets.lock().expect("lock").insert(
            id.clone(),
            Arc::new(
                AtomicLog::builder(Arc::new(InMemoryLogDrive::new()), 0)
                    .build()
                    .expect("log"),
            ),
        );
        let log = VirtualLog::new(
            Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
            resolver as Arc<dyn LogletResolver>,
        );
        log.bootstrap_with_application_fence(
            id,
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

    let observed_fence = CanonFence::new(1, journal(), verse(), owner());
    let claimed_fence = CanonFence::new(
        1,
        journal(),
        verse(),
        CanonOwner::Owned {
            owner_id: OwnerId::from_bytes(*b"other-owner-id!!"),
            endpoint: OwnerEndpoint::new("tcp://other.internal:9000").expect("endpoint"),
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
