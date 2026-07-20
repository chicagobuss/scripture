//! Family 19 — malformed identity / integrity rejection on the real HA path.
//!
//! Each case uses product codecs (`ServingAuthorityRecord` encode/decode) and
//! live `bootstrap_and_serve` / `promote_and_serve` / `evaluate_authority_gate`
//! seams. No hand-written second parser. Memory Holylog only — not live fleet.

use std::sync::Arc;

use bytes::Bytes;
use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
use holylog::virtual_log::{
    ApplicationFence, CompareToken, ConditionalRegister, FenceUpdate, InMemoryConditionalRegister,
    LogletId, LogletResolver, VersionedState, VirtualLog,
};
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, JournalGenerationRef, RouteHint, ServingAuthorityRecord,
    WriterAuthority, WriterTerm,
};
use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound,
    Submission, SystemClock, SystemTimer, VerseId, WriterId,
};
use scripture_runtime::{
    AuthorityGateDecision, AuthorityGateDenial, HolylogJournalFoundation, NodeIdentity,
    ProcessLogletResolver, SharedMemoryPartsFactory, bootstrap_and_serve, evaluate_authority_gate,
    promote_and_serve,
};
use scripture_service::{
    AuthorityCoordinator, DeterministicTransitionIdGenerator, JournalFoundationTransition,
    VerseRuntimeConfig,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"malform-journal!")
}
fn verse() -> VerseId {
    VerseId::from_bytes(*b"malform-verse!!!")
}
fn owner_a() -> OwnerId {
    OwnerId::from_bytes(*b"malform-owner-a!")
}
fn owner_b() -> OwnerId {
    OwnerId::from_bytes(*b"malform-owner-b!")
}
fn key() -> AuthorityKey {
    AuthorityKey {
        journal_id: journal(),
        verse_id: verse(),
    }
}

fn runtime_config(owner: OwnerId) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: journal(),
        verse_id: verse(),
        owner_id: owner,
        cohort_id: CohortId::from_bytes(*b"malform-cohort!!"),
        writer_id: WriterId::from_bytes(*b"malform-writer!!"),
        policy: ChunkPolicy {
            max_chunk_bytes: 64 * 1024,
            max_record_bytes: 16 * 1024,
            max_chunk_records: 8,
            max_chunk_age: std::time::Duration::from_secs(60),
            max_buffered_bytes: 64 * 1024,
            max_inflight_chunks: 1,
            max_uncommitted_age: std::time::Duration::from_secs(60),
            recovery_scan: RecoveryBound::new(8).expect("bound"),
        },
        recovery_bound: RecoveryBound::new(8).expect("bound"),
        queue_capacity: 16,
        dataref_blobs: None,
        blob_sink: None,
        blob_verse_key: None,
    }
}

struct NodeBundle {
    resolver: Arc<ProcessLogletResolver>,
    foundation: Arc<HolylogJournalFoundation>,
    coordinator: AuthorityCoordinator,
}

fn build_node(
    owner: OwnerId,
    endpoint: &str,
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<SharedMemoryPartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
) -> NodeBundle {
    let identity = NodeIdentity {
        owner_id: owner,
        endpoint: OwnerEndpoint::new(endpoint).expect("ep"),
    };
    let resolver = Arc::new(ProcessLogletResolver::default());
    let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
        key(),
        identity,
        Arc::clone(&register),
        Arc::clone(&resolver),
        Arc::clone(&parts) as Arc<dyn scripture_runtime::PartsFactory>,
        Arc::clone(&claims),
        2,
    ));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
        Arc::new(DeterministicTransitionIdGenerator::new()),
        owner,
        RouteHint::new(endpoint).expect("route"),
    );
    NodeBundle {
        resolver,
        foundation,
        coordinator,
    }
}

async fn bootstrap_a(
    register: &Arc<dyn ConditionalRegister>,
    parts: &Arc<SharedMemoryPartsFactory>,
    claims: &Arc<dyn ExclusiveClaimStore>,
) -> (
    NodeBundle,
    scripture_runtime::HaServingSession,
    JournalGenerationRef,
) {
    let a = build_node(
        owner_a(),
        "tcp://malform-a:9000",
        Arc::clone(register),
        Arc::clone(parts),
        Arc::clone(claims),
    );
    let session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("bootstrap A");
    let generation = session.generation().clone();
    (a, session, generation)
}

async fn install_fence(
    register: &Arc<dyn ConditionalRegister>,
    resolver: Arc<dyn LogletResolver>,
    fence: ApplicationFence,
) {
    let virtual_log = VirtualLog::new(Arc::clone(register), resolver);
    let observed = virtual_log
        .observe_membership()
        .await
        .expect("observe before fence install");
    assert!(
        matches!(
            virtual_log
                .update_application_fence(&observed, fence)
                .await
                .expect("fence update"),
            FenceUpdate::Applied { .. }
        ),
        "hostile fence install must apply"
    );
}

fn assert_malformed(decision: &AuthorityGateDecision) {
    assert!(
        matches!(
            decision,
            AuthorityGateDecision::Denied {
                reason: AuthorityGateDenial::AuthorityMalformed { .. },
                ..
            }
        ),
        "expected AuthorityMalformed, got {decision:?}"
    );
}

fn assert_not_effective(decision: &AuthorityGateDecision) {
    assert!(
        matches!(decision, AuthorityGateDecision::Denied { .. }),
        "expected Denied, got {decision:?}"
    );
}

#[tokio::test]
async fn garbage_application_fence_refuses_gate_and_promote() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let (a, session, expected) = bootstrap_a(&register, &parts, &claims).await;
    let active = expected.active_loglet_id.clone();
    drop(session);
    a.resolver.remove(&active);

    install_fence(
        &register,
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        ApplicationFence::new(b"not-a-serving-authority-fence".to_vec()),
    )
    .await;

    let gate = evaluate_authority_gate(
        key(),
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner_a(),
        true,
        false,
    )
    .await;
    assert_malformed(&gate);

    let b = build_node(
        owner_b(),
        "tcp://malform-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let promote = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        key(),
        WriterTerm::new(2).expect("t2"),
        expected,
        runtime_config(owner_b()),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await;
    assert!(
        promote.is_err(),
        "promote must fail-closed on garbage root fence"
    );
}

#[tokio::test]
async fn trailing_fence_bytes_are_malformed_and_emit_no_ack() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let (a, session, expected) = bootstrap_a(&register, &parts, &claims).await;
    let active = expected.active_loglet_id.clone();

    let virtual_log = VirtualLog::new(
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
    );
    let observed = virtual_log
        .observe_membership()
        .await
        .expect("observe live Serving");
    let mut trailing = observed.state.application_fence.as_bytes().to_vec();
    trailing.extend_from_slice(b"EXTRA");
    drop(session);
    a.resolver.remove(&active);

    install_fence(
        &register,
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        ApplicationFence::new(trailing),
    )
    .await;

    let gate = evaluate_authority_gate(
        key(),
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner_a(),
        true,
        false,
    )
    .await;
    assert_malformed(&gate);

    let observed = a
        .coordinator
        .observe_root_authority()
        .await
        .expect("observe root");
    assert!(
        matches!(
            observed,
            scripture_service::ObservedRootAuthority::AbsentOrMalformed { .. }
        ),
        "coordinator must treat trailing fence as AbsentOrMalformed, got {observed:?}"
    );

    let b = build_node(
        owner_b(),
        "tcp://malform-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let promote = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        key(),
        WriterTerm::new(2).expect("t2"),
        expected,
        runtime_config(owner_b()),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await;
    assert!(
        promote.is_err(),
        "promote must fail-closed on trailing-byte fence"
    );
}

#[tokio::test]
async fn fence_identity_mismatch_denies_effective_writer_and_acks() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let (a, session, expected) = bootstrap_a(&register, &parts, &claims).await;
    let active = expected.active_loglet_id.clone();
    drop(session);
    a.resolver.remove(&active);

    let wrong_key = AuthorityKey {
        journal_id: JournalId::from_bytes(*b"wrong-journal-id"),
        verse_id: verse(),
    };
    let hostile = ServingAuthorityRecord::new(
        wrong_key,
        AuthorityState::Serving {
            authority: WriterAuthority {
                owner_id: owner_a(),
                writer_term: WriterTerm::new(1).expect("t1"),
                generation_ref: expected.clone(),
            },
            route_hint: RouteHint::new("tcp://malform-a:9000").expect("route"),
        },
    );
    install_fence(
        &register,
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        hostile.encode_application_fence().expect("encode"),
    )
    .await;

    let gate = evaluate_authority_gate(
        key(),
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner_a(),
        true,
        false,
    )
    .await;
    assert_not_effective(&gate);

    let wrong_verse = AuthorityKey {
        journal_id: journal(),
        verse_id: VerseId::from_bytes(*b"wrong-verse-id!!"),
    };
    let wrong_verse_record = ServingAuthorityRecord::new(
        wrong_verse,
        AuthorityState::Serving {
            authority: WriterAuthority {
                owner_id: owner_a(),
                writer_term: WriterTerm::new(1).expect("t1"),
                generation_ref: expected,
            },
            route_hint: RouteHint::new("tcp://malform-a:9000").expect("route"),
        },
    );
    install_fence(
        &register,
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        wrong_verse_record
            .encode_application_fence()
            .expect("encode"),
    )
    .await;
    let gate_verse = evaluate_authority_gate(
        key(),
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner_a(),
        true,
        false,
    )
    .await;
    assert_not_effective(&gate_verse);
}

#[tokio::test]
async fn wrong_generation_digest_denies_activation() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let (a, session, expected) = bootstrap_a(&register, &parts, &claims).await;
    let active = expected.active_loglet_id.clone();
    drop(session);
    a.resolver.remove(&active);

    let mut hostile_gen = expected.clone();
    hostile_gen.canon_fence_digest = [0xAB; 32];
    let hostile = ServingAuthorityRecord::new(
        key(),
        AuthorityState::Serving {
            authority: WriterAuthority {
                owner_id: owner_a(),
                writer_term: WriterTerm::new(1).expect("t1"),
                generation_ref: hostile_gen,
            },
            route_hint: RouteHint::new("tcp://malform-a:9000").expect("route"),
        },
    );
    install_fence(
        &register,
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        hostile.encode_application_fence().expect("encode"),
    )
    .await;

    let gate = evaluate_authority_gate(
        key(),
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner_a(),
        true,
        false,
    )
    .await;
    assert_not_effective(&gate);

    // Wrong active loglet identity in the fence vs membership.
    let wrong_loglet = ServingAuthorityRecord::new(
        key(),
        AuthorityState::Serving {
            authority: WriterAuthority {
                owner_id: owner_a(),
                writer_term: WriterTerm::new(1).expect("t1"),
                generation_ref: JournalGenerationRef::from_active_generation(
                    expected.virtual_log_revision,
                    LogletId::new("not-the-active-loglet").expect("id"),
                    expected.active_start,
                ),
            },
            route_hint: RouteHint::new("tcp://malform-a:9000").expect("route"),
        },
    );
    install_fence(
        &register,
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        wrong_loglet.encode_application_fence().expect("encode"),
    )
    .await;
    let gate_loglet = evaluate_authority_gate(
        key(),
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner_a(),
        true,
        false,
    )
    .await;
    assert_not_effective(&gate_loglet);
}

#[tokio::test]
async fn stale_root_compare_witness_cannot_overwrite() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let (a, session, expected) = bootstrap_a(&register, &parts, &claims).await;
    let _ = session;

    let live = register
        .read()
        .await
        .expect("read live root")
        .expect("live root present");
    let live_revision = live.state.revision;

    let stale_expected = VersionedState {
        state: live.state.clone(),
        token: CompareToken::new("0|\"stale-not-live-etag\""),
    };
    let mut hostile = live.state.clone();
    hostile.revision = live_revision.saturating_add(1);
    hostile.application_fence = ApplicationFence::new(b"stolen-root".to_vec());
    let applied = register
        .compare_and_swap(Some(&stale_expected), hostile)
        .await
        .expect("stale CAS attempt");
    assert!(!applied, "stale CompareToken must not apply");

    let after = register
        .read()
        .await
        .expect("reread")
        .expect("root retained");
    assert_eq!(after.state.revision, live_revision);
    assert_ne!(
        after.state.application_fence.as_bytes(),
        b"stolen-root",
        "hostile fence must not land"
    );

    // Live Serving still admits for the rightful owner.
    let gate = evaluate_authority_gate(
        key(),
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner_a(),
        a.resolver.is_writable(&expected.active_loglet_id),
        false,
    )
    .await;
    assert!(matches!(
        gate,
        AuthorityGateDecision::EffectiveWriter { .. }
    ));
}

#[tokio::test]
async fn wrong_owner_serving_fence_emits_no_committed_ack() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let (a, session, expected) = bootstrap_a(&register, &parts, &claims).await;
    let active = expected.active_loglet_id.clone();

    // Replace fence with Serving naming owner B while A still holds the writable.
    let hostile = ServingAuthorityRecord::new(
        key(),
        AuthorityState::Serving {
            authority: WriterAuthority {
                owner_id: owner_b(),
                writer_term: WriterTerm::new(2).expect("t2"),
                generation_ref: expected.clone(),
            },
            route_hint: RouteHint::new("tcp://malform-b:9000").expect("route"),
        },
    );
    install_fence(
        &register,
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        hostile.encode_application_fence().expect("encode"),
    )
    .await;

    assert!(
        !session.is_effective_writer().await,
        "wrong-owner Serving fence must revoke A's effective writer"
    );
    let submit = session
        .submit(Submission {
            producer_id: ProducerId::from_bytes(*b"malform-producer"),
            producer_epoch: 0,
            sequence: 0,
            records: vec![Record::new([], Bytes::from_static(b"must-not-ack"))],
        })
        .await;
    assert!(
        submit.is_err(),
        "wrong-owner fence must refuse admission (no committed ACK path)"
    );

    let b = build_node(
        owner_b(),
        "tcp://malform-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    // B sees Serving-for-B bytes but generation/membership may not bind without
    // a lawful promote — gate with B's empty writable must still deny.
    let b_gate = evaluate_authority_gate(
        key(),
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
        owner_b(),
        false,
        false,
    )
    .await;
    assert_not_effective(&b_gate);

    drop(session);
    a.resolver.remove(&active);
    let _ = a;
}
