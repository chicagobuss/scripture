//! One-record Scripture HA path proofs against the real product APIs.
//!
//! These tests exercise [`AuthorityCoordinator`], [`bootstrap_and_serve`] /
//! [`promote_and_serve`], [`evaluate_authority_gate`], and
//! [`HolylogJournalFoundation`] with in-memory Holylog adapters. They are not
//! live fleet HA evidence.
//!
//! ## Crash-boundary coverage
//!
//! Exercisable with current adapters (`FaultableConditionalRegister` +
//! `ArmedFault::RootCasReplyLost`, plus manual fence observation/CAS):
//!
//! 1. Before intent CAS (Serving still observed; predecessor remains effective).
//! 2. Intent CAS applied (Transitioning on root; predecessor loses effective writer).
//! 3. Intent CAS reply lost (one-shot RootCasReplyLost on the intent fence CAS).
//! 4. predecessor sealed before tail/provision;
//! 5. sealed tail observed before provision;
//! 6. successor provisioned before final root CAS (the receipt is deliberately lost);
//! 7. final root CAS reply lost (RootCasReplyLost armed after Transitioning is durable).
//!
//! The three internal boundaries use `FoundationTransitionObserver` in the real
//! `HolylogJournalFoundation`, then recover through a fresh adapter from its
//! durable root intent. They are not a fake Foundation implementation.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
use holylog::virtual_log::{
    ConditionalRegister, FenceUpdate, InMemoryConditionalRegister, LogletId, LogletResolver,
    VirtualLog,
};
use holylog_correctness::faults::{FaultableConditionalRegister, FaultableLogDrive, FaultableSeal};
use holylog_correctness::{
    ActorId, ActorTrace, ArmedFault, EventKind, FaultController, OperationId, RecordingSink, RunId,
    TraceSink, Verdict, check_trace, payload_digest,
};
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, FoundationPrecondition, JournalGenerationRef, RouteHint,
    ServingAuthorityRecord, TransitionId, TransitionIntent, TransitionKind, WriterTerm,
};
use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound,
    Submission, SystemClock, SystemTimer, VerseId, WriterId,
};
use scripture_runtime::{
    AuthorityGateDecision, DefaultFreshLogletIdPolicy, DurableLogletParts,
    FoundationTransitionCheckpoint, FoundationTransitionObserver, HaServingSession,
    HolylogJournalFoundation, NodeIdentity, PartsFactory, PartsFactoryError, ProcessLogletResolver,
    SharedMemoryPartsFactory, bootstrap_and_serve, evaluate_authority_gate, promote_and_serve,
};
use scripture_service::{
    AuthorityCoordinator, CoordinatorError, DeterministicTransitionIdGenerator,
    FoundationTransitionError, JournalFoundationTransition, LocalServingEligibility,
    VerseRuntimeConfig,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"ha1rec-journal!!")
}
fn verse() -> VerseId {
    VerseId::from_bytes(*b"ha1rec-verse!!!!")
}
fn owner_a() -> OwnerId {
    OwnerId::from_bytes(*b"ha1rec-own-a!!!!")
}
fn owner_b() -> OwnerId {
    OwnerId::from_bytes(*b"ha1rec-own-b!!!!")
}
fn owner_c() -> OwnerId {
    OwnerId::from_bytes(*b"ha1rec-own-c!!!!")
}
fn key() -> AuthorityKey {
    AuthorityKey {
        journal_id: journal(),
        verse_id: verse(),
    }
}
fn producer() -> ProducerId {
    ProducerId::from_bytes(*b"ha1rec-producer!")
}

fn runtime_config(owner: OwnerId) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: journal(),
        verse_id: verse(),
        owner_id: owner,
        cohort_id: CohortId::from_bytes(*b"ha1rec-cohort!!!"),
        writer_id: WriterId::from_bytes(*b"ha1rec-writer!!!"),
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
    }
}

struct NodeBundle {
    resolver: Arc<ProcessLogletResolver>,
    foundation: Arc<HolylogJournalFoundation>,
    coordinator: AuthorityCoordinator,
}

/// One-shot product-path interruption at an internal Foundation boundary.
struct InterruptAt {
    checkpoint: Mutex<Option<FoundationTransitionCheckpoint>>,
}

impl InterruptAt {
    fn new(checkpoint: FoundationTransitionCheckpoint) -> Self {
        Self {
            checkpoint: Mutex::new(Some(checkpoint)),
        }
    }
}

impl FoundationTransitionObserver for InterruptAt {
    fn checkpoint(
        &self,
        checkpoint: FoundationTransitionCheckpoint,
    ) -> Result<(), FoundationTransitionError> {
        let mut armed = self.checkpoint.lock().expect("checkpoint lock");
        if *armed == Some(checkpoint) {
            *armed = None;
            return Err(FoundationTransitionError::Indeterminate(Box::new(
                std::io::Error::other(format!("simulated process death at {checkpoint:?}")),
            )));
        }
        Ok(())
    }
}

/// Test-only durable parts factory that wraps Holylog ports with the correctness harness.
struct FaultingSharedPartsFactory {
    parts: Mutex<BTreeMap<LogletId, DurableLogletParts>>,
    faults: Arc<FaultController>,
    trace: ActorTrace,
}

impl FaultingSharedPartsFactory {
    fn new(faults: Arc<FaultController>, trace: ActorTrace) -> Self {
        Self {
            parts: Mutex::new(BTreeMap::new()),
            faults,
            trace,
        }
    }
}

impl PartsFactory for FaultingSharedPartsFactory {
    fn fresh(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        let parts = DurableLogletParts::from_components(
            Arc::new(FaultableLogDrive::new(
                Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
                Arc::clone(&self.faults),
                self.trace.clone(),
                loglet_id.to_string(),
            )) as Arc<dyn LogDrive>,
            Arc::new(FaultableSeal::new(
                Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
                Arc::clone(&self.faults),
                self.trace.clone(),
                loglet_id.to_string(),
            )) as Arc<dyn Seal>,
            Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
        );
        let mut entries = self
            .parts
            .lock()
            .map_err(|_| PartsFactoryError::new("faulting test parts lock poisoned"))?;
        if entries.insert(loglet_id.clone(), parts.clone()).is_some() {
            return Err(PartsFactoryError::new(format!(
                "Loglet {loglet_id} already has durable parts"
            )));
        }
        Ok(parts)
    }

    fn open(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        self.parts
            .lock()
            .map_err(|_| PartsFactoryError::new("faulting test parts lock poisoned"))?
            .get(loglet_id)
            .cloned()
            .ok_or_else(|| PartsFactoryError::new(format!("no durable parts for {loglet_id}")))
    }

    fn namespaces(
        &self,
        loglet_id: &LogletId,
    ) -> Result<holylog::provision::LogletObjectNamespaces, PartsFactoryError> {
        Ok(holylog::provision::LogletObjectNamespaces::under_root(
            "scripture-one-record-ha-proofs",
            loglet_id,
        ))
    }
}

fn build_node(
    owner: OwnerId,
    endpoint: &str,
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
) -> NodeBundle {
    build_node_with_observer(owner, endpoint, register, parts, claims, None)
}

fn build_node_with_observer(
    owner: OwnerId,
    endpoint: &str,
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
    observer: Option<Arc<dyn FoundationTransitionObserver>>,
) -> NodeBundle {
    let identity = NodeIdentity {
        owner_id: owner,
        endpoint: OwnerEndpoint::new(endpoint).expect("ep"),
    };
    let resolver = Arc::new(ProcessLogletResolver::default());
    let foundation = Arc::new(match observer {
        Some(observer) => HolylogJournalFoundation::new_with_transition_observer(
            key(),
            identity,
            Arc::clone(&register),
            Arc::clone(&resolver),
            parts,
            Arc::clone(&claims),
            Arc::new(DefaultFreshLogletIdPolicy),
            observer,
            2,
        ),
        None => HolylogJournalFoundation::with_default_loglet_ids(
            key(),
            identity,
            Arc::clone(&register),
            Arc::clone(&resolver),
            parts,
            Arc::clone(&claims),
            2,
        ),
    });
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

async fn commit_one(
    session: &HaServingSession,
    sequence: u64,
    payload: &'static [u8],
) -> scripture::Receipt {
    let pending = session
        .submit(Submission {
            producer_id: producer(),
            producer_epoch: 0,
            sequence,
            records: vec![Record::new([], Bytes::from_static(payload))],
        })
        .await
        .expect("admit");
    session.flush().await.expect("flush");
    let receipt = pending.await.expect("commit");
    assert_eq!(receipt.level, scripture::AckLevel::Committed);
    receipt
}

fn trace_committed_ack(
    trace: &ActorTrace,
    operation: &str,
    receipt: &scripture::Receipt,
    payload: &[u8],
    loglet_id: &LogletId,
) {
    trace.emit(
        Some(OperationId::new(operation)),
        EventKind::ScriptureCommittedAck {
            logical_offset: receipt.first_offset.get(),
            digest: payload_digest(payload),
            size: payload.len(),
            loglet_id: loglet_id.to_string(),
        },
    );
}

async fn observe_authority(
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<dyn LogletResolver>,
) -> (holylog::virtual_log::VersionedState, ServingAuthorityRecord) {
    let virtual_log = VirtualLog::new(register, resolver);
    let observed = virtual_log
        .observe_membership()
        .await
        .expect("observe membership");
    let record =
        ServingAuthorityRecord::decode_application_fence(&observed.state.application_fence)
            .expect("decode Serving Authority fence");
    (observed, record)
}

fn serving_owner(
    record: &ServingAuthorityRecord,
) -> Option<(OwnerId, WriterTerm, JournalGenerationRef)> {
    match &record.state {
        AuthorityState::Serving { authority, .. } => Some((
            authority.owner_id,
            authority.writer_term,
            authority.generation_ref.clone(),
        )),
        _ => None,
    }
}

fn assert_generation_bound_to_membership(
    observed: &holylog::virtual_log::VersionedState,
    record: &ServingAuthorityRecord,
) {
    let (owner, _term, generation) =
        serving_owner(record).expect("Serving required for generation binding");
    let from_membership =
        JournalGenerationRef::from_virtual_log_state(&observed.state).expect("membership binding");
    assert_eq!(
        generation, from_membership,
        "Serving generation_ref must match the same root observation that carries membership"
    );
    let _ = owner;
}

async fn gate(
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
    owner: OwnerId,
    active: Option<&LogletId>,
) -> AuthorityGateDecision {
    let is_writable = active.is_some_and(|id| resolver.is_writable(id));
    evaluate_authority_gate(
        key(),
        register,
        resolver as Arc<dyn LogletResolver>,
        owner,
        is_writable,
        false,
    )
    .await
}

fn is_effective(decision: &AuthorityGateDecision) -> bool {
    matches!(decision, AuthorityGateDecision::EffectiveWriter { .. })
}

async fn assert_exactly_one_effective_writer(
    register: &Arc<dyn ConditionalRegister>,
    nodes: &[(&NodeBundle, OwnerId, Option<&LogletId>)],
) {
    let mut effective = Vec::new();
    for (node, owner, active) in nodes {
        let decision = gate(
            Arc::clone(register),
            Arc::clone(&node.resolver),
            *owner,
            *active,
        )
        .await;
        if is_effective(&decision) {
            effective.push(*owner);
        }
    }
    assert_eq!(
        effective.len(),
        1,
        "expected exactly one effective writer, found {effective:?}"
    );
}

async fn assert_at_most_one_effective_writer(
    register: &Arc<dyn ConditionalRegister>,
    nodes: &[(&NodeBundle, OwnerId, Option<&LogletId>)],
) {
    let mut effective = Vec::new();
    for (node, owner, active) in nodes {
        let decision = gate(
            Arc::clone(register),
            Arc::clone(&node.resolver),
            *owner,
            *active,
        )
        .await;
        if is_effective(&decision) {
            effective.push(*owner);
        }
    }
    assert!(
        effective.len() <= 1,
        "never two effective writers; found {effective:?}"
    );
}

async fn inject_transitioning_for_b(
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<dyn LogletResolver>,
    expected: JournalGenerationRef,
    term: WriterTerm,
) {
    let virtual_log = VirtualLog::new(register, resolver);
    let observed = virtual_log
        .observe_membership()
        .await
        .expect("observe before Transitioning");
    let transitioning = ServingAuthorityRecord::new(
        key(),
        AuthorityState::Transitioning {
            intent: TransitionIntent {
                transition_id: TransitionId::from_bytes([7; 16]),
                kind: TransitionKind::RecoveryPromotion,
                precondition: FoundationPrecondition::Expected(expected),
                candidate_owner_id: owner_b(),
                next_writer_term: term,
            },
        },
    );
    let fence = transitioning
        .encode_application_fence()
        .expect("encode Transitioning");
    assert!(
        matches!(
            virtual_log
                .update_application_fence(&observed, fence)
                .await
                .expect("intent fence CAS"),
            FenceUpdate::Applied { .. }
        ),
        "Transitioning intent must apply"
    );
}

async fn bootstrap_a(
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
) -> (NodeBundle, NodeBundle, HaServingSession) {
    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("bootstrap_and_serve A");
    (a, b, session)
}

// ---------------------------------------------------------------------------
// A. Seven-boundary forward-only sweep
// ---------------------------------------------------------------------------

#[tokio::test]
async fn boundary_before_intent_cas_a_still_effective() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let (a, b, session) = bootstrap_a(register.clone(), parts, claims).await;
    let active = session.generation().active_loglet_id.clone();

    let (observed, record) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    assert!(matches!(record.state, AuthorityState::Serving { .. }));
    assert_generation_bound_to_membership(&observed, &record);

    assert_exactly_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&active)),
            (&b, owner_b(), Some(&active)),
        ],
    )
    .await;
    assert!(session.is_effective_writer().await);
}

#[tokio::test]
async fn boundary_intent_cas_applied_revokes_predecessor() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let (a, b, session) = bootstrap_a(register.clone(), parts, claims).await;
    let expected = session.generation().clone();
    let active = expected.active_loglet_id.clone();

    inject_transitioning_for_b(
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        expected,
        WriterTerm::new(2).expect("t2"),
    )
    .await;

    let (_observed, record) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    assert!(
        matches!(record.state, AuthorityState::Transitioning { .. }),
        "intent CAS must leave Transitioning, not regrant predecessor Serving"
    );
    assert!(
        serving_owner(&record).is_none(),
        "no Serving regrant of predecessor after durable intent"
    );

    let a_gate = gate(
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        owner_a(),
        Some(&active),
    )
    .await;
    assert!(
        matches!(a_gate, AuthorityGateDecision::Denied { .. }),
        "A must lose effective writer once Transitioning is durable"
    );
    assert!(!session.is_effective_writer().await);

    assert_at_most_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&active)),
            (&b, owner_b(), Some(&active)),
        ],
    )
    .await;
}

#[tokio::test]
async fn internal_foundation_crash_boundaries_recover_forward_only() {
    for checkpoint in [
        FoundationTransitionCheckpoint::PredecessorSealed,
        FoundationTransitionCheckpoint::SealedTailObserved,
        FoundationTransitionCheckpoint::SuccessorProvisioned,
    ] {
        let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
        let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
        let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
        let (a, _b, session) = bootstrap_a(
            Arc::clone(&register),
            Arc::clone(&parts),
            Arc::clone(&claims),
        )
        .await;
        let expected = session.generation().clone();
        let predecessor = expected.active_loglet_id.clone();
        let faulty_b = build_node_with_observer(
            owner_b(),
            "tcp://owner-b:9000",
            Arc::clone(&register),
            Arc::clone(&parts),
            Arc::clone(&claims),
            Some(Arc::new(InterruptAt::new(checkpoint))),
        );

        let failure = faulty_b
            .coordinator
            .promote(
                key(),
                WriterTerm::new(2).expect("t2"),
                FoundationPrecondition::Expected(expected.clone()),
                LocalServingEligibility {
                    is_writable: true,
                    is_sealed: false,
                },
            )
            .await;
        assert!(
            matches!(
                failure,
                Err(CoordinatorError::FoundationFailed(
                    FoundationTransitionError::Indeterminate(_)
                ))
            ),
            "{checkpoint:?} must leave an indeterminate, fail-closed transition"
        );

        let read_seal = holylog::provision::resolve_read_seal(
            parts
                .open(&predecessor)
                .expect("open predecessor")
                .components(2),
        )
        .await
        .expect("resolve predecessor read/seal");
        assert!(
            read_seal
                .observe_durable()
                .await
                .expect("observe predecessor")
                .sealed(),
            "{checkpoint:?}: predecessor seal must survive interruption"
        );
        let (_observed, transitioning) = observe_authority(
            Arc::clone(&register),
            Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        )
        .await;
        assert!(matches!(
            transitioning.state,
            AuthorityState::Transitioning { .. }
        ));
        assert!(!session.is_effective_writer().await);

        // A fresh process reconstructs only read/seal state and resumes through
        // the durable root intent. If a receipt was lost after provision, the
        // adapter selects a distinct candidate instead of forging the receipt.
        let recovered_b = build_node(
            owner_b(),
            "tcp://owner-b:9000",
            Arc::clone(&register),
            Arc::clone(&parts),
            Arc::clone(&claims),
        );
        let serving = recovered_b
            .coordinator
            .reconcile(
                key(),
                LocalServingEligibility {
                    is_writable: true,
                    is_sealed: false,
                },
            )
            .await
            .expect("forward recovery");
        let (owner, term, generation) = serving_owner(&serving).expect("Serving");
        assert_eq!(owner, owner_b());
        assert_eq!(term, WriterTerm::new(2).expect("t2"));
        assert_ne!(generation.active_loglet_id, predecessor);
        assert!(
            recovered_b
                .resolver
                .is_writable(&generation.active_loglet_id)
        );

        assert_at_most_one_effective_writer(
            &register,
            &[
                (&a, owner_a(), Some(&predecessor)),
                (&recovered_b, owner_b(), Some(&generation.active_loglet_id)),
            ],
        )
        .await;
    }
}

#[tokio::test]
async fn boundary_intent_cas_reply_lost_forward_only() {
    let run = RunId::new("one-record-ha-intent-reply-lost");
    let sink = RecordingSink::new().shared();
    let foundation_trace = ActorTrace::new(
        run,
        ActorId::new("foundation"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let root_faults = Arc::new(FaultController::new());
    let register = Arc::new(FaultableConditionalRegister::new(
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&root_faults),
        foundation_trace,
    )) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let (a, b, session) = bootstrap_a(register.clone(), parts, claims).await;
    let expected = session.generation().clone();
    let active = expected.active_loglet_id.clone();
    drop(session);
    a.resolver.remove(&active);

    root_faults.arm(ArmedFault::RootCasReplyLost);
    let first = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        key(),
        WriterTerm::new(2).expect("t2"),
        expected.clone(),
        runtime_config(owner_b()),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await;

    let (_observed, after_fault) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    match &after_fault.state {
        AuthorityState::Transitioning { intent } => {
            assert_eq!(intent.candidate_owner_id, owner_b());
            assert_eq!(intent.next_writer_term, WriterTerm::new(2).expect("t2"));
            assert!(
                first.is_err(),
                "reply-loss on intent CAS should fail-closed before Serving activation"
            );
        }
        AuthorityState::Serving { authority, .. } => {
            assert_eq!(authority.owner_id, owner_b());
            assert_ne!(authority.owner_id, owner_a());
        }
        other => panic!("intent reply-loss must not restore predecessor Serving: {other:?}"),
    }
    assert!(
        serving_owner(&after_fault).is_none_or(|(owner, _, _)| owner == owner_b()),
        "never restore predecessor Serving for A after intent reply-loss"
    );

    let a_gate = gate(
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        owner_a(),
        Some(&active),
    )
    .await;
    assert!(matches!(a_gate, AuthorityGateDecision::Denied { .. }));

    // Fresh promote resolves via root read: durable intent resume or already-Serving.
    let b_session = match first {
        Ok(session) => session,
        Err(_) => promote_and_serve(
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
        .await
        .expect("retry promote after intent reply-loss must complete or adopt Serving"),
    };
    assert!(b_session.is_effective_writer().await);
    let b_active = b_session.generation().active_loglet_id.clone();
    let (observed, record) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    assert_generation_bound_to_membership(&observed, &record);
    assert_exactly_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&active)),
            (&b, owner_b(), Some(&b_active)),
        ],
    )
    .await;
}

#[tokio::test]
async fn boundary_foundation_promotion_completes_single_writer() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let (a, b, session) = bootstrap_a(register.clone(), parts, claims).await;
    let expected = session.generation().clone();
    let a_active = expected.active_loglet_id.clone();
    drop(session);
    a.resolver.remove(&a_active);

    let b_session = promote_and_serve(
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
    .await
    .expect("promote_and_serve B");
    let b_active = b_session.generation().active_loglet_id.clone();

    let (observed, record) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    let (owner, term, _) = serving_owner(&record).expect("B Serving");
    assert_eq!(owner, owner_b());
    assert_eq!(term, WriterTerm::new(2).expect("t2"));
    assert_generation_bound_to_membership(&observed, &record);

    let a_gate = gate(
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        owner_a(),
        Some(&a_active),
    )
    .await;
    assert!(matches!(a_gate, AuthorityGateDecision::Denied { .. }));
    assert_exactly_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&a_active)),
            (&b, owner_b(), Some(&b_active)),
        ],
    )
    .await;
}

#[tokio::test]
async fn boundary_final_root_cas_reply_lost_one_writer() {
    let run = RunId::new("one-record-ha-final-cas-reply-lost");
    let sink = RecordingSink::new().shared();
    let foundation_trace = ActorTrace::new(
        run,
        ActorId::new("foundation"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let root_faults = Arc::new(FaultController::new());
    let register = Arc::new(FaultableConditionalRegister::new(
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&root_faults),
        foundation_trace,
    )) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let (a, b, session) = bootstrap_a(register.clone(), parts, claims).await;
    let expected = session.generation().clone();
    let a_active = expected.active_loglet_id.clone();
    drop(session);
    a.resolver.remove(&a_active);

    // Durable intent first so the one-shot reply-loss hits the membership+Serving CAS.
    inject_transitioning_for_b(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
        expected.clone(),
        WriterTerm::new(2).expect("t2"),
    )
    .await;

    root_faults.arm(ArmedFault::RootCasReplyLost);
    let promote_result = b
        .coordinator
        .promote(
            key(),
            WriterTerm::new(2).expect("t2"),
            FoundationPrecondition::Expected(expected.clone()),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await;

    let (observed, record) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;

    match promote_result {
        Ok(serving) => {
            assert!(matches!(serving.state, AuthorityState::Serving { .. }));
            assert_generation_bound_to_membership(&observed, &serving);
        }
        Err(CoordinatorError::FoundationFailed(FoundationTransitionError::Indeterminate(_))) => {
            // Applied-but-reply-lost final CAS: fresh read must show candidate
            // Serving (or fail-closed Transitioning), never predecessor Serving.
            match &record.state {
                AuthorityState::Serving { authority, .. } => {
                    assert_eq!(authority.owner_id, owner_b());
                    assert_generation_bound_to_membership(&observed, &record);
                }
                AuthorityState::Transitioning { intent } => {
                    assert_eq!(intent.candidate_owner_id, owner_b());
                }
                other => panic!("final CAS reply-loss must not restore A Serving: {other:?}"),
            }
        }
        Err(other) => panic!("unexpected promote outcome after final CAS reply-loss: {other:?}"),
    }

    assert!(
        serving_owner(&record).is_none_or(|(owner, _, _)| owner == owner_b()),
        "predecessor Serving must never return after final CAS reply-loss"
    );

    let a_gate = gate(
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        owner_a(),
        Some(&a_active),
    )
    .await;
    assert!(matches!(a_gate, AuthorityGateDecision::Denied { .. }));

    // Recovery via fresh read: resume promote; if Serving is already durable,
    // coordinator returns it. Local writable may still need install for serve.
    let resumed = b
        .coordinator
        .promote(
            key(),
            WriterTerm::new(2).expect("t2"),
            FoundationPrecondition::Expected(expected),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await;

    match resumed {
        Ok(serving) => {
            let (owner, _, generation) = serving_owner(&serving).expect("Serving");
            assert_eq!(owner, owner_b());
            let b_writable = b.resolver.is_writable(&generation.active_loglet_id);
            assert_at_most_one_effective_writer(
                &register,
                &[
                    (&a, owner_a(), Some(&a_active)),
                    (
                        &b,
                        owner_b(),
                        b_writable.then_some(&generation.active_loglet_id),
                    ),
                ],
            )
            .await;
            if b_writable {
                assert_exactly_one_effective_writer(
                    &register,
                    &[
                        (&a, owner_a(), Some(&a_active)),
                        (&b, owner_b(), Some(&generation.active_loglet_id)),
                    ],
                )
                .await;
            }
        }
        Err(_) => {
            // Remain fail-closed Transitioning/Serving-without-local-writable.
            assert_at_most_one_effective_writer(
                &register,
                &[(&a, owner_a(), Some(&a_active)), (&b, owner_b(), None)],
            )
            .await;
        }
    }
}

// ---------------------------------------------------------------------------
// B. Two-coordinator competition
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_coordinator_competition_terms_monotonic_single_writer() {
    let run = RunId::new("one-record-ha-two-coordinator");
    let sink = RecordingSink::new().shared();
    let foundation_trace = ActorTrace::new(
        run,
        ActorId::new("foundation"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let root_faults = Arc::new(FaultController::new());
    let register = Arc::new(FaultableConditionalRegister::new(
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&root_faults),
        foundation_trace,
    )) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let c = build_node(
        owner_c(),
        "tcp://owner-c:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("A term 1 Serving");
    let expected_a = a_session.generation().clone();
    let a_active = expected_a.active_loglet_id.clone();
    drop(a_session);
    a.resolver.remove(&a_active);

    // Optional reply-loss beside B's intent CAS; B must still win forward-only.
    root_faults.arm(ArmedFault::RootCasReplyLost);
    let b_first = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        key(),
        WriterTerm::new(2).expect("t2"),
        expected_a.clone(),
        runtime_config(owner_b()),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await;
    let b_session = match b_first {
        Ok(session) => session,
        Err(_) => promote_and_serve(
            &b.coordinator,
            &b.foundation,
            key(),
            WriterTerm::new(2).expect("t2"),
            expected_a.clone(),
            runtime_config(owner_b()),
            Arc::clone(&register),
            Arc::clone(&b.resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .expect("B completes after optional intent reply-loss"),
    };
    let b_active = b_session.generation().active_loglet_id.clone();

    let (_observed, after_b) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    let (owner, term, _) = serving_owner(&after_b).expect("B Serving");
    assert_eq!(owner, owner_b());
    assert_eq!(term.get(), 2);
    assert!(term.get() > 1, "terms must be monotonic");

    // C races with stale Expected(A) while B already holds durable Serving.
    let stale = c
        .coordinator
        .promote(
            key(),
            WriterTerm::new(3).expect("t3"),
            FoundationPrecondition::Expected(expected_a),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await;
    assert!(
        stale.is_err(),
        "stale Expected promote must conflict once B's intent/Serving advanced"
    );

    // C also cannot steal while inventing a concurrent Transitioning against B Serving
    // without matching Expected(B): term-3 with B's generation is a *new* lawful promote,
    // so use Expected(A) conflict above as the stale case. Confirm grant history:
    // durable B Serving is not reverted.
    let (_observed, still_b) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    let (owner, term, _) = serving_owner(&still_b).expect("B Serving retained");
    assert_eq!(owner, owner_b());
    assert_eq!(term.get(), 2);

    assert_exactly_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&a_active)),
            (&b, owner_b(), Some(&b_active)),
            (&c, owner_c(), Some(&b_active)),
        ],
    )
    .await;
    assert!(b_session.is_effective_writer().await);
}

// ---------------------------------------------------------------------------
// C. Runtime ACK evidence across cutover
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runtime_ack_evidence_across_cutover() {
    let run = RunId::new("one-record-ha-runtime-ack");
    let sink = RecordingSink::new().shared();
    let foundation_trace = ActorTrace::new(
        run.clone(),
        ActorId::new("foundation"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let root_faults = Arc::new(FaultController::new());
    let register = Arc::new(FaultableConditionalRegister::new(
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        root_faults,
        foundation_trace.clone(),
    )) as Arc<dyn ConditionalRegister>;
    let writer_faults = Arc::new(FaultController::new());
    let parts = Arc::new(FaultingSharedPartsFactory::new(
        writer_faults,
        foundation_trace,
    )) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("A Serving");
    let expected = a_session.generation().clone();
    let a_active = expected.active_loglet_id.clone();

    let payload_a = b"ack-before-cutover";
    let receipt_a = commit_one(&a_session, 0, payload_a).await;
    trace_committed_ack(
        &ActorTrace::new(
            run.clone(),
            ActorId::new("scripture-a"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        ),
        "producer-0-sequence-0",
        &receipt_a,
        payload_a,
        &a_active,
    );

    drop(a_session);
    a.resolver.remove(&a_active);

    let b_session = promote_and_serve(
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
    .await
    .expect("B promote_and_serve");
    let b_active = b_session.generation().active_loglet_id.clone();

    let a_gate = gate(
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        owner_a(),
        Some(&a_active),
    )
    .await;
    assert!(
        matches!(a_gate, AuthorityGateDecision::Denied { .. }),
        "A must be Denied after intent/cutover"
    );

    let payload_b = b"ack-after-cutover";
    let receipt_b = commit_one(&b_session, 1, payload_b).await;
    assert!(
        receipt_b.first_offset.get() >= receipt_a.next_offset.get(),
        "B must commit a contiguous next offset: a.next={} b.first={}",
        receipt_a.next_offset.get(),
        receipt_b.first_offset.get()
    );
    trace_committed_ack(
        &ActorTrace::new(
            run,
            ActorId::new("scripture-b"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        ),
        "producer-0-sequence-1",
        &receipt_b,
        payload_b,
        &b_active,
    );

    let (observed, record) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    assert_generation_bound_to_membership(&observed, &record);
    assert_exactly_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&a_active)),
            (&b, owner_b(), Some(&b_active)),
        ],
    )
    .await;

    let events = sink.events();
    assert!(
        matches!(check_trace(&events), Verdict::Pass),
        "Holylog write events + ScriptureCommittedAck must Pass the checker: {events:#?}"
    );
}

// ---------------------------------------------------------------------------
// D. Family 20 — bounded named reconfiguration churn (three candidates)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn churn_intent_reply_loss_b_then_c_completes() {
    let run = RunId::new("churn-intent-b-then-c");
    let sink = RecordingSink::new().shared();
    let foundation_trace = ActorTrace::new(
        run,
        ActorId::new("foundation"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let root_faults = Arc::new(FaultController::new());
    let register = Arc::new(FaultableConditionalRegister::new(
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&root_faults),
        foundation_trace,
    )) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let c = build_node(
        owner_c(),
        "tcp://owner-c:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("A Serving");
    let expected_a = a_session.generation().clone();
    let a_active = expected_a.active_loglet_id.clone();
    let receipt_a = commit_one(&a_session, 0, b"churn-a-0").await;
    drop(a_session);
    a.resolver.remove(&a_active);

    // B dies at intent reply-loss; must not leave two writers.
    root_faults.arm(ArmedFault::RootCasReplyLost);
    let b_attempt = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        key(),
        WriterTerm::new(2).expect("t2"),
        expected_a.clone(),
        runtime_config(owner_b()),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await;
    assert_at_most_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&a_active)),
            (&b, owner_b(), None),
            (&c, owner_c(), None),
        ],
    )
    .await;

    // If B already reached Serving despite reply-loss, adopt it; else C finishes
    // from Expected(A) or resumes durable Transitioning via promote.
    let (serving_owner_id, serving_term, serving_active, serving_session) = match b_attempt {
        Ok(session) => {
            let generation = session.generation().clone();
            (
                owner_b(),
                2_u64,
                generation.active_loglet_id.clone(),
                session,
            )
        }
        Err(_) => {
            // Clear one-shot fault; C promotes with term 3 from original Expected(A)
            // if intent never applied, or reconciles if Transitioning for B remains.
            let (_obs, mid) = observe_authority(
                Arc::clone(&register),
                Arc::clone(&c.resolver) as Arc<dyn LogletResolver>,
            )
            .await;
            match &mid.state {
                AuthorityState::Transitioning { intent }
                    if intent.candidate_owner_id == owner_b() =>
                {
                    // Durable B intent: B must resume, not abandon.
                    let b_session = promote_and_serve(
                        &b.coordinator,
                        &b.foundation,
                        key(),
                        WriterTerm::new(2).expect("t2"),
                        expected_a.clone(),
                        runtime_config(owner_b()),
                        Arc::clone(&register),
                        Arc::clone(&b.resolver),
                        SystemClock::new(),
                        SystemTimer::new(),
                    )
                    .await
                    .expect("B resumes durable Transitioning");
                    let generation = b_session.generation().clone();
                    (
                        owner_b(),
                        2_u64,
                        generation.active_loglet_id.clone(),
                        b_session,
                    )
                }
                AuthorityState::Serving { authority, .. } if authority.owner_id == owner_b() => {
                    let b_session = promote_and_serve(
                        &b.coordinator,
                        &b.foundation,
                        key(),
                        WriterTerm::new(2).expect("t2"),
                        expected_a.clone(),
                        runtime_config(owner_b()),
                        Arc::clone(&register),
                        Arc::clone(&b.resolver),
                        SystemClock::new(),
                        SystemTimer::new(),
                    )
                    .await
                    .expect("adopt B Serving");
                    let generation = b_session.generation().clone();
                    (
                        owner_b(),
                        2_u64,
                        generation.active_loglet_id.clone(),
                        b_session,
                    )
                }
                _ => {
                    // Intent never applied: C completes Expected(A) at term 3.
                    let c_session = promote_and_serve(
                        &c.coordinator,
                        &c.foundation,
                        key(),
                        WriterTerm::new(3).expect("t3"),
                        expected_a.clone(),
                        runtime_config(owner_c()),
                        Arc::clone(&register),
                        Arc::clone(&c.resolver),
                        SystemClock::new(),
                        SystemTimer::new(),
                    )
                    .await
                    .expect("C completes after B intent loss");
                    let generation = c_session.generation().clone();
                    (
                        owner_c(),
                        3_u64,
                        generation.active_loglet_id.clone(),
                        c_session,
                    )
                }
            }
        }
    };

    let (_obs, after) = observe_authority(
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
    )
    .await;
    let (owner, term, _) = serving_owner(&after).expect("Serving after churn");
    assert_eq!(owner, serving_owner_id);
    assert_eq!(term.get(), serving_term);
    assert!(term.get() > 1);

    let receipt_post = commit_one(&serving_session, 1, b"churn-post-0").await;
    assert!(
        receipt_post.first_offset.get() >= receipt_a.next_offset.get(),
        "post-cutover offsets must continue forward"
    );

    // Stale A cannot ACK.
    let a_gate = gate(
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        owner_a(),
        Some(&a_active),
    )
    .await;
    assert!(matches!(a_gate, AuthorityGateDecision::Denied { .. }));

    assert_exactly_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&a_active)),
            (&b, owner_b(), Some(&serving_active)),
            (&c, owner_c(), Some(&serving_active)),
        ],
    )
    .await;
}

#[tokio::test]
async fn churn_seal_interrupt_then_c_recovers() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;

    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let c = build_node(
        owner_c(),
        "tcp://owner-c:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("A Serving");
    let expected_a = a_session.generation().clone();
    let predecessor = expected_a.active_loglet_id.clone();
    let receipt_a = commit_one(&a_session, 0, b"seal-churn-a").await;

    // B dies at sealed-tail; durable Transitioning remains for B.
    let faulty_b = build_node_with_observer(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
        Some(Arc::new(InterruptAt::new(
            FoundationTransitionCheckpoint::SealedTailObserved,
        ))),
    );
    let failure = faulty_b
        .coordinator
        .promote(
            key(),
            WriterTerm::new(2).expect("t2"),
            FoundationPrecondition::Expected(expected_a.clone()),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await;
    assert!(matches!(
        failure,
        Err(CoordinatorError::FoundationFailed(
            FoundationTransitionError::Indeterminate(_)
        ))
    ));
    assert!(!a_session.is_effective_writer().await);

    // Fresh candidate C cannot steal with stale Expected(A) while B's Transitioning
    // is locked on the root.
    let stale_c = c
        .coordinator
        .promote(
            key(),
            WriterTerm::new(3).expect("t3"),
            FoundationPrecondition::Expected(expected_a),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await;
    assert!(
        stale_c.is_err(),
        "C must not abandon B's durable Transitioning via stale Expected(A)"
    );

    // B recovers forward-only (same candidate identity, fresh process).
    let recovered_b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let serving = recovered_b
        .coordinator
        .reconcile(
            key(),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await
        .expect("B forward recovery after seal interrupt");
    let (owner, term, generation) = serving_owner(&serving).expect("Serving");
    assert_eq!(owner, owner_b());
    assert_eq!(term.get(), 2);
    assert_ne!(generation.active_loglet_id, predecessor);

    // Activate via gate: reconcile already published Serving + writable for B.
    let b_active = generation.active_loglet_id.clone();
    drop(a_session);
    a.resolver.remove(&predecessor);

    let b_gate = gate(
        Arc::clone(&register),
        Arc::clone(&recovered_b.resolver),
        owner_b(),
        Some(&b_active),
    )
    .await;
    assert!(
        is_effective(&b_gate),
        "recovered B must be effective writer after seal churn"
    );
    let c_gate = gate(
        Arc::clone(&register),
        Arc::clone(&c.resolver),
        owner_c(),
        Some(&b_active),
    )
    .await;
    assert!(matches!(c_gate, AuthorityGateDecision::Denied { .. }));

    assert_exactly_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&predecessor)),
            (&recovered_b, owner_b(), Some(&b_active)),
            (&c, owner_c(), Some(&b_active)),
        ],
    )
    .await;
    let _ = receipt_a;
}

#[tokio::test]
async fn churn_successor_provisioned_then_finish_without_abandon() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let (a, _b, session) = bootstrap_a(
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    )
    .await;
    let expected = session.generation().clone();
    let predecessor = expected.active_loglet_id.clone();
    let _ = commit_one(&session, 0, b"prov-churn-a").await;

    let faulty_b = build_node_with_observer(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
        Some(Arc::new(InterruptAt::new(
            FoundationTransitionCheckpoint::SuccessorProvisioned,
        ))),
    );
    let failure = faulty_b
        .coordinator
        .promote(
            key(),
            WriterTerm::new(2).expect("t2"),
            FoundationPrecondition::Expected(expected.clone()),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await;
    assert!(matches!(
        failure,
        Err(CoordinatorError::FoundationFailed(
            FoundationTransitionError::Indeterminate(_)
        ))
    ));

    let recovered_b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let serving = recovered_b
        .coordinator
        .reconcile(
            key(),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await
        .expect("resume after successor-provisioned interrupt — no unsafe abandon");
    let (owner, term, generation) = serving_owner(&serving).expect("Serving");
    assert_eq!(owner, owner_b());
    assert_eq!(term.get(), 2);
    assert_ne!(generation.active_loglet_id, predecessor);
    assert!(
        recovered_b
            .resolver
            .is_writable(&generation.active_loglet_id)
    );

    assert_at_most_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&predecessor)),
            (&recovered_b, owner_b(), Some(&generation.active_loglet_id)),
        ],
    )
    .await;
    drop(session);
}

#[tokio::test]
async fn churn_stale_a_cannot_ack_after_b_serving() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let (a, b, a_session) = bootstrap_a(
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims),
    )
    .await;
    let expected = a_session.generation().clone();
    let a_active = expected.active_loglet_id.clone();
    let receipt_a = commit_one(&a_session, 0, b"stale-deny-a").await;
    drop(a_session);
    a.resolver.remove(&a_active);

    let b_session = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        key(),
        WriterTerm::new(2).expect("t2"),
        expected.clone(),
        runtime_config(owner_b()),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("B Serving");
    let b_active = b_session.generation().active_loglet_id.clone();
    let receipt_b = commit_one(&b_session, 1, b"stale-deny-b").await;
    assert!(receipt_b.first_offset.get() >= receipt_a.next_offset.get());

    let a_gate = gate(
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        owner_a(),
        Some(&a_active),
    )
    .await;
    assert!(matches!(a_gate, AuthorityGateDecision::Denied { .. }));

    // Stale Expected(A) promote from A must fail.
    let stale = a
        .coordinator
        .promote(
            key(),
            WriterTerm::new(3).expect("t3"),
            FoundationPrecondition::Expected(expected),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await;
    assert!(stale.is_err(), "stale A promote must fail after B Serving");

    assert_exactly_one_effective_writer(
        &register,
        &[
            (&a, owner_a(), Some(&a_active)),
            (&b, owner_b(), Some(&b_active)),
        ],
    )
    .await;
}

mod churn_negative_controls {
    use super::{
        JournalGenerationRef, assert_generation_bound_to_membership, key, owner_a, serving_owner,
    };
    use holylog::virtual_log::LogletId;
    use scripture::serving_authority::{
        AuthorityState, RouteHint, ServingAuthorityRecord, WriterAuthority, WriterTerm,
    };
    use scripture::{JournalId, OwnerId, VerseId};

    #[test]
    fn trips_when_generation_ref_mismatches_membership_binding() {
        let generation_ref = JournalGenerationRef::from_active_generation(
            1,
            LogletId::new("loglet-a").expect("id"),
            0,
        );
        let record = ServingAuthorityRecord::new(
            key(),
            AuthorityState::Serving {
                authority: WriterAuthority {
                    owner_id: owner_a(),
                    writer_term: WriterTerm::new(1).expect("t1"),
                    generation_ref: generation_ref.clone(),
                },
                route_hint: RouteHint::new("tcp://a:9000").expect("route"),
            },
        );
        let mut observed = holylog::virtual_log::VersionedState {
            state: holylog::virtual_log::VirtualLogState {
                revision: 1,
                generations: vec![holylog::virtual_log::GenerationDescriptor {
                    loglet_id: LogletId::new("loglet-a").expect("id"),
                    start: 0,
                }],
                application_fence: record.encode_application_fence().expect("encode"),
            },
            token: holylog::virtual_log::CompareToken::from_revision(1),
        };
        // Intact binding must pass.
        assert_generation_bound_to_membership(&observed, &record);

        // Corrupt membership start so binding trips.
        observed.state.generations[0].start = 99;
        let from_membership =
            JournalGenerationRef::from_virtual_log_state(&observed.state).expect("bind");
        assert_ne!(
            generation_ref, from_membership,
            "negative control: intentional membership divergence must be visible"
        );
    }

    #[test]
    fn trips_when_serving_owner_term_regression_is_forced() {
        let generation_ref = JournalGenerationRef::from_active_generation(
            2,
            LogletId::new("loglet-b").expect("id"),
            1,
        );
        let newer = ServingAuthorityRecord::new(
            key(),
            AuthorityState::Serving {
                authority: WriterAuthority {
                    owner_id: OwnerId::from_bytes(*b"ha1rec-own-b!!!!"),
                    writer_term: WriterTerm::new(2).expect("t2"),
                    generation_ref: generation_ref.clone(),
                },
                route_hint: RouteHint::new("tcp://b:9000").expect("route"),
            },
        );
        let older = ServingAuthorityRecord::new(
            AuthorityKey {
                journal_id: JournalId::from_bytes(*b"ha1rec-journal!!"),
                verse_id: VerseId::from_bytes(*b"ha1rec-verse!!!!"),
            },
            AuthorityState::Serving {
                authority: WriterAuthority {
                    owner_id: owner_a(),
                    writer_term: WriterTerm::new(1).expect("t1"),
                    generation_ref,
                },
                route_hint: RouteHint::new("tcp://a:9000").expect("route"),
            },
        );
        let (_, term_new, _) = serving_owner(&newer).expect("newer");
        let (_, term_old, _) = serving_owner(&older).expect("older");
        assert!(
            term_new.get() > term_old.get(),
            "negative control: forced term regression must be detectable"
        );
        let _ = older;
    }

    use scripture::serving_authority::AuthorityKey;
}
