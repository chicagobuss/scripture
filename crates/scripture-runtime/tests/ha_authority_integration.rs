//! Multi-node Holylog Foundation + Serving Authority integration proofs.
//!
//! These tests exercise the real adapter, durable Transitioning bootstrap,
//! in-process activate-and-serve, and committed ACK fencing. They do not claim
//! live fleet HA.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
use holylog::virtual_log::{
    ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver,
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
    AuthorityGateDecision, DurableLogletParts, HaServingSession, HolylogJournalFoundation,
    NodeIdentity, PartsFactory, PartsFactoryError, ProcessLogletResolver, SharedMemoryPartsFactory,
    bootstrap_and_serve, bootstrap_authority_domain, evaluate_authority_gate, promote_and_serve,
};
use scripture_service::{
    AuthorityCoordinator, CasOutcome, DeterministicTransitionIdGenerator,
    InMemoryServingAuthorityStore, JournalFoundationTransition, LocalServingEligibility,
    ServingAuthorityStore, VerseRuntimeConfig,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"ha-int-journal!!")
}
fn verse() -> VerseId {
    VerseId::from_bytes(*b"ha-int-verse!!!!")
}
fn owner_a() -> OwnerId {
    OwnerId::from_bytes(*b"ha-int-owner-a!!")
}
fn owner_b() -> OwnerId {
    OwnerId::from_bytes(*b"ha-int-owner-b!!")
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
        cohort_id: CohortId::from_bytes(*b"ha-int-cohort!!!"),
        writer_id: WriterId::from_bytes(*b"ha-int-writer!!!"),
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
    }
}

struct NodeBundle {
    resolver: Arc<ProcessLogletResolver>,
    foundation: Arc<HolylogJournalFoundation>,
    coordinator: AuthorityCoordinator,
}

/// Test-only durable parts factory that keeps the real Holylog components but
/// wraps their semantic ports with the correctness harness.  It is deliberately
/// a [`PartsFactory`] rather than a mock Foundation: `HolylogJournalFoundation`
/// and the live `HaServingSession` use it unchanged.
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
            "scripture-faulting-ha-test",
            loglet_id,
        ))
    }
}

fn build_node<P>(
    owner: OwnerId,
    endpoint: &str,
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<P>,
    claims: Arc<dyn ExclusiveClaimStore>,
    store: Arc<dyn ServingAuthorityStore>,
) -> NodeBundle
where
    P: PartsFactory + 'static,
{
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
        Arc::clone(&parts) as Arc<dyn PartsFactory>,
        Arc::clone(&claims),
        2,
    ));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&store),
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

fn record_generation(record: &ServingAuthorityRecord) -> JournalGenerationRef {
    match &record.state {
        AuthorityState::Serving { authority, .. } => authority.generation_ref.clone(),
        _ => panic!("expected Serving"),
    }
}

async fn commit_one(
    session: &HaServingSession,
    producer: ProducerId,
    sequence: u64,
    payload: &'static [u8],
) -> scripture::Receipt {
    let pending = session
        .submit(Submission {
            producer_id: producer,
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

#[tokio::test]
async fn bootstrap_and_serve_admits_only_owner_a() {
    let register = Arc::new(InMemoryConditionalRegister::new());
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());
    let store: Arc<dyn ServingAuthorityStore> = Arc::new(InMemoryServingAuthorityStore::default());

    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );

    let session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        Arc::clone(&store),
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("bootstrap_and_serve A");
    assert!(session.is_serving());
    let ack = commit_one(
        &session,
        ProducerId::from_bytes(*b"ha-int-producer!"),
        0,
        b"a-0",
    )
    .await;
    assert!(ack.first_offset.get() < ack.next_offset.get());

    let b_gate = evaluate_authority_gate(
        store.as_ref(),
        key(),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
        owner_b(),
        false,
        false,
    )
    .await;
    assert!(matches!(b_gate, AuthorityGateDecision::Denied { .. }));

    // Promotion revokes client-facing authority before it seals the old
    // Foundation generation. The still-live A runtime must refuse a new
    // submission in that transition window rather than returning a later OK.
    let snapshot = store
        .observe(key())
        .await
        .expect("observe")
        .expect("record");
    let transitioning = ServingAuthorityRecord::new(
        key(),
        AuthorityState::Transitioning {
            intent: TransitionIntent {
                transition_id: TransitionId::from_bytes([9; 16]),
                kind: TransitionKind::RecoveryPromotion,
                precondition: FoundationPrecondition::Expected(session.generation().clone()),
                candidate_owner_id: owner_b(),
                next_writer_term: WriterTerm::new(2).expect("t2"),
            },
        },
    );
    assert_eq!(
        store
            .compare_and_swap(key(), Some(snapshot.version), transitioning)
            .await
            .expect("transition CAS"),
        CasOutcome::Applied
    );
    assert!(
        !session.is_effective_writer().await,
        "a Transitioning record must make the old process unready before Foundation sealing"
    );
    assert!(
        session
            .submit(Submission {
                producer_id: ProducerId::from_bytes(*b"ha-int-producer!"),
                producer_epoch: 0,
                sequence: 1,
                records: vec![Record::new([], Bytes::from_static(b"must-not-ack"))],
            })
            .await
            .is_err(),
        "a Transitioning Serving Authority record must deny A before another admission"
    );
}

#[tokio::test]
async fn promote_and_serve_b_takes_committed_acks_and_denies_a() {
    let register = Arc::new(InMemoryConditionalRegister::new());
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());
    let store: Arc<dyn ServingAuthorityStore> = Arc::new(InMemoryServingAuthorityStore::default());

    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        Arc::clone(&store),
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("A serving");
    let expected = a_session.generation().clone();
    let _ = commit_one(
        &a_session,
        ProducerId::from_bytes(*b"ha-int-producer!"),
        0,
        b"before-cutover",
    )
    .await;

    // Stop A: drop runtime and local writable (simulates process death of soft sequencer).
    let active = expected.active_loglet_id.clone();
    drop(a_session);
    a.resolver.remove(&active);

    let b_session = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        Arc::clone(&store),
        key(),
        WriterTerm::new(2).expect("t2"),
        expected.clone(),
        runtime_config(owner_b()),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("B promote_and_serve");
    assert!(b_session.is_serving());
    let ack = commit_one(
        &b_session,
        ProducerId::from_bytes(*b"ha-int-producer!"),
        1,
        b"after-cutover",
    )
    .await;
    assert!(ack.first_offset.get() < ack.next_offset.get());
    assert_eq!(ack.level, scripture::AckLevel::Committed);

    // A cannot regain effective writer / committed ACKs after cutover.
    let a_gate = evaluate_authority_gate(
        store.as_ref(),
        key(),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner_a(),
        a.resolver.is_writable(&active),
        false,
    )
    .await;
    assert!(matches!(a_gate, AuthorityGateDecision::Denied { .. }));

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
    assert!(stale.is_err());
}

#[tokio::test]
async fn bootstrap_via_coordinator_leaves_empty_rev0_classifiable() {
    let register = Arc::new(InMemoryConditionalRegister::new());
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());
    let store: Arc<dyn ServingAuthorityStore> = Arc::new(InMemoryServingAuthorityStore::default());
    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );

    let record = bootstrap_authority_domain(&a.coordinator, key(), WriterTerm::new(1).expect("t1"))
        .await
        .expect("durable bootstrap");
    let generation = record_generation(&record);
    assert_eq!(generation.virtual_log_revision, 0);

    // Crash boundary: if we had stopped after Transitioning mid-flight, reconcile
    // would classify. After success, Serving is stable.
    assert!(matches!(record.state, AuthorityState::Serving { .. }));
}

#[tokio::test]
async fn dense_offsets_continue_across_cutover() {
    let register = Arc::new(InMemoryConditionalRegister::new());
    let parts = Arc::new(SharedMemoryPartsFactory::default());
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());
    let store: Arc<dyn ServingAuthorityStore> = Arc::new(InMemoryServingAuthorityStore::default());
    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        Arc::clone(&store),
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("A");
    let expected = a_session.generation().clone();
    let r0 = commit_one(
        &a_session,
        ProducerId::from_bytes(*b"ha-int-producer!"),
        0,
        b"rec-0",
    )
    .await;
    let r1 = commit_one(
        &a_session,
        ProducerId::from_bytes(*b"ha-int-producer!"),
        1,
        b"rec-1",
    )
    .await;
    assert_eq!(r0.next_offset, r1.first_offset);
    let active = expected.active_loglet_id.clone();
    drop(a_session);
    a.resolver.remove(&active);

    let b_session = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        Arc::clone(&store),
        key(),
        WriterTerm::new(2).expect("t2"),
        expected,
        runtime_config(owner_b()),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("B");
    let r2 = commit_one(
        &b_session,
        ProducerId::from_bytes(*b"ha-int-producer!"),
        2,
        b"rec-2",
    )
    .await;
    assert!(
        r2.first_offset.get() >= r1.next_offset.get(),
        "successor must not reuse predecessor offsets: r1.next={} r2.first={}",
        r1.next_offset.get(),
        r2.first_offset.get()
    );
    let membership = holylog::virtual_log::VirtualLog::new(
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&b.resolver) as Arc<dyn LogletResolver>,
    )
    .observe_membership()
    .await
    .expect("observe");
    assert!(membership.state.generations.len() >= 2);
}

#[tokio::test]
async fn wedged_payload_is_never_acknowledged_and_ha_recovery_serves_successor() {
    let run = RunId::new("scripture-ha-wedged-payload-recovery-1");
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
    ));
    let writer_faults = Arc::new(FaultController::new());
    let parts = Arc::new(FaultingSharedPartsFactory::new(
        Arc::clone(&writer_faults),
        foundation_trace,
    ));
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());
    let store: Arc<dyn ServingAuthorityStore> = Arc::new(InMemoryServingAuthorityStore::default());

    let a = build_node(
        owner_a(),
        "tcp://owner-a:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );
    let b = build_node(
        owner_b(),
        "tcp://owner-b:9000",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
        Arc::clone(&store),
    );

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        &a.foundation,
        Arc::clone(&store),
        key(),
        WriterTerm::new(1).expect("t1"),
        runtime_config(owner_a()),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("A bootstrap and serve");
    let expected = a_session.generation().clone();
    let producer = ProducerId::from_bytes(*b"ha-int-producer!");

    writer_faults.arm(ArmedFault::DieAfterPayload);
    let pending = a_session
        .submit(Submission {
            producer_id: producer,
            producer_epoch: 0,
            sequence: 0,
            records: vec![Record::new(
                [],
                Bytes::from_static(b"durable-but-uncommitted"),
            )],
        })
        .await
        .expect("A admits before its write wedges");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), a_session.flush())
            .await
            .is_err(),
        "fault must wedge after the real payload write, before a committed receipt"
    );
    assert_eq!(writer_faults.fired_count(), 1);

    let before_recovery = sink.events();
    assert!(
        before_recovery
            .iter()
            .any(|event| matches!(event.event, EventKind::PayloadDurable { .. }))
    );
    assert!(before_recovery.iter().any(|event| matches!(
        event.event,
        EventKind::Fault {
            fault: holylog_correctness::FaultKind::WriterDiesAfterPayload,
            applied: true,
        }
    )));
    assert!(
        !before_recovery
            .iter()
            .any(|event| matches!(event.event, EventKind::ScriptureCommittedAck { .. })),
        "a durable payload without completed sequencing must not produce a committed ACK"
    );

    // Let the simulated dead actor resolve as an error so the real runtime can
    // terminate.  This models process loss; it does not turn the write into a
    // successful commit.
    writer_faults.death_gate().open();
    let rejected = tokio::time::timeout(std::time::Duration::from_secs(1), pending)
        .await
        .expect("wedged receipt resolves once the simulated process is released");
    assert!(
        rejected.is_err(),
        "wedged append must resolve as non-committed"
    );

    let active = expected.active_loglet_id.clone();
    drop(a_session);
    a.resolver.remove(&active);

    let b_session = promote_and_serve(
        &b.coordinator,
        &b.foundation,
        Arc::clone(&store),
        key(),
        WriterTerm::new(2).expect("t2"),
        expected,
        runtime_config(owner_b()),
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("B recovers through the real Foundation + Serving Authority path");
    assert!(b_session.is_effective_writer().await);

    let successor_payload = b"committed-after-recovery";
    let receipt = commit_one(&b_session, producer, 1, successor_payload).await;
    trace_committed_ack(
        &ActorTrace::new(
            run,
            ActorId::new("scripture-b"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        ),
        "producer-0-sequence-1",
        &receipt,
        successor_payload,
        &b_session.generation().active_loglet_id,
    );
    // The payload write reached durable storage before the actor died, so the
    // successor must preserve its physical boundary rather than reuse address
    // zero.  Crucially, that reserved position produced no committed Scripture
    // acknowledgement.
    assert_eq!(receipt.first_offset.get(), 1);

    let events = sink.events();
    assert!(
        matches!(check_trace(&events), Verdict::Pass),
        "real HA recovery trace must satisfy the Holylog checker: {events:#?}"
    );
}
