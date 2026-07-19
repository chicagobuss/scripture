//! Multi-assignment Scribe isolation proofs (in-memory).
//!
//! Demonstrates independent authority roots, store-append isolation, bootstrap
//! races, and targeted promote fencing across assignments hosted by one Scribe
//! process. Does not claim live SSH/fleet HA.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
use scripture::serving_authority::{AuthorityKey, RouteHint, WriterTerm};
use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound,
    Submission, SystemClock, SystemTimer, VerseId, WriterId,
};
use scripture_runtime::{
    AssignmentResourceBudget, AssignmentResourceLimits, AssignmentRuntime, AuthorityGateDecision,
    DurableLogletParts, HaServingSession, HolylogJournalFoundation, NodeIdentity, PartsFactory,
    PartsFactoryError, ProcessLogletResolver, ScribeResourceLimits, ScribeSupervisor,
    SharedMemoryPartsFactory, assignment_durable_root, bootstrap_and_serve,
    evaluate_authority_gate, promote_and_serve,
};
use scripture_service::{
    AuthorityCoordinator, DeterministicTransitionIdGenerator, JournalFoundationTransition,
    VerseRuntimeConfig,
};

fn owner() -> OwnerId {
    OwnerId::from_bytes(*b"multi-owner-a!!!")
}

fn owner_b() -> OwnerId {
    OwnerId::from_bytes(*b"multi-owner-b!!!")
}

fn runtime_config(journal: [u8; 16], verse: [u8; 16], writer: [u8; 16]) -> VerseRuntimeConfig {
    runtime_config_for(owner(), journal, verse, writer)
}

fn runtime_config_for(
    owner_id: OwnerId,
    journal: [u8; 16],
    verse: [u8; 16],
    writer: [u8; 16],
) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: JournalId::from_bytes(journal),
        verse_id: VerseId::from_bytes(verse),
        owner_id,
        cohort_id: CohortId::from_bytes(*b"multi-cohort!!!!"),
        writer_id: WriterId::from_bytes(writer),
        policy: ChunkPolicy {
            max_chunk_bytes: 64 * 1024,
            max_record_bytes: 16 * 1024,
            max_chunk_records: 8,
            max_chunk_age: Duration::from_secs(60),
            max_buffered_bytes: 64 * 1024,
            max_inflight_chunks: 1,
            max_uncommitted_age: Duration::from_secs(60),
            recovery_scan: RecoveryBound::new(8).expect("bound"),
        },
        recovery_bound: RecoveryBound::new(8).expect("bound"),
        queue_capacity: 16,
    }
}

fn default_budget() -> Arc<AssignmentResourceBudget> {
    Arc::new(AssignmentResourceBudget::new(
        AssignmentResourceLimits::default(),
    ))
}

/// Test-only durable parts factory wrapping LogDrive/Seal with a fault controller.
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
            "scripture-multi-assignment-fault-test",
            loglet_id,
        ))
    }
}

struct NodeBundle {
    resolver: Arc<ProcessLogletResolver>,
    foundation: Arc<HolylogJournalFoundation>,
    coordinator: AuthorityCoordinator,
}

fn build_node(
    owner_id: OwnerId,
    endpoint: &str,
    key: AuthorityKey,
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
) -> NodeBundle {
    let identity = NodeIdentity {
        owner_id,
        endpoint: OwnerEndpoint::new(endpoint).expect("ep"),
    };
    let resolver = Arc::new(ProcessLogletResolver::default());
    let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
        key,
        identity,
        Arc::clone(&register),
        Arc::clone(&resolver),
        parts,
        claims,
        2,
    ));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
        Arc::new(DeterministicTransitionIdGenerator::new()),
        owner_id,
        RouteHint::new(endpoint).expect("route"),
    );
    NodeBundle {
        resolver,
        foundation,
        coordinator,
    }
}

struct AssignmentBundle {
    key: AuthorityKey,
    session: HaServingSession,
    store_root: String,
    advertise: String,
}

async fn bootstrap_assignment(
    id: &str,
    journal: [u8; 16],
    verse: [u8; 16],
    writer: [u8; 16],
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
) -> AssignmentBundle {
    let key = AuthorityKey {
        journal_id: JournalId::from_bytes(journal),
        verse_id: VerseId::from_bytes(verse),
    };
    let advertise = format!("tcp://{id}:9000");
    let node = build_node(
        owner(),
        &advertise,
        key,
        Arc::clone(&register),
        parts,
        claims,
    );
    let session = bootstrap_and_serve(
        &node.coordinator,
        node.foundation.as_ref(),
        key,
        WriterTerm::new(1).expect("term"),
        runtime_config(journal, verse, writer),
        register,
        node.resolver,
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("bootstrap");
    AssignmentBundle {
        key,
        session,
        store_root: assignment_durable_root(
            "memory",
            JournalId::from_bytes(journal),
            VerseId::from_bytes(verse),
        ),
        advertise,
    }
}

async fn commit_one(session: &HaServingSession, sequence: u64, payload: &'static [u8]) {
    let _ = commit_one_receipt(session, sequence, payload).await;
}

async fn commit_one_receipt(
    session: &HaServingSession,
    sequence: u64,
    payload: &'static [u8],
) -> scripture::Receipt {
    let pending = session
        .submit(Submission {
            producer_id: ProducerId::from_bytes(*b"multi-producer!!"),
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
async fn one_scribe_hosts_two_independent_serving_assignments() {
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());

    let a = bootstrap_assignment(
        "telemetry-host-a",
        *b"telemetry-jrnl!!",
        *b"telemetry-host-a",
        *b"telemetry-wrtr!!",
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    )
    .await;
    let b = bootstrap_assignment(
        "audit-ingress",
        *b"audit-journal!!!",
        *b"audit-ingress-0!",
        *b"audit-writer!!!!",
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    )
    .await;

    commit_one(&a.session, 0, b"telemetry-one").await;
    commit_one(&b.session, 0, b"audit-one").await;

    let supervisor = ScribeSupervisor::from_assignments(
        ScribeResourceLimits::default(),
        vec![
            AssignmentRuntime::serving(
                "telemetry-host-a",
                a.key,
                a.session,
                a.store_root,
                a.advertise,
                default_budget(),
            ),
            AssignmentRuntime::serving(
                "audit-ingress",
                b.key,
                b.session,
                b.store_root,
                b.advertise,
                default_budget(),
            ),
        ],
    )
    .expect("supervisor");
    let status = supervisor.status_body();
    assert!(status.contains("assignment id=telemetry-host-a disposition=Serving"));
    assert!(status.contains("assignment id=audit-ingress disposition=Serving"));
    assert!(status.contains("canon=telemetry-jrnl!!"));
    assert!(status.contains("verse=audit-ingress-0!"));
    assert!(!status.contains("the scribe failed over"));
}

#[tokio::test]
async fn wedged_assignment_does_not_block_sibling() {
    let run = RunId::new("multi-assignment-wedged-sibling-1");
    let sink = RecordingSink::new().shared();
    let trace = ActorTrace::new(
        run,
        ActorId::new("wedged-a"),
        Arc::clone(&sink) as Arc<dyn TraceSink>,
    );
    let a_faults = Arc::new(FaultController::new());
    let a_parts = Arc::new(FaultingSharedPartsFactory::new(
        Arc::clone(&a_faults),
        trace,
    )) as Arc<dyn PartsFactory>;
    // Sibling uses a separate healthy parts factory (shared-client shape optional;
    // isolation proof is that B's append path is not wedged by A's DieAfterPayload).
    let b_parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());

    let a = bootstrap_assignment(
        "wedged",
        *b"wedged-journal!!",
        *b"wedged-verse!!!!",
        *b"wedged-writer!!!",
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        a_parts,
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    )
    .await;
    let b = bootstrap_assignment(
        "healthy",
        *b"healthy-journal!",
        *b"healthy-verse!!!",
        *b"healthy-writer!!",
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        b_parts,
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    )
    .await;

    a_faults.arm(ArmedFault::DieAfterPayload);
    let pending_a = a
        .session
        .submit(Submission {
            producer_id: ProducerId::from_bytes(*b"multi-producer!!"),
            producer_epoch: 0,
            sequence: 0,
            records: vec![Record::new([], Bytes::from_static(b"wedged-payload"))],
        })
        .await
        .expect("A admits before append wedges");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), a.session.flush())
            .await
            .is_err(),
        "A LogDrive append must wedge after payload write"
    );
    assert_eq!(a_faults.fired_count(), 1);

    let sibling = tokio::time::timeout(
        Duration::from_secs(2),
        commit_one(&b.session, 0, b"sibling-ok"),
    )
    .await;
    assert!(
        sibling.is_ok(),
        "healthy assignment blocked while sibling LogDrive append is wedged"
    );

    // Release A's death gate so the runtime can unwind; must not become a committed ACK.
    a_faults.death_gate().open();
    let rejected = tokio::time::timeout(Duration::from_secs(1), pending_a)
        .await
        .expect("wedged receipt resolves once death gate opens");
    assert!(rejected.is_err(), "wedged append must not commit");

    let _keep = (a, b);
}

#[tokio::test]
async fn empty_bootstrap_race_exactly_one_serving() {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());
    let key = AuthorityKey {
        journal_id: JournalId::from_bytes(*b"race-journal!!!!"),
        verse_id: VerseId::from_bytes(*b"race-verse!!!!!!"),
    };
    let journal = *b"race-journal!!!!";
    let verse = *b"race-verse!!!!!!";

    let a = build_node(
        owner(),
        "tcp://race-a:9000",
        key,
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    );
    let b = build_node(
        owner_b(),
        "tcp://race-b:9000",
        key,
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    );

    let a_fut = bootstrap_and_serve(
        &a.coordinator,
        a.foundation.as_ref(),
        key,
        WriterTerm::new(1).expect("t1"),
        runtime_config_for(owner(), journal, verse, *b"race-writer-a!!!"),
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    );
    let b_fut = bootstrap_and_serve(
        &b.coordinator,
        b.foundation.as_ref(),
        key,
        WriterTerm::new(1).expect("t1"),
        runtime_config_for(owner_b(), journal, verse, *b"race-writer-b!!!"),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    );

    let (a_result, b_result) = tokio::join!(a_fut, b_fut);
    match (&a_result, &b_result) {
        (Ok(session), Err(_)) | (Err(_), Ok(session)) => {
            assert!(session.is_serving(), "winner must be Serving");
        }
        (Ok(_), Ok(_)) => panic!("both contenders became Serving"),
        (Err(a_err), Err(b_err)) => {
            panic!("both contenders failed: a={a_err}; b={b_err}")
        }
    }
}

#[tokio::test]
async fn both_alive_partition_promote_fence() {
    let run = RunId::new("multi-assignment-both-alive-promote-1");
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
        Arc::clone(&writer_faults),
        foundation_trace,
    )) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());
    let key = AuthorityKey {
        journal_id: JournalId::from_bytes(*b"fence-journal!!!"),
        verse_id: VerseId::from_bytes(*b"fence-verse!!!!!"),
    };
    let journal = *b"fence-journal!!!";
    let verse = *b"fence-verse!!!!!";

    let a = build_node(
        owner(),
        "tcp://fence-a:9000",
        key,
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    );
    let b = build_node(
        owner_b(),
        "tcp://fence-b:9000",
        key,
        Arc::clone(&register),
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    );

    let a_session = bootstrap_and_serve(
        &a.coordinator,
        a.foundation.as_ref(),
        key,
        WriterTerm::new(1).expect("t1"),
        runtime_config_for(owner(), journal, verse, *b"fence-writer-a!!"),
        Arc::clone(&register),
        Arc::clone(&a.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("A serving");
    let expected = a_session.generation().clone();

    writer_faults.arm(ArmedFault::DieAfterPayload);
    let pending = a_session
        .submit(Submission {
            producer_id: ProducerId::from_bytes(*b"multi-producer!!"),
            producer_epoch: 0,
            sequence: 0,
            records: vec![Record::new(
                [],
                Bytes::from_static(b"durable-but-uncommitted"),
            )],
        })
        .await
        .expect("A admits before wedge");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), a_session.flush())
            .await
            .is_err(),
        "A store append path must wedge"
    );

    writer_faults.death_gate().open();
    let rejected = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .expect("wedged receipt resolves");
    assert!(rejected.is_err());

    let active = expected.active_loglet_id.clone();
    drop(a_session);
    a.resolver.remove(&active);

    let b_session = promote_and_serve(
        &b.coordinator,
        b.foundation.as_ref(),
        key,
        WriterTerm::new(2).expect("t2"),
        expected,
        runtime_config_for(owner_b(), journal, verse, *b"fence-writer-b!!"),
        Arc::clone(&register),
        Arc::clone(&b.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("B promote_and_serve");
    assert!(b_session.is_effective_writer().await);

    let successor = b"committed-after-promote";
    let receipt = commit_one_receipt(&b_session, 1, successor).await;
    assert!(
        receipt.first_offset.get() >= 1,
        "lawful boundary after wedge"
    );
    trace_committed_ack(
        &ActorTrace::new(
            run,
            ActorId::new("scripture-b"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        ),
        "producer-0-sequence-1",
        &receipt,
        successor,
        &b_session.generation().active_loglet_id,
    );

    // A cannot regain committed ACKs after B holds Serving (stale writable gone).
    let a_gate = evaluate_authority_gate(
        key,
        Arc::clone(&register),
        Arc::clone(&a.resolver) as Arc<dyn LogletResolver>,
        owner(),
        a.resolver.is_writable(&active),
        false,
    )
    .await;
    assert!(
        matches!(a_gate, AuthorityGateDecision::Denied { .. }),
        "A must be denied after B promote fence"
    );

    let events = sink.events();
    assert!(
        matches!(check_trace(&events), Verdict::Pass),
        "promote fence trace must satisfy Holylog checker: {events:#?}"
    );
}

#[tokio::test]
async fn targeted_promote_does_not_disturb_sibling() {
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());

    // Verse A already Serving in this process.
    let a = bootstrap_assignment(
        "verse-a",
        *b"sibling-jrnl-a!!",
        *b"sibling-verse-a!",
        *b"sibling-wrtr-a!!",
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    )
    .await;
    commit_one(&a.session, 0, b"a-before-promote").await;

    // Verse B: bootstrap on owner A, then promote on owner B (standby path).
    let b_register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let b_key = AuthorityKey {
        journal_id: JournalId::from_bytes(*b"sibling-jrnl-b!!"),
        verse_id: VerseId::from_bytes(*b"sibling-verse-b!"),
    };
    let b_journal = *b"sibling-jrnl-b!!";
    let b_verse = *b"sibling-verse-b!";

    let b_primary = build_node(
        owner(),
        "tcp://verse-b-primary:9000",
        b_key,
        Arc::clone(&b_register),
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    );
    let b_standby = build_node(
        owner_b(),
        "tcp://verse-b-standby:9000",
        b_key,
        Arc::clone(&b_register),
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    );

    let b_first = bootstrap_and_serve(
        &b_primary.coordinator,
        b_primary.foundation.as_ref(),
        b_key,
        WriterTerm::new(1).expect("t1"),
        runtime_config(b_journal, b_verse, *b"sibling-wrtr-b!!"),
        Arc::clone(&b_register),
        Arc::clone(&b_primary.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("B primary bootstrap");
    let b_expected = b_first.generation().clone();
    let b_active = b_expected.active_loglet_id.clone();
    drop(b_first);
    b_primary.resolver.remove(&b_active);

    let b_promoted = promote_and_serve(
        &b_standby.coordinator,
        b_standby.foundation.as_ref(),
        b_key,
        WriterTerm::new(2).expect("t2"),
        b_expected,
        runtime_config_for(owner_b(), b_journal, b_verse, *b"sibling-wrtr-b2!"),
        Arc::clone(&b_register),
        Arc::clone(&b_standby.resolver),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("B targeted promote");
    assert!(b_promoted.is_serving());
    commit_one(&b_promoted, 0, b"b-after-promote").await;

    // Verse A was never touched by B's promote — still Serving and can commit.
    assert!(a.session.is_serving());
    commit_one(&a.session, 1, b"a-after-sibling-promote").await;

    let supervisor = ScribeSupervisor::from_assignments(
        ScribeResourceLimits::default(),
        vec![
            AssignmentRuntime::serving(
                "verse-a",
                a.key,
                a.session,
                a.store_root,
                a.advertise,
                default_budget(),
            ),
            AssignmentRuntime::serving(
                "verse-b",
                b_key,
                b_promoted,
                assignment_durable_root(
                    "memory",
                    JournalId::from_bytes(b_journal),
                    VerseId::from_bytes(b_verse),
                ),
                "tcp://verse-b-standby:9000",
                default_budget(),
            ),
        ],
    )
    .expect("supervisor");
    let status = supervisor.status_body();
    assert!(status.contains("assignment id=verse-a disposition=Serving"));
    assert!(status.contains("assignment id=verse-b disposition=Serving"));
    assert!(!status.contains("the scribe failed over"));
}

#[tokio::test]
async fn durable_root_and_advertise_evidence() {
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new());

    let a = bootstrap_assignment(
        "telemetry-host-a",
        *b"telemetry-jrnl!!",
        *b"telemetry-host-a",
        *b"telemetry-wrtr!!",
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&parts),
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    )
    .await;
    let b = bootstrap_assignment(
        "audit-ingress",
        *b"audit-journal!!!",
        *b"audit-ingress-0!",
        *b"audit-writer!!!!",
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        parts,
        Arc::clone(&claims) as Arc<dyn ExclusiveClaimStore>,
    )
    .await;

    assert!(a.store_root.contains("/cv/"));
    assert!(b.store_root.contains("/cv/"));
    assert_ne!(a.store_root, b.store_root);
    // Hex form of Canon/Verse bytes — not the ASCII assignment id as a path segment.
    assert!(
        a.store_root
            .chars()
            .filter(|c| c.is_ascii_hexdigit())
            .count()
            >= 64
    );
    assert_ne!(a.advertise, b.advertise);

    let standby = AssignmentRuntime::standby(
        "standby-a",
        a.key,
        a.store_root.clone(),
        "tcp://standby:9001".to_owned(),
        default_budget(),
    );
    assert!(!standby.admits_committed_acks());
    assert_eq!(standby.disposition.label(), "Standby");

    let supervisor = ScribeSupervisor::from_assignments(
        ScribeResourceLimits::default(),
        vec![
            AssignmentRuntime::serving(
                "telemetry-host-a",
                a.key,
                a.session,
                a.store_root.clone(),
                a.advertise.clone(),
                default_budget(),
            ),
            AssignmentRuntime::serving(
                "audit-ingress",
                b.key,
                b.session,
                b.store_root.clone(),
                b.advertise.clone(),
                default_budget(),
            ),
            standby,
        ],
    )
    .expect("supervisor");
    let status = supervisor.status_body();
    assert!(status.contains(&format!("store_root={}", a.store_root)));
    assert!(status.contains(&format!("store_root={}", b.store_root)));
    assert!(status.contains(&format!("advertise={}", a.advertise)));
    assert!(status.contains(&format!("advertise={}", b.advertise)));
    assert!(status.contains("standby_kind=dormant"));
    assert!(status.contains("/cv/"));
}

#[tokio::test]
async fn standby_assignment_does_not_admit_committed_acks() {
    let key = AuthorityKey {
        journal_id: JournalId::from_bytes(*b"standby-journal!"),
        verse_id: VerseId::from_bytes(*b"standby-verse!!!"),
    };
    let runtime = AssignmentRuntime::standby(
        "standby-a",
        key,
        assignment_durable_root("memory", key.journal_id, key.verse_id),
        "tcp://standby:9000",
        default_budget(),
    );
    assert!(!runtime.admits_committed_acks());
    assert_eq!(runtime.disposition.label(), "Standby");
}
