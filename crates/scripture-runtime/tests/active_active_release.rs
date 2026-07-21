//! Hermetic active-active release proof (foundation producer-continuity).
//!
//! One Canon, ≥2 Verses, ≥2 Scribes concurrently Serving disjoint Verses;
//! ContinuityOutbox + multi-bootstrap route refresh through a Verse promotion
//! under traffic; no stale committed receipt; sibling Verse isolation;
//! stable-identity duplicate accounting; contiguous consumer history.
//!
//! Authoritative gate is this in-memory/shared-memory harness — not k0s.
//! No operator promote/standby path; recovery uses `ScribeLifecycle`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
use holylog::virtual_log::{
    ConditionalRegister, InMemoryConditionalRegister, LogletResolver, VirtualLog,
};
use scripture::serving_authority::{AuthorityKey, RouteHint};
use scripture::{
    AckLevel, ChunkPolicy, CohortId, ContinuityOutbox, InMemorySpoolStorage, JournalId,
    OwnerEndpoint, OwnerId, ProducerId, Receipt, Record, RecoveryBound, Submission, SystemClock,
    SystemTimer, VerseId, WriterId, decode_chunk,
};
use scripture_runtime::{
    HaServingSession, HolylogJournalFoundation, InjectedPeerProbe, NodeIdentity, PartsFactory,
    ProcessLogletResolver, ScribeLifecycle, ScribeRunOptions, ScribeRunOutcome,
    SharedMemoryPartsFactory,
};
use scripture_service::{
    AuthorityCoordinator, DeterministicTransitionIdGenerator, JournalFoundationTransition,
    VerseRuntimeConfig,
};

fn canon() -> JournalId {
    JournalId::from_bytes(*b"aa-release-canon")
}
fn verse_alpha() -> VerseId {
    VerseId::from_bytes(*b"aa-verse-alpha!!")
}
fn verse_beta() -> VerseId {
    VerseId::from_bytes(*b"aa-verse-beta!!!")
}
fn owner_a() -> OwnerId {
    OwnerId::from_bytes(*b"aa-owner-node-a!")
}
fn owner_b() -> OwnerId {
    OwnerId::from_bytes(*b"aa-owner-node-b!")
}
fn owner_c() -> OwnerId {
    OwnerId::from_bytes(*b"aa-owner-node-c!")
}
fn key_alpha() -> AuthorityKey {
    AuthorityKey {
        journal_id: canon(),
        verse_id: verse_alpha(),
    }
}
fn key_beta() -> AuthorityKey {
    AuthorityKey {
        journal_id: canon(),
        verse_id: verse_beta(),
    }
}

fn runtime_config(owner: OwnerId, verse: VerseId, writer: [u8; 16]) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: canon(),
        verse_id: verse,
        owner_id: owner,
        cohort_id: CohortId::from_bytes(*b"aa-release-cohrt"),
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
        queue_capacity: 64,
        dataref_blobs: None,
        blob_sink: None,
        blob_verse_key: None,
    }
}

struct Node {
    owner: OwnerId,
    endpoint: String,
    resolver: Arc<ProcessLogletResolver>,
    foundation: Arc<HolylogJournalFoundation>,
    coordinator: AuthorityCoordinator,
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    key: AuthorityKey,
    verse: VerseId,
    writer_bytes: [u8; 16],
}

#[allow(clippy::too_many_arguments)]
fn build_node(
    owner: OwnerId,
    endpoint: &str,
    key: AuthorityKey,
    verse: VerseId,
    writer_bytes: [u8; 16],
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
) -> Node {
    let resolver = Arc::new(ProcessLogletResolver::default());
    let identity = NodeIdentity {
        owner_id: owner,
        endpoint: OwnerEndpoint::new(endpoint).expect("ep"),
    };
    let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
        key,
        identity,
        Arc::clone(&register),
        Arc::clone(&resolver),
        Arc::clone(&parts),
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
    Node {
        owner,
        endpoint: endpoint.to_owned(),
        resolver,
        foundation,
        coordinator,
        register,
        parts,
        key,
        verse,
        writer_bytes,
    }
}

fn lifecycle<'a>(
    node: &'a Node,
    peer: Arc<InjectedPeerProbe>,
) -> ScribeLifecycle<'a, SystemClock, SystemTimer> {
    let (clock, timer) = (SystemClock::new(), SystemTimer::new());
    ScribeLifecycle {
        coordinator: &node.coordinator,
        foundation: node.foundation.as_ref(),
        key: node.key,
        owner_id: node.owner,
        runtime_config: runtime_config(node.owner, node.verse, node.writer_bytes),
        register: Arc::clone(&node.register),
        resolver: Arc::clone(&node.resolver),
        parts: Arc::clone(&node.parts),
        clock,
        timer,
        options: ScribeRunOptions {
            peer_grace: Duration::from_millis(50),
            initial_term: 1,
        },
        peer,
    }
}

async fn read_payloads(
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
) -> Vec<String> {
    let resolver = Arc::new(ProcessLogletResolver::default());
    let log = VirtualLog::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
    );
    let observed = log.observe_membership().await.expect("membership");
    let mut end = 0_u64;
    for generation in &observed.state.generations {
        let durable = parts.open(&generation.loglet_id).expect("open");
        let view = holylog::provision::resolve_read_seal(durable.components(2))
            .await
            .expect("seal");
        let tail = view
            .observe_durable()
            .await
            .expect("durable")
            .contiguous_tail();
        resolver.insert_read_seal(generation.loglet_id.clone(), Arc::new(view));
        end = generation.start.saturating_add(tail);
    }
    let mut payloads = Vec::new();
    let mut cursor = 0_u64;
    while cursor < end {
        let entry = log.read_next(cursor, end).await.expect("read");
        if let Ok(chunk) = decode_chunk(&entry.payload) {
            for frame in &chunk.frames {
                for record in &frame.records {
                    payloads.push(String::from_utf8_lossy(record.payload.as_ref()).into_owned());
                }
            }
        }
        cursor = entry.position.saturating_add(1);
    }
    payloads
}

/// Hermetic multi-bootstrap route table: ranked candidates per verse.
///
/// A route is never write authority — submit still goes through the Serving
/// Authority gate on the chosen session. ≥2 bootstrap candidates per verse
/// satisfy the foundation producer bootstrap-set requirement.
struct BootstrapRouteTable {
    ranked: Mutex<BTreeMap<VerseId, Vec<String>>>,
    sessions: Mutex<BTreeMap<String, Option<Arc<HaServingSession>>>>,
}

impl BootstrapRouteTable {
    fn new() -> Self {
        Self {
            ranked: Mutex::new(BTreeMap::new()),
            sessions: Mutex::new(BTreeMap::new()),
        }
    }

    fn set_ranked(&self, verse: VerseId, endpoints: Vec<String>) {
        assert!(
            endpoints.len() >= 2,
            "active-active proof requires >1 bootstrap route"
        );
        self.ranked.lock().expect("ranked").insert(verse, endpoints);
    }

    fn publish(&self, endpoint: &str, session: Option<Arc<HaServingSession>>) {
        self.sessions
            .lock()
            .expect("sessions")
            .insert(endpoint.to_owned(), session);
    }

    fn refresh_candidates(&self, verse: VerseId) -> Vec<String> {
        self.ranked
            .lock()
            .expect("ranked")
            .get(&verse)
            .cloned()
            .unwrap_or_default()
    }

    async fn try_commit(&self, endpoint: &str, submission: Submission) -> Result<Receipt, String> {
        let session = {
            let guard = self.sessions.lock().expect("sessions");
            guard.get(endpoint).and_then(|s| s.clone())
        };
        let Some(session) = session else {
            return Err(format!("dead-bootstrap:{endpoint}"));
        };
        let pending = session
            .submit(submission)
            .await
            .map_err(|e| format!("gate:{e}"))?;
        session.flush().await.map_err(|e| format!("flush:{e}"))?;
        let receipt = pending.await.map_err(|e| format!("commit:{e}"))?;
        if receipt.level != AckLevel::Committed {
            return Err(format!("non-committed:{:?}", receipt.level));
        }
        Ok(receipt)
    }

    /// Admit into the durable outbox, then forward via ranked bootstrap routes
    /// with refresh on stale/deposed/unavailable candidates until committed.
    async fn produce_committed(
        &self,
        verse: VerseId,
        outbox: &ContinuityOutbox<InMemorySpoolStorage>,
        submission: Submission,
    ) -> Receipt {
        let id = outbox.admit(submission.clone()).expect("admit");
        let mut last_err = String::from("no-attempt");
        for _refresh in 0..8 {
            let candidates = self.refresh_candidates(verse);
            assert!(
                candidates.len() >= 2,
                "bootstrap set must stay multi-candidate after refresh"
            );
            for endpoint in candidates {
                match self.try_commit(&endpoint, submission.clone()).await {
                    Ok(receipt) => {
                        outbox
                            .mark_committed_with_receipt(id, receipt.clone())
                            .expect("progress");
                        return receipt;
                    }
                    Err(err) => {
                        last_err = err;
                        // Refresh and try next ranked candidate (route ≠ write grant).
                    }
                }
            }
        }
        panic!("exhausted multi-bootstrap routes for {verse:?}: {last_err}");
    }
}

#[tokio::test]
async fn active_active_multi_verse_promotion_under_traffic() {
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let register_alpha =
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let register_beta =
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let peer_alpha = Arc::new(InjectedPeerProbe::new(true));
    let peer_beta = Arc::new(InjectedPeerProbe::new(true));

    // node-a serves verse-alpha; node-b serves verse-beta; node-c is alpha successor.
    let node_a = build_node(
        owner_a(),
        "tcp://10.0.0.1:9000",
        key_alpha(),
        verse_alpha(),
        *b"aa-writer-a!!!!!",
        Arc::clone(&register_alpha),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let node_b = build_node(
        owner_b(),
        "tcp://10.0.0.2:9000",
        key_beta(),
        verse_beta(),
        *b"aa-writer-b!!!!!",
        Arc::clone(&register_beta),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );
    let node_c = build_node(
        owner_c(),
        "tcp://10.0.0.3:9000",
        key_alpha(),
        verse_alpha(),
        *b"aa-writer-c!!!!!",
        Arc::clone(&register_alpha),
        Arc::clone(&parts),
        Arc::clone(&claims),
    );

    let a_life = lifecycle(&node_a, Arc::clone(&peer_alpha));
    let writer_alpha = match a_life
        .reconcile_once(false)
        .await
        .expect("node-a bootstrap")
    {
        ScribeRunOutcome::LawfulWriter(session) => Arc::new(session),
        ScribeRunOutcome::HealthyMember(_) => panic!("node-a must bootstrap as writer"),
    };
    assert!(writer_alpha.is_effective_writer().await);

    let b_life = lifecycle(&node_b, Arc::clone(&peer_beta));
    let writer_beta = match b_life
        .reconcile_once(false)
        .await
        .expect("node-b bootstrap")
    {
        ScribeRunOutcome::LawfulWriter(session) => Arc::new(session),
        ScribeRunOutcome::HealthyMember(_) => panic!("node-b must bootstrap as writer"),
    };
    assert!(writer_beta.is_effective_writer().await);

    // node-c joins verse-alpha as healthy non-writer while node-a serves.
    let c_life = lifecycle(&node_c, Arc::clone(&peer_alpha));
    let member_c = match c_life.reconcile_once(false).await.expect("node-c join") {
        ScribeRunOutcome::HealthyMember(member) => member,
        ScribeRunOutcome::LawfulWriter(_) => panic!("node-c must not steal while node-a reachable"),
    };
    assert!(member_c.member_ready);
    member_c.ensure_not_writer().await.expect("c not writer");

    let routes = BootstrapRouteTable::new();
    // >1 bootstrap route for each Verse (foundation requirement 2).
    routes.set_ranked(
        verse_alpha(),
        vec![node_a.endpoint.clone(), node_c.endpoint.clone()],
    );
    routes.set_ranked(
        verse_beta(),
        vec![
            node_b.endpoint.clone(),
            "tcp://10.0.0.4:9000".to_owned(), // second bootstrap; unused while B serves
        ],
    );
    routes.publish(&node_a.endpoint, Some(Arc::clone(&writer_alpha)));
    routes.publish(&node_b.endpoint, Some(Arc::clone(&writer_beta)));
    routes.publish(&node_c.endpoint, None); // not yet serving
    routes.publish("tcp://10.0.0.4:9000", None);

    let outbox_alpha = ContinuityOutbox::memory(canon(), 256);
    let outbox_beta = ContinuityOutbox::memory(canon(), 256);
    let producer_alpha = ProducerId::from_bytes(*b"aa-prod-alpha!!!");
    let producer_beta = ProducerId::from_bytes(*b"aa-prod-beta!!!!");

    // Concurrent committed traffic on both Verses of one Canon.
    for sequence in 0..8 {
        let alpha_submission = Submission {
            producer_id: producer_alpha,
            producer_epoch: 1,
            sequence,
            records: vec![Record::new(
                [],
                Bytes::from(format!("alpha-pre-{sequence}")),
            )],
        };
        let beta_submission = Submission {
            producer_id: producer_beta,
            producer_epoch: 1,
            sequence,
            records: vec![Record::new([], Bytes::from(format!("beta-pre-{sequence}")))],
        };
        let (alpha_receipt, beta_receipt) = tokio::join!(
            routes.produce_committed(verse_alpha(), &outbox_alpha, alpha_submission),
            routes.produce_committed(verse_beta(), &outbox_beta, beta_submission),
        );
        assert_eq!(alpha_receipt.level, AckLevel::Committed);
        assert_eq!(beta_receipt.level, AckLevel::Committed);
    }

    // Admit pending work into the durable outbox before/during promotion.
    let mut pending_alpha = Vec::new();
    for sequence in 8..14 {
        let payload = format!("alpha-cutover-{sequence}");
        let submission = Submission {
            producer_id: producer_alpha,
            producer_epoch: 1,
            sequence,
            records: vec![Record::new([], Bytes::from(payload))],
        };
        let id = outbox_alpha
            .admit(submission.clone())
            .expect("admit pending");
        pending_alpha.push((id, submission));
    }

    // Promote verse-alpha under traffic via durable-CAS lifecycle (not promote CLI).
    let expected_alpha = writer_alpha.generation().clone();
    node_a.resolver.remove(&expected_alpha.active_loglet_id);
    routes.publish(&node_a.endpoint, None);
    drop(writer_alpha);
    peer_alpha.set_reachable(false);

    // While alpha has durable admissions awaiting the successor, the unrelated
    // Verse continues committing concurrently with the lawful transition.
    let beta_during_transition = Submission {
        producer_id: producer_beta,
        producer_epoch: 1,
        sequence: 8,
        records: vec![Record::new([], Bytes::from_static(b"beta-during-8"))],
    };
    let (alpha_transition, beta_during_receipt) = tokio::join!(
        c_life.reconcile_once(true),
        routes.produce_committed(verse_beta(), &outbox_beta, beta_during_transition),
    );
    assert_eq!(beta_during_receipt.level, AckLevel::Committed);
    let writer_alpha2 = match alpha_transition.expect("node-c recover") {
        ScribeRunOutcome::LawfulWriter(session) => Arc::new(session),
        ScribeRunOutcome::HealthyMember(member) => {
            panic!("node-c should recover verse-alpha: {}", member.reason)
        }
    };
    peer_alpha.set_reachable(true);
    routes.publish(&node_c.endpoint, Some(Arc::clone(&writer_alpha2)));
    // Rank refresh: former writer still listed first (stale), successor second.
    routes.set_ranked(
        verse_alpha(),
        vec![node_a.endpoint.clone(), node_c.endpoint.clone()],
    );

    // Stale route / former writer must not emit a committed receipt.
    assert!(
        !node_a
            .resolver
            .is_writable(&expected_alpha.active_loglet_id)
    );
    let stale_attempt = routes
        .try_commit(
            &node_a.endpoint,
            Submission {
                producer_id: producer_alpha,
                producer_epoch: 1,
                sequence: 999,
                records: vec![Record::new([], Bytes::from_static(b"stale"))],
            },
        )
        .await;
    assert!(
        stale_attempt.is_err(),
        "former writer must not return committed after successor is lawful"
    );

    // Drain every locally durable alpha admission via multi-bootstrap refresh.
    for (id, submission) in pending_alpha {
        let mut committed = None;
        for _ in 0..8 {
            let candidates = routes.refresh_candidates(verse_alpha());
            for endpoint in candidates {
                match routes.try_commit(&endpoint, submission.clone()).await {
                    Ok(receipt) => {
                        outbox_alpha
                            .mark_committed_with_receipt(id, receipt.clone())
                            .expect("progress");
                        committed = Some(receipt);
                        break;
                    }
                    Err(_) => continue,
                }
            }
            if committed.is_some() {
                break;
            }
        }
        assert!(
            committed.is_some(),
            "every outbox-admitted alpha identity must eventually commit"
        );
    }

    // Continue alpha traffic post-promotion (hits stale first candidate, then C).
    for sequence in 14..20 {
        let payload = format!("alpha-post-{sequence}");
        let submission = Submission {
            producer_id: producer_alpha,
            producer_epoch: 1,
            sequence,
            records: vec![Record::new([], Bytes::from(payload))],
        };
        let receipt = routes
            .produce_committed(verse_alpha(), &outbox_alpha, submission)
            .await;
        assert_eq!(receipt.level, AckLevel::Committed);
    }

    // Unrelated verse-beta keeps committing through the promotion window.
    for sequence in 9..16 {
        let payload = format!("beta-during-{sequence}");
        let submission = Submission {
            producer_id: producer_beta,
            producer_epoch: 1,
            sequence,
            records: vec![Record::new([], Bytes::from(payload))],
        };
        let receipt = routes
            .produce_committed(verse_beta(), &outbox_beta, submission)
            .await;
        assert_eq!(receipt.level, AckLevel::Committed);
        assert!(writer_beta.is_effective_writer().await);
    }

    assert!(outbox_alpha.fully_drained());
    assert!(outbox_beta.fully_drained());
    let snap_a = outbox_alpha.snapshot();
    let snap_b = outbox_beta.snapshot();
    assert_eq!(snap_a.pending, 0);
    assert_eq!(snap_a.local_durable.len(), 20);
    assert_eq!(snap_a.committed.len(), 20);
    assert_eq!(snap_b.local_durable.len(), 16);
    assert_eq!(snap_b.committed.len(), 16);

    // Stable-identity duplicate accounting: refuse a second logical admit.
    let dup = Submission {
        producer_id: producer_alpha,
        producer_epoch: 1,
        sequence: 0,
        records: vec![Record::new([], Bytes::from_static(b"dup"))],
    };
    assert!(outbox_alpha.admit(dup).is_err());

    // Contiguous consumer history for each Verse across the promotion.
    let alpha_payloads = read_payloads(Arc::clone(&register_alpha), Arc::clone(&parts)).await;
    let beta_payloads = read_payloads(Arc::clone(&register_beta), Arc::clone(&parts)).await;
    assert_eq!(alpha_payloads.len(), 20, "alpha contiguous history");
    assert_eq!(alpha_payloads[0], "alpha-pre-0");
    assert_eq!(alpha_payloads[8], "alpha-cutover-8");
    assert_eq!(alpha_payloads[14], "alpha-post-14");
    assert_eq!(alpha_payloads[19], "alpha-post-19");
    assert_eq!(beta_payloads.len(), 16, "beta contiguous history");
    assert_eq!(beta_payloads[0], "beta-pre-0");
    assert_eq!(beta_payloads[8], "beta-during-8");
    assert_eq!(beta_payloads[15], "beta-during-15");
}

#[tokio::test]
async fn help_docs_prefer_lifecycle_not_standby_promote() {
    let lifecycle = include_str!("../src/scribe_lifecycle.rs");
    let routing = include_str!("../src/producer_routing.rs");
    assert!(lifecycle.contains("ScribeLifecycle") || lifecycle.contains("scribe run"));
    assert!(!lifecycle.contains("posture: standby"));
    assert!(!routing.contains("posture: standby"));
    // Decision doc names the hermetic proof without advertising promote as normal.
    let decision = include_str!("../../../docs/decisions/0015-multi-scribe-producer-continuity.md");
    assert!(decision.contains("active_active_release") || decision.contains("active-active"));
    assert!(decision.contains("no standby") || decision.contains("no operator promote"));
}
