//! Composition suite: multi-scribe HA with producer-edge continuity.
//!
//! This is deliberately **different** from the legacy drain→seal→replace path
//! that accepts a scribe-side loss budget. Here:
//!
//! 1. Multiple Scribes serve disjoint Verses concurrently (active-active).
//! 2. A ContinuityOutbox admits every record before routing (local-durable).
//! 3. Continuous produce keeps running while Scribes are restarted one-by-one.
//! 4. Temporary route failures retain pending work; after promote, drain resumes.
//! 5. The proof is: every locally durable identity eventually commits — zero drop.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
use holylog::virtual_log::{ConditionalRegister, InMemoryConditionalRegister, LogletResolver};
use scripture::serving_authority::{AuthorityKey, JournalGenerationRef, RouteHint, WriterTerm};
use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, ProducerId, Record, RecoveryBound,
    Submission, SystemClock, SystemTimer, VerseId, WriterId,
};
use scripture_producer::{ContinuityId, ContinuityOutbox, PendingEntry};
use scripture_runtime::{
    HaServingSession, HolylogJournalFoundation, NodeIdentity, PartsFactory, ProcessLogletResolver,
    SharedMemoryPartsFactory, bootstrap_and_serve, promote_and_serve,
};
use scripture_service::{
    AuthorityCoordinator, DeterministicTransitionIdGenerator, JournalFoundationTransition,
    VerseRuntimeConfig,
};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::CampaignError;

const FOUNDATION_K: u64 = 2;
const SCRIBE_COUNT: usize = 3;
/// Bursts of concurrent produce across all lanes (each burst = SCRIBE_COUNT records).
const PRODUCE_BURST: u64 = 200;
const RESTART_PAUSE: Duration = Duration::from_millis(15);

#[derive(Clone, Copy)]
struct VerseLane {
    index: usize,
    verse: VerseId,
    owner_a: OwnerId,
    owner_b: OwnerId,
}

fn journal() -> JournalId {
    JournalId::from_bytes(*b"fleet-journal!!!")
}

fn producer_for(lane: VerseLane) -> ProducerId {
    let mut bytes = *b"fleet-prod-0!!!!";
    bytes[10] = b'0' + lane.index as u8;
    ProducerId::from_bytes(bytes)
}

fn submission_for(sequence: u64, lane: VerseLane) -> Submission {
    Submission {
        producer_id: producer_for(lane),
        producer_epoch: 1,
        sequence,
        records: vec![Record::new(
            [],
            Bytes::from(format!("lane-{}-seq-{sequence}", lane.index)),
        )],
    }
}

fn lane(index: usize) -> VerseLane {
    let mut verse = *b"fleet-verse-0!!!";
    verse[12] = b'0' + index as u8;
    let mut owner_a = *b"fleet-own-a-0!!!";
    owner_a[13] = b'0' + index as u8;
    let mut owner_b = *b"fleet-own-b-0!!!";
    owner_b[13] = b'0' + index as u8;
    VerseLane {
        index,
        verse: VerseId::from_bytes(verse),
        owner_a: OwnerId::from_bytes(owner_a),
        owner_b: OwnerId::from_bytes(owner_b),
    }
}

fn key_for(lane: VerseLane) -> AuthorityKey {
    AuthorityKey {
        journal_id: journal(),
        verse_id: lane.verse,
    }
}

fn config_for(lane: VerseLane, owner: OwnerId) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: journal(),
        verse_id: lane.verse,
        owner_id: owner,
        cohort_id: CohortId::from_bytes(*b"fleet-cohort!!!!"),
        writer_id: WriterId::from_bytes(*b"fleet-writer!!!!"),
        policy: ChunkPolicy {
            max_chunk_bytes: 64 * 1024,
            max_record_bytes: 16 * 1024,
            max_chunk_records: 8,
            max_chunk_age: Duration::from_secs(60),
            max_buffered_bytes: 64 * 1024,
            max_inflight_chunks: 1,
            max_uncommitted_age: Duration::from_secs(60),
            recovery_scan: RecoveryBound::new(8).expect("recovery bound"),
        },
        recovery_bound: RecoveryBound::new(8).expect("recovery bound"),
        queue_capacity: 16,
    }
}

struct ScribeBundle {
    lane: VerseLane,
    register: Arc<dyn ConditionalRegister>,
    parts: Arc<dyn PartsFactory>,
    claims: Arc<dyn ExclusiveClaimStore>,
    resolver: Mutex<Arc<ProcessLogletResolver>>,
    session: Mutex<Option<HaServingSession>>,
    term: Mutex<u64>,
}

impl ScribeBundle {
    fn new(
        lane: VerseLane,
        parts: Arc<dyn PartsFactory>,
        claims: Arc<dyn ExclusiveClaimStore>,
    ) -> Result<Self, CampaignError> {
        let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
        Ok(Self {
            lane,
            register,
            parts,
            claims,
            resolver: Mutex::new(Arc::new(ProcessLogletResolver::default())),
            session: Mutex::new(None),
            term: Mutex::new(1),
        })
    }

    fn build_node(
        &self,
        owner: OwnerId,
        endpoint: &str,
        resolver: Arc<ProcessLogletResolver>,
    ) -> Result<(Arc<HolylogJournalFoundation>, AuthorityCoordinator), CampaignError> {
        let identity = NodeIdentity {
            owner_id: owner,
            endpoint: OwnerEndpoint::new(endpoint)
                .map_err(|error| CampaignError::Scenario(format!("endpoint: {error}")))?,
        };
        let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
            key_for(self.lane),
            identity,
            Arc::clone(&self.register),
            Arc::clone(&resolver),
            Arc::clone(&self.parts),
            Arc::clone(&self.claims),
            FOUNDATION_K,
        ));
        let coordinator = AuthorityCoordinator::new(
            Arc::clone(&self.register),
            Arc::clone(&resolver) as Arc<dyn LogletResolver>,
            Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
            Arc::new(DeterministicTransitionIdGenerator::new()),
            owner,
            RouteHint::new(endpoint)
                .map_err(|error| CampaignError::Scenario(format!("route: {error}")))?,
        );
        Ok((foundation, coordinator))
    }

    async fn bootstrap_a(&self) -> Result<(), CampaignError> {
        let resolver = Arc::clone(&*self.resolver.lock().await);
        let endpoint = format!("tcp://fleet-scribe-{}:9000", self.lane.index);
        let (foundation, coordinator) =
            self.build_node(self.lane.owner_a, &endpoint, Arc::clone(&resolver))?;
        let term = *self.term.lock().await;
        let session = bootstrap_and_serve(
            &coordinator,
            &foundation,
            key_for(self.lane),
            WriterTerm::new(term)
                .map_err(|error| CampaignError::Scenario(format!("term: {error}")))?,
            config_for(self.lane, self.lane.owner_a),
            Arc::clone(&self.register),
            resolver,
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .map_err(|error| {
            CampaignError::Scenario(format!("bootstrap lane {}: {error}", self.lane.index))
        })?;
        *self.session.lock().await = Some(session);
        Ok(())
    }

    /// Stops the live session without draining producer work (outbox absorbs).
    async fn crash_for_restart(&self) -> Result<JournalGenerationRef, CampaignError> {
        let mut guard = self.session.lock().await;
        let session = guard.take().ok_or_else(|| {
            CampaignError::Scenario(format!("lane {} not serving", self.lane.index))
        })?;
        let generation = session.generation().clone();
        let active = generation.active_loglet_id.clone();
        drop(session);
        self.resolver.lock().await.remove(&active);
        Ok(generation)
    }

    async fn promote_b(&self, expected: JournalGenerationRef) -> Result<(), CampaignError> {
        self.promote_owner(self.lane.owner_b, "b", expected).await
    }

    async fn promote_owner(
        &self,
        owner: OwnerId,
        tag: &str,
        expected: JournalGenerationRef,
    ) -> Result<(), CampaignError> {
        let mut term = self.term.lock().await;
        *term += 1;
        let next = *term;
        drop(term);

        let resolver = Arc::new(ProcessLogletResolver::default());
        let endpoint = format!("tcp://fleet-scribe-{}-{tag}:9000", self.lane.index);
        let (foundation, coordinator) = self.build_node(owner, &endpoint, Arc::clone(&resolver))?;
        let session = promote_and_serve(
            &coordinator,
            &foundation,
            key_for(self.lane),
            WriterTerm::new(next)
                .map_err(|error| CampaignError::Scenario(format!("term {tag}: {error}")))?,
            expected,
            config_for(self.lane, owner),
            Arc::clone(&self.register),
            Arc::clone(&resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .map_err(|error| {
            CampaignError::Scenario(format!(
                "promote {tag} lane {}: {error}",
                self.lane.index
            ))
        })?;
        *self.resolver.lock().await = resolver;
        *self.session.lock().await = Some(session);
        Ok(())
    }
}

async fn forward_one(
    lanes: &BTreeMap<usize, Arc<ScribeBundle>>,
    outbox: &ContinuityOutbox,
    lane_index: usize,
    entry: &PendingEntry,
) -> bool {
    let Some(bundle) = lanes.get(&lane_index) else {
        return false;
    };
    let guard = bundle.session.lock().await;
    let Some(session) = guard.as_ref() else {
        return false;
    };
    match session.submit(entry.submission.clone()).await {
        Ok(pending) => {
            if session.flush().await.is_err() {
                return false;
            }
            match pending.await {
                Ok(receipt) => {
                    outbox.mark_committed_with_receipt(entry.id, receipt);
                    true
                }
                Err(_) => false,
            }
        }
        Err(_) => false,
    }
}

async fn drain_pending(
    lanes: &BTreeMap<usize, Arc<ScribeBundle>>,
    outbox: &ContinuityOutbox,
    lane_of: &BTreeMap<ContinuityId, usize>,
) {
    for entry in outbox.pending_snapshot() {
        let Some(&lane_index) = lane_of.get(&entry.id) else {
            continue;
        };
        let _ = forward_one(lanes, outbox, lane_index, &entry).await;
    }
}

/// Runs the multi-scribe rolling-restart continuity proof (in-memory backend).
pub(crate) async fn run_multi_scribe_rolling_restart(
    _run_id: &str,
) -> Result<(serde_json::Value, serde_json::Value), CampaignError> {
    // One shared parts factory across all lane promotions so sealed gens reopen.
    let parts = Arc::new(SharedMemoryPartsFactory::default()) as Arc<dyn PartsFactory>;

    let mut lanes = BTreeMap::new();
    for index in 0..SCRIBE_COUNT {
        // Per-lane claim store (exclusive namespaces are lane-scoped via loglet ids).
        let claims =
            Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
        let bundle = Arc::new(ScribeBundle::new(
            lane(index),
            Arc::clone(&parts),
            claims,
        )?);
        bundle.bootstrap_a().await?;
        lanes.insert(index, bundle);
    }

    let outbox = Arc::new(ContinuityOutbox::new(10_000));
    let lane_of = Arc::new(Mutex::new(BTreeMap::<ContinuityId, usize>::new()));

    let produce_lanes = lanes.clone();
    let produce_outbox = Arc::clone(&outbox);
    let produce_map = Arc::clone(&lane_of);
    let producer_task = tokio::spawn(async move {
        // Dense sequences are per-Verse driver — keep a counter per lane.
        let mut per_lane_seq = [0_u64; SCRIBE_COUNT];
        let mut admitted = 0_u64;
        for _ in 0..PRODUCE_BURST {
            for index in 0..SCRIBE_COUNT {
                let lane = lane(index);
                let sequence = per_lane_seq[index];
                per_lane_seq[index] += 1;
                let submission = submission_for(sequence, lane);
                let id = produce_outbox
                    .admit(submission.clone())
                    .map_err(|error| CampaignError::Scenario(format!("admit: {error}")))?;
                produce_map.lock().await.insert(id, index);
                let entry = PendingEntry { id, submission };
                let _ = forward_one(&produce_lanes, &produce_outbox, index, &entry).await;
                admitted += 1;
            }
            sleep(Duration::from_millis(5)).await;
            let map = produce_map.lock().await.clone();
            drain_pending(&produce_lanes, &produce_outbox, &map).await;
        }
        Ok::<u64, CampaignError>(admitted)
    });

    // One rolling pass: crash each serving scribe and promote its standby while
        // the producer keeps admitting into the continuity outbox.
        for index in 0..SCRIBE_COUNT {
            sleep(RESTART_PAUSE).await;
            let bundle = lanes
                .get(&index)
                .ok_or_else(|| CampaignError::Scenario("missing lane".into()))?;
            let expected = bundle.crash_for_restart().await?;
            sleep(RESTART_PAUSE).await;
            bundle.promote_b(expected).await?;
            let map = lane_of.lock().await.clone();
            drain_pending(&lanes, &outbox, &map).await;
        }

    let produced = producer_task
        .await
        .map_err(|error| CampaignError::Scenario(format!("producer join: {error}")))??;

    for _ in 0..200 {
        let map = lane_of.lock().await.clone();
        drain_pending(&lanes, &outbox, &map).await;
        if outbox.fully_drained() {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }

    let snap = outbox.snapshot();
    if !outbox.fully_drained() {
        return Err(CampaignError::Scenario(format!(
            "continuity not drained after rolling restart: pending={} local={} committed={}",
            snap.pending,
            snap.local_durable.len(),
            snap.committed.len()
        )));
    }
    if snap.local_durable.len() as u64 != produced {
        return Err(CampaignError::Scenario(format!(
            "local_durable count {} != produced {produced}",
            snap.local_durable.len()
        )));
    }

    let final_root = serde_json::json!({
        "design": "producer-edge-continuity",
        "scribes": SCRIBE_COUNT,
        "produced": produced,
        "committed": snap.committed.len(),
        "pending": snap.pending,
        "dropped": 0,
    });
    let final_authority = serde_json::json!({
        "invariant": "every locally durable submission committed after rolling restart",
        "dropped": 0,
    });
    Ok((final_root, final_authority))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn multi_scribe_rolling_restart_drops_nothing() {
        let (root, authority) = run_multi_scribe_rolling_restart("test-run")
            .await
            .expect("rolling restart continuity");
        assert_eq!(root["dropped"], 0);
        assert_eq!(authority["dropped"], 0);
        assert_eq!(root["pending"], 0);
        assert!(root["committed"].as_u64().unwrap_or(0) > 0);
        assert_eq!(root["produced"], root["committed"]);
    }
}
