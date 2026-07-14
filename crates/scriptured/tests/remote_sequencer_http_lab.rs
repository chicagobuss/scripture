//! Phase B1.2 lab integration: Canon admission gates remote sequencer HTTP.
//!
//! Run with:
//! `cargo test -p scriptured --features remote-sequencer-http-lab --locked`

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use holylog::atomic::{AtomicLog, InMemorySeal, InMemoryTrimPoint, Sequencer, SequencerError};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::remote_sequencer::{ActivateOutcome, InMemoryRemoteSequencer, SequencerEpoch};
use holylog::virtual_log::InMemoryConditionalRegister;
use holylog_remote_sequencer_http::{
    FixedCapabilityAuthenticator, RemoteSequencerHttpClient, RemoteSequencerHttpMetrics,
    RemoteSequencerHttpServer, SequencerCapability,
};
use scripture::{
    CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, OwnedSequencerBinding, OwnerEndpoint,
    OwnerId, ProducerId, RecoveryBound, SystemClock, VerseId, WriterId,
    sequencer_request_key_for_submission,
};
use scripture_service::{
    AdmissionDisposition, CanonOwnerRequest, CanonRoute, ChunkJournalService, LocalCanonOwnerMatch,
    admission_for, recover_canon_owner, resolve_canon_route, resolve_canon_route_with_epoch,
    virtuallog_test_support::VirtualLogHarness,
};
use tokio::net::TcpListener;

struct LabFixture {
    server: RemoteSequencerHttpServer,
    base_url: String,
    capability: SequencerCapability,
    epoch: SequencerEpoch,
    k: u64,
    server_metrics: Arc<RemoteSequencerHttpMetrics>,
}

async fn spawn_lab(k: u64, epoch: SequencerEpoch) -> LabFixture {
    let mut sequencer = InMemoryRemoteSequencer::new();
    assert_eq!(sequencer.activate(epoch, k, 0), ActivateOutcome::Active);

    let capability = SequencerCapability::test();
    let authenticator = Arc::new(FixedCapabilityAuthenticator::new(capability));
    let server_metrics = Arc::new(RemoteSequencerHttpMetrics::new());
    let server =
        RemoteSequencerHttpServer::new(sequencer, authenticator, Arc::clone(&server_metrics));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    server.spawn(listener).expect("spawn");

    LabFixture {
        server,
        base_url: format!("http://{addr}"),
        capability,
        epoch,
        k,
        server_metrics,
    }
}

fn http_client(
    fixture: &LabFixture,
    client_metrics: Arc<RemoteSequencerHttpMetrics>,
) -> RemoteSequencerHttpClient {
    RemoteSequencerHttpClient::new(
        &fixture.base_url,
        fixture.capability,
        fixture.epoch,
        fixture.k,
        client_metrics,
    )
}

fn journal() -> JournalId {
    JournalId::from_bytes(*b"http-lab-jrnl!!!")
}

fn verse() -> VerseId {
    VerseId::from_bytes(*b"http-lab-verse!!")
}

fn owner() -> OwnerId {
    OwnerId::from_bytes(*b"http-lab-owner!!")
}

fn cohort() -> CohortId {
    CohortId::from_bytes(*b"http-lab-cohort!")
}

fn writer_id() -> WriterId {
    WriterId::from_bytes(*b"http-lab-writer!")
}

fn policy() -> ChunkPolicy {
    ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 8,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 64 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    }
}

fn owned_v2(epoch: SequencerEpoch) -> CanonOwner {
    let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
    CanonOwner::Owned {
        owner_id: owner(),
        endpoint: endpoint.clone(),
        sequencer: Some(OwnedSequencerBinding {
            epoch,
            sequencer_endpoint: endpoint,
        }),
    }
}

fn owned_v1() -> CanonOwner {
    let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
    CanonOwner::Owned {
        owner_id: owner(),
        endpoint,
        sequencer: None,
    }
}

fn fence_v2(revision: u64, epoch: SequencerEpoch) -> CanonFence {
    CanonFence::new(revision, journal(), verse(), owned_v2(epoch))
}

fn fence_v1(revision: u64) -> CanonFence {
    CanonFence::new(revision, journal(), verse(), owned_v1())
}

fn canon_owner_request() -> CanonOwnerRequest {
    CanonOwnerRequest {
        journal_id: journal(),
        verse_id: verse(),
        owner_id: owner(),
        cohort_id: cohort(),
        writer_id: writer_id(),
        policy: policy(),
        recovery_bound: RecoveryBound::new(8).expect("bound"),
        queue_capacity: 16,
    }
}

async fn http_harness() -> VirtualLogHarness {
    VirtualLogHarness::with_ids(
        "http-lab-first",
        "http-lab-second",
        "http-lab-third",
        Arc::new(InMemoryConditionalRegister::new()),
    )
    .await
}

async fn register_serving_owner(harness: &VirtualLogHarness) -> ChunkJournalService {
    let recovered = recover_canon_owner(
        canon_owner_request(),
        harness.virtual_log(),
        SystemClock::new(),
        scripture::SystemTimer::new(),
    )
    .await
    .expect("recover");
    let mut service = ChunkJournalService::new();
    service.register_canon_owner(recovered).expect("register");
    service
}

#[tokio::test]
async fn canon_v2_missing_local_epoch_refuses_before_remote_acquire() {
    let fixture = spawn_lab(4, SequencerEpoch::test(7)).await;
    let fence = fence_v2(0, fixture.epoch);

    assert_eq!(
        admission_for(
            owner(),
            None,
            false,
            &fence,
            LocalCanonOwnerMatch::ServeReady,
        ),
        AdmissionDisposition::Fenced
    );

    let harness = http_harness().await;
    harness.bootstrap_first(fence.encode()).await;
    let service = register_serving_owner(&harness).await;
    assert!(matches!(
        resolve_canon_route_with_epoch(
            &harness.virtual_log(),
            &service,
            journal(),
            verse(),
            owner(),
            None,
            false,
        )
        .await
        .expect("route"),
        CanonRoute::Fenced {
            sequencer_epoch,
            ..
        } if sequencer_epoch == fixture.epoch
    ));

    assert_eq!(fixture.server_metrics.snapshot().acquire_attempts, 0);
}

#[tokio::test]
async fn canon_v2_mismatched_local_epoch_refuses_before_remote_acquire() {
    let fixture = spawn_lab(4, SequencerEpoch::test(8)).await;
    let stale = SequencerEpoch::test(3);
    let fence = fence_v2(0, fixture.epoch);

    assert_eq!(
        admission_for(
            owner(),
            Some(stale),
            false,
            &fence,
            LocalCanonOwnerMatch::ServeReady,
        ),
        AdmissionDisposition::Fenced
    );

    let harness = http_harness().await;
    harness.bootstrap_first(fence.encode()).await;
    let service = register_serving_owner(&harness).await;
    assert!(matches!(
        resolve_canon_route_with_epoch(
            &harness.virtual_log(),
            &service,
            journal(),
            verse(),
            owner(),
            Some(stale),
            false,
        )
        .await
        .expect("route"),
        CanonRoute::Fenced {
            sequencer_epoch,
            ..
        } if sequencer_epoch == fixture.epoch
    ));

    assert_eq!(fixture.server_metrics.snapshot().acquire_attempts, 0);
}

#[tokio::test]
async fn matching_epoch_http_sequencer_keyed_submission_append_succeeds() {
    let fixture = spawn_lab(4, SequencerEpoch::test(9)).await;
    let client_metrics = Arc::new(RemoteSequencerHttpMetrics::new());
    let client_impl = http_client(&fixture, Arc::clone(&client_metrics));
    let client: Arc<dyn Sequencer> = Arc::new(client_impl);

    let log = AtomicLog::new(
        Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
        client,
        Arc::new(InMemorySeal::new()),
        Arc::new(InMemoryTrimPoint::new()),
        fixture.k,
    )
    .expect("build");

    let producer = ProducerId::from_bytes(*b"http-lab-prod!!!");
    let key = sequencer_request_key_for_submission(producer, 2, 17);
    let address = log
        .append_with_request_key(Bytes::from_static(b"scripture-phase-b1-2"), key)
        .await
        .expect("keyed append");
    assert_eq!(address.get(), 0);

    assert_eq!(client_metrics.snapshot().acquire_attempts, 1);
    assert_eq!(fixture.server_metrics.snapshot().acquire_attempts, 1);
}

#[tokio::test]
async fn legacy_v1_fence_admits_without_epoch_and_blocks_remote_sequencer() {
    let fence = fence_v1(0);
    assert!(!fence.allows_remote_sequencer());
    assert_eq!(
        admission_for(
            owner(),
            None,
            false,
            &fence,
            LocalCanonOwnerMatch::ServeReady,
        ),
        AdmissionDisposition::Serving
    );

    let harness = http_harness().await;
    harness.bootstrap_first(fence.encode()).await;
    let service = register_serving_owner(&harness).await;

    assert!(matches!(
        resolve_canon_route(
            &harness.virtual_log(),
            &service,
            journal(),
            verse(),
            owner(),
        )
        .await
        .expect("route"),
        CanonRoute::Serve {
            sequencer_epoch: None,
            ..
        }
    ));
}

#[tokio::test]
async fn bad_capability_and_server_fence_surface_without_tail_leak() {
    let fixture = spawn_lab(2, SequencerEpoch::test(10)).await;
    let wrong = SequencerCapability::from_bytes([0xCD; 32]);
    let client = RemoteSequencerHttpClient::new(
        &fixture.base_url,
        wrong,
        fixture.epoch,
        fixture.k,
        Arc::new(RemoteSequencerHttpMetrics::new()),
    );

    let producer = ProducerId::from_bytes(*b"http-lab-badcap!");
    let key = sequencer_request_key_for_submission(producer, 0, 1);
    let acquire_err = client.acquire_slot_with_key(key).await.expect_err("auth");
    assert!(matches!(
        acquire_err,
        SequencerError::Backend(source) if source.to_string().contains("authorization")
    ));

    let tail_err = client.completed_tail().await.expect_err("auth");
    assert!(matches!(
        tail_err,
        SequencerError::Backend(source) if source.to_string().contains("authorization")
    ));
    assert_eq!(fixture.server_metrics.snapshot().auth_failures, 2);

    let good = http_client(&fixture, Arc::new(RemoteSequencerHttpMetrics::new()));
    let assigned = good.acquire_slot_with_key(key).await.expect("assign");
    assert_eq!(assigned.get(), 0);

    fixture.server.fence(fixture.epoch);
    let fenced = good
        .acquire_slot_with_key(sequencer_request_key_for_submission(producer, 0, 2))
        .await
        .expect_err("fenced");
    assert!(matches!(
        fenced,
        SequencerError::Backend(source) if source.to_string().contains("fenced")
    ));
}
