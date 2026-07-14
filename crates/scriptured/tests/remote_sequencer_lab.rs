//! Phase B1.3 remote sequencer TCP lab drills (not production provisioning).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use holylog::atomic::{AtomicLog, AtomicLogError, InMemorySeal, InMemoryTrimPoint, Sequencer};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::remote_sequencer::{
    ActivateOutcome, InMemoryRemoteSequencer, SequencerEpoch, SequencerRequestKey,
};
use holylog_remote_sequencer_tcp::{
    ClientError, FixedCapabilityAuthenticator, RemoteSequencerTcpClient, RemoteSequencerTcpServer,
    SequencerCapability,
};
use scripture::{
    CanonFence, CanonOwner, JournalId, OwnedSequencerBinding, OwnerEndpoint, OwnerId, ProducerId,
    VerseId, sequencer_request_key_for_submission,
};
use scripture_service::{
    AdmissionDisposition, CanonOwnerRequest, CanonRoute, ChunkJournalService, LocalCanonOwnerMatch,
    admission_for, recover_canon_owner, resolve_canon_route_with_epoch,
};
use tokio::net::TcpListener;

fn test_capability() -> SequencerCapability {
    SequencerCapability::test()
}

fn test_epoch() -> SequencerEpoch {
    SequencerEpoch::test(42)
}

fn journal() -> JournalId {
    JournalId::from_bytes(*b"remote-lab-jrnl!")
}

fn verse() -> VerseId {
    VerseId::from_bytes(*b"remote-lab-verse")
}

fn owner() -> OwnerId {
    OwnerId::from_bytes(*b"remote-lab-ownr!")
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

fn fence_v2(revision: u64, epoch: SequencerEpoch) -> CanonFence {
    CanonFence::new(revision, journal(), verse(), owned_v2(epoch))
}

async fn spawn_server(
    k: u64,
    initial_tail: u64,
) -> (std::net::SocketAddr, RemoteSequencerTcpServer) {
    let mut sequencer = InMemoryRemoteSequencer::new();
    assert_eq!(
        sequencer.activate(test_epoch(), k, initial_tail),
        ActivateOutcome::Active
    );
    let server = RemoteSequencerTcpServer::new(
        sequencer,
        Arc::new(FixedCapabilityAuthenticator::new(test_capability())),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let background = server.clone();
    tokio::spawn(async move {
        background.serve(listener).await.expect("serve");
    });
    (addr, server)
}

fn make_client(addr: std::net::SocketAddr, k: u64) -> RemoteSequencerTcpClient {
    RemoteSequencerTcpClient::new(addr, test_capability(), test_epoch(), k)
        .with_retry_deadline(Duration::from_millis(250))
}

fn submission_key(sequence: u64) -> SequencerRequestKey {
    sequencer_request_key_for_submission(ProducerId::from_bytes(*b"remote-lab-prod!"), 1, sequence)
}

#[tokio::test]
async fn keyed_atomic_log_append_over_real_tcp_succeeds() {
    let (addr, _server) = spawn_server(4, 0).await;
    let sequencer = Arc::new(make_client(addr, 4)) as Arc<dyn Sequencer>;
    let log = AtomicLog::new(
        Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
        Arc::clone(&sequencer),
        Arc::new(InMemorySeal::new()),
        Arc::new(InMemoryTrimPoint::new()),
        4,
    )
    .expect("atomic log");

    assert!(log.requires_stable_request_keys());
    assert!(matches!(
        log.append(Bytes::from_static(b"nope")).await,
        Err(AtomicLogError::StableRequestKeyRequired)
    ));

    let key = submission_key(7);
    let address = log
        .append_with_request_key(Bytes::from_static(b"ok"), key)
        .await
        .expect("keyed append");
    assert_eq!(address.get(), 0);
}

#[tokio::test]
async fn acquire_retry_with_same_submission_key_returns_same_address() {
    let (addr, _server) = spawn_server(4, 0).await;
    let client = make_client(addr, 4);
    let key = submission_key(99);

    let first = client
        .acquire_slot_with_key(key)
        .await
        .expect("first acquire");
    let second = client
        .acquire_slot_with_key(key)
        .await
        .expect("retry acquire");
    assert_eq!(first.get(), second.get());
    assert_eq!(first.get(), 0);
}

#[test]
fn v2_canon_missing_local_epoch_is_fenced_before_append() {
    let fence = fence_v2(1, SequencerEpoch::test(10));
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
}

#[test]
fn v2_canon_mismatched_local_epoch_is_fenced_before_append() {
    let fence = fence_v2(1, SequencerEpoch::test(10));
    assert_eq!(
        admission_for(
            owner(),
            Some(SequencerEpoch::test(9)),
            false,
            &fence,
            LocalCanonOwnerMatch::ServeReady,
        ),
        AdmissionDisposition::Fenced
    );
}

#[tokio::test]
async fn fenced_sequencer_epoch_refuses_allocation_over_tcp() {
    let (addr, server) = spawn_server(4, 0).await;
    let client = make_client(addr, 4);
    server.fence(test_epoch()).await;
    let outcome = client.acquire_slot_with_key(submission_key(1)).await;
    assert!(matches!(
        outcome,
        Err(holylog::atomic::SequencerError::Backend(_))
    ));
}

#[tokio::test]
async fn invalid_capability_returns_auth_failed_without_tail_leak() {
    let (addr, _server) = spawn_server(4, 5).await;
    let bad_client = RemoteSequencerTcpClient::new(
        addr,
        SequencerCapability::from_bytes([0xCD; 32]),
        test_epoch(),
        4,
    );
    let acquire = bad_client.acquire_slot_with_key(submission_key(1)).await;
    assert!(matches!(
        acquire,
        Err(holylog::atomic::SequencerError::Backend(_))
    ));
    let tail = bad_client.completed_tail().await;
    assert!(matches!(
        tail,
        Err(holylog::atomic::SequencerError::Backend(_))
    ));
    if let Err(holylog::atomic::SequencerError::Backend(error)) = tail {
        assert!(
            error
                .downcast_ref::<ClientError>()
                .is_some_and(|error| matches!(error, ClientError::AuthFailed))
        );
    }
}

#[tokio::test]
async fn process_loss_surfaces_unavailable_and_stale_epoch_cannot_resume() {
    let (addr, server) = spawn_server(4, 0).await;
    let client = make_client(addr, 4);
    client
        .acquire_slot_with_key(submission_key(1))
        .await
        .expect("warm acquire");

    assert!(server.simulate_process_loss().await.is_some());
    let fenced = client.acquire_slot_with_key(submission_key(2)).await;
    assert!(matches!(
        fenced,
        Err(holylog::atomic::SequencerError::Backend(_))
    ));

    let stale_epoch = SequencerEpoch::test(42);
    let (addr2, server2) = spawn_server(4, 0).await;
    let stale_client = RemoteSequencerTcpClient::new(addr2, test_capability(), stale_epoch, 4);
    server2.stop_serving();
    let stopped = stale_client.acquire_slot_with_key(submission_key(3)).await;
    assert!(matches!(
        stopped,
        Err(holylog::atomic::SequencerError::Backend(_))
    ));
}

#[tokio::test]
async fn resolve_canon_route_marks_stale_epoch_fenced() {
    use holylog::atomic::AtomicLog;
    use holylog::virtual_log::{
        ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver, ResolveFuture,
        VirtualLog,
    };
    use scripture::{ChunkPolicy, CohortId, RecoveryBound, SystemClock, SystemTimer, WriterId};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Resolver {
        loglets: Mutex<BTreeMap<LogletId, Arc<AtomicLog>>>,
    }

    impl LogletResolver for Resolver {
        fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<Arc<AtomicLog>>> {
            let loglets = self.loglets.lock().expect("lock");
            let found = loglets.get(id).cloned();
            Box::pin(async move { Ok(found) })
        }
    }

    let resolver = Arc::new(Resolver::default());
    let first = LogletId::new("remote-lab-first").expect("id");
    let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn holylog::drive::LogDrive>;
    resolver.loglets.lock().expect("lock").insert(
        first.clone(),
        Arc::new(
            AtomicLog::builder(Arc::clone(&drive), 0)
                .build()
                .expect("log"),
        ),
    );
    let virtual_log = VirtualLog::new(
        Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>,
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
    );

    let fence_bytes = fence_v2(0, SequencerEpoch::test(10)).encode();
    virtual_log
        .bootstrap_with_application_fence(first.clone(), fence_bytes)
        .await
        .expect("bootstrap");

    let request = CanonOwnerRequest {
        journal_id: journal(),
        verse_id: verse(),
        owner_id: owner(),
        cohort_id: CohortId::from_bytes(*b"remote-lab-cohrt"),
        writer_id: WriterId::from_bytes(*b"remote-lab-wrtr!"),
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
    };
    let recovered = recover_canon_owner(
        request,
        virtual_log.clone(),
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("recover");
    let mut service = ChunkJournalService::new();
    service.register_canon_owner(recovered).expect("register");

    assert!(matches!(
        resolve_canon_route_with_epoch(
            &virtual_log,
            &service,
            journal(),
            verse(),
            owner(),
            Some(SequencerEpoch::test(9)),
            false,
        )
        .await
        .expect("stale route"),
        CanonRoute::Fenced {
            sequencer_epoch,
            owner_id,
            ..
        } if sequencer_epoch == SequencerEpoch::test(10) && owner_id == owner()
    ));
}
