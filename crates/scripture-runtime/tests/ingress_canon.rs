//! Canon-gated temporary ingress tests (product-internal; not a public protocol).

use std::sync::Arc;
use std::time::Duration;

use holylog::virtual_log::{ConditionalRegister, InMemoryConditionalRegister, LogletId};
use scripture::{
    CanonFence, CanonOwner, ChunkPolicy, CohortId, JournalId, OwnedSequencerBinding, OwnerEndpoint,
    OwnerId, RecoveryBound, SequencerEpoch, SystemClock, VerseId, WriterId,
};
use scripture_service::virtuallog_test_support::VirtualLogHarness;
use scripture_service::{VerseHandoffRequest, VerseRuntime, VerseRuntimeConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use scripture_runtime::{
    BatchingSnapshot, RawLinesConfig, RawLinesConnectionMetrics, serve_canon_raw_lines_connection,
    serve_canon_raw_lines_connection_with_metrics,
};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"canon-raw-jrnl!!")
}

fn verse() -> VerseId {
    VerseId::from_bytes(*b"canon-raw-line!!")
}

fn owner_a() -> OwnerId {
    OwnerId::from_bytes(*b"canon-raw-own-a!")
}

fn owner_b() -> OwnerId {
    OwnerId::from_bytes(*b"canon-raw-own-b!")
}

fn config(owner: OwnerId) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: journal(),
        verse_id: verse(),
        owner_id: owner,
        cohort_id: CohortId::from_bytes(*b"canon-raw-cohrt!"),
        writer_id: WriterId::from_bytes(*b"canon-raw-writer"),
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
        dataref_blobs: None,
        blob_sink: None,
        blob_verse_key: None,
    }
}

fn fence(revision: u64, owner: OwnerId) -> CanonFence {
    let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
    CanonFence::new(
        revision,
        journal(),
        verse(),
        CanonOwner::Owned {
            owner_id: owner,
            endpoint,
            sequencer: None,
            writer_term: None,
        },
    )
}

async fn raw_harness() -> VirtualLogHarness {
    VirtualLogHarness::with_ids(
        "canon-raw-first",
        "canon-raw-second",
        "canon-raw-third",
        Arc::new(InMemoryConditionalRegister::new()),
    )
    .await
}

async fn exchange(runtime: Arc<VerseRuntime>, payload: &[u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        serve_canon_raw_lines_connection(stream, runtime, RawLinesConfig::default())
            .await
            .expect("serve")
    });
    let mut client = TcpStream::connect(address).await.expect("connect");
    if !payload.is_empty() {
        client.write_all(payload).await.expect("write");
    }
    client.shutdown().await.expect("shutdown");
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.expect("read");
    server.await.expect("join");
    String::from_utf8(response).expect("utf8")
}

#[tokio::test]
async fn serving_node_writes_committed_ok() {
    let harness = raw_harness().await;
    harness.bootstrap_first(fence(0, owner_a()).encode()).await;
    let runtime = VerseRuntime::start(
        config(owner_a()),
        harness.virtual_log(),
        SystemClock::new(),
        scripture::SystemTimer::new(),
    )
    .await
    .expect("start");
    assert!(runtime.is_serving());
    let response = exchange(Arc::new(runtime), b"hello\n").await;
    assert_eq!(response, "OK 0 1\n");
}

#[tokio::test]
async fn pipelined_small_lines_share_one_committed_chunk() {
    let harness = raw_harness().await;
    harness.bootstrap_first(fence(0, owner_a()).encode()).await;
    let mut cfg = config(owner_a());
    cfg.policy.max_chunk_records = 8;
    cfg.policy.max_chunk_bytes = 64 * 1024;
    let runtime = Arc::new(
        VerseRuntime::start(
            cfg,
            harness.virtual_log(),
            SystemClock::new(),
            scripture::SystemTimer::new(),
        )
        .await
        .expect("start"),
    );
    let metrics = Arc::new(RawLinesConnectionMetrics::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let serve_runtime = Arc::clone(&runtime);
    let serve_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let cfg = RawLinesConfig {
            idle_flush: None,
            max_pending_records: 16,
            ..RawLinesConfig::default()
        };
        serve_canon_raw_lines_connection_with_metrics(
            stream,
            serve_runtime,
            cfg,
            Some(serve_metrics),
        )
        .await
        .expect("serve")
    });

    let mut client = TcpStream::connect(address).await.expect("connect");
    for i in 0..8 {
        client
            .write_all(format!("line-{i}\n").as_bytes())
            .await
            .expect("write");
    }
    client.shutdown().await.expect("shutdown");
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.expect("read");
    server.await.expect("join");
    let text = String::from_utf8(response).expect("utf8");
    let oks: Vec<_> = text
        .lines()
        .filter(|line| line.starts_with("OK "))
        .collect();
    assert_eq!(
        oks,
        [
            "OK 0 1", "OK 1 2", "OK 2 3", "OK 3 4", "OK 4 5", "OK 5 6", "OK 6 7", "OK 7 8",
        ],
        "expected ordered committed OKs, got {text}"
    );

    let driver = runtime.driver_metrics().expect("serving metrics");
    let batching = BatchingSnapshot::from_parts(driver, metrics.snapshot());
    assert_eq!(
        batching.committed_chunks, 1,
        "eight small lines must co-pack into one data chunk; got {batching:?}"
    );
    assert_eq!(batching.admitted_records, 8);
    assert!(batching.records_per_chunk >= 7.9);
}

#[tokio::test]
async fn other_owner_returns_exact_not_owner_without_append() {
    let harness = raw_harness().await;
    harness.bootstrap_first(fence(0, owner_a()).encode()).await;
    let runtime = VerseRuntime::start(
        config(owner_a()),
        harness.virtual_log(),
        SystemClock::new(),
        scripture::SystemTimer::new(),
    )
    .await
    .expect("start a");
    assert!(runtime.is_serving());
    let second = LogletId::new("canon-raw-second").expect("id");
    harness
        .reconfigure_id(&second, fence(1, owner_b()).encode())
        .await;
    let response = exchange(Arc::new(runtime), b"should-not-append\n").await;
    assert_eq!(
        response,
        "ERR not-owner canon=1 endpoint=tcp://owner.local:9000\n"
    );
    let tail = harness.virtual_log().check_tail().await.expect("tail");
    assert_eq!(tail.tail, 0);
    assert_eq!(tail.loglet_id, second);
}

#[tokio::test]
async fn unowned_returns_recovering_without_append() {
    let harness = raw_harness().await;
    harness.bootstrap_first(fence(0, owner_a()).encode()).await;
    let runtime = VerseRuntime::start(
        config(owner_a()),
        harness.virtual_log(),
        SystemClock::new(),
        scripture::SystemTimer::new(),
    )
    .await
    .expect("start");
    assert!(runtime.is_serving());
    let second = LogletId::new("canon-raw-unowned").expect("id");
    harness
        .reconfigure_id(
            &second,
            CanonFence::new(1, journal(), verse(), CanonOwner::Unowned).encode(),
        )
        .await;
    let response = exchange(Arc::new(runtime), b"nope\n").await;
    assert_eq!(response, "ERR recovering canon=1\n");
    let tail = harness.virtual_log().check_tail().await.expect("tail");
    assert_eq!(tail.tail, 0);
    assert_eq!(tail.loglet_id, second);
}

#[tokio::test]
async fn resolver_failure_returns_unavailable() {
    use holylog::virtual_log::{RegisterError, RegisterFuture, VersionedState, VirtualLogState};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailAfterArm {
        inner: InMemoryConditionalRegister,
        armed: AtomicBool,
    }

    impl ConditionalRegister for FailAfterArm {
        fn read(&self) -> RegisterFuture<'_, Option<VersionedState>> {
            Box::pin(async {
                if self.armed.load(Ordering::Acquire) {
                    return Err(RegisterError::backend(std::io::Error::other(
                        "register unavailable",
                    )));
                }
                self.inner.read().await
            })
        }

        fn compare_and_swap(
            &self,
            expected: Option<&VersionedState>,
            new_state: VirtualLogState,
        ) -> RegisterFuture<'_, bool> {
            self.inner.compare_and_swap(expected, new_state)
        }
    }

    let register = Arc::new(FailAfterArm {
        inner: InMemoryConditionalRegister::new(),
        armed: AtomicBool::new(false),
    });
    let harness = VirtualLogHarness::with_ids(
        "canon-raw-fail",
        "canon-raw-fail-2",
        "canon-raw-fail-3",
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
    )
    .await;
    harness.bootstrap_first(fence(0, owner_a()).encode()).await;
    let runtime = VerseRuntime::start(
        config(owner_a()),
        harness.virtual_log(),
        SystemClock::new(),
        scripture::SystemTimer::new(),
    )
    .await
    .expect("start");
    assert!(runtime.is_serving());
    register.armed.store(true, Ordering::Release);
    let response = exchange(Arc::new(runtime), b"nope\n").await;
    assert_eq!(response, "ERR unavailable\n");
}

#[tokio::test]
async fn standby_runtime_listener_returns_not_owner_without_append() {
    let harness = raw_harness().await;
    harness.bootstrap_first(fence(0, owner_b()).encode()).await;
    let runtime = VerseRuntime::start(
        config(owner_a()),
        harness.virtual_log(),
        SystemClock::new(),
        scripture::SystemTimer::new(),
    )
    .await
    .expect("standby");
    assert!(runtime.is_standby());
    let response = exchange(Arc::new(runtime), b"nope\n").await;
    assert_eq!(
        response,
        "ERR not-owner canon=0 endpoint=tcp://owner.local:9000\n"
    );
    assert_eq!(
        harness.virtual_log().check_tail().await.expect("tail").tail,
        0
    );
}

#[tokio::test]
async fn after_handoff_old_runtime_never_accepts_payload() {
    let harness = raw_harness().await;
    harness.bootstrap_first(fence(0, owner_a()).encode()).await;
    let runtime = VerseRuntime::start(
        config(owner_a()),
        harness.virtual_log(),
        SystemClock::new(),
        scripture::SystemTimer::new(),
    )
    .await
    .expect("start");
    let second = LogletId::new("canon-raw-handoff").expect("id");
    let successor = harness.provision(&second, 0).await;
    let (runtime, outcome) = runtime
        .drain_seal_publish(VerseHandoffRequest {
            successor,
            next_owner: {
                let endpoint = OwnerEndpoint::new("tcp://owner.local:9000").expect("endpoint");
                CanonOwner::Owned {
                    owner_id: owner_b(),
                    endpoint: endpoint.clone(),
                    sequencer: Some(OwnedSequencerBinding {
                        epoch: SequencerEpoch::test(1),
                        sequencer_endpoint: endpoint,
                    }),
                    writer_term: None,
                }
            },
            journal_id: journal(),
            verse_id: verse(),
        })
        .await
        .expect("handoff");
    assert!(matches!(
        outcome,
        scripture_service::CanonTransitionOutcome::Published(_)
    ));
    assert!(runtime.is_terminal());
    let response = exchange(Arc::new(runtime), b"should-fail\n").await;
    assert_eq!(
        response,
        "ERR not-owner canon=1 endpoint=tcp://owner.local:9000\n"
    );
}
