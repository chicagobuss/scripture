//! End-to-end bounded load against an in-process VerseNodeSupervisor.

use std::sync::Arc;
use std::time::Duration;

use holylog::virtual_log::{ConditionalRegister, InMemoryConditionalRegister, LogletId};
use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, SystemClock, VerseId,
    WriterId,
};
use scripture_load::{LoadConfig, NamedChunkPolicy, run_load};
use scripture_service::VerseRuntimeConfig;
use scriptured::{
    FleetLabResolver, NodeIdentity, RawLinesConfig, VerseControlOutcome, VerseNodeSupervisor,
    serve_canon_raw_lines_connection,
};
use tokio::net::TcpListener;

fn journal() -> JournalId {
    JournalId::from_bytes(*b"load-lab-jrnl!!!")
}

fn verse() -> VerseId {
    VerseId::from_bytes(*b"load-lab-verse!!")
}

fn owner() -> OwnerId {
    OwnerId::from_bytes(*b"load-lab-owner!!")
}

fn load_policy() -> ChunkPolicy {
    ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 256,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 256 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_load_receives_ok_acks() {
    let register = Arc::new(InMemoryConditionalRegister::new());
    let resolver = Arc::new(FleetLabResolver::default());
    let config = VerseRuntimeConfig {
        journal_id: journal(),
        verse_id: verse(),
        owner_id: owner(),
        cohort_id: CohortId::from_bytes(*b"load-lab-cohort!"),
        writer_id: WriterId::from_bytes(*b"load-lab-writer!"),
        policy: load_policy(),
        recovery_bound: RecoveryBound::new(8).expect("bound"),
        queue_capacity: 256,
    };
    let node = VerseNodeSupervisor::new(
        NodeIdentity {
            owner_id: owner(),
            endpoint: OwnerEndpoint::new("tcp://owner.lab:9000").expect("endpoint"),
        },
        Arc::clone(&register) as Arc<dyn ConditionalRegister>,
        Arc::clone(&resolver),
        config,
    );
    let outcome = node
        .bootstrap_verse(
            LogletId::new("load-gen-0").expect("id"),
            SystemClock::new(),
            scripture::SystemTimer::new(),
            2,
        )
        .await
        .expect("bootstrap");
    assert!(
        matches!(outcome, VerseControlOutcome::Serving),
        "{outcome:?}"
    );

    let runtime = node.runtime().await.expect("runtime");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let _ =
                    serve_canon_raw_lines_connection(stream, runtime, RawLinesConfig::default())
                        .await;
            });
        }
    });

    let report = run_load(LoadConfig {
        endpoint: addr.to_string(),
        connections: 2,
        record_bytes: 64,
        duration: Duration::from_secs(2),
        max_bytes: 64 * 1024,
        target_records_per_sec: Some(200),
        run_id: "load-itest".to_owned(),
        ack_timeout: Duration::from_secs(2),
        chunk_policy: NamedChunkPolicy::fleet_lab_default(),
        backend: "in-memory".to_owned(),
    })
    .await
    .expect("load");

    assert!(report.accepted_records > 0, "{report:?}");
    assert_eq!(report.errors, 0, "{report:?}");
    assert_eq!(report.transport_failures, 0, "{report:?}");
    assert_eq!(
        report.accepted_bytes,
        report.accepted_records * 64,
        "{report:?}"
    );
}
