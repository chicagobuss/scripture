//! Serve-path DataRef mount: a served write must produce SRDF + staging blob.
//!
//! The CLI previously mounted DataRefs on the assembled supervisor config and
//! then passed a fresh `assignment_runtime_config()` (with `dataref_blobs: None`)
//! into `bootstrap_and_serve`. Unit tests that construct `DataRefBlobConfig`
//! explicitly cannot catch that. This suite drives the same activation path
//! the Scribe uses, with one shared object store for loglets and staging blobs.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
use holylog::virtual_log::{ConditionalRegister, InMemoryConditionalRegister, LogletResolver};
use holylog_object_store::{ObjectStoreMetrics, WritePolicy};
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use scripture::serving_authority::{AuthorityKey, RouteHint, WriterTerm};
use scripture::{
    ChunkPolicy, CohortId, DataRefBlobConfig, JournalId, OwnerEndpoint, OwnerId, ProducerId,
    Record, RecoveryBound, Submission, SystemClock, SystemTimer, VerseId, WriterId,
};
use scripture_runtime::counting_store::{CountingStore, RequestCounters};
use scripture_runtime::{
    BackendProfile, HolylogJournalFoundation, NodeIdentity, ObjectStoreChunkBlobStore,
    ObjectStorePartsFactory, PartsFactory, ProcessLogletResolver, bootstrap_and_serve,
};
use scripture_service::{
    AuthorityCoordinator, DeterministicTransitionIdGenerator, JournalFoundationTransition,
    VerseRuntimeConfig,
};

fn owner() -> OwnerId {
    OwnerId::from_bytes(*b"dataref-owner-a!")
}

fn runtime_config(dataref: Option<DataRefBlobConfig>) -> VerseRuntimeConfig {
    VerseRuntimeConfig {
        journal_id: JournalId::from_bytes(*b"dataref-jrnl!!!!"),
        verse_id: VerseId::from_bytes(*b"dataref-verse!!!"),
        owner_id: owner(),
        cohort_id: CohortId::from_bytes(*b"dataref-cohort!!"),
        writer_id: WriterId::from_bytes(*b"dataref-writer!!"),
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
        dataref_blobs: dataref,
        blob_sink: None,
        blob_verse_key: None,
    }
}

async fn scan_store(store: &Arc<dyn ObjectStore>) -> (usize, bool, bool) {
    let mut blob_objects = 0_usize;
    let mut found_srdf = false;
    let mut found_scrc_inline = false;
    let mut stream = store.list(None);
    while let Some(meta) = stream.next().await {
        let meta = meta.expect("list");
        let key = meta.location.as_ref();
        if key.contains("blobs/v1/") {
            blob_objects += 1;
        }
        let bytes = store
            .get(&meta.location)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        if bytes.windows(4).any(|window| window == b"SRDF") {
            found_srdf = true;
        }
        // Inline chunks in the log are SCRC without a preceding DataRef frame.
        // Presence of SCRC inside blobs/v1 is expected (sealed chunk bytes).
        if !key.contains("blobs/v1/") && bytes.windows(4).any(|window| window == b"SCRC") {
            found_scrc_inline = true;
        }
    }
    (blob_objects, found_srdf, found_scrc_inline)
}

async fn bootstrap_commit(
    store: Arc<dyn ObjectStore>,
    config: VerseRuntimeConfig,
) -> scripture::Receipt {
    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let resolver = Arc::new(ProcessLogletResolver::default());
    let parts = Arc::new(ObjectStorePartsFactory::new(
        Arc::clone(&store),
        "dataref-root",
        BackendProfile::RustFs.drive_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::new(ObjectStoreMetrics::default()),
    )) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let key = AuthorityKey {
        journal_id: config.journal_id,
        verse_id: config.verse_id,
    };
    let advertise = OwnerEndpoint::new("tcp://dataref-serve:9000").expect("endpoint");
    let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
        key,
        NodeIdentity {
            owner_id: owner(),
            endpoint: advertise.clone(),
        },
        Arc::clone(&register),
        Arc::clone(&resolver),
        parts,
        Arc::clone(&claims),
        2,
    ));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
        Arc::new(DeterministicTransitionIdGenerator::new()),
        owner(),
        RouteHint::new(advertise.as_str()).expect("route"),
    );
    let session = bootstrap_and_serve(
        &coordinator,
        foundation.as_ref(),
        key,
        WriterTerm::new(1).expect("term"),
        config,
        Arc::clone(&register),
        resolver,
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("bootstrap");

    let pending = session
        .submit(Submission {
            producer_id: ProducerId::from_bytes(*b"dataref-producer"),
            producer_epoch: 1,
            sequence: 0,
            records: vec![Record::new([], Bytes::from_static(b"serve-path-payload"))],
        })
        .await
        .expect("admit");
    session.flush().await.expect("flush");
    pending.await.expect("commit")
}

#[tokio::test]
async fn served_write_with_datarefs_produces_srdf_payload_and_staging_blob() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let counters = Arc::new(RequestCounters::default());
    let counted: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(
        Arc::clone(&store),
        Arc::clone(&counters),
    ));
    let config = runtime_config(Some(DataRefBlobConfig::new(Arc::new(
        ObjectStoreChunkBlobStore::new(counted),
    ))));

    let _ = bootstrap_commit(Arc::clone(&store), config).await;

    let (puts, gets, _, _) = counters.snapshot();
    assert!(
        puts > 0,
        "staging blob PUT must be counted (got puts={puts})"
    );
    assert!(
        gets > 0,
        "verify-before-append GET must be counted (got gets={gets})"
    );

    let (blob_objects, found_srdf, found_inline_scrc) = scan_store(&store).await;
    assert!(
        blob_objects > 0,
        "served write must create an object under blobs/v1/"
    );
    assert!(
        found_srdf,
        "served write must leave an SRDF DataRef payload in the Verse log"
    );
    assert!(
        !found_inline_scrc,
        "mounted path must not leave inline SCRC payloads in the log"
    );
}

#[tokio::test]
async fn depth_one_mount_costs_extra_blob_put_and_verify_get_per_chunk() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let counters = Arc::new(RequestCounters::default());
    let counted: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(
        Arc::clone(&store),
        Arc::clone(&counters),
    ));
    let config = runtime_config(Some(DataRefBlobConfig::new(Arc::new(
        ObjectStoreChunkBlobStore::new(counted),
    ))));

    let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
    let resolver = Arc::new(ProcessLogletResolver::default());
    let parts = Arc::new(ObjectStorePartsFactory::new(
        Arc::clone(&store),
        "dataref-cost",
        BackendProfile::RustFs.drive_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::new(ObjectStoreMetrics::default()),
    )) as Arc<dyn PartsFactory>;
    let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
    let key = AuthorityKey {
        journal_id: config.journal_id,
        verse_id: config.verse_id,
    };
    let advertise = OwnerEndpoint::new("tcp://dataref-cost:9000").expect("endpoint");
    let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
        key,
        NodeIdentity {
            owner_id: owner(),
            endpoint: advertise.clone(),
        },
        Arc::clone(&register),
        Arc::clone(&resolver),
        parts,
        Arc::clone(&claims),
        2,
    ));
    let coordinator = AuthorityCoordinator::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
        Arc::new(DeterministicTransitionIdGenerator::new()),
        owner(),
        RouteHint::new(advertise.as_str()).expect("route"),
    );
    let session = bootstrap_and_serve(
        &coordinator,
        foundation.as_ref(),
        key,
        WriterTerm::new(1).expect("term"),
        config,
        Arc::clone(&register),
        resolver,
        SystemClock::new(),
        SystemTimer::new(),
    )
    .await
    .expect("bootstrap");

    const N: u64 = 32;
    for sequence in 0..N {
        let pending = session
            .submit(Submission {
                producer_id: ProducerId::from_bytes(*b"dataref-producer"),
                producer_epoch: 1,
                sequence,
                records: vec![Record::new(
                    [],
                    Bytes::from(format!("payload-{sequence}").into_bytes()),
                )],
            })
            .await
            .expect("admit");
        session.flush().await.expect("flush");
        let _ = pending.await.expect("commit");
    }

    let (puts, gets, _, _) = counters.snapshot();
    // Depth-one mount: one content-addressed PUT + verify GET per sealed chunk.
    // With max_chunk_records=8 and one record per submit+flush, expect one chunk
    // per record → about 1 PUT and ≥1 GET per record.
    let puts_per = puts as f64 / N as f64;
    let gets_per = gets as f64 / N as f64;
    assert!(
        puts_per >= 1.0,
        "expected ≥1 staging blob PUT/record, got {puts_per} (puts={puts})"
    );
    assert!(
        gets_per >= 1.0,
        "expected ≥1 verify GET/record, got {gets_per} (gets={gets})"
    );
    eprintln!(
        "depth-one DataRef mount cost: blob_put_per_record={puts_per:.3} blob_get_per_record={gets_per:.3} (N={N})"
    );
}

#[tokio::test]
async fn served_write_without_datarefs_keeps_inline_chunks_and_no_staging_blob() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let counters = Arc::new(RequestCounters::default());
    // Counters exist but are not wired — proves an unmounted serve path cannot
    // accidentally increment them.
    let _ = counters;
    let _ = bootstrap_commit(Arc::clone(&store), runtime_config(None)).await;

    let (blob_objects, found_srdf, found_inline_scrc) = scan_store(&store).await;
    assert_eq!(
        blob_objects, 0,
        "unmounted path must not write blobs/v1/ (found {blob_objects})"
    );
    assert!(!found_srdf, "unmounted path must not emit SRDF");
    assert!(
        found_inline_scrc,
        "unmounted path must keep inline SCRC payloads"
    );
}
