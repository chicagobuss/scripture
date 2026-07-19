//! Deterministic in-memory contract tests + Parquet crash/replay proofs.

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use scripture_workload::{
    ArrowFieldConfig, ArrowSchemaConfig, BatchBoundsConfig, BindingToken, CanonRecord, CanonRef,
    ConsumerBinding, ConsumerCheckpoint, ConsumerProgressStore, HostError, InMemoryProgressStore,
    JsonArrowParquetMaterializer, MalformedPolicy, OutputCommit, ProcessOutcome, ProgressVersion,
    ReconcileOutcome, SchemaRef, SourceOffset, SourceRange, VerseRef, Workload, WorkloadError,
    WorkloadHost, WorkloadId, WorkloadMetadata,
};
use tempfile::tempdir;

fn binding_epoch(epoch: u64) -> ConsumerBinding {
    ConsumerBinding {
        workload_id: WorkloadId::new("wl-parquet").expect("id"),
        canon_id: CanonRef::new("events").expect("canon"),
        verse_id: VerseRef::new("v0").expect("verse"),
        binding_epoch: epoch,
    }
}

fn batch_limits() -> BatchBoundsConfig {
    BatchBoundsConfig {
        max_records: 100,
        max_bytes: 1_048_576,
        max_wall_ms: None,
    }
}

fn schema() -> ArrowSchemaConfig {
    ArrowSchemaConfig {
        fields: vec![
            ArrowFieldConfig {
                name: "id".into(),
                data_type: "utf8".into(),
                nullable: false,
            },
            ArrowFieldConfig {
                name: "amount".into(),
                data_type: "int64".into(),
                nullable: true,
            },
        ],
    }
}

fn range(first: u64, payloads: &[&str]) -> SourceRange {
    let records: Vec<CanonRecord> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| CanonRecord {
            offset: SourceOffset::new(first + u64::try_from(index).expect("idx")),
            payload: Bytes::from(payload.as_bytes().to_vec()),
        })
        .collect();
    let next = first + u64::try_from(records.len()).expect("len");
    SourceRange {
        canon_id: CanonRef::new("events").expect("canon"),
        verse_id: VerseRef::new("v0").expect("verse"),
        first_offset: SourceOffset::new(first),
        next_offset: SourceOffset::new(next),
        schema_ref: SchemaRef::new("events.v1").expect("schema"),
        records,
    }
}

fn materializer(dir: &std::path::Path) -> JsonArrowParquetMaterializer {
    JsonArrowParquetMaterializer::new(
        WorkloadId::new("wl-parquet").expect("id"),
        CanonRef::new("events").expect("canon"),
        VerseRef::new("v0").expect("verse"),
        SchemaRef::new("events.v1").expect("schema"),
        schema(),
        dir,
        MalformedPolicy::FailBatch,
    )
    .expect("materializer")
}

fn acquire(
    host: &WorkloadHost<InMemoryProgressStore>,
    epoch: u64,
    token: &BindingToken,
) -> scripture_workload::AcquiredBinding {
    host.acquire_binding(binding_epoch(epoch), token)
        .expect("acquire")
}

#[test]
fn apply_then_replay_reconcile_advances_without_duplicating_parquet() {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let workload = materializer(dir.path());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, 1, &token);
    let batch = range(0, &[r#"{"id":"a","amount":1}"#, r#"{"id":"b","amount":2}"#]);

    let first = host
        .process_range(&workload, &batch, &fence, &batch_limits())
        .expect("apply");
    assert!(matches!(first, ProcessOutcome::Applied { .. }));
    let parquet_files: Vec<_> = fs::read_dir(dir.path())
        .expect("list")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    assert_eq!(parquet_files.len(), 1);
    let digest_before = fs::read(parquet_files[0].path()).expect("read");

    let replay = workload.reconcile(&batch, &fence).expect("reconcile");
    assert!(matches!(replay, ReconcileOutcome::AlreadyCommitted(_)));
    let digest_after = fs::read(parquet_files[0].path()).expect("read again");
    assert_eq!(digest_before, digest_after);

    let next = range(2, &[r#"{"id":"c","amount":3}"#]);
    let second = host
        .process_range(&workload, &next, &fence, &batch_limits())
        .expect("second apply");
    assert!(matches!(second, ProcessOutcome::Applied { .. }));
    let parquet_count = fs::read_dir(dir.path())
        .expect("list")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();
    assert_eq!(parquet_count, 2);
}

#[test]
fn crash_after_output_before_checkpoint_replays_without_duplicate() {
    let dir = tempdir().expect("temp");
    let workload = materializer(dir.path());
    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    let writer_token = BindingToken::new("writer").expect("token");
    let writer_fence = scripture_workload::AcquiredBinding {
        binding: binding_epoch(1),
        owner_token: writer_token,
    };
    let commit = workload
        .apply(&batch, &writer_fence)
        .expect("apply durable output");
    assert!(commit.output_identity.contains("parquet:"));
    assert_eq!(commit.binding_epoch, 1);
    assert_eq!(commit.owner_token, "writer");

    // Progress store empty (crash before CAS). Restart acquires under same epoch.
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let restart = BindingToken::new("restart").expect("token");
    let fence = acquire(&host, 1, &restart);
    let outcome = host
        .process_range(&workload, &batch, &fence, &batch_limits())
        .expect("replay after crash");
    assert!(matches!(outcome, ProcessOutcome::Replayed { .. }));

    let observed = store
        .observe(
            &WorkloadId::new("wl-parquet").expect("id"),
            &CanonRef::new("events").expect("canon"),
            &VerseRef::new("v0").expect("verse"),
        )
        .expect("observe")
        .expect("checkpoint");
    assert_eq!(observed.0.next_offset, SourceOffset::new(1));
    assert_eq!(observed.0.owner_token.as_str(), "restart");

    let parquet_count = fs::read_dir(dir.path())
        .expect("list")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();
    assert_eq!(parquet_count, 1);
}

#[test]
fn partial_parquet_is_indeterminate_never_deleted_and_does_not_advance() {
    let dir = tempdir().expect("temp");
    let workload = materializer(dir.path());
    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    fs::create_dir_all(dir.path()).expect("dir");
    let key = workload.object_key(&batch);
    let partial = dir.path().join(format!("{key}.parquet.tmp"));
    fs::write(&partial, b"truncated").expect("partial");

    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, 1, &token);
    let err = host
        .process_range(&workload, &batch, &fence, &batch_limits())
        .expect_err("must fail closed");
    assert!(matches!(err, HostError::Indeterminate(_)));
    assert!(partial.exists(), "unknown/partial tmp must not be deleted");
    assert!(
        store
            .observe(
                &WorkloadId::new("wl-parquet").expect("id"),
                &CanonRef::new("events").expect("canon"),
                &VerseRef::new("v0").expect("verse"),
            )
            .expect("observe")
            .is_none()
    );
}

#[test]
fn path_injection_stays_under_output_dir() {
    let dir = tempdir().expect("temp");
    let evil_canon = CanonRef::new("../evil").expect("canon");
    let workload = JsonArrowParquetMaterializer::new(
        WorkloadId::new("wl-parquet").expect("id"),
        evil_canon.clone(),
        VerseRef::new("v0/../x").expect("verse"),
        SchemaRef::new("events.v1").expect("schema"),
        schema(),
        dir.path(),
        MalformedPolicy::FailBatch,
    )
    .expect("materializer");
    let batch = SourceRange {
        canon_id: evil_canon,
        verse_id: VerseRef::new("v0/../x").expect("verse"),
        first_offset: SourceOffset::new(0),
        next_offset: SourceOffset::new(1),
        schema_ref: SchemaRef::new("events.v1").expect("schema"),
        records: vec![CanonRecord {
            offset: SourceOffset::new(0),
            payload: Bytes::from(r#"{"id":"a","amount":1}"#.as_bytes().to_vec()),
        }],
    };
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token = BindingToken::new("worker-a").expect("token");
    let fence = host
        .acquire_binding(
            ConsumerBinding {
                workload_id: WorkloadId::new("wl-parquet").expect("id"),
                canon_id: batch.canon_id.clone(),
                verse_id: batch.verse_id.clone(),
                binding_epoch: 1,
            },
            &token,
        )
        .expect("acquire");
    host.process_range(&workload, &batch, &fence, &batch_limits())
        .expect("apply");
    for entry in fs::read_dir(dir.path()).expect("list") {
        let entry = entry.expect("entry");
        let path = entry.path();
        assert!(path.starts_with(dir.path()));
        assert!(!path.to_string_lossy().contains(".."));
    }
}

#[test]
fn cross_workload_same_dir_does_not_collide() {
    let dir = tempdir().expect("temp");
    let a = JsonArrowParquetMaterializer::new(
        WorkloadId::new("wl-a").expect("id"),
        CanonRef::new("events").expect("canon"),
        VerseRef::new("v0").expect("verse"),
        SchemaRef::new("events.v1").expect("schema"),
        schema(),
        dir.path(),
        MalformedPolicy::FailBatch,
    )
    .expect("a");
    let b = JsonArrowParquetMaterializer::new(
        WorkloadId::new("wl-b").expect("id"),
        CanonRef::new("events").expect("canon"),
        VerseRef::new("v0").expect("verse"),
        SchemaRef::new("events.v1").expect("schema"),
        schema(),
        dir.path(),
        MalformedPolicy::FailBatch,
    )
    .expect("b");
    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    assert_ne!(a.object_key(&batch), b.object_key(&batch));
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token_a = BindingToken::new("a").expect("token");
    let token_b = BindingToken::new("b").expect("token");
    let fence_a = host
        .acquire_binding(
            ConsumerBinding {
                workload_id: WorkloadId::new("wl-a").expect("id"),
                canon_id: CanonRef::new("events").expect("canon"),
                verse_id: VerseRef::new("v0").expect("verse"),
                binding_epoch: 1,
            },
            &token_a,
        )
        .expect("acquire a");
    let fence_b = host
        .acquire_binding(
            ConsumerBinding {
                workload_id: WorkloadId::new("wl-b").expect("id"),
                canon_id: CanonRef::new("events").expect("canon"),
                verse_id: VerseRef::new("v0").expect("verse"),
                binding_epoch: 1,
            },
            &token_b,
        )
        .expect("acquire b");
    host.process_range(&a, &batch, &fence_a, &batch_limits())
        .expect("apply a");
    host.process_range(&b, &batch, &fence_b, &batch_limits())
        .expect("apply b");
    let parquet_count = fs::read_dir(dir.path())
        .expect("list")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();
    assert_eq!(parquet_count, 2);
}

struct CountingWorkload {
    inner: JsonArrowParquetMaterializer,
    applies: Arc<AtomicUsize>,
}

impl Workload for CountingWorkload {
    fn metadata(&self) -> &WorkloadMetadata {
        self.inner.metadata()
    }

    fn reconcile(
        &self,
        range: &SourceRange,
        fence: &scripture_workload::AcquiredBinding,
    ) -> Result<ReconcileOutcome, WorkloadError> {
        self.inner.reconcile(range, fence)
    }

    fn apply(
        &self,
        range: &SourceRange,
        fence: &scripture_workload::AcquiredBinding,
    ) -> Result<OutputCommit, WorkloadError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(range, fence)
    }
}

#[test]
fn two_host_race_loser_never_reaches_apply() {
    let dir = tempdir().expect("temp");
    let applies = Arc::new(AtomicUsize::new(0));
    let workload = CountingWorkload {
        inner: materializer(dir.path()),
        applies: Arc::clone(&applies),
    };
    let store = InMemoryProgressStore::new();
    let host_a = WorkloadHost::new(store.clone());
    let host_b = WorkloadHost::new(store);
    let token_a = BindingToken::new("host-a").expect("token");
    let token_b = BindingToken::new("host-b").expect("token");

    let fence_a = host_a
        .acquire_binding(binding_epoch(1), &token_a)
        .expect("a wins");
    let err = host_b
        .acquire_binding(binding_epoch(1), &token_b)
        .expect_err("b loses");
    assert!(matches!(err, HostError::FenceHeld));
    assert_eq!(applies.load(Ordering::SeqCst), 0);

    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    host_a
        .process_range(&workload, &batch, &fence_a, &batch_limits())
        .expect("winner applies");
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    // Loser still cannot acquire, so cannot call process_range / apply.
    let again = host_b
        .acquire_binding(binding_epoch(1), &token_b)
        .expect_err("still held");
    assert!(matches!(again, HostError::FenceHeld));
    assert_eq!(applies.load(Ordering::SeqCst), 1);
}

struct LyingWorkload {
    metadata: WorkloadMetadata,
    lie: OutputCommit,
}

impl Workload for LyingWorkload {
    fn metadata(&self) -> &WorkloadMetadata {
        &self.metadata
    }

    fn reconcile(
        &self,
        _range: &SourceRange,
        _fence: &scripture_workload::AcquiredBinding,
    ) -> Result<ReconcileOutcome, WorkloadError> {
        Ok(ReconcileOutcome::AlreadyCommitted(self.lie.clone()))
    }

    fn apply(
        &self,
        _range: &SourceRange,
        _fence: &scripture_workload::AcquiredBinding,
    ) -> Result<OutputCommit, WorkloadError> {
        Err(WorkloadError::Config(
            "apply must not run when AlreadyCommitted is returned".into(),
        ))
    }
}

#[test]
fn mismatched_already_committed_cannot_advance() {
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, 1, &token);
    let delivered = range(0, &[r#"{"id":"a","amount":1}"#]);
    let wrong = range(9, &[r#"{"id":"z","amount":9}"#]);
    let lying = LyingWorkload {
        metadata: WorkloadMetadata {
            workload_id: WorkloadId::new("wl-parquet").expect("id"),
            kind: "lie".into(),
        },
        lie: OutputCommit {
            workload_id: WorkloadId::new("wl-parquet").expect("id"),
            binding_epoch: 1,
            owner_token: token.as_str().to_owned(),
            source_range: wrong,
            output_identity: "fake".into(),
        },
    };
    let err = host
        .process_range(&lying, &delivered, &fence, &batch_limits())
        .expect_err("mismatch");
    assert!(matches!(err, HostError::OutputMismatch(_)));
    assert!(
        store
            .observe(
                &WorkloadId::new("wl-parquet").expect("id"),
                &CanonRef::new("events").expect("canon"),
                &VerseRef::new("v0").expect("verse"),
            )
            .expect("observe")
            .is_none()
    );
}

#[test]
fn batch_limits_enforced_before_reconcile() {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let workload = materializer(dir.path());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, 1, &token);
    let batch = range(0, &[r#"{"id":"a","amount":1}"#, r#"{"id":"b","amount":2}"#]);
    let tight = BatchBoundsConfig {
        max_records: 1,
        max_bytes: 1_048_576,
        max_wall_ms: None,
    };
    let err = host
        .process_range(&workload, &batch, &fence, &tight)
        .expect_err("records");
    assert!(matches!(err, HostError::BatchLimits(_)));

    let wall = BatchBoundsConfig {
        max_records: 100,
        max_bytes: 1_048_576,
        max_wall_ms: Some(5),
    };
    let err = host
        .process_range(&workload, &batch, &fence, &wall)
        .expect_err("wall");
    assert!(matches!(err, HostError::BatchLimits(_)));
}

#[test]
fn malformed_json_fails_batch() {
    let dir = tempdir().expect("temp");
    let workload = materializer(dir.path());
    let batch = range(0, &["not-json"]);
    let fence = scripture_workload::AcquiredBinding {
        binding: binding_epoch(1),
        owner_token: BindingToken::new("t").expect("token"),
    };
    let err = workload.apply(&batch, &fence).expect_err("malformed");
    assert!(matches!(err, WorkloadError::MalformedRecord { .. }));
}

#[test]
fn schema_mismatch_fails_batch() {
    let dir = tempdir().expect("temp");
    let workload = materializer(dir.path());
    let batch = range(0, &[r#"{"id":"a","amount":"nope"}"#]);
    let fence = scripture_workload::AcquiredBinding {
        binding: binding_epoch(1),
        owner_token: BindingToken::new("t").expect("token"),
    };
    let err = workload.apply(&batch, &fence).expect_err("type");
    assert!(matches!(err, WorkloadError::MalformedRecord { .. }));
}

#[test]
fn stale_binding_epoch_rejected_on_renew() {
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token = BindingToken::new("worker-a").expect("token");
    acquire(&host, 1, &token);
    let err = host
        .acquire_binding(binding_epoch(2), &token)
        .expect_err("epoch bump without release");
    assert!(matches!(err, HostError::StaleBinding));
}

#[test]
fn cas_conflict_surfaces() {
    let store = InMemoryProgressStore::new();
    let token = BindingToken::new("worker-a").expect("token");
    store
        .acquire_or_renew(binding_epoch(1), &token)
        .expect("fence");
    let mut cp = ConsumerCheckpoint {
        binding: binding_epoch(1),
        owner_token: token.clone(),
        next_offset: SourceOffset::new(0),
    };
    let v = store
        .compare_and_swap(cp.clone(), None, &token)
        .expect("install");
    cp.next_offset = SourceOffset::new(5);
    store
        .compare_and_swap(cp.clone(), Some(v), &token)
        .expect("advance");
    let err = store
        .compare_and_swap(cp, Some(v), &token)
        .expect_err("stale version");
    assert_eq!(err, scripture_workload::ProgressError::CasConflict);
    let _ = ProgressVersion::new(0);
}

#[test]
fn non_contiguous_range_rejected() {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let workload = materializer(dir.path());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, 1, &token);
    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    host.process_range(&workload, &batch, &fence, &batch_limits())
        .expect("first");
    let skipped = range(5, &[r#"{"id":"z","amount":9}"#]);
    let err = host
        .process_range(&workload, &skipped, &fence, &batch_limits())
        .expect_err("gap");
    assert!(matches!(err, HostError::NonContiguous { .. }));
}

#[test]
fn shared_workload_arc_usable_as_dyn() {
    let dir = tempdir().expect("temp");
    let workload: Arc<dyn Workload> = Arc::new(materializer(dir.path()));
    assert_eq!(workload.metadata().kind, "json_arrow_parquet");
}

#[test]
fn zero_binding_epoch_rejected() {
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token = BindingToken::new("worker-a").expect("token");
    let err = host
        .acquire_binding(binding_epoch(0), &token)
        .expect_err("zero epoch");
    assert!(matches!(
        err,
        HostError::StaleBinding | HostError::Progress(_)
    ));
}
