//! Deterministic in-memory contract tests + Parquet crash/replay proofs.
//!
//! Progress store remains an in-memory model/proof — not a durability claim.

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::executor::block_on;
use scripture_workload::{
    ArrowFieldConfig, ArrowSchemaConfig, BatchBoundsConfig, BindingKey, BindingToken, CanonRecord,
    CanonRef, ConsumerProgressStore, HostError, InMemoryProgressStore,
    JsonArrowParquetMaterializer, MalformedPolicy, OutputCommit, ParquetCommitManifest,
    ProcessOutcome, ProgressError, ProgressRegister, ReconcileOutcome, SchemaRef, SourceOffset,
    SourceRange, VerseRef, Workload, WorkloadError, WorkloadHost, WorkloadId, WorkloadMetadata,
};
use tempfile::tempdir;

fn binding_key() -> BindingKey {
    BindingKey::new(
        WorkloadId::new("wl-parquet").expect("id"),
        CanonRef::new("events").expect("canon"),
        VerseRef::new("v0").expect("verse"),
    )
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
    token: &BindingToken,
) -> scripture_workload::AcquiredBinding {
    block_on(host.acquire_binding(binding_key(), token)).expect("acquire")
}

fn observe_register(store: &InMemoryProgressStore) -> ProgressRegister {
    block_on(store.observe(
        &WorkloadId::new("wl-parquet").expect("id"),
        &CanonRef::new("events").expect("canon"),
        &VerseRef::new("v0").expect("verse"),
    ))
    .expect("observe")
    .expect("register present")
    .0
}

#[test]
fn apply_then_replay_reconcile_advances_without_duplicating_parquet() {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let workload = materializer(dir.path());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, &token);
    let batch = range(0, &[r#"{"id":"a","amount":1}"#, r#"{"id":"b","amount":2}"#]);

    let first =
        block_on(host.process_range(&workload, &batch, &fence, &batch_limits())).expect("apply");
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
    let second = block_on(host.process_range(&workload, &next, &fence, &batch_limits()))
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
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let writer_token = BindingToken::new("writer").expect("token");
    let writer_fence = acquire(&host, &writer_token);
    assert_eq!(writer_fence.binding.binding_epoch, 1);
    let commit = workload
        .apply(&batch, &writer_fence)
        .expect("apply durable output");
    assert!(commit.output_identity.contains("parquet:"));
    assert_eq!(commit.binding_epoch, 1);
    assert_eq!(commit.owner_token, "writer");
    // Crash before register advance: frontier still 0 under epoch 1.

    // Restart presents a fresh process token → epoch always bumps.
    let restart = BindingToken::new("restart").expect("token");
    let fence = acquire(&host, &restart);
    assert_eq!(fence.binding.binding_epoch, 2);
    let outcome = block_on(host.process_range(&workload, &batch, &fence, &batch_limits()))
        .expect("re-execute after crash under new epoch");
    // Stale epoch-1 objects are under different keys; epoch-2 path is Absent → Applied.
    assert!(matches!(outcome, ProcessOutcome::Applied { .. }));

    let observed = observe_register(&store);
    assert_eq!(observed.frontier, SourceOffset::new(1));
    assert_eq!(observed.binding_token.as_str(), "restart");
    assert_eq!(observed.binding.binding_epoch, 2);
    assert!(observed.last_commit_ref.is_some());
    // Epoch-1 commit ref is not the canonical register value after restart re-apply.
    assert_ne!(
        observed.last_commit_ref.as_deref(),
        Some(commit.last_commit_ref())
    );

    let parquet_count = fs::read_dir(dir.path())
        .expect("list")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();
    // Epoch-1 garbage + epoch-2 canonical file may both sit; register is the index.
    assert_eq!(parquet_count, 2);
}

#[test]
fn manifest_with_wrong_owner_token_is_not_adopted() {
    let dir = tempdir().expect("temp");
    let workload = materializer(dir.path());
    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, &token);

    workload.apply(&batch, &fence).expect("publish output");
    let manifest_path = fs::read_dir(dir.path())
        .expect("list output")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.to_string_lossy().ends_with(".commit.json"))
        .expect("manifest");
    let mut manifest: ParquetCommitManifest =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
            .expect("decode manifest");
    manifest.owner_token = "not-the-fence-holder".into();
    fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest).expect("encode tampered manifest"),
    )
    .expect("write tampered manifest");

    assert!(matches!(
        workload.reconcile(&batch, &fence).expect("reconcile"),
        ReconcileOutcome::Indeterminate { .. }
    ));
}

#[test]
fn partial_parquet_is_indeterminate_never_deleted_and_does_not_advance() {
    let dir = tempdir().expect("temp");
    let workload = materializer(dir.path());
    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, &token);
    fs::create_dir_all(dir.path()).expect("dir");
    let key = workload.object_key(&batch, fence.binding.binding_epoch);
    let partial = dir.path().join(format!("{key}.parquet.tmp"));
    fs::write(&partial, b"truncated").expect("partial");

    let err = block_on(host.process_range(&workload, &batch, &fence, &batch_limits()))
        .expect_err("must fail closed");
    assert!(matches!(err, HostError::Indeterminate(_)));
    assert!(partial.exists(), "unknown/partial tmp must not be deleted");
    let observed = observe_register(&store);
    assert_eq!(observed.frontier, SourceOffset::new(0));
    assert!(observed.last_commit_ref.is_none());
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
        canon_id: evil_canon.clone(),
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
    let fence = block_on(host.acquire_binding(
        BindingKey::new(
            WorkloadId::new("wl-parquet").expect("id"),
            evil_canon,
            VerseRef::new("v0/../x").expect("verse"),
        ),
        &token,
    ))
    .expect("acquire");
    block_on(host.process_range(&workload, &batch, &fence, &batch_limits())).expect("apply");
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
    assert_ne!(a.object_key(&batch, 1), b.object_key(&batch, 1));
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token_a = BindingToken::new("a").expect("token");
    let token_b = BindingToken::new("b").expect("token");
    let fence_a = block_on(host.acquire_binding(
        BindingKey::new(
            WorkloadId::new("wl-a").expect("id"),
            CanonRef::new("events").expect("canon"),
            VerseRef::new("v0").expect("verse"),
        ),
        &token_a,
    ))
    .expect("acquire a");
    let fence_b = block_on(host.acquire_binding(
        BindingKey::new(
            WorkloadId::new("wl-b").expect("id"),
            CanonRef::new("events").expect("canon"),
            VerseRef::new("v0").expect("verse"),
        ),
        &token_b,
    ))
    .expect("acquire b");
    block_on(host.process_range(&a, &batch, &fence_a, &batch_limits())).expect("apply a");
    block_on(host.process_range(&b, &batch, &fence_b, &batch_limits())).expect("apply b");
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
fn two_host_race_loser_never_advances_canonical_progress() {
    // Under the one-register model a second token takeovers (bumps epoch).
    // Safety: the deposed holder never produces a canonical register advance.
    let dir = tempdir().expect("temp");
    let applies = Arc::new(AtomicUsize::new(0));
    let workload = CountingWorkload {
        inner: materializer(dir.path()),
        applies: Arc::clone(&applies),
    };
    let store = InMemoryProgressStore::new();
    let host_a = WorkloadHost::new(store.clone());
    let host_b = WorkloadHost::new(store.clone());
    let token_a = BindingToken::new("host-a").expect("token");
    let token_b = BindingToken::new("host-b").expect("token");

    let fence_a =
        block_on(host_a.acquire_binding(binding_key(), &token_a)).expect("a wins epoch 1");
    assert_eq!(fence_a.binding.binding_epoch, 1);
    let fence_b =
        block_on(host_b.acquire_binding(binding_key(), &token_b)).expect("b takeovers epoch 2");
    assert_eq!(fence_b.binding.binding_epoch, 2);
    assert_eq!(applies.load(Ordering::SeqCst), 0);

    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    let err = block_on(host_a.process_range(&workload, &batch, &fence_a, &batch_limits()))
        .expect_err("deposed A cannot process");
    assert!(matches!(
        err,
        HostError::FenceHeld | HostError::StaleBinding
    ));
    assert_eq!(applies.load(Ordering::SeqCst), 0);

    block_on(host_b.process_range(&workload, &batch, &fence_b, &batch_limits()))
        .expect("winner applies");
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    let observed = observe_register(&store);
    assert_eq!(observed.binding.binding_epoch, 2);
    assert_eq!(observed.frontier, SourceOffset::new(1));
    assert_eq!(observed.binding_token.as_str(), "host-b");
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
    let fence = acquire(&host, &token);
    let delivered = range(0, &[r#"{"id":"a","amount":1}"#]);
    let wrong = range(9, &[r#"{"id":"z","amount":9}"#]);
    let lying = LyingWorkload {
        metadata: WorkloadMetadata {
            workload_id: WorkloadId::new("wl-parquet").expect("id"),
            kind: "lie".into(),
        },
        lie: OutputCommit {
            workload_id: WorkloadId::new("wl-parquet").expect("id"),
            binding_epoch: fence.binding.binding_epoch,
            owner_token: token.as_str().to_owned(),
            source_range: wrong,
            output_identity: "fake".into(),
        },
    };
    let err = block_on(host.process_range(&lying, &delivered, &fence, &batch_limits()))
        .expect_err("mismatch");
    assert!(matches!(err, HostError::OutputMismatch(_)));
    let observed = observe_register(&store);
    assert_eq!(observed.frontier, SourceOffset::new(0));
    assert!(observed.last_commit_ref.is_none());
}

#[test]
fn batch_limits_enforced_before_reconcile() {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let workload = materializer(dir.path());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, &token);
    let batch = range(0, &[r#"{"id":"a","amount":1}"#, r#"{"id":"b","amount":2}"#]);
    let tight = BatchBoundsConfig {
        max_records: 1,
        max_bytes: 1_048_576,
        max_wall_ms: None,
    };
    let err = block_on(host.process_range(&workload, &batch, &fence, &tight)).expect_err("records");
    assert!(matches!(err, HostError::BatchLimits(_)));

    let wall = BatchBoundsConfig {
        max_records: 100,
        max_bytes: 1_048_576,
        max_wall_ms: Some(5),
    };
    let err = block_on(host.process_range(&workload, &batch, &fence, &wall)).expect_err("wall");
    assert!(matches!(err, HostError::BatchLimits(_)));
}

#[test]
fn malformed_json_fails_batch() {
    let dir = tempdir().expect("temp");
    let workload = materializer(dir.path());
    let batch = range(0, &["not-json"]);
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token = BindingToken::new("t").expect("token");
    let fence = acquire(&host, &token);
    let err = workload.apply(&batch, &fence).expect_err("malformed");
    assert!(matches!(err, WorkloadError::MalformedRecord { .. }));
}

#[test]
fn schema_mismatch_fails_batch() {
    let dir = tempdir().expect("temp");
    let workload = materializer(dir.path());
    let batch = range(0, &[r#"{"id":"a","amount":"nope"}"#]);
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token = BindingToken::new("t").expect("token");
    let fence = acquire(&host, &token);
    let err = workload.apply(&batch, &fence).expect_err("type");
    assert!(matches!(err, WorkloadError::MalformedRecord { .. }));
}

#[test]
fn same_token_renew_retains_epoch() {
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token = BindingToken::new("worker-a").expect("token");
    let first = acquire(&host, &token);
    let renewed = acquire(&host, &token);
    assert_eq!(first.binding.binding_epoch, 1);
    assert_eq!(renewed.binding.binding_epoch, 1);
}

#[test]
fn frontier_regression_rejected() {
    let store = InMemoryProgressStore::new();
    let token = BindingToken::new("worker-a").expect("token");
    let fence = block_on(store.acquire_or_renew(binding_key(), &token)).expect("fence");
    block_on(store.advance(&fence, SourceOffset::new(5), "commit-a".into())).expect("advance");
    let err = block_on(store.advance(&fence, SourceOffset::new(5), "commit-b".into()))
        .expect_err("no equal frontier");
    assert_eq!(err, ProgressError::FrontierRegression);
    let err = block_on(store.advance(&fence, SourceOffset::new(3), "commit-c".into()))
        .expect_err("no regress");
    assert_eq!(err, ProgressError::FrontierRegression);
    let observed = observe_register(&store);
    assert_eq!(observed.frontier, SourceOffset::new(5));
    assert_eq!(observed.last_commit_ref.as_deref(), Some("commit-a"));
}

#[test]
fn non_contiguous_range_rejected() {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let workload = materializer(dir.path());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, &token);
    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    block_on(host.process_range(&workload, &batch, &fence, &batch_limits())).expect("first");
    let skipped = range(5, &[r#"{"id":"z","amount":9}"#]);
    let err = block_on(host.process_range(&workload, &skipped, &fence, &batch_limits()))
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
fn first_acquire_assigns_nonzero_epoch() {
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store);
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, &token);
    assert_ne!(fence.binding.binding_epoch, 0);
    assert_eq!(fence.binding.binding_epoch, 1);
}

#[test]
fn restart_always_bumps_epoch() {
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let t1 = BindingToken::new("proc-1").expect("token");
    let fence = acquire(&host, &t1);
    assert_eq!(fence.binding.binding_epoch, 1);
    block_on(store.advance(&fence, SourceOffset::new(4), "c1".into())).expect("advance");
    let t2 = BindingToken::new("proc-2").expect("token");
    let restarted = acquire(&host, &t2);
    assert_eq!(restarted.binding.binding_epoch, 2);
    let observed = observe_register(&store);
    assert_eq!(observed.frontier, SourceOffset::new(4));
    assert_eq!(observed.last_commit_ref.as_deref(), Some("c1"));
    assert_eq!(observed.binding_token.as_str(), "proc-2");
}

#[test]
fn schema_ref_mismatch_reconcile_refuses() {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let workload = materializer(dir.path());
    let token = BindingToken::new("worker-a").expect("token");
    let fence = acquire(&host, &token);
    let mut batch = range(0, &[r#"{"id":"a","amount":1}"#]);
    batch.schema_ref = SchemaRef::new("events.v-other").expect("schema");
    let err = block_on(host.process_range(&workload, &batch, &fence, &batch_limits()))
        .expect_err("schema mismatch");
    assert!(matches!(err, HostError::Workload(WorkloadError::Schema(_))));
    let observed = observe_register(&store);
    assert_eq!(observed.frontier, SourceOffset::new(0));
    assert!(observed.last_commit_ref.is_none());
}

fn zombie_schedule(a_resumes_before_b_advance: bool) {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let workload = materializer(dir.path());
    let batch = range(0, &[r#"{"id":"a","amount":1}"#]);

    let token_a = BindingToken::new("zombie-a").expect("token");
    let fence_a = acquire(&host, &token_a);
    assert_eq!(fence_a.binding.binding_epoch, 1);

    // A produces output, then pauses before register advance (manifest publish done).
    let commit_a = workload
        .apply(&batch, &fence_a)
        .expect("A publishes under epoch 1");
    assert_eq!(commit_a.binding_epoch, 1);

    let token_b = BindingToken::new("worker-b").expect("token");
    let fence_b = acquire(&host, &token_b);
    assert_eq!(fence_b.binding.binding_epoch, 2);

    if a_resumes_before_b_advance {
        // A resumes before B advances: stale CAS must fail; frontier stays 0.
        let err = block_on(store.advance(
            &fence_a,
            batch.next_offset,
            commit_a.last_commit_ref().to_owned(),
        ))
        .expect_err("A stale before B");
        assert!(matches!(
            err,
            ProgressError::StaleBinding | ProgressError::FenceHeld
        ));
        assert_eq!(observe_register(&store).frontier, SourceOffset::new(0));
    }

    // B reconciles (epoch-2 keys absent), re-executes, publishes, advances.
    let outcome_b =
        block_on(host.process_range(&workload, &batch, &fence_b, &batch_limits())).expect("B wins");
    assert!(matches!(outcome_b, ProcessOutcome::Applied { .. }));
    let after_b = observe_register(&store);
    assert_eq!(after_b.binding.binding_epoch, 2);
    assert_eq!(after_b.frontier, SourceOffset::new(1));
    assert_eq!(after_b.binding_token.as_str(), "worker-b");
    let b_ref = after_b.last_commit_ref.clone().expect("b commit ref");

    if !a_resumes_before_b_advance {
        // A resumes after B advanced (late multipart-complete analogue).
        let err = block_on(store.advance(
            &fence_a,
            batch.next_offset,
            commit_a.last_commit_ref().to_owned(),
        ))
        .expect_err("A stale after B");
        assert!(matches!(
            err,
            ProgressError::StaleBinding | ProgressError::FenceHeld
        ));
    } else {
        // A tries again after B advanced; still cannot win.
        let err = block_on(store.advance(
            &fence_a,
            SourceOffset::new(99),
            commit_a.last_commit_ref().to_owned(),
        ))
        .expect_err("A still stale");
        assert!(matches!(
            err,
            ProgressError::StaleBinding | ProgressError::FenceHeld
        ));
    }

    let final_reg = observe_register(&store);
    assert_eq!(final_reg.frontier, SourceOffset::new(1));
    assert_eq!(final_reg.last_commit_ref.as_deref(), Some(b_ref.as_str()));
    assert_eq!(final_reg.binding.binding_epoch, 2);
    // Frontier never regresses; A's epoch-1 commit is never adopted.
    assert_ne!(
        final_reg.last_commit_ref.as_deref(),
        Some(commit_a.last_commit_ref())
    );

    // Deposed A also cannot process_range.
    let err = block_on(host.process_range(&workload, &batch, &fence_a, &batch_limits()))
        .expect_err("zombie process_range");
    assert!(matches!(
        err,
        HostError::FenceHeld | HostError::StaleBinding
    ));
}

#[test]
fn zombie_schedule_a_resumes_before_b_advance() {
    zombie_schedule(true);
}

#[test]
fn zombie_schedule_a_resumes_after_b_advance() {
    zombie_schedule(false);
}
