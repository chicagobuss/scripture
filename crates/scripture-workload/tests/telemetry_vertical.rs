//! Telemetry Canon → Parquet evidence vertical (deterministic, local-only).
//!
//! Proves: MemoryCanonSource → WorkloadHost/materializer → register/manifest
//! chain selection → independent Parquet summary → run-bundle-v1 emission + verifier.
//! Does not contact cloud, Kubernetes, or live Scribes.

use std::fs;

use bytes::Bytes;
use futures::executor::block_on;
use scripture_workload::{
    ArrowFieldConfig, ArrowSchemaConfig, BatchBoundsConfig, BindingKey, BindingToken,
    CanonHistorySource, CanonRef, ConsumerBinding, ConsumerProgressStore, HostError,
    IcebergEvidenceState, InMemoryProgressStore, JsonArrowParquetMaterializer, MalformedPolicy,
    MemoryCanonSource, ParquetCommitManifest, ParquetOutputSummary, ProcessOutcome, ProgressError,
    ProgressRegister, RunBundleEmit, SchemaRef, SourceOffset, SourceOffsetDigest, SummaryError,
    VerseRef, WorkloadHost, WorkloadId, emit_run_bundle_v1, payload_digest,
    summarize_canonical_parquet, verify_vertical_equality, walk_manifest_chain,
};
use serde_json::json;
use tempfile::tempdir;

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
            ArrowFieldConfig {
                name: "source_digest".into(),
                data_type: "utf8".into(),
                nullable: false,
            },
        ],
    }
}

fn batch_limits() -> BatchBoundsConfig {
    BatchBoundsConfig {
        max_records: 100,
        max_bytes: 1_048_576,
        max_wall_ms: None,
    }
}

fn binding_key() -> BindingKey {
    BindingKey::new(
        WorkloadId::new("wl-telemetry").expect("id"),
        CanonRef::new("telemetry").expect("canon"),
        VerseRef::new("host-metrics").expect("verse"),
    )
}

fn test_register(
    frontier: u64,
    epoch: u64,
    last_commit_ref: Option<&str>,
) -> ProgressRegister {
    ProgressRegister {
        binding: ConsumerBinding {
            workload_id: WorkloadId::new("wl-telemetry").expect("id"),
            canon_id: CanonRef::new("telemetry").expect("canon"),
            verse_id: VerseRef::new("host-metrics").expect("verse"),
            binding_epoch: epoch,
        },
        binding_token: BindingToken::new("test-token").expect("token"),
        frontier: SourceOffset::new(frontier),
        last_commit_ref: last_commit_ref.map(str::to_owned),
    }
}

fn write_manifest(path: &std::path::Path, manifest: &ParquetCommitManifest) {
    fs::write(path, serde_json::to_vec_pretty(manifest).expect("json")).expect("write");
}

/// Core acceptance: two adjacent batches, takeover between them, chain covers frontier once.
#[test]
fn multi_batch_manifest_chain_covers_frontier_exactly_once() {
    let dir = tempdir().expect("temp");
    let out = dir.path().join("parquet-out");

    let canon = CanonRef::new("telemetry").expect("canon");
    let verse = VerseRef::new("host-metrics").expect("verse");
    let schema_ref = SchemaRef::new("events.v1").expect("schema");

    let payloads: Vec<&str> = (0..6)
        .map(|index| match index {
            0 => r#"{"id":"a","amount":1}"#,
            1 => r#"{"id":"b","amount":2}"#,
            2 => r#"{"id":"c","amount":3}"#,
            3 => r#"{"id":"d","amount":4}"#,
            4 => r#"{"id":"e","amount":5}"#,
            _ => r#"{"id":"f","amount":6}"#,
        })
        .collect();

    let mut source = MemoryCanonSource::new(canon.clone());
    for (index, payload) in payloads.iter().enumerate() {
        source.commit(
            &verse,
            u64::try_from(index).expect("idx"),
            Bytes::from(payload.as_bytes().to_vec()),
        );
    }

    let range_a = source
        .read_range(
            &canon,
            &verse,
            SourceOffset::new(0),
            SourceOffset::new(3),
            &schema_ref,
        )
        .expect("range a");
    let range_b = source
        .read_range(
            &canon,
            &verse,
            SourceOffset::new(3),
            SourceOffset::new(6),
            &schema_ref,
        )
        .expect("range b");

    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let workload = JsonArrowParquetMaterializer::new(
        WorkloadId::new("wl-telemetry").expect("id"),
        canon.clone(),
        verse.clone(),
        schema_ref.clone(),
        schema(),
        &out,
        MalformedPolicy::FailBatch,
    )
    .expect("materializer");

    let token_a = BindingToken::new("materializer-a").expect("token");
    let fence_a = block_on(host.acquire_binding(binding_key(), &token_a)).expect("acquire a");
    assert_eq!(fence_a.binding.binding_epoch, 1);

    let outcome_a =
        block_on(host.process_range(&workload, &range_a, &fence_a, &batch_limits())).expect("a");
    assert!(matches!(outcome_a, ProcessOutcome::Applied { .. }));

    // Takeover between batches — epoch bumps, frontier and chain head carry forward.
    let token_b = BindingToken::new("materializer-b").expect("token");
    let fence_b = block_on(host.acquire_binding(binding_key(), &token_b)).expect("takeover");
    assert_eq!(fence_b.binding.binding_epoch, 2);

    let mid = block_on(store.observe(
        &WorkloadId::new("wl-telemetry").expect("id"),
        &canon,
        &verse,
    ))
    .expect("observe")
    .expect("register")
    .0;
    assert_eq!(mid.frontier, SourceOffset::new(3));
    assert_eq!(mid.binding.binding_epoch, 2);
    let head_after_a = mid.last_commit_ref.clone().expect("head after batch a");

    let outcome_b =
        block_on(host.process_range(&workload, &range_b, &fence_b, &batch_limits())).expect("b");
    assert!(matches!(outcome_b, ProcessOutcome::Applied { .. }));

    let observed = block_on(store.observe(
        &WorkloadId::new("wl-telemetry").expect("id"),
        &canon,
        &verse,
    ))
    .expect("observe")
    .expect("register")
    .0;
    assert_eq!(observed.frontier, SourceOffset::new(6));
    assert_eq!(observed.binding.binding_epoch, 2);

    let chain = walk_manifest_chain(
        &out,
        observed.last_commit_ref.as_deref().expect("head ref"),
        &observed,
    )
    .expect("chain");
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0].1.first_offset, 0);
    assert_eq!(chain[0].1.next_offset, 3);
    assert_eq!(chain[1].1.first_offset, 3);
    assert_eq!(chain[1].1.next_offset, 6);
    assert_eq!(
        chain[1].1.previous_commit_ref.as_deref(),
        Some(head_after_a.as_str())
    );
    assert!(chain[0].1.previous_commit_ref.is_none());

    let summary = summarize_canonical_parquet(&out, &observed).expect("summary");
    assert_eq!(summary.manifest_count, 2);
    assert_eq!(summary.first_offset, 0);
    assert_eq!(summary.next_offset, 6);
    assert_eq!(summary.row_count, 6);
    assert_eq!(summary.source_offset_digests.len(), 6);
    let offsets: Vec<u64> = summary
        .source_offset_digests
        .iter()
        .map(|item| item.offset)
        .collect();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4, 5]);

    let producer: Vec<(u64, &[u8])> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| (u64::try_from(index).expect("i"), payload.as_bytes()))
        .collect();
    let report = verify_vertical_equality(
        &producer,
        &producer,
        &summary,
        SourceOffset::new(6),
        &canon,
        &verse,
    );
    assert_eq!(report.verdict, "pass");
}

#[test]
fn chain_rejects_cycle() {
    let dir = tempdir().expect("temp");
    let out = dir.path().join("out");
    fs::create_dir_all(&out).expect("mkdir");

    let a = ParquetCommitManifest {
        workload_id: "wl-telemetry".into(),
        binding_epoch: 1,
        owner_token: "a".into(),
        schema_ref: "events.v1".into(),
        canon_id: "telemetry".into(),
        verse_id: "host-metrics".into(),
        first_offset: 0,
        next_offset: 3,
        parquet_file: "a.parquet".into(),
        parquet_digest: "blake3:aa".into(),
        previous_commit_ref: Some("parquet:b.parquet#bb".into()),
    };
    let b = ParquetCommitManifest {
        workload_id: "wl-telemetry".into(),
        binding_epoch: 1,
        owner_token: "a".into(),
        schema_ref: "events.v1".into(),
        canon_id: "telemetry".into(),
        verse_id: "host-metrics".into(),
        first_offset: 3,
        next_offset: 6,
        parquet_file: "b.parquet".into(),
        parquet_digest: "blake3:bb".into(),
        previous_commit_ref: Some("parquet:a.parquet#aa".into()),
    };
    write_manifest(&out.join("a.commit.json"), &a);
    write_manifest(&out.join("b.commit.json"), &b);

    let register = test_register(6, 1, Some("parquet:b.parquet#bb"));
    let err = walk_manifest_chain(&out, "parquet:b.parquet#bb", &register).expect_err("cycle");
    assert!(matches!(err, SummaryError::Chain(_)));
    assert!(err.to_string().contains("cycle"));
}

#[test]
fn chain_rejects_escaped_commit_ref() {
    let dir = tempdir().expect("temp");
    let register = test_register(1, 1, Some("../evil.commit.json"));
    let err = walk_manifest_chain(dir.path(), "../evil.commit.json", &register).expect_err("escape");
    assert!(matches!(err, SummaryError::Manifest(_)));
}

#[test]
fn chain_rejects_continuity_gap() {
    let dir = tempdir().expect("temp");
    let out = dir.path().join("out");
    fs::create_dir_all(&out).expect("mkdir");

    let older = ParquetCommitManifest {
        workload_id: "wl-telemetry".into(),
        binding_epoch: 1,
        owner_token: "a".into(),
        schema_ref: "events.v1".into(),
        canon_id: "telemetry".into(),
        verse_id: "host-metrics".into(),
        first_offset: 0,
        next_offset: 2,
        parquet_file: "older.parquet".into(),
        parquet_digest: "blake3:01".into(),
        previous_commit_ref: None,
    };
    let newer = ParquetCommitManifest {
        workload_id: "wl-telemetry".into(),
        binding_epoch: 1,
        owner_token: "a".into(),
        schema_ref: "events.v1".into(),
        canon_id: "telemetry".into(),
        verse_id: "host-metrics".into(),
        first_offset: 3,
        next_offset: 5,
        parquet_file: "newer.parquet".into(),
        parquet_digest: "blake3:02".into(),
        previous_commit_ref: Some("parquet:older.parquet#01".into()),
    };
    write_manifest(&out.join("older.commit.json"), &older);
    write_manifest(&out.join("newer.commit.json"), &newer);

    let register = test_register(5, 1, Some("parquet:newer.parquet#02"));
    let err = walk_manifest_chain(&out, "parquet:newer.parquet#02", &register).expect_err("gap");
    assert!(matches!(err, SummaryError::Chain(_)));
    assert!(err.to_string().contains("continuity gap"));
}

#[test]
fn summary_rejects_frontier_mismatch() {
    let dir = tempdir().expect("temp");
    let out = dir.path().join("out");
    fs::create_dir_all(&out).expect("mkdir");

    let manifest = ParquetCommitManifest {
        workload_id: "wl-telemetry".into(),
        binding_epoch: 1,
        owner_token: "a".into(),
        schema_ref: "events.v1".into(),
        canon_id: "telemetry".into(),
        verse_id: "host-metrics".into(),
        first_offset: 0,
        next_offset: 3,
        parquet_file: "only.parquet".into(),
        parquet_digest: "blake3:only".into(),
        previous_commit_ref: None,
    };
    write_manifest(&out.join("only.commit.json"), &manifest);
    // Register claims frontier 6 but chain terminal is 3.
    let register = test_register(6, 1, Some("parquet:only.parquet#only"));
    let err = summarize_canonical_parquet(&out, &register).expect_err("frontier");
    assert!(matches!(
        err,
        SummaryError::FrontierMismatch {
            frontier: 6,
            terminal_next: 3
        }
    ));
}

#[test]
fn summary_rejects_stale_epoch() {
    let dir = tempdir().expect("temp");
    let out = dir.path().join("out");
    fs::create_dir_all(&out).expect("mkdir");

    let manifest = ParquetCommitManifest {
        workload_id: "wl-telemetry".into(),
        binding_epoch: 3,
        owner_token: "future".into(),
        schema_ref: "events.v1".into(),
        canon_id: "telemetry".into(),
        verse_id: "host-metrics".into(),
        first_offset: 0,
        next_offset: 1,
        parquet_file: "future.parquet".into(),
        parquet_digest: "blake3:future".into(),
        previous_commit_ref: None,
    };
    write_manifest(&out.join("future.commit.json"), &manifest);
    let register = test_register(1, 2, Some("parquet:future.parquet#future"));
    let err = walk_manifest_chain(&out, "parquet:future.parquet#future", &register).expect_err("epoch");
    assert!(matches!(err, SummaryError::Chain(_)));
    assert!(err.to_string().contains("stale register snapshot"));
}

#[test]
fn verifier_rejects_duplicate_parquet_offsets() {
    let summary = ParquetOutputSummary {
        status: "present".into(),
        binding_epoch: 1,
        row_count: 2,
        schema_fields: vec!["source_digest".into()],
        source_digests: vec!["d0".into(), "d0".into()],
        source_offset_digests: vec![
            SourceOffsetDigest {
                offset: 0,
                digest: "d0".into(),
            },
            SourceOffsetDigest {
                offset: 0,
                digest: "d0".into(),
            },
        ],
        data_objects: vec!["x.parquet".into()],
        canonical_manifest: "x.commit.json".into(),
        first_offset: 0,
        next_offset: 1,
        manifest_count: 1,
        note: "test".into(),
    };
    let report = verify_vertical_equality(
        &[(0, b"x")],
        &[(0, b"x")],
        &summary,
        SourceOffset::new(1),
        &CanonRef::new("telemetry").expect("canon"),
        &VerseRef::new("host-metrics").expect("verse"),
    );
    assert_eq!(report.verdict, "fail");
    assert!(
        report
            .notes
            .iter()
            .any(|note| note.contains("row_count") || note.contains("exactly once"))
    );
}

#[test]
fn canon_to_parquet_register_summary_bundle_and_verifier() {
    let dir = tempdir().expect("temp");
    let out = dir.path().join("parquet-out");
    let bundle_dir = dir.path().join("run-bundle");

    let canon = CanonRef::new("telemetry").expect("canon");
    let verse = VerseRef::new("host-metrics").expect("verse");
    let schema_ref = SchemaRef::new("events.v1").expect("schema");

    let payloads = [
        r#"{"id":"a","amount":1}"#,
        r#"{"id":"b","amount":2}"#,
        r#"{"id":"c","amount":3}"#,
    ];

    let mut source = MemoryCanonSource::new(canon.clone());
    for (index, payload) in payloads.iter().enumerate() {
        source.commit(
            &verse,
            u64::try_from(index).expect("idx"),
            Bytes::from(payload.as_bytes().to_vec()),
        );
    }

    let range = source
        .read_range(
            &canon,
            &verse,
            SourceOffset::new(0),
            SourceOffset::new(3),
            &schema_ref,
        )
        .expect("canon read");

    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let workload = JsonArrowParquetMaterializer::new(
        WorkloadId::new("wl-telemetry").expect("id"),
        canon.clone(),
        verse.clone(),
        schema_ref,
        schema(),
        &out,
        MalformedPolicy::FailBatch,
    )
    .expect("materializer");

    let token = BindingToken::new("materializer-1").expect("token");
    let fence = block_on(host.acquire_binding(binding_key(), &token)).expect("acquire");
    block_on(host.process_range(&workload, &range, &fence, &batch_limits())).expect("process");

    let token_b = BindingToken::new("materializer-2").expect("token");
    let fence_b = block_on(host.acquire_binding(binding_key(), &token_b)).expect("takeover");
    assert_eq!(fence_b.binding.binding_epoch, 2);

    let observed = block_on(store.observe(
        &WorkloadId::new("wl-telemetry").expect("id"),
        &canon,
        &verse,
    ))
    .expect("observe")
    .expect("register")
    .0;
    assert_eq!(observed.frontier, SourceOffset::new(3));

    let summary = summarize_canonical_parquet(&out, &observed).expect("summary");
    assert_eq!(summary.row_count, 3);
    assert_eq!(summary.binding_epoch, 1);
    assert_eq!(summary.manifest_count, 1);

    let producer: Vec<(u64, &[u8])> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| (u64::try_from(index).expect("i"), payload.as_bytes()))
        .collect();
    let report = verify_vertical_equality(
        &producer,
        &producer,
        &summary,
        SourceOffset::new(3),
        &canon,
        &verse,
    );
    assert_eq!(report.verdict, "pass");

    emit_run_bundle_v1(
        &bundle_dir,
        &RunBundleEmit {
            run_id: "local-vertical-proof-001".into(),
            collected_at: "2026-07-21T06:00:00.000Z".into(),
            scripture_rev: "local".into(),
            holylog_rev: "local".into(),
            namespace: "scripture-lab-local".into(),
            object_prefixes: vec!["lab/local-vertical-proof-001/out/".into()],
            producer_ledger_rows: payloads
                .iter()
                .enumerate()
                .map(|(index, payload)| {
                    json!({
                        "row_type": "send",
                        "producer_id": "otel-lab-a",
                        "verse": verse.as_str(),
                        "seq": index,
                        "payload_digest": payload_digest(payload.as_bytes()),
                        "ack_status": format!("committed:{index}:{}", index + 1),
                        "unacked": false
                    })
                })
                .collect(),
            message_rows: payloads
                .iter()
                .enumerate()
                .map(|(index, payload)| {
                    json!({
                        "digest": payload_digest(payload.as_bytes()),
                        "seq": index,
                        "offset": index,
                        "byte_length": payload.len(),
                        "canon": canon.as_str(),
                        "verse": verse.as_str()
                    })
                })
                .collect(),
            scribe_logs: vec![(
                "scribe-a".into(),
                vec![json!({
                    "at": "2026-07-21T06:00:00.000Z",
                    "level": "info",
                    "event_kind": "serving",
                    "message": "local fixture — not a live authority claim"
                })],
            )],
            object_inventory: json!({
                "provider": "local-fs",
                "label": "inventory observation",
                "objects": []
            }),
            register: json!({
                "workload_id": "wl-telemetry",
                "binding_epoch": observed.binding.binding_epoch,
                "frontier": observed.frontier.get(),
                "last_commit_ref": observed.last_commit_ref
            }),
            manifests: vec![(
                "epoch-canonical.json".into(),
                json!({
                    "binding_epoch": summary.binding_epoch,
                    "canonical": true,
                    "row_count": summary.row_count,
                    "source_digests": summary.source_digests
                }),
            )],
            parquet_summary: summary.clone(),
            iceberg: IcebergEvidenceState::Absent,
            iceberg_detail: "No Iceberg metadata/snapshot commit in this local vertical proof."
                .into(),
            holylog_oracle: None,
            iceberg_verified: None,
        },
    )
    .expect("emit bundle");

    assert!(bundle_dir.join("manifest.json").is_file());
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(bundle_dir.join("manifest.json")).expect("man"))
            .expect("json");
    assert!(
        manifest["verdicts"]
            .as_array()
            .expect("verdicts")
            .iter()
            .any(|item| item["label"] == "Holylog oracle" && item["verdict"] == "not_run")
    );
}

fn pre_manifest_zombie(a_manifest_before_b_advance: bool) {
    let dir = tempdir().expect("temp");
    let store = InMemoryProgressStore::new();
    let host = WorkloadHost::new(store.clone());
    let workload = JsonArrowParquetMaterializer::new(
        WorkloadId::new("wl-parquet").expect("id"),
        CanonRef::new("events").expect("canon"),
        VerseRef::new("v0").expect("verse"),
        SchemaRef::new("events.v1").expect("schema"),
        schema(),
        dir.path(),
        MalformedPolicy::FailBatch,
    )
    .expect("materializer");

    let mut source = MemoryCanonSource::new(CanonRef::new("events").expect("canon"));
    source.commit(
        &VerseRef::new("v0").expect("verse"),
        0,
        Bytes::from_static(br#"{"id":"a","amount":1}"#),
    );
    let batch = source
        .read_range(
            &CanonRef::new("events").expect("canon"),
            &VerseRef::new("v0").expect("verse"),
            SourceOffset::new(0),
            SourceOffset::new(1),
            &SchemaRef::new("events.v1").expect("schema"),
        )
        .expect("read");

    let key = BindingKey::new(
        WorkloadId::new("wl-parquet").expect("id"),
        CanonRef::new("events").expect("canon"),
        VerseRef::new("v0").expect("verse"),
    );
    let token_a = BindingToken::new("zombie-a").expect("token");
    let fence_a = block_on(host.acquire_binding(key.clone(), &token_a)).expect("A");
    assert_eq!(fence_a.binding.binding_epoch, 1);

    let parquet_a = workload
        .publish_parquet_stage(&batch, &fence_a)
        .expect("A parquet");

    let token_b = BindingToken::new("worker-b").expect("token");
    let fence_b = block_on(host.acquire_binding(key, &token_b)).expect("B");
    assert_eq!(fence_b.binding.binding_epoch, 2);

    if a_manifest_before_b_advance {
        let commit_a = workload
            .publish_manifest_stage(&batch, &fence_a, &parquet_a, None)
            .expect("A late manifest before B CAS");
        let err = block_on(store.advance(
            &fence_a,
            batch.next_offset,
            commit_a.last_commit_ref().to_owned(),
        ))
        .expect_err("A CAS before B");
        assert!(matches!(
            err,
            ProgressError::StaleBinding | ProgressError::FenceHeld
        ));
    }

    let outcome_b =
        block_on(host.process_range(&workload, &batch, &fence_b, &batch_limits())).expect("B");
    assert!(matches!(outcome_b, ProcessOutcome::Applied { .. }));
    let after_b = block_on(store.observe(
        &WorkloadId::new("wl-parquet").expect("id"),
        &CanonRef::new("events").expect("canon"),
        &VerseRef::new("v0").expect("verse"),
    ))
    .expect("obs")
    .expect("reg")
    .0;
    assert_eq!(after_b.binding.binding_epoch, 2);
    assert_eq!(after_b.frontier, SourceOffset::new(1));
    let b_ref = after_b.last_commit_ref.clone().expect("b ref");

    if !a_manifest_before_b_advance {
        let commit_a = workload
            .publish_manifest_stage(&batch, &fence_a, &parquet_a, None)
            .expect("A late manifest after B");
        let err = block_on(store.advance(
            &fence_a,
            batch.next_offset,
            commit_a.last_commit_ref().to_owned(),
        ))
        .expect_err("A CAS after B");
        assert!(matches!(
            err,
            ProgressError::StaleBinding | ProgressError::FenceHeld
        ));
    }

    let final_reg = block_on(store.observe(
        &WorkloadId::new("wl-parquet").expect("id"),
        &CanonRef::new("events").expect("canon"),
        &VerseRef::new("v0").expect("verse"),
    ))
    .expect("obs")
    .expect("reg")
    .0;
    assert_eq!(final_reg.frontier, SourceOffset::new(1));
    assert_eq!(final_reg.last_commit_ref.as_deref(), Some(b_ref.as_str()));
    assert_eq!(final_reg.binding.binding_epoch, 2);

    let err = block_on(host.process_range(&workload, &batch, &fence_a, &batch_limits()))
        .expect_err("zombie process");
    assert!(matches!(
        err,
        HostError::FenceHeld | HostError::StaleBinding
    ));

    let summary = summarize_canonical_parquet(dir.path(), &final_reg).expect("summary");
    assert_eq!(summary.binding_epoch, 2);
}

#[test]
fn zombie_pre_manifest_a_publishes_before_b_advance() {
    pre_manifest_zombie(true);
}

#[test]
fn zombie_pre_manifest_a_publishes_after_b_advance() {
    pre_manifest_zombie(false);
}

#[test]
fn emit_rejects_bad_scribe_id() {
    let dir = tempdir().expect("temp");
    let summary = ParquetOutputSummary {
        status: "present".into(),
        binding_epoch: 1,
        row_count: 0,
        schema_fields: vec![],
        source_digests: vec![],
        source_offset_digests: vec![],
        data_objects: vec![],
        canonical_manifest: "x.commit.json".into(),
        first_offset: 0,
        next_offset: 0,
        manifest_count: 0,
        note: "test".into(),
    };
    let err = emit_run_bundle_v1(
        dir.path(),
        &RunBundleEmit {
            run_id: "x".into(),
            collected_at: "t".into(),
            scripture_rev: "s".into(),
            holylog_rev: "h".into(),
            namespace: "n".into(),
            object_prefixes: vec![],
            producer_ledger_rows: vec![],
            message_rows: vec![],
            scribe_logs: vec![("../evil".into(), vec![])],
            object_inventory: json!({}),
            register: json!({}),
            manifests: vec![],
            parquet_summary: summary,
            iceberg: IcebergEvidenceState::Absent,
            iceberg_detail: "absent".into(),
            holylog_oracle: None,
            iceberg_verified: None,
        },
    )
    .expect_err("bad scribe");
    assert!(err.to_string().contains("scribe id"));
}
