//! Holylog/Scripture API oracle for producer raw-lines resilience (families 12–17).
//!
//! Proves exact predecessor sealed-tail = successor global start (when cutover),
//! canonical Serving authority (owner/term), and cross-generation chunk readback.
//! Does not parse private S3 object layout by hand.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use holylog::provision::{ResolvedLoglet, resolve_read_seal};
use holylog::virtual_log::{ConditionalRegister, LogletResolver, VersionedState, VirtualLog};
use holylog_object_store::{ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::path::Path;
use scripture::serving_authority::{AuthorityState, ServingAuthorityRecord, WriterTerm};
use scripture::{OwnerId, decode_chunk};
use scripture_runtime::{
    BackendProfile, ObjectStorePartsFactory, PartsFactory, ProcessLogletResolver, connect_s3_compat,
};

use crate::CampaignError;

const LOGLET_K: u64 = 2;

/// Expected Serving authority after promote / baseline serve.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExpectedAuthority {
    /// Canonical owner bytes (16).
    pub owner: OwnerId,
    /// Writer term.
    pub term: u64,
}

/// Outcome of the Holylog-state / readback oracle.
#[derive(Debug, Clone)]
pub(crate) struct CutoverOracleReport {
    /// Redacted observation bundle for artifacts.
    pub observation: serde_json::Value,
}

/// Proves lawful lossless cutover against the shared HA root via Holylog APIs.
pub(crate) async fn prove_raw_lines_cutover(
    rustfs_endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    ha_prefix: &str,
    expected_payloads: &[&str],
    expected_authority: ExpectedAuthority,
) -> Result<CutoverOracleReport, CampaignError> {
    prove_membership_authority_readback(
        rustfs_endpoint,
        bucket,
        access_key,
        secret_key,
        ha_prefix,
        expected_payloads,
        None,
        expected_authority,
        true,
    )
    .await
}

/// Family 12: single-generation Serving + exact payload readback.
pub(crate) async fn prove_serving_baseline(
    rustfs_endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    ha_prefix: &str,
    expected_payloads: &[&str],
    expected_authority: ExpectedAuthority,
) -> Result<CutoverOracleReport, CampaignError> {
    prove_membership_authority_readback(
        rustfs_endpoint,
        bucket,
        access_key,
        secret_key,
        ha_prefix,
        expected_payloads,
        None,
        expected_authority,
        false,
    )
    .await
}

/// Family 16: required payloads plus an optional in-flight identity.
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) async fn prove_allowed_set_cutover(
    rustfs_endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    ha_prefix: &str,
    required_payloads: &[&str],
    optional_inflight: Option<&str>,
    expected_authority: ExpectedAuthority,
) -> Result<CutoverOracleReport, CampaignError> {
    prove_membership_authority_readback(
        rustfs_endpoint,
        bucket,
        access_key,
        secret_key,
        ha_prefix,
        required_payloads,
        optional_inflight,
        expected_authority,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prove_membership_authority_readback(
    rustfs_endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    ha_prefix: &str,
    required_payloads: &[&str],
    optional_inflight: Option<&str>,
    expected_authority: ExpectedAuthority,
    require_cutover: bool,
) -> Result<CutoverOracleReport, CampaignError> {
    let store = connect_s3_compat(rustfs_endpoint, bucket, "us-east-1", access_key, secret_key)
        .map_err(|error| CampaignError::Scenario(format!("oracle store connect: {error}")))?;

    let root = ha_prefix.trim_end_matches('/');
    let register_object = Path::from(root.to_owned()).join(register_path("verse").as_ref());
    let register = Arc::new(
        ObjectStoreConditionalRegister::new(
            Arc::clone(&store),
            register_object,
            BackendProfile::RustFs.register_capabilities(),
        )
        .map_err(|error| CampaignError::Scenario(format!("oracle register: {error}")))?,
    ) as Arc<dyn ConditionalRegister>;

    let parts = Arc::new(ObjectStorePartsFactory::new(
        Arc::clone(&store),
        root,
        BackendProfile::RustFs.drive_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::new(ObjectStoreMetrics::default()),
    ));
    let resolver = Arc::new(ProcessLogletResolver::default());

    let membership_log = VirtualLog::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
    );
    let observed = membership_log
        .observe_membership()
        .await
        .map_err(|error| CampaignError::Scenario(format!("oracle observe_membership: {error}")))?;

    let generations = &observed.state.generations;
    if require_cutover && generations.len() < 2 {
        return Err(CampaignError::Scenario(format!(
            "cutover oracle expected predecessor+successor generations, got {}",
            generations.len()
        )));
    }
    if generations.is_empty() {
        return Err(CampaignError::Scenario(
            "oracle expected at least one generation".into(),
        ));
    }

    for generation in generations {
        let durable = parts
            .open(&generation.loglet_id)
            .map_err(|error| CampaignError::Scenario(format!("oracle open loglet: {error}")))?;
        let view = resolve_read_seal(durable.components(LOGLET_K))
            .await
            .map_err(|error| {
                CampaignError::Scenario(format!("oracle resolve_read_seal: {error}"))
            })?;
        resolver.insert_read_seal(generation.loglet_id.clone(), Arc::new(view));
    }

    let mut boundary = serde_json::Value::Null;
    if require_cutover {
        let predecessor = generations[generations.len() - 2].clone();
        let successor = generations[generations.len() - 1].clone();
        let pred_resolved = resolver
            .resolve(&predecessor.loglet_id)
            .await
            .map_err(|error| {
                CampaignError::Scenario(format!("oracle resolve predecessor: {error}"))
            })?
            .ok_or_else(|| {
                CampaignError::Scenario("predecessor loglet missing from resolver".into())
            })?;
        let sealed = match &pred_resolved {
            ResolvedLoglet::ReadSeal(view) => view
                .check_tail_if_sealed()
                .await
                .map_err(|error| {
                    CampaignError::Scenario(format!("oracle pred check_tail: {error}"))
                })?
                .ok_or_else(|| {
                    CampaignError::Scenario(
                        "predecessor is not durably sealed; cannot prove exact handoff boundary"
                            .into(),
                    )
                })?,
            ResolvedLoglet::Writable(_) => {
                return Err(CampaignError::Scenario(
                    "predecessor unexpectedly resolved as writable".into(),
                ));
            }
        };
        let expected_successor_start = predecessor
            .start
            .checked_add(sealed.tail)
            .ok_or_else(|| CampaignError::Scenario("sealed-tail address overflow".into()))?;
        if successor.start != expected_successor_start {
            return Err(CampaignError::Scenario(format!(
                "handoff boundary mismatch: predecessor.start={} sealed.tail={} ⇒ expect successor.start={expected_successor_start}, got {}",
                predecessor.start, sealed.tail, successor.start
            )));
        }
        boundary = serde_json::json!({
            "predecessor": {
                "loglet_id": predecessor.loglet_id.as_str(),
                "start": predecessor.start,
                "sealed_tail": sealed.tail,
                "seal_status": format!("{:?}", sealed.seal_status),
            },
            "successor": {
                "loglet_id": successor.loglet_id.as_str(),
                "start": successor.start,
            },
            "predicate": "predecessor.start + sealed.tail == successor.start",
            "holds": true,
        });
    }

    let active = generations
        .last()
        .cloned()
        .ok_or_else(|| CampaignError::Scenario("no active generation".into()))?;

    let authority =
        ServingAuthorityRecord::decode_application_fence(&observed.state.application_fence)
            .map_err(|error| {
                CampaignError::Scenario(format!("oracle decode authority fence: {error}"))
            })?;
    let (owner_id, writer_term, generation_ref) = match &authority.state {
        AuthorityState::Serving { authority, .. } => (
            authority.owner_id,
            authority.writer_term,
            authority.generation_ref.clone(),
        ),
        other => {
            return Err(CampaignError::Scenario(format!(
                "expected Serving authority, got {other:?}"
            )));
        }
    };
    if owner_id != expected_authority.owner {
        return Err(CampaignError::Scenario(format!(
            "active Serving owner {:?} want {:?}",
            owner_id.as_bytes(),
            expected_authority.owner.as_bytes()
        )));
    }
    let want_term = WriterTerm::new(expected_authority.term)
        .map_err(|error| CampaignError::Scenario(format!("invalid expected term: {error}")))?;
    if writer_term != want_term {
        return Err(CampaignError::Scenario(format!(
            "active Serving writer_term {} want {}",
            writer_term.get(),
            expected_authority.term
        )));
    }
    if generation_ref.active_loglet_id != active.loglet_id {
        return Err(CampaignError::Scenario(format!(
            "Serving generation_ref loglet {} does not match active {}",
            generation_ref.active_loglet_id.as_str(),
            active.loglet_id.as_str()
        )));
    }
    if generation_ref.active_start != active.start {
        return Err(CampaignError::Scenario(format!(
            "Serving generation_ref active_start {} does not match active.start {}",
            generation_ref.active_start, active.start
        )));
    }

    let active_resolved = resolver
        .resolve(&active.loglet_id)
        .await
        .map_err(|error| CampaignError::Scenario(format!("oracle resolve active: {error}")))?
        .ok_or_else(|| CampaignError::Scenario("active loglet missing from resolver".into()))?;
    let durable_active = match &active_resolved {
        ResolvedLoglet::ReadSeal(view) => view.observe_durable().await.map_err(|error| {
            CampaignError::Scenario(format!("oracle active observe_durable: {error}"))
        })?,
        ResolvedLoglet::Writable(_) => {
            return Err(CampaignError::Scenario(
                "active unexpectedly resolved as writable in read-only oracle".into(),
            ));
        }
    };
    let global_end = active
        .start
        .checked_add(durable_active.contiguous_tail())
        .ok_or_else(|| CampaignError::Scenario("active durable tail overflow".into()))?;

    let read_log = VirtualLog::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
    );
    let mut payloads = Vec::new();
    let mut record_rows = Vec::new();
    let mut positions = Vec::new();
    let mut producer_summary: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut successor_records = 0_usize;
    let successor_id = require_cutover
        .then(|| generations.last().map(|g| g.loglet_id.as_str().to_owned()))
        .flatten();
    let mut cursor = 0_u64;
    let mut global_record: u64 = 0;
    while cursor < global_end {
        let entry = read_log
            .read_next(cursor, global_end)
            .await
            .map_err(|error| {
                CampaignError::Scenario(format!("oracle read_next@{cursor}: {error}"))
            })?;
        positions.push(serde_json::json!({
            "position": entry.position,
            "loglet_id": entry.loglet_id.as_str(),
            "payload_len": entry.payload.len(),
        }));
        let on_successor = successor_id
            .as_ref()
            .is_some_and(|id| entry.loglet_id.as_str() == id);
        let chunk = decode_chunk(&entry.payload).map_err(|error| {
            CampaignError::Scenario(format!(
                "oracle decode_chunk at virtual position {}: {error}",
                entry.position
            ))
        })?;
        for frame in &chunk.frames {
            for (record_idx, record) in frame.records.iter().enumerate() {
                let identity = bytes_to_payload_identity(&record.payload);
                let digest = payload_sha256_hex(record.payload.as_ref());
                let submission = frame.submissions.iter().find(|sub| {
                    let start = sub.first_record as usize;
                    let end = start + sub.record_count as usize;
                    record_idx >= start && record_idx < end
                });
                let producer_hex = submission
                    .map(|sub| producer_id_hex(&sub.producer_id.as_bytes()))
                    .unwrap_or_else(|| "unknown".into());
                let sequence = submission.map(|sub| sub.sequence);
                if on_successor {
                    successor_records = successor_records.saturating_add(1);
                }
                producer_summary
                    .entry(producer_hex.clone())
                    .and_modify(|value| {
                        if let Some(sequences) =
                            value.get_mut("sequences").and_then(|v| v.as_array_mut())
                            && let Some(seq) = sequence
                        {
                            sequences.push(serde_json::json!(seq));
                        }
                    })
                    .or_insert_with(|| {
                        serde_json::json!({
                            "producer_id_hex": producer_hex,
                            "sequences": sequence.map(|s| vec![s]).unwrap_or_default(),
                        })
                    });
                record_rows.push(serde_json::json!({
                    "global_record_offset": global_record,
                    "chunk_position": entry.position,
                    "loglet_id": entry.loglet_id.as_str(),
                    "on_successor": on_successor,
                    "payload_identity": identity,
                    "payload_digest_sha256": digest,
                    "producer_id_hex": producer_hex,
                    "producer_sequence": sequence,
                    "frame_base_offset": frame.base_offset.get(),
                }));
                payloads.push(identity);
                global_record = global_record.saturating_add(1);
            }
        }
        cursor = entry
            .position
            .checked_add(1)
            .ok_or_else(|| CampaignError::Scenario("read cursor overflow".into()))?;
    }

    match_payloads(&payloads, required_payloads, optional_inflight)?;

    if require_cutover {
        let post_start = optional_inflight.map_or(required_payloads.len(), |_| {
            // For allowed-set, caller still passes required without optional.
            required_payloads.len()
        });
        // When cutover expects post payloads on successor, require successor durable records.
        if !required_payloads.is_empty() && successor_records == 0 {
            // Family 12 baseline uses require_cutover=false. Cutover/recovery
            // paths with B payloads must show successor data.
            let looks_like_post = required_payloads
                .iter()
                .any(|p| p.contains("-b-") || p.contains("post") || p.contains("reply-loss-b"));
            if looks_like_post {
                return Err(CampaignError::Scenario(format!(
                    "successor has no decoded durable records but post payloads were expected (required={required_payloads:?})"
                )));
            }
        }
        let _ = post_start;
        // Distinct producer phases: records on predecessor vs successor should
        // not share producer_id (PR #7 / WP07 anti-replay).
        let mut pred_producers = std::collections::BTreeSet::new();
        let mut succ_producers = std::collections::BTreeSet::new();
        for row in &record_rows {
            let producer = row
                .get("producer_id_hex")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if row
                .get("on_successor")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                succ_producers.insert(producer.to_owned());
            } else {
                pred_producers.insert(producer.to_owned());
            }
        }
        if !pred_producers.is_empty() && !succ_producers.is_empty() {
            let overlap: Vec<_> = pred_producers
                .intersection(&succ_producers)
                .cloned()
                .collect();
            if !overlap.is_empty() {
                return Err(CampaignError::Scenario(format!(
                    "post-cutover producer IDs overlap predecessor (dedup-replay risk): {overlap:?}"
                )));
            }
        }
    }

    Ok(CutoverOracleReport {
        observation: serde_json::json!({
            "ha_prefix": root,
            "evidence_class": "Holylog-state/readback oracle (authoritative; not OK-alone)",
            "boundary": boundary,
            "active": {
                "loglet_id": active.loglet_id.as_str(),
                "start": active.start,
                "durable_contiguous_tail": durable_active.contiguous_tail(),
            },
            "serving_authority": {
                "owner_id_bytes": owner_id.as_bytes().as_slice(),
                "writer_term": writer_term.get(),
                "active_loglet_id": generation_ref.active_loglet_id.as_str(),
                "active_start": generation_ref.active_start,
            },
            "readback": {
                "global_end": global_end,
                "payload_identities": payloads,
                "records": record_rows,
                "virtual_positions": positions,
                "optional_inflight": optional_inflight,
                "successor_record_count": successor_records,
                "producers": producer_summary.values().cloned().collect::<Vec<_>>(),
            },
            "membership_revision": observed.state.revision,
            "generation_count": generations.len(),
        }),
    })
}

fn match_payloads(
    payloads: &[String],
    required: &[&str],
    optional_inflight: Option<&str>,
) -> Result<(), CampaignError> {
    let required: Vec<String> = required
        .iter()
        .map(|payload| (*payload).to_owned())
        .collect();
    match optional_inflight {
        None => {
            if payloads != required.as_slice() {
                return Err(CampaignError::Scenario(format!(
                    "readback mismatch: got {payloads:?} want {required:?}"
                )));
            }
        }
        Some(inflight) => {
            let count = payloads.iter().filter(|p| p.as_str() == inflight).count();
            if count > 1 {
                return Err(CampaignError::Scenario(format!(
                    "optional inflight {inflight:?} duplicated in readback"
                )));
            }
            let mut candidates = vec![required.clone()];
            for i in 0..=required.len() {
                let mut candidate = required.clone();
                candidate.insert(i, inflight.to_owned());
                candidates.push(candidate);
            }
            if !candidates
                .iter()
                .any(|candidate| candidate.as_slice() == payloads)
            {
                return Err(CampaignError::Scenario(format!(
                    "allowed-set readback mismatch: got {payloads:?} required={required:?} optional={inflight:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Attempts a stale root CAS that must not overwrite the applied revision.
pub(crate) async fn prove_stale_cas_cannot_overwrite(
    rustfs_endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    ha_prefix: &str,
) -> Result<serde_json::Value, CampaignError> {
    let store = connect_s3_compat(rustfs_endpoint, bucket, "us-east-1", access_key, secret_key)
        .map_err(|error| CampaignError::Scenario(format!("stale-cas store connect: {error}")))?;
    let root = ha_prefix.trim_end_matches('/');
    let register_object = Path::from(root.to_owned()).join(register_path("verse").as_ref());
    let register = Arc::new(
        ObjectStoreConditionalRegister::new(
            Arc::clone(&store),
            register_object,
            BackendProfile::RustFs.register_capabilities(),
        )
        .map_err(|error| CampaignError::Scenario(format!("stale-cas register: {error}")))?,
    ) as Arc<dyn ConditionalRegister>;

    let live = register
        .read()
        .await
        .map_err(|error| CampaignError::Scenario(format!("stale-cas read: {error}")))?
        .ok_or_else(|| CampaignError::Scenario("stale-cas expected applied root".into()))?;
    let live_revision = live.state.revision;

    // Stale competitor: correct next revision, but a witness that does not match
    // the live object. Must not apply.
    let stale_expected = VersionedState {
        state: live.state.clone(),
        token: holylog::virtual_log::CompareToken::new("0|\"stale-not-live-etag\""),
    };
    let mut hostile = live.state.clone();
    hostile.revision = live_revision.saturating_add(1);
    let applied = register
        .compare_and_swap(Some(&stale_expected), hostile)
        .await
        .map_err(|error| CampaignError::Scenario(format!("stale-cas swap: {error}")))?;
    if applied {
        return Err(CampaignError::Scenario(
            "stale competing CAS overwrote the applied root".into(),
        ));
    }
    let after = register
        .read()
        .await
        .map_err(|error| CampaignError::Scenario(format!("stale-cas reread: {error}")))?
        .ok_or_else(|| CampaignError::Scenario("stale-cas root missing after conflict".into()))?;
    if after.state.revision != live_revision {
        return Err(CampaignError::Scenario(format!(
            "stale-cas changed revision {} → {}",
            live_revision, after.state.revision
        )));
    }
    Ok(serde_json::json!({
        "live_revision": live_revision,
        "stale_cas_applied": false,
        "revision_unchanged": true,
    }))
}

fn bytes_to_payload_identity(payload: &Bytes) -> String {
    String::from_utf8_lossy(payload).into_owned()
}

fn payload_sha256_hex(payload: &[u8]) -> String {
    // Redacted content digest for artifacts (not a crypto claim beyond uniqueness).
    holylog_correctness::payload_digest(payload)
}

fn producer_id_hex(bytes: &[u8; 16]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Polls the cutover/baseline oracle until decoded payloads match or `limit` elapses.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn wait_for_durable_payloads(
    rustfs_endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    ha_prefix: &str,
    expected_payloads: &[&str],
    expected_authority: ExpectedAuthority,
    require_cutover: bool,
    limit: Duration,
) -> Result<CutoverOracleReport, CampaignError> {
    let started = Instant::now();
    let mut last_error = None;
    while started.elapsed() < limit {
        let result = if require_cutover {
            prove_raw_lines_cutover(
                rustfs_endpoint,
                bucket,
                access_key,
                secret_key,
                ha_prefix,
                expected_payloads,
                expected_authority,
            )
            .await
        } else {
            prove_serving_baseline(
                rustfs_endpoint,
                bucket,
                access_key,
                secret_key,
                ha_prefix,
                expected_payloads,
                expected_authority,
            )
            .await
        };
        match result {
            Ok(report) => return Ok(report),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Err(CampaignError::Scenario(format!(
        "timed out waiting for durable payloads {expected_payloads:?}: {}",
        last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no attempts".into())
    )))
}

/// Polls the allowed-set cutover oracle until match or `limit` elapses.
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) async fn wait_for_allowed_set_payloads(
    rustfs_endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    ha_prefix: &str,
    required_payloads: &[&str],
    optional_inflight: Option<&str>,
    expected_authority: ExpectedAuthority,
    limit: Duration,
) -> Result<CutoverOracleReport, CampaignError> {
    let started = Instant::now();
    let mut last_error = None;
    while started.elapsed() < limit {
        match prove_allowed_set_cutover(
            rustfs_endpoint,
            bucket,
            access_key,
            secret_key,
            ha_prefix,
            required_payloads,
            optional_inflight,
            expected_authority,
        )
        .await
        {
            Ok(report) => return Ok(report),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Err(CampaignError::Scenario(format!(
        "timed out waiting for allowed-set payloads required={required_payloads:?} optional={optional_inflight:?}: {}",
        last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no attempts".into())
    )))
}

/// Actor A owner used by the temporary bootstrap adapter.
#[must_use]
pub(crate) fn actor_a_owner() -> OwnerId {
    OwnerId::from_bytes(*b"scripture-own-a!")
}

/// Actor B owner used by the temporary bootstrap/promote adapter.
#[must_use]
pub(crate) fn actor_b_owner() -> OwnerId {
    OwnerId::from_bytes(*b"scripture-own-b!")
}

pub(crate) fn actor_c_owner() -> OwnerId {
    OwnerId::from_bytes(*b"scripture-own-c!")
}

#[cfg(test)]
mod negative_controls {
    use super::match_payloads;
    use holylog::virtual_log::{ConditionalRegister, InMemoryConditionalRegister, VirtualLogState};
    use holylog_correctness::faults::{FaultController, FaultableConditionalRegister};
    use holylog_correctness::{ActorId, ActorTrace, ArmedFault, RecordingSink, RunId, TraceSink};
    use std::sync::Arc;

    #[test]
    fn rejects_wrong_required_sequence() {
        assert!(match_payloads(&["a".into(), "c".into()], &["a", "b"], None).is_err());
    }

    #[test]
    fn accepts_required_without_optional() {
        assert!(match_payloads(&["a".into(), "b".into()], &["a", "b"], Some("inflight")).is_ok());
    }

    #[test]
    fn accepts_optional_once() {
        assert!(
            match_payloads(
                &["a".into(), "inflight".into(), "b".into()],
                &["a", "b"],
                Some("inflight")
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_optional_duplicate() {
        assert!(
            match_payloads(
                &["a".into(), "inflight".into(), "inflight".into(), "b".into()],
                &["a", "b"],
                Some("inflight")
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn reply_loss_wrapper_does_not_leak_applied_bool() {
        let sink = RecordingSink::new().shared();
        let trace = ActorTrace::new(
            RunId::new("neg"),
            ActorId::new("reg"),
            Arc::clone(&sink) as Arc<dyn TraceSink>,
        );
        let faults = Arc::new(FaultController::new());
        faults.arm(ArmedFault::RootCasReplyLost);
        let inner =
            Arc::new(InMemoryConditionalRegister::default()) as Arc<dyn ConditionalRegister>;
        let wrapped = FaultableConditionalRegister::new(inner, faults, trace);
        let state = VirtualLogState {
            revision: 1,
            generations: Vec::new(),
            application_fence: Default::default(),
        };
        let result = wrapped.compare_and_swap(None, state).await;
        assert!(
            result.is_err(),
            "reply-loss must return Err/Indeterminate, not Ok(applied=true); got {result:?}"
        );
    }
}
