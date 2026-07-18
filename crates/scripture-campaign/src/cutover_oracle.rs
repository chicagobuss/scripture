//! Holylog/Scripture API oracle for producer raw-lines A→B cutover (family 13).
//!
//! Proves exact predecessor sealed-tail = successor global start, canonical B
//! Serving authority (owner/term), and cross-generation chunk readback of every
//! acknowledged payload exactly once and in order. Does not parse private S3
//! object layout by hand.

use std::sync::Arc;

use bytes::Bytes;
use holylog::provision::{ResolvedLoglet, resolve_read_seal};
use holylog::virtual_log::{ConditionalRegister, LogletResolver, VirtualLog};
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

/// Expected Serving authority after promote B.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExpectedAuthority {
    /// Canonical owner bytes (16).
    pub owner: OwnerId,
    /// Writer term after promote.
    pub term: u64,
}

/// Outcome of the Holylog-state / readback cutover oracle.
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
    let store = connect_s3_compat(
        rustfs_endpoint,
        bucket,
        "us-east-1",
        access_key,
        secret_key,
    )
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
    if generations.len() < 2 {
        return Err(CampaignError::Scenario(format!(
            "cutover oracle expected predecessor+successor generations, got {}",
            generations.len()
        )));
    }
    let predecessor = generations[generations.len() - 2].clone();
    let successor = generations[generations.len() - 1].clone();

    for generation in generations {
        let durable = parts
            .open(&generation.loglet_id)
            .map_err(|error| CampaignError::Scenario(format!("oracle open loglet: {error}")))?;
        let view = resolve_read_seal(durable.components(LOGLET_K))
            .await
            .map_err(|error| CampaignError::Scenario(format!("oracle resolve_read_seal: {error}")))?;
        resolver.insert_read_seal(generation.loglet_id.clone(), Arc::new(view));
    }

    let pred_resolved = resolver
        .resolve(&predecessor.loglet_id)
        .await
        .map_err(|error| CampaignError::Scenario(format!("oracle resolve predecessor: {error}")))?
        .ok_or_else(|| CampaignError::Scenario("predecessor loglet missing from resolver".into()))?;
    let sealed = match &pred_resolved {
        ResolvedLoglet::ReadSeal(view) => view
            .check_tail_if_sealed()
            .await
            .map_err(|error| CampaignError::Scenario(format!("oracle pred check_tail: {error}")))?
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

    let authority = ServingAuthorityRecord::decode_application_fence(
        &observed.state.application_fence,
    )
    .map_err(|error| CampaignError::Scenario(format!("oracle decode authority fence: {error}")))?;
    let (owner_id, writer_term, generation_ref) = match &authority.state {
        AuthorityState::Serving { authority, .. } => (
            authority.owner_id,
            authority.writer_term,
            authority.generation_ref.clone(),
        ),
        other => {
            return Err(CampaignError::Scenario(format!(
                "expected Serving authority after B promote, got {other:?}"
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
    let want_term = WriterTerm::new(expected_authority.term).map_err(|error| {
        CampaignError::Scenario(format!("invalid expected term: {error}"))
    })?;
    if writer_term != want_term {
        return Err(CampaignError::Scenario(format!(
            "active Serving writer_term {} want {}",
            writer_term.get(),
            expected_authority.term
        )));
    }
    if generation_ref.active_loglet_id != successor.loglet_id {
        return Err(CampaignError::Scenario(format!(
            "Serving generation_ref loglet {} does not match successor {}",
            generation_ref.active_loglet_id.as_str(),
            successor.loglet_id.as_str()
        )));
    }
    if generation_ref.active_start != successor.start {
        return Err(CampaignError::Scenario(format!(
            "Serving generation_ref active_start {} does not match successor.start {}",
            generation_ref.active_start, successor.start
        )));
    }

    let succ_resolved = resolver
        .resolve(&successor.loglet_id)
        .await
        .map_err(|error| CampaignError::Scenario(format!("oracle resolve successor: {error}")))?
        .ok_or_else(|| CampaignError::Scenario("successor loglet missing from resolver".into()))?;
    let durable_succ = match &succ_resolved {
        ResolvedLoglet::ReadSeal(view) => view
            .observe_durable()
            .await
            .map_err(|error| {
                CampaignError::Scenario(format!("oracle succ observe_durable: {error}"))
            })?,
        ResolvedLoglet::Writable(_) => {
            return Err(CampaignError::Scenario(
                "successor unexpectedly resolved as writable in read-only oracle".into(),
            ));
        }
    };
    let global_end = successor
        .start
        .checked_add(durable_succ.contiguous_tail())
        .ok_or_else(|| CampaignError::Scenario("successor durable tail overflow".into()))?;

    let read_log = VirtualLog::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
    );
    let mut payloads = Vec::new();
    let mut positions = Vec::new();
    let mut cursor = 0_u64;
    while cursor < global_end {
        let entry = read_log
            .read_next(cursor, global_end)
            .await
            .map_err(|error| CampaignError::Scenario(format!("oracle read_next@{cursor}: {error}")))?;
        positions.push(serde_json::json!({
            "position": entry.position,
            "loglet_id": entry.loglet_id.as_str(),
            "payload_len": entry.payload.len(),
        }));
        let chunk = decode_chunk(&entry.payload).map_err(|error| {
            CampaignError::Scenario(format!(
                "oracle decode_chunk at virtual position {}: {error}",
                entry.position
            ))
        })?;
        for frame in &chunk.frames {
            for record in &frame.records {
                payloads.push(bytes_to_payload_identity(&record.payload));
            }
        }
        cursor = entry
            .position
            .checked_add(1)
            .ok_or_else(|| CampaignError::Scenario("read cursor overflow".into()))?;
    }

    let expected: Vec<String> = expected_payloads
        .iter()
        .map(|payload| (*payload).to_owned())
        .collect();
    if payloads != expected {
        return Err(CampaignError::Scenario(format!(
            "cross-generation readback mismatch: got {payloads:?} want {expected:?}"
        )));
    }

    Ok(CutoverOracleReport {
        observation: serde_json::json!({
            "ha_prefix": root,
            "evidence_class": "producer-ack + Holylog-state/readback oracle",
            "predecessor": {
                "loglet_id": predecessor.loglet_id.as_str(),
                "start": predecessor.start,
                "sealed_tail": sealed.tail,
                "seal_status": format!("{:?}", sealed.seal_status),
            },
            "successor": {
                "loglet_id": successor.loglet_id.as_str(),
                "start": successor.start,
                "durable_contiguous_tail": durable_succ.contiguous_tail(),
            },
            "boundary": {
                "predicate": "predecessor.start + sealed.tail == successor.start",
                "holds": true,
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
                "virtual_positions": positions,
            },
            "membership_revision": observed.state.revision,
            "generation_count": generations.len(),
        }),
    })
}

fn bytes_to_payload_identity(payload: &Bytes) -> String {
    String::from_utf8_lossy(payload).into_owned()
}

/// Actor B owner used by the temporary bootstrap/promote adapter.
#[must_use]
pub(crate) fn actor_b_owner() -> OwnerId {
    OwnerId::from_bytes(*b"scripture-own-b!")
}
