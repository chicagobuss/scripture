//! Shared durable-store + supervisor assembly for product CLI commands.

use std::error::Error;
use std::sync::Arc;

use holylog::provision::ExclusiveClaimStore;
use holylog::virtual_log::ConditionalRegister;
use holylog_object_store::{ObjectStoreExclusiveClaim, ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::ObjectStore;
use object_store::path::Path;
use scripture::DataRefBlobConfig;
use scripture_runtime::counting_store::{CountingStore, RequestCounters};
use scripture_runtime::{
    BackendProfile, NodeIdentity, ObjectStoreChunkBlobStore, ObjectStorePartsFactory, PartsFactory,
    ProcessLogletResolver, VerseNodeSupervisor, connect_s3_compat, resolve_credentials,
};
use scripture_service::VerseRuntimeConfig;

use crate::config::{AssignmentConfig, ScriptureConfig};

pub struct AssembledNode {
    pub node: VerseNodeSupervisor,
    /// Shared Holylog seams retained for a future durable HA composition root.
    pub register: Arc<dyn ConditionalRegister>,
    pub resolver: Arc<ProcessLogletResolver>,
    pub parts: Arc<dyn PartsFactory>,
    pub claims: Arc<dyn ExclusiveClaimStore>,
    pub backend: BackendProfile,
    pub store_root: String,
    pub advertise: scripture::OwnerEndpoint,
    /// Runtime config actually used to assemble the supervisor — including
    /// mounted DataRef blob config. Pass this to `bootstrap_and_serve` /
    /// `promote_and_serve`; a fresh `assignment_runtime_config()` has
    /// `dataref_blobs: None` and silently leaves the driver on inline chunks.
    pub verse_config: VerseRuntimeConfig,
}

/// Shared object-store connection reused across multi-assignment roots.
pub struct SharedStore {
    pub store: Arc<dyn ObjectStore>,
    /// Request counts for the Serving-Authority register / claim paths.
    pub authority_counters: Arc<RequestCounters>,
    /// Request counts for DataRef staging blob PUT/GET (separate from authority
    /// and from the LogDrive metrics path).
    pub blob_counters: Arc<RequestCounters>,
    pub backend: BackendProfile,
    /// Process-level advertise (single-assignment / SharedStore identity).
    /// Multi-assignment seams must use each assignment's advertise instead.
    pub advertise: scripture::OwnerEndpoint,
    pub owner_id: scripture::OwnerId,
    pub metrics: Arc<ObjectStoreMetrics>,
}

pub fn connect_shared_store(config: &ScriptureConfig) -> Result<SharedStore, Box<dyn Error>> {
    let owner_id = config.owner_id()?;
    let advertise = config.advertise()?;
    let backend = config.backend()?;
    let credentials = resolve_credentials(backend)?;
    if matches!(
        backend,
        BackendProfile::CloudflareR2 | BackendProfile::AmazonS3
    ) && config.store.endpoint.starts_with("http://")
    {
        return Err(format!("{} requires an HTTPS endpoint", backend.label()).into());
    }

    let store = connect_s3_compat(
        &config.store.endpoint,
        &config.store.bucket,
        &config.store.region,
        &credentials.access_key,
        &credentials.secret_key,
    )?;
    drop(credentials);
    Ok(SharedStore {
        store,
        authority_counters: Arc::new(RequestCounters::default()),
        blob_counters: Arc::new(RequestCounters::default()),
        backend,
        advertise,
        owner_id,
        metrics: Arc::new(ObjectStoreMetrics::default()),
    })
}

/// Assembles Holylog seams under an exclusive assignment store root.
pub fn assemble_assignment_seams(
    shared: &SharedStore,
    store_root: &str,
    mut verse_config: VerseRuntimeConfig,
    advertise: scripture::OwnerEndpoint,
) -> Result<AssembledNode, Box<dyn Error>> {
    let store_root = store_root.trim_end_matches('/').to_owned();
    // Mount DataRefs on the live serve path: sealed chunks become staging blobs
    // and the Verse log receives pointers. Without this the blob writer stays a
    // lab seam and requests-per-record cannot move.
    //
    // Count staging blob traffic on its own ledger. Building on the raw store
    // would make PUTs invisible to every counter — the same class of defect the
    // authority CountingStore was added to fix.
    if verse_config.dataref_blobs.is_none() {
        let blob_store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(
            Arc::clone(&shared.store),
            Arc::clone(&shared.blob_counters),
        ));
        verse_config.dataref_blobs = Some(DataRefBlobConfig::new(Arc::new(
            ObjectStoreChunkBlobStore::new(blob_store),
        )));
    }
    // The register and claim store carry the Serving-Authority traffic, which
    // Holylog cannot count: neither type takes metrics. Wrap the store for
    // those two paths only, so authority requests are attributable separately
    // from the LogDrive data path that ObjectStorePartsFactory already counts.
    let authority_store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(
        Arc::clone(&shared.store),
        Arc::clone(&shared.authority_counters),
    ));
    let register = Arc::new(ObjectStoreConditionalRegister::new(
        Arc::clone(&authority_store),
        Path::from(store_root.clone()).join(register_path("verse").as_ref()),
        shared.backend.register_capabilities(),
    )?) as Arc<dyn ConditionalRegister>;
    let claims = Arc::new(ObjectStoreExclusiveClaim::new(
        Arc::clone(&authority_store),
        shared.backend.drive_capabilities(),
    )?) as Arc<dyn ExclusiveClaimStore>;
    let parts = Arc::new(ObjectStorePartsFactory::new(
        Arc::clone(&shared.store),
        store_root.clone(),
        shared.backend.drive_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::clone(&shared.metrics),
    )) as Arc<dyn PartsFactory>;
    let resolver = Arc::new(ProcessLogletResolver::default());
    let node = VerseNodeSupervisor::with_parts_factory_and_claims(
        NodeIdentity {
            owner_id: shared.owner_id,
            endpoint: advertise.clone(),
        },
        Arc::clone(&register),
        Arc::clone(&resolver),
        Arc::clone(&parts),
        verse_config.clone(),
        Arc::clone(&claims),
    );
    Ok(AssembledNode {
        node,
        register,
        resolver,
        parts,
        claims,
        backend: shared.backend,
        store_root,
        advertise,
        verse_config,
    })
}

pub fn assemble_supervisor(config: &ScriptureConfig) -> Result<AssembledNode, Box<dyn Error>> {
    let shared = connect_shared_store(config)?;
    let store_root = config.store.prefix.trim_end_matches('/').to_owned();
    let verse_config = config.verse_runtime_config()?;
    assemble_assignment_seams(&shared, &store_root, verse_config, shared.advertise.clone())
}

/// Convenience: assemble seams for one multi-assignment entry.
pub fn assemble_assignment(
    config: &ScriptureConfig,
    shared: &SharedStore,
    assignment: &AssignmentConfig,
) -> Result<AssembledNode, Box<dyn Error>> {
    let store_root = config.assignment_store_root(assignment)?;
    let verse_config = config.assignment_runtime_config(assignment)?;
    let advertise = config.assignment_advertise(assignment)?;
    assemble_assignment_seams(shared, &store_root, verse_config, advertise)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use scripture::{
        ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, VerseId, WriterId,
    };
    use scripture_runtime::BackendProfile;
    use std::time::Duration;

    fn test_shared() -> SharedStore {
        SharedStore {
            store: Arc::new(InMemory::new()),
            authority_counters: Arc::new(RequestCounters::default()),
            blob_counters: Arc::new(RequestCounters::default()),
            backend: BackendProfile::RustFs,
            advertise: OwnerEndpoint::new("tcp://assemble-test:9000").expect("endpoint"),
            owner_id: OwnerId::from_bytes(*b"assemble-owner!!"),
            metrics: Arc::new(ObjectStoreMetrics::default()),
        }
    }

    fn bare_verse_config() -> VerseRuntimeConfig {
        VerseRuntimeConfig {
            journal_id: JournalId::from_bytes(*b"assemble-jrnl!!!"),
            verse_id: VerseId::from_bytes(*b"assemble-verse!!"),
            owner_id: OwnerId::from_bytes(*b"assemble-owner!!"),
            cohort_id: CohortId::from_bytes(*b"assemble-cohort!"),
            writer_id: WriterId::from_bytes(*b"assemble-writer!"),
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
        }
    }

    #[test]
    fn assemble_mounts_dataref_blobs_onto_the_verse_config_passed_to_serve() {
        let shared = test_shared();
        let assembled = assemble_assignment_seams(
            &shared,
            "assemble-root",
            bare_verse_config(),
            shared.advertise.clone(),
        )
        .expect("assemble");
        assert!(
            assembled.verse_config.dataref_blobs.is_some(),
            "assemble must mount DataRefs onto the config bootstrap_and_serve uses"
        );
        // A fresh config from the YAML path is still None — that is the bug this
        // field exists to prevent. Serve must use assembled.verse_config, not a
        // newly built assignment_runtime_config().
        assert!(bare_verse_config().dataref_blobs.is_none());
    }
}
