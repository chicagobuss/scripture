//! Shared durable-store + supervisor assembly for product CLI commands.

use std::error::Error;
use std::sync::Arc;

use holylog::provision::ExclusiveClaimStore;
use holylog::virtual_log::ConditionalRegister;
use holylog_object_store::{ObjectStoreExclusiveClaim, ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::ObjectStore;
use object_store::path::Path;
use scripture_runtime::{
    BackendProfile, NodeIdentity, ObjectStorePartsFactory, PartsFactory, ProcessLogletResolver,
    VerseNodeSupervisor, connect_s3_compat, resolve_credentials,
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
}

/// Shared object-store connection reused across multi-assignment roots.
pub struct SharedStore {
    pub store: Arc<dyn ObjectStore>,
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
    if matches!(backend, BackendProfile::CloudflareR2)
        && config.store.endpoint.starts_with("http://")
    {
        return Err("r2 requires an HTTPS endpoint".into());
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
    verse_config: VerseRuntimeConfig,
    advertise: scripture::OwnerEndpoint,
) -> Result<AssembledNode, Box<dyn Error>> {
    let store_root = store_root.trim_end_matches('/').to_owned();
    let register = Arc::new(ObjectStoreConditionalRegister::new(
        Arc::clone(&shared.store),
        Path::from(store_root.clone()).join(register_path("verse").as_ref()),
        shared.backend.register_capabilities(),
    )?) as Arc<dyn ConditionalRegister>;
    let claims = Arc::new(ObjectStoreExclusiveClaim::new(
        Arc::clone(&shared.store),
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
        verse_config,
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
