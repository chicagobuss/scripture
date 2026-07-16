//! Shared durable-store + supervisor assembly for product CLI commands.

use std::error::Error;
use std::sync::Arc;

use holylog::provision::ExclusiveClaimStore;
use holylog::virtual_log::ConditionalRegister;
use holylog_object_store::{ObjectStoreExclusiveClaim, ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::path::Path;
use scripture_runtime::{
    BackendProfile, NodeIdentity, ObjectStorePartsFactory, PartsFactory, ProcessLogletResolver,
    VerseNodeSupervisor, connect_s3_compat, resolve_credentials,
};

use crate::config::ScriptureConfig;

pub struct AssembledNode {
    pub node: VerseNodeSupervisor,
    /// Shared Holylog seams retained for a future durable HA composition root.
    #[allow(dead_code)]
    pub register: Arc<dyn ConditionalRegister>,
    #[allow(dead_code)]
    pub resolver: Arc<ProcessLogletResolver>,
    #[allow(dead_code)]
    pub parts: Arc<dyn PartsFactory>,
    #[allow(dead_code)]
    pub claims: Arc<dyn ExclusiveClaimStore>,
    pub backend: BackendProfile,
    pub store_root: String,
    pub advertise: scripture::OwnerEndpoint,
}

pub fn assemble_supervisor(config: &ScriptureConfig) -> Result<AssembledNode, Box<dyn Error>> {
    let owner_id = config.owner_id()?;
    let advertise = config.advertise()?;
    let backend = config.backend()?;
    let credentials = resolve_credentials(backend)?;
    let store_root = config.store.prefix.trim_end_matches('/').to_owned();
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

    let register = Arc::new(ObjectStoreConditionalRegister::new(
        Arc::clone(&store),
        Path::from(store_root.clone()).join(register_path("verse").as_ref()),
        backend.register_capabilities(),
    )?) as Arc<dyn ConditionalRegister>;
    let metrics = Arc::new(ObjectStoreMetrics::default());
    let claims = Arc::new(ObjectStoreExclusiveClaim::new(
        Arc::clone(&store),
        backend.drive_capabilities(),
    )?) as Arc<dyn ExclusiveClaimStore>;
    let parts = Arc::new(ObjectStorePartsFactory::new(
        store,
        store_root.clone(),
        backend.drive_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::clone(&metrics),
    )) as Arc<dyn PartsFactory>;
    let resolver = Arc::new(ProcessLogletResolver::default());
    let verse_config = config.verse_runtime_config()?;
    let node = VerseNodeSupervisor::with_parts_factory_and_claims(
        NodeIdentity {
            owner_id,
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
        backend,
        store_root,
        advertise,
    })
}
