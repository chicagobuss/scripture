//! Object-store durable parts for the Scripture fleet lab (RustFS / S3).

use std::sync::Arc;

use bytes::Bytes;
use holylog::atomic::{LogDriveSeal, LogDriveTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::logdrive::Address;
use holylog::provision::LogletObjectNamespaces;
use holylog::virtual_log::LogletId;
use holylog_object_store::{
    BackendCapabilities, ConditionalCreate, ListingOrder, ListingVisibility, ObjectStoreLogDrive,
    ObjectStoreMetrics, PointSemantics, WritePolicy,
};
use object_store::path::Path;
use object_store::{ObjectStore, aws::AmazonS3Builder};

use crate::fleet_lab::{DurableLogletParts, PartsFactory};

/// Shared object-store root for one fleet-lab run.
pub struct ObjectStorePartsFactory {
    store: Arc<dyn ObjectStore>,
    root: String,
    capabilities: BackendCapabilities,
    write_policy: WritePolicy,
    /// Shared adapter counters for load reports.
    pub metrics: Arc<ObjectStoreMetrics>,
}

impl ObjectStorePartsFactory {
    /// Builds a factory over an exclusive run prefix (never the whole bucket).
    pub fn new(
        store: Arc<dyn ObjectStore>,
        root: impl Into<String>,
        capabilities: BackendCapabilities,
        write_policy: WritePolicy,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> Self {
        Self {
            store,
            root: root.into().trim_end_matches('/').to_owned(),
            capabilities,
            write_policy,
            metrics,
        }
    }

    /// RustFS local-lab defaults (attested conditional-create + lex listing).
    #[must_use]
    pub fn rustfs_capabilities() -> BackendCapabilities {
        BackendCapabilities::new(
            PointSemantics::LinearizableSingleValue,
            ListingOrder::Lexicographic,
            ListingVisibility::Strong,
            ConditionalCreate::Atomic,
        )
    }

    fn parts_for(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, ObjectStoreLabError> {
        // Prefer the provision helper's root layout; LogDrive seal/trim use exclusive
        // prefixes under the same base (address 0), matching VirtualLog hosted tests.
        let ns = LogletObjectNamespaces::under_root(&self.root, loglet_id);
        let data_prefix = ns.data_prefix.trim_end_matches('/');
        let base = data_prefix
            .strip_suffix("/data")
            .ok_or_else(|| ObjectStoreLabError::Namespace(ns.data_prefix.clone()))?;
        let data = self.drive(Path::from(data_prefix))?;
        let seal_drive = self.drive(Path::from(format!("{base}/seal")))?;
        let trim_drive = self.drive(Path::from(format!("{base}/trim")))?;
        let seal = Arc::new(LogDriveSeal::new(
            seal_drive,
            Address::new(0).map_err(ObjectStoreLabError::Address)?,
            Bytes::from_static(b"sealed"),
        )) as Arc<dyn Seal>;
        let trim = Arc::new(LogDriveTrimPoint::new(trim_drive)) as Arc<dyn TrimPoint>;
        Ok(DurableLogletParts::from_components(data, seal, trim))
    }

    fn drive(&self, prefix: Path) -> Result<Arc<dyn LogDrive>, ObjectStoreLabError> {
        Ok(Arc::new(ObjectStoreLogDrive::new(
            Arc::clone(&self.store),
            prefix,
            self.capabilities,
            self.write_policy,
        )?) as Arc<dyn LogDrive>)
    }
}

impl PartsFactory for ObjectStorePartsFactory {
    fn fresh(
        &self,
        loglet_id: &LogletId,
    ) -> Result<DurableLogletParts, crate::fleet_lab::PartsFactoryError> {
        self.parts_for(loglet_id)
            .map_err(|error| crate::fleet_lab::PartsFactoryError::new(error.to_string()))
    }

    fn open(
        &self,
        loglet_id: &LogletId,
    ) -> Result<DurableLogletParts, crate::fleet_lab::PartsFactoryError> {
        self.fresh(loglet_id)
    }
}

/// Failures while constructing fleet-lab object-store adapters.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreLabError {
    /// Object-store LogDrive rejected the capability declaration or path.
    #[error(transparent)]
    Drive(#[from] holylog_object_store::Error),
    /// Invalid seal address.
    #[error(transparent)]
    Address(#[from] holylog::Error),
    /// Unexpected namespace layout from [`LogletObjectNamespaces`].
    #[error("unexpected Loglet namespace layout: {0}")]
    Namespace(String),
}

/// Connects to a path-style S3-compatible endpoint (RustFS lab defaults).
pub fn connect_rustfs(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    Ok(Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()?,
    ) as Arc<dyn ObjectStore>)
}
