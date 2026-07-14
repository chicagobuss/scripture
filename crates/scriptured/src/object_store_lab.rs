//! Object-store durable parts for the Scripture fleet exercise (RustFS / R2).

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
use holylog_object_store_register::RegisterCapabilities;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;

use crate::fleet_lab::{DurableLogletParts, PartsFactory};

/// Exclusive object-store root prefix for one fleet-exercise run.
pub const FLEET_EXERCISE_ROOT_PREFIX: &str = "scripture-fleet-exercise";

/// Attested backend profiles the fleet-lab runner may construct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendProfile {
    /// Local RustFS path-style S3 (Holylog local-s3 compose).
    RustFs,
    /// Cloudflare R2 (S3-compatible); register attested via [`RegisterCapabilities::cloudflare_r2`].
    CloudflareR2,
}

impl BackendProfile {
    /// Parses `rustfs` or `r2`. An `s3` token is reserved and rejected.
    pub fn parse(raw: &str) -> Result<Self, ObjectStoreLabError> {
        match raw {
            "rustfs" => Ok(Self::RustFs),
            "r2" => Ok(Self::CloudflareR2),
            "s3" => Err(ObjectStoreLabError::BackendProfile(
                "backend profile 's3' is reserved for a follow-up AWS exercise; use rustfs or r2"
                    .into(),
            )),
            other => Err(ObjectStoreLabError::BackendProfile(format!(
                "unknown backend profile '{other}' (expected rustfs|r2)"
            ))),
        }
    }

    /// Stable report label (also used by `scripture-load --backend`).
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::RustFs => "rustfs",
            Self::CloudflareR2 => "r2",
        }
    }

    /// Register capability claim for the shared Verse register pointer.
    #[must_use]
    pub fn register_capabilities(self) -> RegisterCapabilities {
        match self {
            Self::RustFs => RegisterCapabilities::amazon_s3(),
            Self::CloudflareR2 => RegisterCapabilities::cloudflare_r2(),
        }
    }

    /// LogDrive capability declaration for this profile.
    #[must_use]
    pub fn drive_capabilities(self) -> BackendCapabilities {
        // RustFS and R2 are both attested for atomic conditional create + lex listing.
        BackendCapabilities::new(
            PointSemantics::LinearizableSingleValue,
            ListingOrder::Lexicographic,
            ListingVisibility::Strong,
            ConditionalCreate::Atomic,
        )
    }
}

/// Validates a run id and builds the exclusive root under [`FLEET_EXERCISE_ROOT_PREFIX`].
pub fn fleet_exercise_root(run_id: &str) -> Result<String, ObjectStoreLabError> {
    if run_id.is_empty() || run_id.contains('/') || run_id.contains('\\') {
        return Err(ObjectStoreLabError::RunId(
            "run-id must be non-empty and must not contain path separators".into(),
        ));
    }
    if run_id.contains("..") {
        return Err(ObjectStoreLabError::RunId(
            "run-id must not contain '..'".into(),
        ));
    }
    Ok(format!("{FLEET_EXERCISE_ROOT_PREFIX}/{run_id}"))
}

/// Shared object-store root for one fleet-exercise run.
pub struct ObjectStorePartsFactory {
    store: Arc<dyn ObjectStore>,
    root: String,
    capabilities: BackendCapabilities,
    write_policy: WritePolicy,
    /// Shared adapter counters across data/seal/trim drives for this factory.
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
        BackendProfile::RustFs.drive_capabilities()
    }

    /// Cloudflare R2 drive capabilities (same point/listing/create claims as RustFS).
    #[must_use]
    pub fn r2_capabilities() -> BackendCapabilities {
        BackendProfile::CloudflareR2.drive_capabilities()
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
        Ok(Arc::new(ObjectStoreLogDrive::with_metrics(
            Arc::clone(&self.store),
            prefix,
            self.capabilities,
            self.write_policy,
            Arc::clone(&self.metrics),
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

/// Failures while constructing fleet-exercise object-store adapters.
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
    /// Rejected backend profile token.
    #[error("backend profile: {0}")]
    BackendProfile(String),
    /// Rejected run id / root.
    #[error("run id: {0}")]
    RunId(String),
}

/// Connects to a path-style S3-compatible endpoint (RustFS or R2).
pub fn connect_s3_compat(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    let mut builder = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);
    // `object_store` deliberately refuses plaintext by default. The only
    // accepted HTTP endpoint in this lab is RustFS on loopback; R2 profile
    // construction rejects HTTP above before reaching this helper.
    if endpoint.starts_with("http://") {
        builder = builder.with_allow_http(true);
    }
    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}

/// Connects to a path-style S3-compatible endpoint (RustFS lab defaults).
///
/// Prefer [`connect_s3_compat`] for new call sites.
pub fn connect_rustfs(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    connect_s3_compat(endpoint, bucket, region, access_key, secret_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_backend_profiles_and_rejects_s3_reservation() {
        assert_eq!(
            BackendProfile::parse("rustfs").expect("rustfs"),
            BackendProfile::RustFs
        );
        assert_eq!(
            BackendProfile::parse("r2").expect("r2"),
            BackendProfile::CloudflareR2
        );
        assert!(BackendProfile::parse("s3").is_err());
        assert!(BackendProfile::parse("garage").is_err());
    }

    #[test]
    fn fleet_exercise_root_rejects_path_escape() {
        assert_eq!(
            fleet_exercise_root("run-1").expect("ok"),
            "scripture-fleet-exercise/run-1"
        );
        assert!(fleet_exercise_root("").is_err());
        assert!(fleet_exercise_root("a/b").is_err());
        assert!(fleet_exercise_root("..").is_err());
    }
}
