//! Object-store durable Loglet parts for Scripture (file / RustFS / R2 / Amazon S3).

use std::path::PathBuf;
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
use holylog_object_store_register::{ConditionalWrite, ReadConsistency, RegisterCapabilities};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;

use crate::local_file_store::ConditionalLocalFileStore;
use crate::node::{DurableLogletParts, PartsFactory};

/// Attested backend profiles Scripture may construct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendProfile {
    /// Local directory via [`ConditionalLocalFileStore`] (cargo-only try-it).
    ///
    /// Single-host, single writer process. Create is POSIX hard-link atomic;
    /// Update is etag-conditioned within the writer process.
    LocalFile,
    /// Local RustFS path-style S3 (Holylog local-s3 compose).
    RustFs,
    /// Cloudflare R2 (S3-compatible); register attested via [`RegisterCapabilities::cloudflare_r2`].
    CloudflareR2,
    /// Amazon S3; register attested via [`RegisterCapabilities::amazon_s3`].
    ///
    /// S3 gained conditional writes only in 2024, so a bucket configured
    /// without them is not a register whatever the endpoint claims. The
    /// attestation is a named claim about a tested configuration, never an
    /// inference from the API shape.
    AmazonS3,
}

impl BackendProfile {
    /// Parses `file`, `rustfs`, `r2`, or `s3`.
    pub fn parse(raw: &str) -> Result<Self, ObjectStoreError> {
        match raw {
            "file" => Ok(Self::LocalFile),
            "rustfs" => Ok(Self::RustFs),
            "r2" => Ok(Self::CloudflareR2),
            "s3" => Ok(Self::AmazonS3),
            other => Err(ObjectStoreError::BackendProfile(format!(
                "unknown backend profile '{other}' (expected file|rustfs|r2|s3)"
            ))),
        }
    }

    /// Stable report label for logs and status.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::LocalFile => "file",
            Self::RustFs => "rustfs",
            Self::CloudflareR2 => "r2",
            Self::AmazonS3 => "s3",
        }
    }

    /// Register capability claim for the shared Verse register pointer.
    #[must_use]
    pub fn register_capabilities(self) -> RegisterCapabilities {
        match self {
            Self::LocalFile => RegisterCapabilities::new(
                ConditionalWrite::VersionMatch,
                ReadConsistency::StronglyConsistent,
            ),
            Self::RustFs | Self::AmazonS3 => RegisterCapabilities::amazon_s3(),
            Self::CloudflareR2 => RegisterCapabilities::cloudflare_r2(),
        }
    }

    /// LogDrive capability declaration for this profile.
    #[must_use]
    pub fn drive_capabilities(self) -> BackendCapabilities {
        // file / RustFS / R2 / S3: atomic conditional create + lex listing.
        // file Create is hard-link publish; Update is Scripture's etag adapter.
        BackendCapabilities::new(
            PointSemantics::LinearizableSingleValue,
            ListingOrder::Lexicographic,
            ListingVisibility::Strong,
            ConditionalCreate::Atomic,
        )
    }
}

/// Exclusive object-store root for one deployment prefix.
pub struct ObjectStorePartsFactory {
    store: Arc<dyn ObjectStore>,
    root: String,
    capabilities: BackendCapabilities,
    write_policy: WritePolicy,
    /// Shared adapter counters across data/seal/trim drives for this factory.
    pub metrics: Arc<ObjectStoreMetrics>,
}

impl ObjectStorePartsFactory {
    /// Builds a factory over an exclusive store prefix (never the whole bucket).
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

    fn parts_for(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, ObjectStoreError> {
        // Prefer the provision helper's root layout; LogDrive seal/trim use exclusive
        // prefixes under the same base (address 0), matching VirtualLog hosted tests.
        let ns = LogletObjectNamespaces::under_root(&self.root, loglet_id);
        let data_prefix = ns.data_prefix.trim_end_matches('/');
        let base = data_prefix
            .strip_suffix("/data")
            .ok_or_else(|| ObjectStoreError::Namespace(ns.data_prefix.clone()))?;
        let data = self.drive(Path::from(data_prefix))?;
        let seal_drive = self.drive(Path::from(format!("{base}/seal")))?;
        let trim_drive = self.drive(Path::from(format!("{base}/trim")))?;
        let seal = Arc::new(LogDriveSeal::new(
            seal_drive,
            Address::new(0).map_err(ObjectStoreError::Address)?,
            Bytes::from_static(b"sealed"),
        )) as Arc<dyn Seal>;
        let trim = Arc::new(LogDriveTrimPoint::new(trim_drive)) as Arc<dyn TrimPoint>;
        Ok(DurableLogletParts::from_components(data, seal, trim))
    }

    fn drive(&self, prefix: Path) -> Result<Arc<dyn LogDrive>, ObjectStoreError> {
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
    ) -> Result<DurableLogletParts, crate::node::PartsFactoryError> {
        self.parts_for(loglet_id)
            .map_err(|error| crate::node::PartsFactoryError::new(error.to_string()))
    }

    fn open(
        &self,
        loglet_id: &LogletId,
    ) -> Result<DurableLogletParts, crate::node::PartsFactoryError> {
        self.fresh(loglet_id)
    }

    fn namespaces(
        &self,
        loglet_id: &LogletId,
    ) -> Result<holylog::provision::LogletObjectNamespaces, crate::node::PartsFactoryError> {
        Ok(LogletObjectNamespaces::under_root(&self.root, loglet_id))
    }
}

/// Failures while constructing object-store adapters.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
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
    /// Local filesystem store could not be opened.
    #[error("local file store: {0}")]
    LocalFile(object_store::Error),
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

/// Opens a local-directory store rooted at `path` (created if missing).
pub fn connect_local_file(
    path: impl Into<PathBuf>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    ConditionalLocalFileStore::shared(path).map_err(ObjectStoreError::LocalFile)
}

#[cfg(test)]
mod tests {
    use super::*;
    use holylog_object_store::{ConditionalCreate, ObjectStoreExclusiveClaim};
    use object_store::memory::InMemory;

    #[test]
    fn parses_attested_profiles_and_rejects_unattested_ones() {
        assert_eq!(
            BackendProfile::parse("file").expect("file"),
            BackendProfile::LocalFile
        );
        assert_eq!(
            BackendProfile::parse("rustfs").expect("rustfs"),
            BackendProfile::RustFs
        );
        assert_eq!(
            BackendProfile::parse("r2").expect("r2"),
            BackendProfile::CloudflareR2
        );
        assert_eq!(
            BackendProfile::parse("s3").expect("s3"),
            BackendProfile::AmazonS3
        );
        // Garage speaks the same API and the same headers, and silently ignores
        // the preconditions a register depends on. It is the recorded
        // falsification, and the reason a profile is a tested claim rather than
        // an inference from the API shape.
        assert!(BackendProfile::parse("garage").is_err());
    }

    #[test]
    fn file_profile_attests_atomic_create_and_version_match() {
        assert_eq!(BackendProfile::LocalFile.label(), "file");
        assert_eq!(
            BackendProfile::LocalFile
                .drive_capabilities()
                .conditional_create,
            ConditionalCreate::Atomic
        );
        assert_eq!(
            BackendProfile::LocalFile.register_capabilities(),
            RegisterCapabilities::new(
                ConditionalWrite::VersionMatch,
                ReadConsistency::StronglyConsistent,
            )
        );
    }

    #[test]
    fn s3_declares_the_attested_amazon_register_capabilities() {
        assert_eq!(
            BackendProfile::AmazonS3.register_capabilities(),
            RegisterCapabilities::amazon_s3()
        );
        assert_eq!(BackendProfile::AmazonS3.label(), "s3");
    }

    #[test]
    fn object_store_claim_refuses_non_atomic_conditional_create() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let caps = BackendCapabilities::new(
            PointSemantics::LinearizableSingleValue,
            ListingOrder::Lexicographic,
            ListingVisibility::Strong,
            ConditionalCreate::Unsupported,
        );
        assert!(ObjectStoreExclusiveClaim::new(store, caps).is_err());
    }

    #[tokio::test]
    async fn local_file_register_bootstrap_second_create_loses_then_cas_updates() {
        use holylog::virtual_log::{
            ApplicationFence, ConditionalRegister, GenerationDescriptor, LogletId, VirtualLogState,
        };
        use holylog_object_store_register::ObjectStoreConditionalRegister;
        use object_store::path::Path as ObjectPath;

        let dir = tempfile::tempdir().expect("tempdir");
        let store = connect_local_file(dir.path()).expect("open");
        let register = ObjectStoreConditionalRegister::new(
            Arc::clone(&store),
            ObjectPath::from("tryit/register-pointer"),
            BackendProfile::LocalFile.register_capabilities(),
        )
        .expect("register seams");

        let membership = |revision: u64, loglet: &str| VirtualLogState {
            revision,
            generations: vec![GenerationDescriptor {
                loglet_id: LogletId::new(loglet).expect("id"),
                start: 0,
            }],
            application_fence: ApplicationFence::default(),
        };

        assert!(
            register
                .compare_and_swap(None, membership(0, "gen-a"))
                .await
                .expect("bootstrap"),
            "first bootstrap must win"
        );
        assert!(
            !register
                .compare_and_swap(None, membership(0, "gen-b"))
                .await
                .expect("second bootstrap"),
            "second Create must lose"
        );

        let observed = register.read().await.expect("read").expect("present");
        assert_eq!(observed.state.generations[0].loglet_id.as_str(), "gen-a");
        assert!(
            register
                .compare_and_swap(Some(&observed), membership(1, "gen-c"))
                .await
                .expect("cas"),
            "matching Update must win"
        );
        let after = register.read().await.expect("read").expect("present");
        assert_eq!(after.state.generations[0].loglet_id.as_str(), "gen-c");
        assert!(
            !register
                .compare_and_swap(Some(&observed), membership(1, "gen-d"))
                .await
                .expect("stale witness cas"),
            "stale etag witness must lose"
        );
    }
}
