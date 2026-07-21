//! Local filesystem object store with etag-conditioned `PutMode::Update`.
//!
//! Stock [`object_store::local::LocalFileSystem`] supports atomic
//! [`PutMode::Create`] (hard-link publish) but returns `NotImplemented` for
//! [`PutMode::Update`]. Holylog's `ObjectStoreConditionalRegister` needs both
//! for bootstrap + CAS. This adapter adds single-host Update by checking the
//! observed etag under a process mutex, then overwriting.
//!
//! Attested try-it bound: one writer process on one host. Create remains the
//! cross-process atomic primitive; Update is exclusive within this process.

use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;

use futures::FutureExt;
use futures::stream::{BoxStream, StreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use tokio::sync::Mutex;

/// Local filesystem store that implements etag-conditioned Update.
#[derive(Debug)]
pub struct ConditionalLocalFileStore {
    inner: LocalFileSystem,
    /// Serializes Update so check-then-overwrite cannot interleave in-process.
    update_lock: Mutex<()>,
}

impl ConditionalLocalFileStore {
    /// Opens (creating if needed) a filesystem root used as the object-store prefix.
    pub fn new(root: impl Into<PathBuf>) -> ObjectStoreResult<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|source| object_store::Error::Generic {
            store: "ConditionalLocalFileStore",
            source: Box::new(source),
        })?;
        Ok(Self {
            inner: LocalFileSystem::new_with_prefix(root)?,
            update_lock: Mutex::new(()),
        })
    }

    /// Shared handle for product assembly.
    pub fn shared(root: impl Into<PathBuf>) -> ObjectStoreResult<Arc<dyn ObjectStore>> {
        Ok(Arc::new(Self::new(root)?) as Arc<dyn ObjectStore>)
    }
}

impl Display for ConditionalLocalFileStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConditionalLocalFileStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ConditionalLocalFileStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        match &opts.mode {
            PutMode::Create | PutMode::Overwrite => {
                self.inner.put_opts(location, payload, opts).await
            }
            PutMode::Update(version) => {
                let _guard = self.update_lock.lock().await;
                let meta = match self.inner.head(location).await {
                    Ok(meta) => meta,
                    Err(object_store::Error::NotFound { path, .. }) => {
                        return Err(object_store::Error::Precondition {
                            path,
                            source: format!("object at {location} not found").into(),
                        });
                    }
                    Err(error) => return Err(error),
                };
                let expected =
                    version
                        .e_tag
                        .as_deref()
                        .ok_or_else(|| object_store::Error::Generic {
                            store: "ConditionalLocalFileStore",
                            source: Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "PutMode::Update requires an e_tag witness",
                            )),
                        })?;
                let existing =
                    meta.e_tag
                        .as_deref()
                        .ok_or_else(|| object_store::Error::Generic {
                            store: "ConditionalLocalFileStore",
                            source: Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "local object missing e_tag",
                            )),
                        })?;
                if existing != expected {
                    return Err(object_store::Error::Precondition {
                        path: location.to_string(),
                        source: format!("{existing} does not match {expected}").into(),
                    });
                }
                let mut overwrite = opts;
                overwrite.mode = PutMode::Overwrite;
                self.inner.put_opts(location, payload, overwrite).await
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        // Local filesystem walk order is not lexicographic; Holylog LogDrive
        // requires ordered listings when attested as Lexicographic.
        let inner = self.inner.list(prefix);
        Box::pin(
            async move {
                let collected = StreamExt::collect::<Vec<_>>(inner).await;
                let mut ok = Vec::with_capacity(collected.len());
                for item in collected {
                    ok.push(item?);
                }
                ok.sort_by(|a, b| a.location.cmp(&b.location));
                Ok::<_, object_store::Error>(ok)
            }
            .into_stream()
            .map(|result| match result {
                Ok(items) => futures::stream::iter(items.into_iter().map(Ok)).left_stream(),
                Err(error) => futures::stream::iter(std::iter::once(Err(error))).right_stream(),
            })
            .flatten(),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects.sort_by(|a, b| a.location.cmp(&b.location));
        result.common_prefixes.sort();
        Ok(result)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::UpdateVersion;

    #[tokio::test]
    async fn conditional_create_second_loser_already_exists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ConditionalLocalFileStore::new(dir.path()).expect("open");
        let path = Path::from("cas/create-key");
        store
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"first")),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("first create");
        let err = store
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"second")),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect_err("second create must lose");
        assert!(
            matches!(err, object_store::Error::AlreadyExists { .. }),
            "expected AlreadyExists, got {err:?}"
        );
    }

    #[tokio::test]
    async fn etag_update_matches_then_rejects_stale_witness() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ConditionalLocalFileStore::new(dir.path()).expect("open");
        let path = Path::from("cas/update-key");
        let first = store
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"v1")),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("create");
        let etag = first.e_tag.expect("etag");
        store
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"v2")),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: Some(etag.clone()),
                    version: None,
                })),
            )
            .await
            .expect("matching update");
        let err = store
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"v3")),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: Some(etag),
                    version: None,
                })),
            )
            .await
            .expect_err("stale etag must fail");
        assert!(
            matches!(err, object_store::Error::Precondition { .. }),
            "expected Precondition, got {err:?}"
        );
    }
}
