//! [`scripture::ChunkBlobStore`] over an [`object_store::ObjectStore`].

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use scripture::{ChunkBlobStore, ChunkDigest, ChunkLogError};

use crate::blob_writer::put_and_verify;

/// Adapts a shared object store into the scripture DataRef blob seam.
#[derive(Debug, Clone)]
pub struct ObjectStoreChunkBlobStore {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreChunkBlobStore {
    /// Wraps `store` for DataRef commit and recovery fetches.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl ChunkBlobStore for ObjectStoreChunkBlobStore {
    fn put_verified<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        digest: ChunkDigest,
    ) -> BoxFuture<'a, Result<(), ChunkLogError>> {
        Box::pin(async move {
            put_and_verify(&self.store, key, bytes, digest)
                .await
                .map_err(|error| ChunkLogError::BlobStore(error.to_string()))
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Bytes, ChunkLogError>> {
        Box::pin(async move {
            let path = ObjectPath::from(key);
            match self.store.get(&path).await {
                Ok(result) => result
                    .bytes()
                    .await
                    .map_err(|error| ChunkLogError::BlobStore(error.to_string())),
                Err(object_store::Error::NotFound { .. }) => {
                    Err(ChunkLogError::DataRefBlobMissing { key: key.into() })
                }
                Err(error) => Err(ChunkLogError::BlobStore(error.to_string())),
            }
        })
    }
}
