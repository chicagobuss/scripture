//! Object-store seam for DataRef blob placement without depending on a concrete
//! adapter.
//!
//! The live driver path puts sealed chunk bytes into a content-addressed blob,
//! then appends a [`crate::DataRef`]. Holylog still fences the pointer append;
//! the blob PUT grants nothing. Recovery and readers fetch through the same
//! seam so a missing object fails closed rather than silently dropping records.

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;

use crate::chunk::{ChunkDigest, SealedChunk, decode_index};
use crate::chunklog::{ChunkAppendAck, ChunkLogError, ChunkLogWriter};
use crate::dataref::DataRef;

/// Default staging prefix shared with the runtime blob writer.
pub const DEFAULT_STAGING_BLOB_PREFIX: &str = "blobs/v1";

/// Content-addressed put/get used by DataRef commit and recovery.
pub trait ChunkBlobStore: Send + Sync {
    /// Writes `bytes` under `key` and verifies the durable object matches `digest`.
    ///
    /// Callers must not append a DataRef until this returns Ok — an indeterminate
    /// PUT without a verified reread must not become a committed pointer.
    fn put_verified<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        digest: ChunkDigest,
    ) -> BoxFuture<'a, Result<(), ChunkLogError>>;

    /// Fetches the complete object at `key`.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Bytes, ChunkLogError>>;
}

/// Configures the driver to emit DataRefs instead of inline chunk payloads.
#[derive(Clone)]
pub struct DataRefBlobConfig {
    /// Object store used for staging blobs.
    pub store: Arc<dyn ChunkBlobStore>,
    /// Key prefix for content-addressed staging objects (`blobs/v1/...`).
    pub blob_prefix: String,
}

impl DataRefBlobConfig {
    /// Builds a config with the default staging prefix.
    #[must_use]
    pub fn new(store: Arc<dyn ChunkBlobStore>) -> Self {
        Self {
            store,
            blob_prefix: DEFAULT_STAGING_BLOB_PREFIX.into(),
        }
    }
}

impl std::fmt::Debug for DataRefBlobConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DataRefBlobConfig")
            .field("blob_prefix", &self.blob_prefix)
            .finish_non_exhaustive()
    }
}

/// Puts one sealed chunk as a content-addressed staging blob and appends its DataRef.
///
/// Single-chunk blobs are the depth-one driver mount: cross-Verse linger batching
/// still lives in the runtime blob writer, but the live append path must emit
/// pointers before that amortisation can be measured. A null cost result here is
/// still a real finding.
pub async fn commit_sealed_as_data_ref(
    writer: &mut ChunkLogWriter,
    store: &dyn ChunkBlobStore,
    blob_prefix: &str,
    sealed: &SealedChunk,
) -> Result<ChunkAppendAck, ChunkLogError> {
    if blob_prefix.trim().is_empty() {
        return Err(ChunkLogError::BlobStore(
            "blob_prefix must be nonempty".into(),
        ));
    }
    let index = decode_index(&sealed.bytes)?;
    let [frame] = index.frames.as_slice() else {
        return Err(ChunkLogError::JournalFrameMismatch {
            journal: writer.journal_id(),
        });
    };
    let length = u64::try_from(sealed.bytes.len())
        .map_err(|_| ChunkLogError::BlobStore("sealed chunk length exceeds u64".into()))?;
    let blob_digest = ChunkDigest::of(&sealed.bytes);
    let blob_key = format!("{blob_prefix}/{blob_digest}");
    store
        .put_verified(&blob_key, sealed.bytes.clone(), blob_digest)
        .await?;
    let data_ref = DataRef {
        blob_key,
        offset: 0,
        length,
        record_count: frame.record_count,
        chunk_id: sealed.chunk_id,
        chunk_digest: sealed.digest,
        blob_digest,
    };
    writer.append_data_ref(sealed, &data_ref).await
}
