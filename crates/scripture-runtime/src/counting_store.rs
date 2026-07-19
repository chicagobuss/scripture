//! An `ObjectStore` decorator that counts requests at the call boundary.
//!
//! The adapter metrics threaded into `ObjectStorePartsFactory` cover the
//! LogDrive data path only. The Serving-Authority register and the exclusive
//! claim store are constructed without metrics, and Holylog's register has no
//! metrics support to thread, so authority work is invisible in the cost line
//! — despite being the larger term.
//!
//! Counting here rather than inside Holylog keeps the change in the measuring
//! layer and, more usefully, counts at the boundary the provider actually
//! bills: every call that reaches the store is counted, so the ledger cannot
//! silently omit a path someone adds later.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};

/// Request counts for one decorated store.
#[derive(Debug, Default)]
pub struct RequestCounters {
    puts: AtomicU64,
    gets: AtomicU64,
    lists: AtomicU64,
    deletes: AtomicU64,
}

impl RequestCounters {
    /// Current counts as `(puts, gets, lists, deletes)`.
    #[must_use]
    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.puts.load(Ordering::Relaxed),
            self.gets.load(Ordering::Relaxed),
            self.lists.load(Ordering::Relaxed),
            self.deletes.load(Ordering::Relaxed),
        )
    }
}

/// Wraps a store and counts every call that reaches it.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<RequestCounters>,
}

impl CountingStore {
    /// Decorates `inner`, recording into `counters`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, counters: Arc<RequestCounters>) -> Self {
        Self { inner, counters }
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.counters.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.counters.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> ObjectStoreResult<GetResult> {
        self.counters.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, opts).await
    }

    // `delete` is a provided method and is deliberately not overridden: this
    // decorator exists to count the authority read/write path, where deletes
    // do not occur. `delete_stream` is counted so a bulk path is not silent.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.counters.deletes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.counters.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.counters.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> ObjectStoreResult<()> {
        self.counters.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_opts(from, to, opts).await
    }
}
