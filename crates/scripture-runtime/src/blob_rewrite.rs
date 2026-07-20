//! Two-format lifecycle: rewrite staging blobs into per-Verse read-optimised objects.
//!
//! Write-optimised cross-Verse blobs under [`DEFAULT_STAGING_PREFIX`] are
//! short-lived staging. A background rewrite concatenates each Verse's chunks
//! into one content-addressed object under [`DEFAULT_REWRITTEN_PREFIX`], then
//! appends **superseding** [`DataRef`] log entries. Readers scan the log in
//! order and keep the best pointer per [`ChunkId`] (rewritten beats staging),
//! so a mid-scan client never sees records vanish or double.
//!
//! Staging bytes are never deleted by this module. They become collectable only
//! when every [`ChunkId`] they carried has a durable superseding rewritten
//! pointer in the log — evaluated without a live refcount.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use scripture::{
    Chunk, ChunkAppendAck, ChunkDigest, ChunkError, ChunkId, ChunkLogError, DataRef, DataRefError,
    LogPayload, RecordOffset, WriterId, decode_chunk, decode_log_payload, encode_data_ref,
    scan_sealed_chunk_ids,
};

use crate::blob_reader::{BlobReadError, ResolvedChunk, fetch_data_ref, resolve_log_payload};
use crate::blob_writer::{BlobWriterError, put_and_verify};

/// Object-store prefix for write-optimised cross-Verse staging blobs.
pub const DEFAULT_STAGING_PREFIX: &str = "blobs/v1";
/// Object-store prefix for per-Verse read-optimised rewritten objects.
pub const DEFAULT_REWRITTEN_PREFIX: &str = "verses/v1";

/// Configuration for a rewrite pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteConfig {
    /// Staging blob prefix (`blobs/v1/...`).
    pub staging_prefix: String,
    /// Rewritten per-Verse prefix (`verses/v1/{verse}/...`).
    pub rewritten_prefix: String,
}

impl Default for RewriteConfig {
    fn default() -> Self {
        Self {
            staging_prefix: DEFAULT_STAGING_PREFIX.into(),
            rewritten_prefix: DEFAULT_REWRITTEN_PREFIX.into(),
        }
    }
}

/// One staging [`DataRef`] to rewrite, in log order.
#[derive(Debug, Clone)]
pub struct StagingPointer {
    /// Verse owning this pointer.
    pub verse_key: String,
    /// Committed staging pointer.
    pub data_ref: DataRef,
    /// Offset the staging chunk already occupies in the Verse's dense space.
    ///
    /// Carried explicitly because a superseding append must name the range it
    /// replaces. Deriving it from the writer's tail is wrong the moment newer
    /// chunks sit behind the one being rewritten, which is the ordinary case
    /// for a background rewrite of a live Verse.
    pub first_offset: RecordOffset,
}

/// Placement inside a rewritten per-Verse object.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RewrittenPlacement {
    chunk_id: ChunkId,
    /// Offset the superseded staging chunk occupies.
    superseded_first_offset: RecordOffset,
    offset: u64,
    length: u64,
    chunk_digest: ChunkDigest,
    record_count: u32,
    writer_id: WriterId,
}

/// Progress persisted across an interrupted rewrite (resume seam).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerseRewriteProgress {
    /// Content-addressed rewritten object, once PUT+verified.
    pub rewritten_blob_key: Option<String>,
    /// Superseding pointers already fenced into the log.
    pub superseded_chunk_ids: BTreeSet<ChunkId>,
}

/// Outcome of rewriting one Verse's staging pointers.
#[derive(Debug)]
pub struct VerseRewriteOutcome {
    /// Verse rewritten.
    pub verse_key: String,
    /// Final progress (for resume).
    pub progress: VerseRewriteProgress,
    /// Superseding append outcomes keyed by immutable chunk identity.
    pub append_outcomes: BTreeMap<ChunkId, Result<ChunkAppendAck, RewriteError>>,
}

/// Rewrite failures.
#[derive(Debug, thiserror::Error)]
pub enum RewriteError {
    /// Object-store / writer error surface.
    #[error(transparent)]
    Blob(#[from] BlobWriterError),
    /// DataRef codec error.
    #[error(transparent)]
    DataRef(#[from] DataRefError),
    /// Read/verify error while materialising staging bytes.
    #[error(transparent)]
    Read(#[from] BlobReadError),
    /// Chunk decode failed.
    #[error(transparent)]
    Chunk(#[from] ChunkError),
    /// Fenced log append failed.
    #[error(transparent)]
    ChunkLog(#[from] ChunkLogError),
    /// Internal invariant violation.
    #[error("rewrite invariant: {0}")]
    Invariant(String),
}

/// Append target for superseding rewritten pointers (fenced per Verse).
#[async_trait]
pub trait SupersedingAppendTarget: Send {
    /// Appends a superseding [`DataRef`] that names rewritten bytes already verified.
    async fn append_superseding(
        &mut self,
        writer_id: WriterId,
        data_ref: &DataRef,
        superseded_first_offset: RecordOffset,
    ) -> Result<ChunkAppendAck, RewriteError>;
}

/// True when `key` names a write-optimised staging blob.
pub fn is_staging_blob_key(key: &str, config: &RewriteConfig) -> bool {
    key.starts_with(&format!("{}/", config.staging_prefix))
}

/// True when `key` names a per-Verse rewritten object.
pub fn is_rewritten_blob_key(key: &str, config: &RewriteConfig) -> bool {
    key.starts_with(&format!("{}/", config.rewritten_prefix))
}

/// Among two DataRefs for the same [`ChunkId`], prefer the rewritten pointer.
pub fn prefer_data_ref<'a>(
    current: &'a DataRef,
    candidate: &'a DataRef,
    config: &RewriteConfig,
) -> &'a DataRef {
    let current_rewritten = is_rewritten_blob_key(&current.blob_key, config);
    let candidate_rewritten = is_rewritten_blob_key(&candidate.blob_key, config);
    if candidate_rewritten && !current_rewritten {
        candidate
    } else {
        current
    }
}

/// Scan log payloads in order and resolve exactly one record set per [`ChunkId`].
///
/// Superseding rewritten pointers replace staging pointers without duplicates.
/// Inline chunks participate as their own [`ChunkId`] entries.
pub async fn scan_log_deduped(
    store: &Arc<dyn ObjectStore>,
    payloads: &[Vec<u8>],
    config: &RewriteConfig,
) -> Result<Vec<ResolvedChunk>, RewriteError> {
    enum Entry {
        Inline(Chunk),
        Ref(DataRef),
    }

    let mut best: BTreeMap<ChunkId, Entry> = BTreeMap::new();
    let mut order: Vec<ChunkId> = Vec::new();

    for payload in payloads {
        match decode_log_payload(payload)? {
            LogPayload::InlineChunk(bytes) => {
                let chunk = decode_chunk(&bytes)?;
                let chunk_id = chunk.header.chunk_id;
                match best.get(&chunk_id) {
                    None => {
                        order.push(chunk_id);
                        best.insert(chunk_id, Entry::Inline(chunk));
                    }
                    Some(Entry::Ref(existing))
                        if is_rewritten_blob_key(&existing.blob_key, config) => {}
                    Some(_) => {
                        best.insert(chunk_id, Entry::Inline(chunk));
                    }
                }
            }
            LogPayload::DataRef(data_ref) => {
                let chunk_id = data_ref.chunk_id;
                match best.get(&chunk_id) {
                    None => {
                        order.push(chunk_id);
                        best.insert(chunk_id, Entry::Ref(data_ref));
                    }
                    Some(Entry::Ref(existing)) => {
                        let chosen = prefer_data_ref(existing, &data_ref, config);
                        if chosen.blob_key != existing.blob_key || chosen.offset != existing.offset
                        {
                            best.insert(chunk_id, Entry::Ref(chosen.clone()));
                        }
                    }
                    Some(Entry::Inline(_)) => {
                        if is_rewritten_blob_key(&data_ref.blob_key, config) {
                            best.insert(chunk_id, Entry::Ref(data_ref));
                        }
                    }
                }
            }
        }
    }

    let mut out = Vec::with_capacity(order.len());
    for chunk_id in order {
        let entry = best
            .remove(&chunk_id)
            .ok_or_else(|| RewriteError::Invariant("missing chosen entry".into()))?;
        match entry {
            Entry::Inline(chunk) => out.push(ResolvedChunk {
                chunk,
                data_ref: None,
            }),
            Entry::Ref(data_ref) => {
                out.push(resolve_log_payload(store, &encode_data_ref(&data_ref)?).await?);
            }
        }
    }
    Ok(out)
}

/// Rewrite one Verse's staging pointers into a per-Verse object and append superseding refs.
///
/// `progress` enables resume: an interrupted pass skips PUT when the rewritten
/// object is already verified, and skips chunk ids already superseded.
pub async fn rewrite_verse_staging(
    store: &Arc<dyn ObjectStore>,
    config: &RewriteConfig,
    pointers: &[StagingPointer],
    progress: &mut VerseRewriteProgress,
    target: &mut dyn SupersedingAppendTarget,
) -> Result<VerseRewriteOutcome, RewriteError> {
    if pointers.is_empty() {
        return Err(RewriteError::Invariant(
            "rewrite_verse_staging requires at least one pointer".into(),
        ));
    }
    let verse_key = pointers[0].verse_key.clone();
    if !pointers.iter().all(|p| p.verse_key == verse_key) {
        return Err(RewriteError::Invariant(
            "all pointers in one rewrite must share a verse_key".into(),
        ));
    }

    let placements = if progress.rewritten_blob_key.is_none() {
        build_rewritten_object(store, pointers).await?
    } else {
        load_rewritten_placements(store, progress, pointers).await?
    };

    if progress.rewritten_blob_key.is_none() {
        let blob_key = rewritten_object_key(&verse_key, placements.blob_digest, config);
        put_and_verify(
            store,
            &blob_key,
            placements.bytes.clone(),
            placements.blob_digest,
        )
        .await
        .map_err(RewriteError::Blob)?;
        progress.rewritten_blob_key = Some(blob_key);
    }

    let blob_key = progress
        .rewritten_blob_key
        .clone()
        .expect("rewritten blob key set after PUT");

    let mut append_outcomes = BTreeMap::new();
    for placement in &placements.items {
        if progress.superseded_chunk_ids.contains(&placement.chunk_id) {
            continue;
        }
        let superseding = DataRef {
            blob_key: blob_key.clone(),
            offset: placement.offset,
            length: placement.length,
            record_count: placement.record_count,
            chunk_id: placement.chunk_id,
            chunk_digest: placement.chunk_digest,
            blob_digest: placements.blob_digest,
        };
        let result = target
            .append_superseding(
                placement.writer_id,
                &superseding,
                placement.superseded_first_offset,
            )
            .await;
        if result.is_ok() {
            progress.superseded_chunk_ids.insert(placement.chunk_id);
        }
        append_outcomes.insert(placement.chunk_id, result);
    }

    Ok(VerseRewriteOutcome {
        verse_key,
        progress: progress.clone(),
        append_outcomes,
    })
}

struct BuiltRewrittenObject {
    bytes: Bytes,
    blob_digest: ChunkDigest,
    items: Vec<RewrittenPlacement>,
}

async fn build_rewritten_object(
    store: &Arc<dyn ObjectStore>,
    pointers: &[StagingPointer],
) -> Result<BuiltRewrittenObject, RewriteError> {
    let mut blob = BytesMut::new();
    let mut items = Vec::with_capacity(pointers.len());
    for pointer in pointers {
        let bytes = fetch_data_ref(store, &pointer.data_ref).await?;
        let chunk = decode_chunk(&bytes)?;
        let offset = u64::try_from(blob.len())
            .map_err(|_| RewriteError::Invariant("rewrite offset overflow".into()))?;
        let length = u64::try_from(bytes.len())
            .map_err(|_| RewriteError::Invariant("rewrite length overflow".into()))?;
        blob.extend_from_slice(&bytes);
        items.push(RewrittenPlacement {
            chunk_id: pointer.data_ref.chunk_id,
            superseded_first_offset: pointer.first_offset,
            offset,
            length,
            chunk_digest: pointer.data_ref.chunk_digest,
            record_count: pointer.data_ref.record_count,
            writer_id: chunk.header.writer_id,
        });
    }
    let bytes = blob.freeze();
    let blob_digest = ChunkDigest::of(&bytes);
    Ok(BuiltRewrittenObject {
        bytes,
        blob_digest,
        items,
    })
}

async fn load_rewritten_placements(
    store: &Arc<dyn ObjectStore>,
    progress: &VerseRewriteProgress,
    pointers: &[StagingPointer],
) -> Result<BuiltRewrittenObject, RewriteError> {
    let blob_key = progress
        .rewritten_blob_key
        .as_ref()
        .ok_or_else(|| RewriteError::Invariant("resume without rewritten blob".into()))?;
    let path = ObjectPath::from(blob_key.as_str());
    let bytes = store
        .get(&path)
        .await
        .map_err(|e| RewriteError::Blob(BlobWriterError::ObjectStore(e.to_string())))?
        .bytes()
        .await
        .map_err(|e| RewriteError::Blob(BlobWriterError::ObjectStore(e.to_string())))?;
    let blob_digest = ChunkDigest::of(&bytes);
    let mut items = Vec::with_capacity(pointers.len());
    let mut offset = 0_u64;
    for pointer in pointers {
        let length = pointer.data_ref.length;
        let end = offset
            .checked_add(length)
            .ok_or(DataRefError::RangeOverflow)?;
        let slice = bytes
            .get(offset as usize..end as usize)
            .ok_or_else(|| RewriteError::Invariant("rewritten blob layout mismatch".into()))?;
        pointer.data_ref.verify_chunk_bytes(slice)?;
        let chunk = decode_chunk(&Bytes::copy_from_slice(slice))?;
        items.push(RewrittenPlacement {
            chunk_id: pointer.data_ref.chunk_id,
            superseded_first_offset: pointer.first_offset,
            offset,
            length,
            chunk_digest: pointer.data_ref.chunk_digest,
            record_count: pointer.data_ref.record_count,
            writer_id: chunk.header.writer_id,
        });
        offset = end;
    }
    Ok(BuiltRewrittenObject {
        bytes,
        blob_digest,
        items,
    })
}

/// Every chunk a staging blob carries, across **all** Verses that shared it.
///
/// This exists so collectability cannot be answered from partial information.
/// Logs are per-Verse, so the natural thing for a caller to hold is one Verse's
/// pointers, while a staging blob spans several. A predicate that inferred the
/// blob's contents from whatever pointers it was handed would report a shared
/// blob collectable while a sibling Verse still referenced it, and deleting on
/// that answer loses the sibling's records.
///
/// Prefer [`staging_blob_contents_from_bytes`]: a staging blob is a concatenation
/// of sealed chunks whose headers carry [`ChunkId`], so membership is derived
/// from the object you must fetch before deleting — no sidecar to drift from
/// the bytes it describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingBlobContents {
    /// Object-store key of the staging blob.
    pub blob_key: String,
    /// Every chunk in the blob, regardless of which Verse owns it.
    pub chunk_ids: BTreeSet<ChunkId>,
}

/// Derives staging-blob membership by scanning concatenated sealed-chunk bytes.
///
/// Chosen over a sidecar because one GET of the object already required for
/// deletion recovers the full set, with no second write and no manifest that
/// can disagree with the bytes it names.
pub fn staging_blob_contents_from_bytes(
    blob_key: impl Into<String>,
    bytes: &[u8],
) -> Result<StagingBlobContents, ChunkError> {
    let ids = scan_sealed_chunk_ids(bytes)?;
    if ids.is_empty() {
        return Err(ChunkError::EmptyChunk);
    }
    Ok(StagingBlobContents {
        blob_key: blob_key.into(),
        chunk_ids: ids.into_iter().collect(),
    })
}

/// Collects the chunk ids that already have a superseding rewritten pointer.
///
/// Callers must union this across every Verse that shared the blob; supplying
/// one Verse's payloads yields one Verse's superseded set, which
/// [`staging_blob_collectable`] will correctly judge insufficient.
pub fn superseded_chunk_ids(
    log_payloads: &[Vec<u8>],
    config: &RewriteConfig,
) -> Result<BTreeSet<ChunkId>, DataRefError> {
    let mut superseded = BTreeSet::new();
    for payload in log_payloads {
        let LogPayload::DataRef(data_ref) = decode_log_payload(payload)? else {
            continue;
        };
        if is_rewritten_blob_key(&data_ref.blob_key, config) {
            superseded.insert(data_ref.chunk_id);
        }
    }
    Ok(superseded)
}

/// Returns true only when **every** chunk the blob carries has been rewritten.
///
/// Safe by construction rather than by convention: `contents` names the blob's
/// full membership, so a `superseded` set covering only some of the sharing
/// Verses cannot produce a true answer.
#[must_use]
pub fn staging_blob_collectable(
    contents: &StagingBlobContents,
    superseded: &BTreeSet<ChunkId>,
) -> bool {
    if contents.chunk_ids.is_empty() {
        return false;
    }
    contents
        .chunk_ids
        .iter()
        .all(|chunk_id| superseded.contains(chunk_id))
}

fn rewritten_object_key(verse_key: &str, digest: ChunkDigest, config: &RewriteConfig) -> String {
    let verse_hex = hex_encode(verse_key.as_bytes());
    format!("{}/{verse_hex}/{digest}", config.rewritten_prefix)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}
