//! Resolve Holylog payloads that may be inline chunks or DataRefs.
//!
//! Readers must not assume every log entry is an inline `SCRC` chunk. A DataRef
//! (`SRDF`) is resolved only after verifying both the blob and the exact chunk
//! evidence it names. Adjacent DataRefs retain a coalescing plan for a future
//! verified range-read cache.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use scripture::{
    Chunk, ChunkError, DataRef, DataRefError, LogPayload, decode_chunk, decode_log_payload,
};

/// Failures resolving a log payload into decoded chunk(s).
#[derive(Debug, thiserror::Error)]
pub enum BlobReadError {
    /// Payload was neither a chunk nor a DataRef.
    #[error(transparent)]
    Payload(#[from] DataRefError),
    /// Chunk bytes were malformed.
    #[error(transparent)]
    Chunk(#[from] ChunkError),
    /// Object-store ranged GET failed.
    #[error("blob ranged get: {0}")]
    ObjectStore(String),
    /// Fetched bytes were shorter than the DataRef claimed.
    #[error("blob range returned {got} bytes, dataref claimed {expected}")]
    ShortRead {
        /// Bytes returned.
        got: usize,
        /// Bytes claimed by the DataRef.
        expected: usize,
    },
    /// The decoded header did not carry the committed chunk identity.
    #[error("decoded chunk_id does not match DataRef")]
    ChunkIdMismatch,
}

/// One decoded chunk recovered from a log payload (inline or via DataRef).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedChunk {
    /// Decoded chunk.
    pub chunk: Chunk,
    /// When resolved via DataRef, the pointer that located it.
    pub data_ref: Option<DataRef>,
}

/// Resolves one Holylog payload into a decoded chunk.
pub async fn resolve_log_payload(
    store: &Arc<dyn ObjectStore>,
    payload: &[u8],
) -> Result<ResolvedChunk, BlobReadError> {
    match decode_log_payload(payload)? {
        LogPayload::InlineChunk(bytes) => Ok(ResolvedChunk {
            chunk: decode_chunk(&bytes)?,
            data_ref: None,
        }),
        LogPayload::DataRef(data_ref) => {
            let bytes = fetch_data_ref(store, &data_ref).await?;
            let chunk = decode_chunk(&bytes)?;
            if chunk.header.chunk_id != data_ref.chunk_id {
                return Err(BlobReadError::ChunkIdMismatch);
            }
            Ok(ResolvedChunk {
                chunk,
                data_ref: Some(data_ref),
            })
        }
    }
}

/// Fetches and verifies the object before extracting the exact referenced range.
///
/// A ranged response alone cannot prove `blob_digest`; fetching the complete
/// object is the correctness-first implementation until a verified blob cache
/// can amortise this check.
pub async fn fetch_data_ref(
    store: &Arc<dyn ObjectStore>,
    data_ref: &DataRef,
) -> Result<Bytes, BlobReadError> {
    data_ref.validate()?;
    let end = data_ref
        .offset
        .checked_add(data_ref.length)
        .ok_or(DataRefError::RangeOverflow)?;
    let path = ObjectPath::from(data_ref.blob_key.as_str());
    let result = store
        .get(&path)
        .await
        .map_err(|error| BlobReadError::ObjectStore(error.to_string()))?
        .bytes()
        .await
        .map_err(|error| BlobReadError::ObjectStore(error.to_string()))?;
    if scripture::ChunkDigest::of(&result) != data_ref.blob_digest {
        return Err(DataRefError::BlobDigestMismatch.into());
    }
    let start = usize::try_from(data_ref.offset).map_err(|_| DataRefError::RangeOverflow)?;
    let end = usize::try_from(end).map_err(|_| DataRefError::RangeOverflow)?;
    let bytes = result.get(start..end).ok_or(BlobReadError::ShortRead {
        got: result.len().saturating_sub(start),
        expected: usize::try_from(data_ref.length).map_err(|_| DataRefError::RangeOverflow)?,
    })?;
    data_ref.verify_chunk_bytes(bytes)?;
    Ok(Bytes::copy_from_slice(bytes))
}

/// Plans coalesced ranged GETs for a sequence of DataRefs.
///
/// Adjacent refs with the same `blob_key` and contiguous offsets become one
/// GET; the returned slices map back to each original DataRef.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalescedGet {
    /// Blob key.
    pub blob_key: String,
    /// Inclusive start / exclusive end covering one or more DataRefs.
    pub byte_range: Range<u64>,
    /// How many original DataRefs this GET satisfies, in order.
    pub data_ref_count: usize,
}

/// Builds a minimal GET plan for `refs` in iteration order.
pub fn plan_coalesced_gets(refs: &[DataRef]) -> Result<Vec<CoalescedGet>, DataRefError> {
    let mut plan = Vec::new();
    let mut index = 0;
    while index < refs.len() {
        let first = &refs[index];
        first.validate()?;
        let mut end = first.end_offset()?;
        let mut count = 1;
        while index + count < refs.len() {
            let next = &refs[index + count];
            next.validate()?;
            if next.blob_key != first.blob_key || next.offset != end {
                break;
            }
            end = next.end_offset()?;
            count += 1;
        }
        plan.push(CoalescedGet {
            blob_key: first.blob_key.clone(),
            byte_range: first.offset..end,
            data_ref_count: count,
        });
        index += count;
    }
    Ok(plan)
}

/// Resolves many DataRefs, coalescing adjacent ranges into fewer GETs.
pub async fn resolve_data_refs_coalesced(
    store: &Arc<dyn ObjectStore>,
    refs: &[DataRef],
) -> Result<Vec<ResolvedChunk>, BlobReadError> {
    let plan = plan_coalesced_gets(refs)?;
    let mut out = Vec::with_capacity(refs.len());
    let mut cursor = 0;
    for get in plan {
        let path = ObjectPath::from(get.blob_key.as_str());
        let blob = store
            .get(&path)
            .await
            .map_err(|error| BlobReadError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| BlobReadError::ObjectStore(error.to_string()))?;
        for _ in 0..get.data_ref_count {
            let data_ref = &refs[cursor];
            if scripture::ChunkDigest::of(&blob) != data_ref.blob_digest {
                return Err(DataRefError::BlobDigestMismatch.into());
            }
            let start =
                usize::try_from(data_ref.offset).map_err(|_| DataRefError::RangeOverflow)?;
            let end =
                usize::try_from(data_ref.end_offset()?).map_err(|_| DataRefError::RangeOverflow)?;
            let slice = blob.get(start..end).ok_or(BlobReadError::ShortRead {
                got: blob.len().saturating_sub(start),
                expected: usize::try_from(data_ref.length)
                    .map_err(|_| DataRefError::RangeOverflow)?,
            })?;
            data_ref.verify_chunk_bytes(slice)?;
            let slice = Bytes::copy_from_slice(slice);
            let chunk = decode_chunk(&slice)?;
            if chunk.header.chunk_id != data_ref.chunk_id {
                return Err(BlobReadError::ChunkIdMismatch);
            }
            out.push(ResolvedChunk {
                chunk,
                data_ref: Some(data_ref.clone()),
            });
            cursor += 1;
        }
    }
    Ok(out)
}
