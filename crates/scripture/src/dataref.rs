//! DataRef: a log payload that points at a chunk inside a shared blob.
//!
//! Cross-Verse batching writes one object that concatenates sealed chunks from
//! many Verses, then appends a DataRef to each Verse's log. The fence still
//! applies to the pointer append, not to the blob PUT — that is what keeps
//! per-Verse authority intact when one blob spans many Verses.
//!
//! A DataRef is **self-verifying evidence**: it carries the sealed [`ChunkId`]
//! and digest so a reader can prove the ranged bytes are exactly the chunk
//! whose pointer was committed. Locating a byte range alone is not enough —
//! a truncated blob, reused key, or off-by-one offset must fail closed.
//!
//! Staging blobs under `blobs/v1/` are short-lived write-optimised objects.
//! A background rewrite (see `scripture_runtime::blob_rewrite`) materialises
//! per-Verse read-optimised objects and appends superseding pointers. Prefix-
//! based retention applies to rewritten objects; staging blobs become
//! collectable only after every referenced [`ChunkId`] has a durable superseding
//! pointer in the log.
//!
//! # ReferenceBatch
//!
//! Several DataRefs may share **one lawful Holylog append for one Verse**
//! (`SRRB`). Cross-Verse metadata append-many is out of scope and must preserve
//! per-Verse fences; do not batch metadata across Verses in one authoritative
//! append.

use bytes::{BufMut, Bytes, BytesMut};

use crate::chunk::{ChunkDigest, ChunkId};

/// Inline chunk magic (`scripture::chunk` uses the same four bytes).
const INLINE_CHUNK_MAGIC: &[u8; 4] = b"SCRC";
/// Magic for a Scripture DataRef log payload (`SCRC` is an inline chunk).
const DATAREF_MAGIC: &[u8; 4] = b"SRDF";
/// Magic for a per-Verse ReferenceBatch of DataRefs.
const REFERENCE_BATCH_MAGIC: &[u8; 4] = b"SRRB";
/// Codec version 2 adds immutable chunk/blob evidence fields.
const DATAREF_VERSION: u8 = 2;
/// ReferenceBatch codec version.
const REFERENCE_BATCH_VERSION: u8 = 1;
/// Bound blob keys so a malformed pointer cannot allocate unbounded memory.
pub const MAX_BLOB_KEY_BYTES: usize = 1024;

/// Locates one sealed chunk inside a shared write-optimised blob.
///
/// `chunk_id` / `chunk_digest` bind the pointer to immutable sealed bytes.
/// `blob_digest` binds the shared object; content-addressed keys should embed
/// the same digest so a retry targets one identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataRef {
    /// Object-store key of the shared blob (preferably content-addressed).
    pub blob_key: String,
    /// Byte offset of this chunk inside the blob.
    pub offset: u64,
    /// Byte length of this chunk inside the blob.
    pub length: u64,
    /// Records carried by the referenced chunk (offset accounting without a fetch).
    pub record_count: u32,
    /// Sealed chunk identity — stable across retries and handoffs.
    pub chunk_id: ChunkId,
    /// BLAKE3 digest of the sealed chunk bytes at `offset..offset+length`.
    pub chunk_digest: ChunkDigest,
    /// BLAKE3 digest of the entire blob object.
    pub blob_digest: ChunkDigest,
}

impl DataRef {
    /// Validates nonempty key, positive length/count, and range arithmetic.
    pub fn validate(&self) -> Result<(), DataRefError> {
        if self.blob_key.is_empty() {
            return Err(DataRefError::EmptyBlobKey);
        }
        if self.blob_key.len() > MAX_BLOB_KEY_BYTES {
            return Err(DataRefError::BlobKeyTooLong {
                len: self.blob_key.len(),
            });
        }
        if self.length == 0 {
            return Err(DataRefError::ZeroLength);
        }
        if self.record_count == 0 {
            return Err(DataRefError::ZeroRecordCount);
        }
        self.offset
            .checked_add(self.length)
            .ok_or(DataRefError::RangeOverflow)?;
        Ok(())
    }

    /// Exclusive end offset inside the blob (`offset + length`).
    pub fn end_offset(&self) -> Result<u64, DataRefError> {
        self.validate()?;
        Ok(self.offset + self.length)
    }

    /// Checks fetched chunk bytes against this DataRef's immutable evidence.
    pub fn verify_chunk_bytes(&self, bytes: &[u8]) -> Result<(), DataRefError> {
        self.validate()?;
        let expected_len = usize::try_from(self.length).map_err(|_| DataRefError::RangeOverflow)?;
        if bytes.len() != expected_len {
            return Err(DataRefError::LengthMismatch {
                expected: expected_len,
                actual: bytes.len(),
            });
        }
        let digest = ChunkDigest::of(bytes);
        if digest != self.chunk_digest {
            return Err(DataRefError::ChunkDigestMismatch);
        }
        Ok(())
    }
}

/// A Holylog entry payload: inline chunk, single DataRef, or ReferenceBatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogPayload {
    /// Legacy path: the log entry bytes *are* the sealed chunk (`SCRC…`).
    InlineChunk(Bytes),
    /// Pointer at a portion of a shared blob (`SRDF…`).
    DataRef(DataRef),
    /// Ordered DataRefs under one Verse fence (`SRRB…`).
    ReferenceBatch(Vec<DataRef>),
}

/// DataRef codec / validation failures.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum DataRefError {
    /// Blob key must be nonempty.
    #[error("dataref blob_key is empty")]
    EmptyBlobKey,
    /// Blob key exceeds [`MAX_BLOB_KEY_BYTES`].
    #[error("dataref blob_key length {len} exceeds {MAX_BLOB_KEY_BYTES}")]
    BlobKeyTooLong {
        /// Observed key length.
        len: usize,
    },
    /// A zero-length pointer cannot name a chunk.
    #[error("dataref length must be nonzero")]
    ZeroLength,
    /// Record count must be nonzero (empty chunks are not valid progress).
    #[error("dataref record_count must be nonzero")]
    ZeroRecordCount,
    /// `offset + length` overflowed.
    #[error("dataref offset+length overflowed")]
    RangeOverflow,
    /// Fetched length did not match the DataRef.
    #[error("dataref length mismatch: expected {expected}, got {actual}")]
    LengthMismatch {
        /// Length claimed by the DataRef.
        expected: usize,
        /// Bytes actually fetched.
        actual: usize,
    },
    /// Fetched bytes did not match the sealed chunk digest.
    #[error("dataref chunk digest mismatch")]
    ChunkDigestMismatch,
    /// Decoded chunk identity did not match the DataRef.
    #[error("dataref chunk_id mismatch")]
    ChunkIdMismatch,
    /// Fetched blob bytes did not match the blob digest.
    #[error("dataref blob digest mismatch")]
    BlobDigestMismatch,
    /// Input ended before a complete value.
    #[error("dataref is truncated")]
    Truncated,
    /// Magic marker was wrong.
    #[error("invalid dataref magic")]
    InvalidMagic,
    /// Format version is not understood.
    #[error("unsupported dataref version {version}")]
    UnsupportedVersion {
        /// Version byte seen.
        version: u8,
    },
    /// Trailing bytes after a complete value.
    #[error("dataref has trailing bytes")]
    TrailingBytes,
    /// Blob key bytes were not valid UTF-8.
    #[error("dataref blob_key is not valid UTF-8")]
    InvalidUtf8,
    /// Payload is neither an inline chunk, a DataRef, nor a ReferenceBatch.
    #[error("unrecognized log payload magic")]
    UnrecognizedPayload,
    /// A ReferenceBatch must name at least one DataRef.
    #[error("reference batch is empty")]
    EmptyBatch,
    /// ReferenceBatch count exceeds what a u16 can name.
    #[error("reference batch length {len} exceeds u16::MAX")]
    BatchTooLarge {
        /// Observed item count.
        len: usize,
    },
    /// Decoded member count did not match the header count.
    #[error("reference batch count mismatch: header {expected}, decoded {actual}")]
    BatchCountMismatch {
        /// Count from the SRRB header.
        expected: usize,
        /// Members successfully decoded.
        actual: usize,
    },
}

/// Encodes a DataRef as a Holylog payload.
pub fn encode_data_ref(data_ref: &DataRef) -> Result<Bytes, DataRefError> {
    data_ref.validate()?;
    let key = data_ref.blob_key.as_bytes();
    let key_len =
        u16::try_from(key.len()).map_err(|_| DataRefError::BlobKeyTooLong { len: key.len() })?;
    let mut out = BytesMut::with_capacity(4 + 1 + 2 + key.len() + 8 + 8 + 4 + 16 + 32 + 32);
    out.put_slice(DATAREF_MAGIC);
    out.put_u8(DATAREF_VERSION);
    out.put_u16(key_len);
    out.put_slice(key);
    out.put_u64(data_ref.offset);
    out.put_u64(data_ref.length);
    out.put_u32(data_ref.record_count);
    out.put_slice(&data_ref.chunk_id.as_bytes());
    out.put_slice(&data_ref.chunk_digest.as_bytes());
    out.put_slice(&data_ref.blob_digest.as_bytes());
    Ok(out.freeze())
}

/// Decodes one DataRef from a byte prefix; returns the value and bytes consumed.
///
/// Used by ReferenceBatch decoding where several full `SRDF` blobs are
/// concatenated. Trailing bytes after this member are allowed.
fn decode_data_ref_prefix(bytes: &[u8]) -> Result<(DataRef, usize), DataRefError> {
    // Minimum: magic+ver+key_len+empty key+offset+len+count+ids
    if bytes.len() < 4 + 1 + 2 + 8 + 8 + 4 + 16 + 32 + 32 {
        return Err(DataRefError::Truncated);
    }
    if bytes[..4] != *DATAREF_MAGIC {
        return Err(DataRefError::InvalidMagic);
    }
    let version = bytes[4];
    if version != DATAREF_VERSION {
        return Err(DataRefError::UnsupportedVersion { version });
    }
    let key_len = usize::from(u16::from_be_bytes([bytes[5], bytes[6]]));
    let key_start: usize = 7;
    let key_end = key_start
        .checked_add(key_len)
        .ok_or(DataRefError::Truncated)?;
    let fixed_end = key_end
        .checked_add(8 + 8 + 4 + 16 + 32 + 32)
        .ok_or(DataRefError::Truncated)?;
    if bytes.len() < fixed_end {
        return Err(DataRefError::Truncated);
    }
    let key =
        std::str::from_utf8(&bytes[key_start..key_end]).map_err(|_| DataRefError::InvalidUtf8)?;
    let offset = u64::from_be_bytes(
        bytes[key_end..key_end + 8]
            .try_into()
            .map_err(|_| DataRefError::Truncated)?,
    );
    let length = u64::from_be_bytes(
        bytes[key_end + 8..key_end + 16]
            .try_into()
            .map_err(|_| DataRefError::Truncated)?,
    );
    let record_count = u32::from_be_bytes(
        bytes[key_end + 16..key_end + 20]
            .try_into()
            .map_err(|_| DataRefError::Truncated)?,
    );
    let chunk_id = ChunkId::from_bytes(
        bytes[key_end + 20..key_end + 36]
            .try_into()
            .map_err(|_| DataRefError::Truncated)?,
    );
    let chunk_digest = ChunkDigest::from_bytes(
        bytes[key_end + 36..key_end + 68]
            .try_into()
            .map_err(|_| DataRefError::Truncated)?,
    );
    let blob_digest = ChunkDigest::from_bytes(
        bytes[key_end + 68..key_end + 100]
            .try_into()
            .map_err(|_| DataRefError::Truncated)?,
    );
    let data_ref = DataRef {
        blob_key: key.to_owned(),
        offset,
        length,
        record_count,
        chunk_id,
        chunk_digest,
        blob_digest,
    };
    data_ref.validate()?;
    Ok((data_ref, fixed_end))
}

/// Decodes a DataRef Holylog payload.
pub fn decode_data_ref(bytes: &[u8]) -> Result<DataRef, DataRefError> {
    let (data_ref, consumed) = decode_data_ref_prefix(bytes)?;
    if consumed != bytes.len() {
        return Err(DataRefError::TrailingBytes);
    }
    Ok(data_ref)
}

/// Encodes several DataRefs as one per-Verse ReferenceBatch Holylog payload.
///
/// Cross-Verse metadata append-many is out of scope: this payload is lawful
/// only under a single Verse fence.
pub fn encode_reference_batch(refs: &[DataRef]) -> Result<Bytes, DataRefError> {
    if refs.is_empty() {
        return Err(DataRefError::EmptyBatch);
    }
    let count =
        u16::try_from(refs.len()).map_err(|_| DataRefError::BatchTooLarge { len: refs.len() })?;
    let mut out = BytesMut::new();
    out.put_slice(REFERENCE_BATCH_MAGIC);
    out.put_u8(REFERENCE_BATCH_VERSION);
    out.put_u16(count);
    for data_ref in refs {
        out.extend_from_slice(&encode_data_ref(data_ref)?);
    }
    Ok(out.freeze())
}

/// Decodes a ReferenceBatch Holylog payload into ordered DataRefs.
pub fn decode_reference_batch(bytes: &[u8]) -> Result<Vec<DataRef>, DataRefError> {
    if bytes.len() < 4 + 1 + 2 {
        return Err(DataRefError::Truncated);
    }
    if bytes[..4] != *REFERENCE_BATCH_MAGIC {
        return Err(DataRefError::InvalidMagic);
    }
    let version = bytes[4];
    if version != REFERENCE_BATCH_VERSION {
        return Err(DataRefError::UnsupportedVersion { version });
    }
    let expected = usize::from(u16::from_be_bytes([bytes[5], bytes[6]]));
    if expected == 0 {
        return Err(DataRefError::EmptyBatch);
    }
    let mut cursor = 7;
    let mut out = Vec::with_capacity(expected);
    while out.len() < expected {
        if cursor >= bytes.len() {
            return Err(DataRefError::BatchCountMismatch {
                expected,
                actual: out.len(),
            });
        }
        let (data_ref, consumed) = decode_data_ref_prefix(&bytes[cursor..])?;
        cursor = cursor
            .checked_add(consumed)
            .ok_or(DataRefError::Truncated)?;
        out.push(data_ref);
    }
    if cursor != bytes.len() {
        return Err(DataRefError::TrailingBytes);
    }
    if out.len() != expected {
        return Err(DataRefError::BatchCountMismatch {
            expected,
            actual: out.len(),
        });
    }
    Ok(out)
}

/// Dispatches a Holylog payload to an inline chunk, DataRef, or ReferenceBatch.
///
/// Chunk magic is `SCRC`; DataRef magic is `SRDF`; ReferenceBatch is `SRRB`.
/// Anything else fails closed.
pub fn decode_log_payload(bytes: &[u8]) -> Result<LogPayload, DataRefError> {
    if bytes.len() < 4 {
        return Err(DataRefError::Truncated);
    }
    if bytes[..4] == *DATAREF_MAGIC {
        return Ok(LogPayload::DataRef(decode_data_ref(bytes)?));
    }
    if bytes[..4] == *REFERENCE_BATCH_MAGIC {
        return Ok(LogPayload::ReferenceBatch(decode_reference_batch(bytes)?));
    }
    if bytes[..4] == *INLINE_CHUNK_MAGIC {
        return Ok(LogPayload::InlineChunk(Bytes::copy_from_slice(bytes)));
    }
    Err(DataRefError::UnrecognizedPayload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DataRef {
        DataRef {
            blob_key: "blobs/v1/abc".into(),
            offset: 128,
            length: 64,
            record_count: 3,
            chunk_id: ChunkId::from_bytes([7; 16]),
            chunk_digest: ChunkDigest::from_bytes([8; 32]),
            blob_digest: ChunkDigest::from_bytes([9; 32]),
        }
    }

    fn sample_at(offset: u64, chunk_byte: u8) -> DataRef {
        DataRef {
            blob_key: "blobs/v1/abc".into(),
            offset,
            length: 64,
            record_count: 2,
            chunk_id: ChunkId::from_bytes([chunk_byte; 16]),
            chunk_digest: ChunkDigest::from_bytes([chunk_byte.wrapping_add(1); 32]),
            blob_digest: ChunkDigest::from_bytes([9; 32]),
        }
    }

    #[test]
    fn round_trip_including_nonzero_offset() {
        let encoded = encode_data_ref(&sample()).expect("encode");
        let decoded = decode_data_ref(&encoded).expect("decode");
        assert_eq!(decoded, sample());
        assert_eq!(decoded.offset, 128);
    }

    #[test]
    fn dispatch_distinguishes_dataref_from_chunk_magic() {
        let encoded = encode_data_ref(&sample()).expect("encode");
        match decode_log_payload(&encoded).expect("dispatch") {
            LogPayload::DataRef(data_ref) => assert_eq!(data_ref, sample()),
            LogPayload::InlineChunk(_) => panic!("expected DataRef"),
            LogPayload::ReferenceBatch(_) => panic!("expected DataRef"),
        }
    }

    #[test]
    fn reference_batch_round_trip_preserves_order() {
        let refs = vec![sample_at(0, 1), sample_at(64, 2), sample_at(128, 3)];
        let encoded = encode_reference_batch(&refs).expect("encode");
        assert_eq!(&encoded[..4], b"SRRB");
        let decoded = decode_reference_batch(&encoded).expect("decode");
        assert_eq!(decoded, refs);
        match decode_log_payload(&encoded).expect("dispatch") {
            LogPayload::ReferenceBatch(batch) => assert_eq!(batch, refs),
            other => panic!("expected ReferenceBatch, got {other:?}"),
        }
    }

    #[test]
    fn reference_batch_rejects_empty_and_count_mismatch() {
        assert!(matches!(
            encode_reference_batch(&[]),
            Err(DataRefError::EmptyBatch)
        ));
        let mut encoded = encode_reference_batch(&[sample()])
            .expect("encode")
            .to_vec();
        // Corrupt the count to 2 while leaving one member — decode must fail closed.
        encoded[5..7].copy_from_slice(&2u16.to_be_bytes());
        assert!(matches!(
            decode_reference_batch(&encoded),
            Err(DataRefError::BatchCountMismatch {
                expected: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn rejects_empty_key_and_zero_length() {
        assert!(matches!(
            DataRef {
                blob_key: String::new(),
                offset: 0,
                length: 1,
                record_count: 1,
                chunk_id: ChunkId::from_bytes([1; 16]),
                chunk_digest: ChunkDigest::from_bytes([2; 32]),
                blob_digest: ChunkDigest::from_bytes([3; 32]),
            }
            .validate(),
            Err(DataRefError::EmptyBlobKey)
        ));
        assert!(matches!(
            DataRef {
                blob_key: "k".into(),
                offset: 0,
                length: 0,
                record_count: 1,
                chunk_id: ChunkId::from_bytes([1; 16]),
                chunk_digest: ChunkDigest::from_bytes([2; 32]),
                blob_digest: ChunkDigest::from_bytes([3; 32]),
            }
            .validate(),
            Err(DataRefError::ZeroLength)
        ));
    }

    #[test]
    fn verify_chunk_bytes_rejects_digest_mismatch() {
        let data_ref = sample();
        let err = data_ref
            .verify_chunk_bytes(&[0u8; 64])
            .expect_err("digest must fail");
        assert!(matches!(err, DataRefError::ChunkDigestMismatch));
    }
}
