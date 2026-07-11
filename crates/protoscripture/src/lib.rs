//! Protoscripture: a disposable spike that consumes the holylog kernel the way
//! the real Scripture will — through its public API only.
//!
//! Nothing in this crate is the product. The envelope, batch codec, and
//! journal exist to pressure-test the kernel surface from a consumer's seat
//! and to feed `docs/kernel-gap-report.md`. Every design question they raise
//! is answered by the Scripture design obligations, not by this code.

use std::collections::BTreeMap;
use std::sync::Mutex;

use bytes::{BufMut, Bytes, BytesMut};
use holylog::atomic::{AtomicLog, AtomicLogError, LogEntry};
use holylog::drive::{DriveError, DriveFuture, LogDrive};
use holylog::logdrive::{Address, ReferenceLogDrive, TailDescription};

/// A deterministic in-memory primitive LogDrive for spike runs.
///
/// Kernel gap: holylog does not export a reusable asynchronous in-memory
/// drive, so every consumer rebuilds this wrapper around `ReferenceLogDrive`.
#[derive(Debug, Default)]
pub struct InMemoryDrive {
    model: Mutex<ReferenceLogDrive>,
}

#[derive(Debug, thiserror::Error)]
#[error("in-memory drive lock poisoned")]
struct LockPoisoned;

impl InMemoryDrive {
    /// Creates an empty drive.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogDrive for InMemoryDrive {
    fn write(&self, address: Address, value: Bytes) -> DriveFuture<'_, ()> {
        Box::pin(async move {
            self.model
                .lock()
                .map_err(|_| DriveError::backend(LockPoisoned))?
                .write(address, value)?;
            Ok(())
        })
    }

    fn read(&self, address: Address) -> DriveFuture<'_, Option<Bytes>> {
        Box::pin(async move {
            Ok(self
                .model
                .lock()
                .map_err(|_| DriveError::backend(LockPoisoned))?
                .read(address)
                .cloned())
        })
    }

    fn weak_tail(&self, k: u64) -> DriveFuture<'_, TailDescription> {
        Box::pin(async move {
            Ok(self
                .model
                .lock()
                .map_err(|_| DriveError::backend(LockPoisoned))?
                .weak_tail(k)?)
        })
    }
}

/// Envelope format version written as the first byte of every batch.
pub const BATCH_FORMAT_VERSION: u8 = 0;

/// One application record inside a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Typed-envelope attributes used for filtering.
    pub attributes: BTreeMap<String, String>,
    /// Opaque application payload.
    pub payload: Bytes,
}

impl Record {
    /// Builds a record from attribute pairs and a payload.
    #[must_use]
    pub fn new<K: Into<String>, V: Into<String>>(
        attributes: impl IntoIterator<Item = (K, V)>,
        payload: Bytes,
    ) -> Self {
        Self {
            attributes: attributes
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            payload,
        }
    }

    /// Returns whether the record carries `key = value`.
    #[must_use]
    pub fn matches(&self, key: &str, value: &str) -> bool {
        self.attributes
            .get(key)
            .is_some_and(|observed| observed == value)
    }
}

/// Errors produced by the batch codec.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    /// The batch declared a format version this build does not understand.
    #[error("unsupported batch format version {version}")]
    UnsupportedVersion { version: u8 },

    /// The batch ended before its declared contents.
    #[error("batch is truncated")]
    Truncated,

    /// An attribute key or value was not valid UTF-8.
    #[error("attribute is not valid UTF-8")]
    InvalidAttribute,

    /// A record count or length exceeded the format's u32 framing.
    #[error("batch component exceeds u32 framing")]
    Oversized,
}

fn put_len_prefixed(buffer: &mut BytesMut, bytes: &[u8]) -> Result<(), CodecError> {
    let length = u32::try_from(bytes.len()).map_err(|_| CodecError::Oversized)?;
    buffer.put_u32(length);
    buffer.put_slice(bytes);
    Ok(())
}

/// Encodes records into one versioned batch payload.
pub fn encode_batch(records: &[Record]) -> Result<Bytes, CodecError> {
    let mut buffer = BytesMut::new();
    buffer.put_u8(BATCH_FORMAT_VERSION);
    let count = u32::try_from(records.len()).map_err(|_| CodecError::Oversized)?;
    buffer.put_u32(count);
    for record in records {
        let attributes =
            u32::try_from(record.attributes.len()).map_err(|_| CodecError::Oversized)?;
        buffer.put_u32(attributes);
        for (key, value) in &record.attributes {
            put_len_prefixed(&mut buffer, key.as_bytes())?;
            put_len_prefixed(&mut buffer, value.as_bytes())?;
        }
        put_len_prefixed(&mut buffer, &record.payload)?;
    }
    Ok(buffer.freeze())
}

struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], CodecError> {
        if self.bytes.len() < count {
            return Err(CodecError::Truncated);
        }
        let (taken, rest) = self.bytes.split_at(count);
        self.bytes = rest;
        Ok(taken)
    }

    fn take_u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn take_u32(&mut self) -> Result<u32, CodecError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| CodecError::Truncated)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn take_len_prefixed(&mut self) -> Result<&'a [u8], CodecError> {
        let length = self.take_u32()?;
        self.take(length as usize)
    }

    fn take_string(&mut self) -> Result<String, CodecError> {
        let bytes = self.take_len_prefixed()?;
        String::from_utf8(bytes.to_vec()).map_err(|_| CodecError::InvalidAttribute)
    }
}

/// Decodes one versioned batch payload into records.
pub fn decode_batch(batch: &Bytes) -> Result<Vec<Record>, CodecError> {
    let mut cursor = Cursor { bytes: batch };
    let version = cursor.take_u8()?;
    if version != BATCH_FORMAT_VERSION {
        return Err(CodecError::UnsupportedVersion { version });
    }
    let count = cursor.take_u32()?;
    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let attribute_count = cursor.take_u32()?;
        let mut attributes = BTreeMap::new();
        for _ in 0..attribute_count {
            let key = cursor.take_string()?;
            let value = cursor.take_string()?;
            attributes.insert(key, value);
        }
        let payload = Bytes::copy_from_slice(cursor.take_len_prefixed()?);
        records.push(Record {
            attributes,
            payload,
        });
    }
    if cursor.bytes.is_empty() {
        Ok(records)
    } else {
        Err(CodecError::Truncated)
    }
}

/// Errors produced by journal operations.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// The kernel rejected or failed the operation.
    #[error(transparent)]
    Log(#[from] AtomicLogError),

    /// A stored batch could not be decoded.
    #[error(transparent)]
    Codec(#[from] CodecError),
}

/// One decoded batch and the log position it occupies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchAt {
    /// The batch's slot in the underlying AtomicLog.
    pub position: u64,
    /// The decoded records.
    pub records: Vec<Record>,
}

/// A named log of record batches over one AtomicLog.
///
/// This is the spike's stand-in for Scripture's named-log surface. Naming,
/// directories, retention policy, subscriptions, and manifests are design
/// obligations, not features of this type.
pub struct Journal {
    name: String,
    log: AtomicLog,
}

impl Journal {
    /// Wraps an AtomicLog as a named journal.
    #[must_use]
    pub fn new(name: impl Into<String>, log: AtomicLog) -> Self {
        Self {
            name: name.into(),
            log,
        }
    }

    /// Returns the journal's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Encodes and appends one batch, returning its log position.
    pub async fn append_batch(&self, records: &[Record]) -> Result<u64, JournalError> {
        let encoded = encode_batch(records)?;
        let address = self.log.append(encoded).await?;
        Ok(address.get())
    }

    /// Returns the current checked tail (one past the last readable batch).
    pub async fn checked_tail(&self) -> Result<u64, JournalError> {
        Ok(self.log.check_tail().await?.tail)
    }

    /// Reads and decodes the batch at `position`, which must be below
    /// `checked_tail`.
    pub async fn read_batch(
        &self,
        position: u64,
        checked_tail: u64,
    ) -> Result<BatchAt, JournalError> {
        let LogEntry { address, payload } = self.log.read_next(position, checked_tail).await?;
        Ok(BatchAt {
            position: address.get(),
            records: decode_batch(&payload)?,
        })
    }

    /// Logically trims every batch below `position`.
    pub async fn trim_to(&self, position: u64) -> Result<u64, JournalError> {
        Ok(self.log.prefix_trim(position).await?)
    }

    /// Irreversibly seals the underlying log.
    pub async fn seal(&self) -> Result<(), JournalError> {
        Ok(self.log.seal().await?)
    }
}

#[cfg(test)]
mod tests {
    use proptest::collection::{btree_map, vec};
    use proptest::prelude::*;

    use super::{BATCH_FORMAT_VERSION, Bytes, CodecError, Record, decode_batch, encode_batch};

    #[test]
    fn rejects_unknown_versions_and_truncation() {
        let batch = encode_batch(&[Record::new([("kind", "order")], Bytes::from_static(b"x"))])
            .expect("encode");

        let mut wrong_version = batch.to_vec();
        wrong_version[0] = BATCH_FORMAT_VERSION + 1;
        assert_eq!(
            decode_batch(&Bytes::from(wrong_version)),
            Err(CodecError::UnsupportedVersion {
                version: BATCH_FORMAT_VERSION + 1
            })
        );

        let truncated = batch.slice(0..batch.len() - 1);
        assert_eq!(decode_batch(&truncated), Err(CodecError::Truncated));

        let mut padded = batch.to_vec();
        padded.push(0);
        assert_eq!(
            decode_batch(&Bytes::from(padded)),
            Err(CodecError::Truncated)
        );
    }

    proptest! {
        #[test]
        fn batches_round_trip(
            raw_records in vec(
                (
                    btree_map("[a-z]{1,8}", "[a-z0-9]{0,12}", 0..4),
                    vec(any::<u8>(), 0..64),
                ),
                0..12,
            ),
        ) {
            let records = raw_records
                .into_iter()
                .map(|(attributes, payload)| Record {
                    attributes,
                    payload: Bytes::from(payload),
                })
                .collect::<Vec<_>>();

            let encoded = encode_batch(&records).expect("encode generated batch");
            prop_assert_eq!(decode_batch(&encoded).expect("decode"), records);
        }
    }
}
