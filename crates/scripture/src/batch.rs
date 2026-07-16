use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};

use crate::model::{AttributeValue, JournalId, Record, RecordOffset};

const HEADER_MAGIC: &[u8; 4] = b"SCRB";
const FOOTER_MAGIC: &[u8; 4] = b"SCMF";
const END_MAGIC: &[u8; 4] = b"SCFE";
const MAJOR_VERSION: u8 = 1;
const MINOR_VERSION: u8 = 0;

const STRING_TAG: u8 = 1;
const I64_TAG: u8 = 2;
const BOOL_TAG: u8 = 3;

/// A fully decoded durable batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// Journal identity embedded in the batch.
    pub journal_id: JournalId,
    /// Dense offset of the first record.
    pub base_offset: RecordOffset,
    /// Ordered records.
    pub records: Vec<Record>,
}

/// Durable batch encoding failures.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Input ended before a complete value was available.
    #[error("batch is truncated")]
    Truncated,
    /// A magic marker was invalid.
    #[error("invalid batch magic")]
    InvalidMagic,
    /// The major format is not understood.
    #[error("unsupported batch major version {major}")]
    UnsupportedMajor { major: u8 },
    /// The minor format is newer than this reader.
    #[error("unsupported batch minor version {minor}")]
    UnsupportedMinor { minor: u8 },
    /// An attribute type tag is not understood.
    #[error("unsupported attribute type {tag}")]
    UnsupportedAttributeType { tag: u8 },
    /// UTF-8 text was malformed.
    #[error("batch text is not valid UTF-8")]
    InvalidUtf8,
    /// A bool was not encoded canonically.
    #[error("boolean attribute has invalid value {value}")]
    InvalidBool { value: u8 },
    /// A fixed-width attribute used the wrong encoded length.
    #[error("attribute type {tag} has length {actual}, expected {expected}")]
    InvalidAttributeLength {
        tag: u8,
        expected: usize,
        actual: usize,
    },
    /// Attribute keys were duplicated or not in canonical ascending order.
    #[error("attribute keys are not unique and canonically ordered")]
    NonCanonicalAttributes,
    /// A length or count cannot be represented by the format.
    #[error("batch component exceeds format limits")]
    Oversized,
    /// Record offsets overflowed u64.
    #[error("record offset space is exhausted")]
    OffsetOverflow,
    /// Footer record offsets did not describe the encoded record section.
    #[error("batch footer index is corrupt")]
    CorruptFooter,
    /// Bytes remained after the declared value.
    #[error("batch has trailing or misplaced bytes")]
    TrailingBytes,
}

fn put_u32_len(buffer: &mut BytesMut, length: usize) -> Result<(), CodecError> {
    buffer.put_u32(u32::try_from(length).map_err(|_| CodecError::Oversized)?);
    Ok(())
}

fn put_len_bytes(buffer: &mut BytesMut, value: &[u8]) -> Result<(), CodecError> {
    put_u32_len(buffer, value.len())?;
    buffer.put_slice(value);
    Ok(())
}

/// Encodes a canonical self-contained batch.
pub fn encode_batch(
    journal_id: JournalId,
    base_offset: RecordOffset,
    records: &[Record],
) -> Result<Bytes, CodecError> {
    base_offset
        .checked_add(records.len())
        .ok_or(CodecError::OffsetOverflow)?;
    let mut output = BytesMut::new();
    output.put_slice(HEADER_MAGIC);
    output.put_u8(MAJOR_VERSION);
    output.put_u8(MINOR_VERSION);
    output.put_slice(&journal_id.as_bytes());
    output.put_u64(base_offset.get());
    put_u32_len(&mut output, records.len())?;

    let mut offsets = Vec::with_capacity(records.len());
    for record in records {
        offsets.push(u64::try_from(output.len()).map_err(|_| CodecError::Oversized)?);
        put_u32_len(&mut output, record.attributes.len())?;
        for (key, value) in &record.attributes {
            put_len_bytes(&mut output, key.as_bytes())?;
            match value {
                AttributeValue::String(value) => {
                    output.put_u8(STRING_TAG);
                    put_len_bytes(&mut output, value.as_bytes())?;
                }
                AttributeValue::I64(value) => {
                    output.put_u8(I64_TAG);
                    output.put_u32(8);
                    output.put_i64(*value);
                }
                AttributeValue::Bool(value) => {
                    output.put_u8(BOOL_TAG);
                    output.put_u32(1);
                    output.put_u8(u8::from(*value));
                }
            }
        }
        put_len_bytes(&mut output, &record.payload)?;
    }

    let footer_start = output.len();
    output.put_slice(FOOTER_MAGIC);
    put_u32_len(&mut output, offsets.len())?;
    for offset in offsets {
        output.put_u64(offset);
    }
    let footer_len = output
        .len()
        .checked_sub(footer_start)
        .and_then(|length| u64::try_from(length).ok())
        .ok_or(CodecError::Oversized)?;
    output.put_u64(footer_len);
    output.put_slice(END_MAGIC);
    Ok(output.freeze())
}

/// Returns the exact encoded length without retaining the encoded bytes.
pub fn encoded_batch_len(records: &[Record]) -> Result<usize, CodecError> {
    // Header (34) + footer fixed framing (20) + one footer u64 per record.
    records.iter().try_fold(54_usize, |length, record| {
        length
            .checked_add(8)
            .and_then(|length| length.checked_add(encoded_record_len(record).ok()?))
            .ok_or(CodecError::Oversized)
    })
}

pub(crate) fn encoded_record_len(record: &Record) -> Result<usize, CodecError> {
    // Attribute count and payload length.
    let mut length = 8_usize
        .checked_add(record.payload.len())
        .ok_or(CodecError::Oversized)?;
    for (key, value) in &record.attributes {
        // Key length, key bytes, type tag, value length, value bytes.
        let value_length = match value {
            AttributeValue::String(value) => value.len(),
            AttributeValue::I64(_) => 8,
            AttributeValue::Bool(_) => 1,
        };
        length = length
            .checked_add(4)
            .and_then(|length| length.checked_add(key.len()))
            .and_then(|length| length.checked_add(1 + 4))
            .and_then(|length| length.checked_add(value_length))
            .ok_or(CodecError::Oversized)?;
    }
    Ok(length)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CodecError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CodecError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        ))
    }

    fn len_bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let length = usize::try_from(self.u32()?).map_err(|_| CodecError::Oversized)?;
        self.take(length)
    }
}

/// Decodes and validates one complete canonical batch.
pub fn decode_batch(bytes: &Bytes) -> Result<Batch, CodecError> {
    if bytes.len() < 12 {
        return Err(CodecError::Truncated);
    }
    let end_magic = bytes.get(bytes.len() - 4..).ok_or(CodecError::Truncated)?;
    if end_magic != END_MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    let footer_length = u64::from_be_bytes(
        bytes[bytes.len() - 12..bytes.len() - 4]
            .try_into()
            .map_err(|_| CodecError::Truncated)?,
    );
    let footer_length = usize::try_from(footer_length).map_err(|_| CodecError::Oversized)?;
    let footer_start = bytes
        .len()
        .checked_sub(12)
        .and_then(|end| end.checked_sub(footer_length))
        .ok_or(CodecError::Truncated)?;

    let mut cursor = Cursor::new(&bytes[..footer_start]);
    if cursor.take(4)? != HEADER_MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    let major = cursor.u8()?;
    if major != MAJOR_VERSION {
        return Err(CodecError::UnsupportedMajor { major });
    }
    let minor = cursor.u8()?;
    if minor > MINOR_VERSION {
        return Err(CodecError::UnsupportedMinor { minor });
    }
    let journal_id = JournalId::from_bytes(
        cursor
            .take(16)?
            .try_into()
            .map_err(|_| CodecError::Truncated)?,
    );
    let base_offset = RecordOffset::new(cursor.u64()?);
    let record_count = usize::try_from(cursor.u32()?).map_err(|_| CodecError::Oversized)?;
    base_offset
        .checked_add(record_count)
        .ok_or(CodecError::OffsetOverflow)?;

    let mut observed_offsets = Vec::with_capacity(record_count);
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        observed_offsets.push(u64::try_from(cursor.position).map_err(|_| CodecError::Oversized)?);
        let attribute_count = usize::try_from(cursor.u32()?).map_err(|_| CodecError::Oversized)?;
        let mut attributes = BTreeMap::new();
        let mut previous_key: Option<String> = None;
        for _ in 0..attribute_count {
            let key = std::str::from_utf8(cursor.len_bytes()?)
                .map_err(|_| CodecError::InvalidUtf8)?
                .to_owned();
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            {
                return Err(CodecError::NonCanonicalAttributes);
            }
            previous_key = Some(key.clone());
            let tag = cursor.u8()?;
            let value_bytes = cursor.len_bytes()?;
            let value = match tag {
                STRING_TAG => AttributeValue::String(
                    std::str::from_utf8(value_bytes)
                        .map_err(|_| CodecError::InvalidUtf8)?
                        .to_owned(),
                ),
                I64_TAG => {
                    let value: [u8; 8] =
                        value_bytes
                            .try_into()
                            .map_err(|_| CodecError::InvalidAttributeLength {
                                tag: I64_TAG,
                                expected: 8,
                                actual: value_bytes.len(),
                            })?;
                    AttributeValue::I64(i64::from_be_bytes(value))
                }
                BOOL_TAG => match value_bytes {
                    [0] => AttributeValue::Bool(false),
                    [1] => AttributeValue::Bool(true),
                    [value] => return Err(CodecError::InvalidBool { value: *value }),
                    _ => {
                        return Err(CodecError::InvalidAttributeLength {
                            tag: BOOL_TAG,
                            expected: 1,
                            actual: value_bytes.len(),
                        });
                    }
                },
                tag => return Err(CodecError::UnsupportedAttributeType { tag }),
            };
            if attributes.insert(key, value).is_some() {
                return Err(CodecError::NonCanonicalAttributes);
            }
        }
        let payload = Bytes::copy_from_slice(cursor.len_bytes()?);
        records.push(Record {
            attributes,
            payload,
        });
    }
    if cursor.position != footer_start {
        return Err(CodecError::TrailingBytes);
    }

    let mut footer = Cursor::new(&bytes[footer_start..bytes.len() - 12]);
    if footer.take(4)? != FOOTER_MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    let footer_count = usize::try_from(footer.u32()?).map_err(|_| CodecError::Oversized)?;
    if footer_count != record_count {
        return Err(CodecError::CorruptFooter);
    }
    let mut declared_offsets = Vec::with_capacity(footer_count);
    for _ in 0..footer_count {
        declared_offsets.push(footer.u64()?);
    }
    if footer.position != footer.bytes.len() {
        return Err(CodecError::TrailingBytes);
    }
    if declared_offsets != observed_offsets {
        return Err(CodecError::CorruptFooter);
    }

    Ok(Batch {
        journal_id,
        base_offset,
        records,
    })
}
