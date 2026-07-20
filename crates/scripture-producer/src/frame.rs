//! Versioned length-delimited spool frames (CRC32C Castagnoli).

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};

use scripture::chunk::ProducerId;
use scripture::driver::Submission;
use scripture::model::{AttributeValue, JournalId, Record};

use super::progress::ProgressIdentity;

const MAGIC: &[u8; 4] = b"SSWF";
const VERSION: u8 = 1;
const KIND_SUBMISSION: u8 = 1;
const KIND_PROGRESS: u8 = 2;
const STRING_TAG: u8 = 1;
const I64_TAG: u8 = 2;
const BOOL_TAG: u8 = 3;

/// Decoded spool frame body (CRC already verified).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolFrame {
    /// Durable submission identity + records (no placement).
    Submission {
        /// Journal this submission targets.
        journal_id: JournalId,
        /// Unchanged driver submission.
        submission: Submission,
    },
    /// Local evidence that a wrapped committed receipt completed.
    Progress(ProgressIdentity),
}

/// Frame kind discriminator for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// Submission WAL frame.
    Submission,
    /// Progress evidence frame.
    Progress,
}

impl SpoolFrame {
    /// Frame kind.
    #[must_use]
    pub fn kind(&self) -> FrameKind {
        match self {
            Self::Submission { .. } => FrameKind::Submission,
            Self::Progress(_) => FrameKind::Progress,
        }
    }

    /// Submission identity when present.
    #[must_use]
    pub fn identity(&self) -> Option<ProgressIdentity> {
        match self {
            Self::Submission {
                journal_id,
                submission,
            } => Some(ProgressIdentity {
                journal_id: *journal_id,
                producer_id: submission.producer_id,
                producer_epoch: submission.producer_epoch,
                sequence: submission.sequence,
            }),
            Self::Progress(identity) => Some(*identity),
        }
    }
}

/// Frame codec failures. Decoding arbitrary bytes must yield these, never panic.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum SpoolFrameError {
    /// Input ended before a complete value was available.
    #[error("spool frame is truncated")]
    Truncated,
    /// Magic marker was invalid.
    #[error("invalid spool frame magic")]
    InvalidMagic,
    /// Frame version is not understood.
    #[error("unsupported spool frame version {version}")]
    UnsupportedVersion {
        /// Observed version.
        version: u8,
    },
    /// Kind tag is not understood.
    #[error("unsupported spool frame kind {kind}")]
    UnsupportedKind {
        /// Observed kind.
        kind: u8,
    },
    /// CRC32C mismatch.
    #[error("spool frame checksum mismatch")]
    ChecksumMismatch,
    /// Attribute / record body is malformed.
    #[error("spool frame payload is corrupt")]
    CorruptPayload,
    /// UTF-8 text was malformed.
    #[error("spool frame text is not valid UTF-8")]
    InvalidUtf8,
    /// A bool was not encoded canonically.
    #[error("boolean attribute has invalid value {value}")]
    InvalidBool {
        /// Observed byte.
        value: u8,
    },
    /// Attribute keys were duplicated or not canonically ordered.
    #[error("attribute keys are not unique and canonically ordered")]
    NonCanonicalAttributes,
    /// A length or count cannot be represented.
    #[error("spool frame component exceeds format limits")]
    Oversized,
    /// Bytes remained after the declared value inside a payload section.
    #[error("spool frame has trailing payload bytes")]
    TrailingBytes,
}

/// Encodes one frame to length-delimited durable bytes.
pub fn encode_frame(frame: &SpoolFrame) -> Result<Bytes, SpoolFrameError> {
    let mut body = BytesMut::new();
    body.put_slice(MAGIC);
    body.put_u8(VERSION);
    match frame {
        SpoolFrame::Submission {
            journal_id,
            submission,
        } => {
            body.put_u8(KIND_SUBMISSION);
            body.put_slice(&journal_id.as_bytes());
            body.put_slice(&submission.producer_id.as_bytes());
            body.put_u32(submission.producer_epoch);
            body.put_u64(submission.sequence);
            encode_records(&mut body, &submission.records)?;
        }
        SpoolFrame::Progress(identity) => {
            body.put_u8(KIND_PROGRESS);
            body.put_slice(&identity.journal_id.as_bytes());
            body.put_slice(&identity.producer_id.as_bytes());
            body.put_u32(identity.producer_epoch);
            body.put_u64(identity.sequence);
        }
    }
    let crc = crc32c::crc32c(&body);
    body.put_u32(crc);

    let mut out = BytesMut::new();
    let len = u32::try_from(body.len()).map_err(|_| SpoolFrameError::Oversized)?;
    out.put_u32(len);
    out.extend_from_slice(&body);
    Ok(out.freeze())
}

/// Decodes one length-delimited frame starting at `bytes[0]`.
///
/// Returns `(frame, bytes_consumed)`. Arbitrary input returns [`SpoolFrameError`].
pub fn decode_frame(bytes: &[u8]) -> Result<(SpoolFrame, usize), SpoolFrameError> {
    if bytes.len() < 4 {
        return Err(SpoolFrameError::Truncated);
    }
    let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let total = 4usize.checked_add(len).ok_or(SpoolFrameError::Oversized)?;
    if bytes.len() < total {
        return Err(SpoolFrameError::Truncated);
    }
    let body = &bytes[4..total];
    if body.len() < 4 + 1 + 1 + 4 {
        return Err(SpoolFrameError::Truncated);
    }
    let (prefix, crc_bytes) = body.split_at(body.len() - 4);
    let expected = u32::from_be_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    if crc32c::crc32c(prefix) != expected {
        return Err(SpoolFrameError::ChecksumMismatch);
    }
    if prefix.len() < 6 {
        return Err(SpoolFrameError::Truncated);
    }
    if &prefix[0..4] != MAGIC {
        return Err(SpoolFrameError::InvalidMagic);
    }
    let version = prefix[4];
    if version != VERSION {
        return Err(SpoolFrameError::UnsupportedVersion { version });
    }
    let kind = prefix[5];
    let payload = &prefix[6..];
    let frame = match kind {
        KIND_SUBMISSION => decode_submission(payload)?,
        KIND_PROGRESS => SpoolFrame::Progress(decode_progress(payload)?),
        other => return Err(SpoolFrameError::UnsupportedKind { kind: other }),
    };
    Ok((frame, total))
}

fn decode_submission(payload: &[u8]) -> Result<SpoolFrame, SpoolFrameError> {
    if payload.len() < 16 + 16 + 4 + 8 {
        return Err(SpoolFrameError::Truncated);
    }
    let journal_id = JournalId::from_bytes(id16(&payload[0..16])?);
    let producer_id = ProducerId::from_bytes(id16(&payload[16..32])?);
    let producer_epoch = u32::from_be_bytes([payload[32], payload[33], payload[34], payload[35]]);
    let sequence = u64::from_be_bytes([
        payload[36],
        payload[37],
        payload[38],
        payload[39],
        payload[40],
        payload[41],
        payload[42],
        payload[43],
    ]);
    let records = decode_records(&payload[44..])?;
    Ok(SpoolFrame::Submission {
        journal_id,
        submission: Submission {
            producer_id,
            producer_epoch,
            sequence,
            records,
        },
    })
}

fn decode_progress(payload: &[u8]) -> Result<ProgressIdentity, SpoolFrameError> {
    if payload.len() != 16 + 16 + 4 + 8 {
        return Err(if payload.len() < 44 {
            SpoolFrameError::Truncated
        } else {
            SpoolFrameError::TrailingBytes
        });
    }
    Ok(ProgressIdentity {
        journal_id: JournalId::from_bytes(id16(&payload[0..16])?),
        producer_id: ProducerId::from_bytes(id16(&payload[16..32])?),
        producer_epoch: u32::from_be_bytes([payload[32], payload[33], payload[34], payload[35]]),
        sequence: u64::from_be_bytes([
            payload[36],
            payload[37],
            payload[38],
            payload[39],
            payload[40],
            payload[41],
            payload[42],
            payload[43],
        ]),
    })
}

fn id16(bytes: &[u8]) -> Result<[u8; 16], SpoolFrameError> {
    let mut out = [0_u8; 16];
    if bytes.len() != 16 {
        return Err(SpoolFrameError::Truncated);
    }
    out.copy_from_slice(bytes);
    Ok(out)
}

fn encode_records(out: &mut BytesMut, records: &[Record]) -> Result<(), SpoolFrameError> {
    put_u32_len(out, records.len())?;
    for record in records {
        put_u32_len(out, record.attributes.len())?;
        for (key, value) in &record.attributes {
            put_len_bytes(out, key.as_bytes())?;
            match value {
                AttributeValue::String(text) => {
                    out.put_u8(STRING_TAG);
                    put_len_bytes(out, text.as_bytes())?;
                }
                AttributeValue::I64(number) => {
                    out.put_u8(I64_TAG);
                    out.put_u32(8);
                    out.put_i64(*number);
                }
                AttributeValue::Bool(flag) => {
                    out.put_u8(BOOL_TAG);
                    out.put_u32(1);
                    out.put_u8(u8::from(*flag));
                }
            }
        }
        put_len_bytes(out, &record.payload)?;
    }
    Ok(())
}

fn decode_records(mut bytes: &[u8]) -> Result<Vec<Record>, SpoolFrameError> {
    let count = take_u32(&mut bytes)? as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let attr_count = take_u32(&mut bytes)? as usize;
        let mut attributes = BTreeMap::new();
        let mut last_key: Option<String> = None;
        for _ in 0..attr_count {
            let key_bytes = take_len_bytes(&mut bytes)?;
            let key = std::str::from_utf8(key_bytes)
                .map_err(|_| SpoolFrameError::InvalidUtf8)?
                .to_owned();
            if last_key.as_ref().is_some_and(|prior| key <= *prior) {
                return Err(SpoolFrameError::NonCanonicalAttributes);
            }
            last_key = Some(key.clone());
            let tag = take_u8(&mut bytes)?;
            let value = match tag {
                STRING_TAG => {
                    let text = take_len_bytes(&mut bytes)?;
                    AttributeValue::String(
                        std::str::from_utf8(text)
                            .map_err(|_| SpoolFrameError::InvalidUtf8)?
                            .to_owned(),
                    )
                }
                I64_TAG => {
                    let len = take_u32(&mut bytes)?;
                    if len != 8 {
                        return Err(SpoolFrameError::CorruptPayload);
                    }
                    AttributeValue::I64(take_i64(&mut bytes)?)
                }
                BOOL_TAG => {
                    let len = take_u32(&mut bytes)?;
                    if len != 1 {
                        return Err(SpoolFrameError::CorruptPayload);
                    }
                    let flag = take_u8(&mut bytes)?;
                    match flag {
                        0 => AttributeValue::Bool(false),
                        1 => AttributeValue::Bool(true),
                        value => return Err(SpoolFrameError::InvalidBool { value }),
                    }
                }
                _ => return Err(SpoolFrameError::CorruptPayload),
            };
            if attributes.insert(key, value).is_some() {
                return Err(SpoolFrameError::NonCanonicalAttributes);
            }
        }
        let payload = Bytes::copy_from_slice(take_len_bytes(&mut bytes)?);
        records.push(Record {
            attributes,
            payload,
        });
    }
    if !bytes.is_empty() {
        return Err(SpoolFrameError::TrailingBytes);
    }
    Ok(records)
}

fn put_u32_len(buffer: &mut BytesMut, length: usize) -> Result<(), SpoolFrameError> {
    buffer.put_u32(u32::try_from(length).map_err(|_| SpoolFrameError::Oversized)?);
    Ok(())
}

fn put_len_bytes(buffer: &mut BytesMut, value: &[u8]) -> Result<(), SpoolFrameError> {
    put_u32_len(buffer, value.len())?;
    buffer.put_slice(value);
    Ok(())
}

fn take_u8(bytes: &mut &[u8]) -> Result<u8, SpoolFrameError> {
    let (head, rest) = bytes.split_first().ok_or(SpoolFrameError::Truncated)?;
    *bytes = rest;
    Ok(*head)
}

fn take_u32(bytes: &mut &[u8]) -> Result<u32, SpoolFrameError> {
    if bytes.len() < 4 {
        return Err(SpoolFrameError::Truncated);
    }
    let value = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    *bytes = &bytes[4..];
    Ok(value)
}

fn take_i64(bytes: &mut &[u8]) -> Result<i64, SpoolFrameError> {
    if bytes.len() < 8 {
        return Err(SpoolFrameError::Truncated);
    }
    let value = i64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    *bytes = &bytes[8..];
    Ok(value)
}

fn take_len_bytes<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], SpoolFrameError> {
    let len = take_u32(bytes)? as usize;
    if bytes.len() < len {
        return Err(SpoolFrameError::Truncated);
    }
    let (head, rest) = bytes.split_at(len);
    *bytes = rest;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use scripture::model::AttributeValue;
    use bytes::Bytes;

    fn sample_submission() -> SpoolFrame {
        SpoolFrame::Submission {
            journal_id: JournalId::from_bytes(*b"spool-journal!!!"),
            submission: Submission {
                producer_id: ProducerId::from_bytes(*b"spool-producer!!"),
                producer_epoch: 3,
                sequence: 9,
                records: vec![Record::new(
                    [("k".into(), AttributeValue::I64(7))],
                    Bytes::from_static(b"payload"),
                )],
            },
        }
    }

    #[test]
    fn round_trip_submission_and_progress() {
        let frame = sample_submission();
        let encoded = encode_frame(&frame).expect("encode");
        let (decoded, consumed) = decode_frame(&encoded).expect("decode");
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, frame);

        let progress = SpoolFrame::Progress(ProgressIdentity {
            journal_id: JournalId::from_bytes(*b"spool-journal!!!"),
            producer_id: ProducerId::from_bytes(*b"spool-producer!!"),
            producer_epoch: 3,
            sequence: 9,
        });
        let encoded = encode_frame(&progress).expect("encode");
        let (decoded, _) = decode_frame(&encoded).expect("decode");
        assert_eq!(decoded, progress);
    }

    #[test]
    fn crc32c_known_vector_is_castagnoli() {
        const CHECK_VECTOR: &[u8] = b"123456789";
        const CRC32C_CHECK: u32 = 0xE306_9283;
        assert_eq!(crc32c::crc32c(CHECK_VECTOR), CRC32C_CHECK);
        let encoded = encode_frame(&sample_submission()).expect("encode");
        let body_len = u32::from_be_bytes(encoded[0..4].try_into().expect("len")) as usize;
        let body = &encoded[4..4 + body_len];
        let prefix = &body[..body.len() - 4];
        let stored = u32::from_be_bytes(body[body.len() - 4..].try_into().expect("crc"));
        assert_eq!(stored, crc32c::crc32c(prefix));
    }

    #[test]
    fn corrupt_truncated_and_arbitrary_bytes_do_not_panic() {
        let encoded = encode_frame(&sample_submission()).expect("encode");
        assert!(decode_frame(&encoded[..encoded.len().saturating_sub(3)]).is_err());
        let mut bad = encoded.to_vec();
        if let Some(byte) = bad.last_mut() {
            *byte ^= 0xff;
        }
        assert!(matches!(
            decode_frame(&bad),
            Err(SpoolFrameError::ChecksumMismatch)
        ));
        assert!(decode_frame(&[]).is_err());
        assert!(decode_frame(&[0xff; 64]).is_err());
        assert!(decode_frame(b"not a frame at all!!!!!!").is_err());
    }
}
