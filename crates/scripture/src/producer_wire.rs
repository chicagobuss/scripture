//! Experimental native producer-wire v1 codec.
//!
//! This is deliberately a codec, not a stable public compatibility promise.
//! The experimental runtime listener carries one stable producer identity,
//! epoch, and submission sequence across a reconnect so the existing
//! submission deduplication path can return the *original* receipt.
//!
//! Raw-lines remains a separate, per-connection compatibility ingress. It
//! cannot acquire retry safety merely by sharing these types.

use bytes::Bytes;

use crate::ProducerId;

/// Maximum complete frame body accepted by this codec.
///
/// A Scribe listener may impose a lower assignment-specific cap before it
/// allocates a submission. This absolute cap prevents a malformed network peer
/// from asking the generic decoder for unbounded memory.
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
/// Maximum records in one wire submission.
pub const MAX_RECORDS_PER_SUBMIT: usize = 1024;
/// Maximum UTF-8 diagnostic bytes in an error frame.
pub const MAX_ERROR_MESSAGE_BYTES: usize = 1024;

const MAGIC: [u8; 4] = *b"SPW1";
const TYPE_HELLO: u8 = 1;
const TYPE_SUBMIT: u8 = 2;
const TYPE_ACK: u8 = 3;
const TYPE_ERROR: u8 = 4;
const TYPE_CLOSE: u8 = 5;

/// One bounded machine-readable refusal class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ProducerWireErrorCode {
    /// The addressed Scribe is not the effective writer.
    NotServing = 1,
    /// Admission policy or pending-work budget refused the submission.
    Backpressure = 2,
    /// Identity/sequence is inconsistent with a known durable submission.
    IdentityConflict = 3,
    /// The peer sent a syntactically valid but unsupported request.
    Unsupported = 4,
    /// A valid request has an indeterminate terminal outcome.
    Ambiguous = 5,
}

impl ProducerWireErrorCode {
    fn decode(raw: u8) -> Result<Self, ProducerWireError> {
        match raw {
            1 => Ok(Self::NotServing),
            2 => Ok(Self::Backpressure),
            3 => Ok(Self::IdentityConflict),
            4 => Ok(Self::Unsupported),
            5 => Ok(Self::Ambiguous),
            other => Err(ProducerWireError::UnknownErrorCode(other)),
        }
    }
}

/// A v1 producer-wire message before length framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerWireFrame {
    /// Opens a connection with durable submission identity fields.
    Hello {
        producer_id: ProducerId,
        producer_epoch: u32,
    },
    /// One ordered producer submission. Records are arbitrary bytes.
    Submit { sequence: u64, records: Vec<Bytes> },
    /// The exact committed receipt for one `(epoch, sequence)` submission.
    Ack {
        producer_epoch: u32,
        sequence: u64,
        first_offset: u64,
        next_offset: u64,
    },
    /// A terminal bounded diagnostic for one submission where applicable.
    Error {
        producer_epoch: u32,
        sequence: u64,
        code: ProducerWireErrorCode,
        message: String,
    },
    /// Cleanly ends a connection without a trailing payload.
    Close,
}

/// Codec failures. All failures are terminal for the offending frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerWireError {
    /// The length prefix is absent, over the absolute cap, or does not match.
    InvalidFrameLength,
    /// The message is too short for its header.
    Truncated,
    /// The framing magic/version differs from v1.
    InvalidMagic,
    /// The type byte is not defined by v1.
    UnknownFrameType(u8),
    /// A semantic bound is invalid before encoding or after decoding.
    InvalidField(&'static str),
    /// The error-code byte is unknown.
    UnknownErrorCode(u8),
    /// Error diagnostics must be valid UTF-8.
    InvalidDiagnosticUtf8,
    /// Bytes remain after a successfully decoded message.
    TrailingBytes,
}

impl std::fmt::Display for ProducerWireError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFrameLength => formatter.write_str("invalid producer-wire frame length"),
            Self::Truncated => formatter.write_str("truncated producer-wire frame"),
            Self::InvalidMagic => formatter.write_str("unsupported producer-wire magic/version"),
            Self::UnknownFrameType(kind) => {
                write!(formatter, "unknown producer-wire frame type {kind}")
            }
            Self::InvalidField(field) => write!(formatter, "invalid producer-wire field {field}"),
            Self::UnknownErrorCode(code) => {
                write!(formatter, "unknown producer-wire error code {code}")
            }
            Self::InvalidDiagnosticUtf8 => {
                formatter.write_str("producer-wire diagnostic is not UTF-8")
            }
            Self::TrailingBytes => formatter.write_str("trailing bytes after producer-wire frame"),
        }
    }
}

impl std::error::Error for ProducerWireError {}

/// Encodes exactly one v1 message with its big-endian u32 byte-length prefix.
pub fn encode_producer_wire_frame(frame: &ProducerWireFrame) -> Result<Vec<u8>, ProducerWireError> {
    let mut body = Vec::new();
    body.extend_from_slice(&MAGIC);
    match frame {
        ProducerWireFrame::Hello {
            producer_id,
            producer_epoch,
        } => {
            if *producer_epoch == 0 {
                return Err(ProducerWireError::InvalidField("producer_epoch"));
            }
            body.push(TYPE_HELLO);
            body.extend_from_slice(&producer_id.as_bytes());
            push_u32(&mut body, *producer_epoch);
        }
        ProducerWireFrame::Submit { sequence, records } => {
            if records.is_empty() || records.len() > MAX_RECORDS_PER_SUBMIT {
                return Err(ProducerWireError::InvalidField("record_count"));
            }
            body.push(TYPE_SUBMIT);
            push_u64(&mut body, *sequence);
            push_u32(
                &mut body,
                u32::try_from(records.len())
                    .map_err(|_| ProducerWireError::InvalidField("record_count"))?,
            );
            for record in records {
                push_len_prefixed(&mut body, record)?;
            }
        }
        ProducerWireFrame::Ack {
            producer_epoch,
            sequence,
            first_offset,
            next_offset,
        } => {
            if *producer_epoch == 0 || first_offset >= next_offset {
                return Err(ProducerWireError::InvalidField("ack"));
            }
            body.push(TYPE_ACK);
            push_u32(&mut body, *producer_epoch);
            push_u64(&mut body, *sequence);
            push_u64(&mut body, *first_offset);
            push_u64(&mut body, *next_offset);
        }
        ProducerWireFrame::Error {
            producer_epoch,
            sequence,
            code,
            message,
        } => {
            if *producer_epoch == 0 || message.len() > MAX_ERROR_MESSAGE_BYTES {
                return Err(ProducerWireError::InvalidField("error"));
            }
            body.push(TYPE_ERROR);
            push_u32(&mut body, *producer_epoch);
            push_u64(&mut body, *sequence);
            body.push(*code as u8);
            push_len_prefixed(&mut body, message.as_bytes())?;
        }
        ProducerWireFrame::Close => body.push(TYPE_CLOSE),
    }
    if body.len() > MAX_FRAME_BYTES {
        return Err(ProducerWireError::InvalidFrameLength);
    }
    let body_len = u32::try_from(body.len()).map_err(|_| ProducerWireError::InvalidFrameLength)?;
    let mut encoded = Vec::with_capacity(body.len() + 4);
    push_u32(&mut encoded, body_len);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

/// Decodes exactly one length-prefixed v1 message and rejects trailing bytes.
pub fn decode_producer_wire_frame(bytes: &[u8]) -> Result<ProducerWireFrame, ProducerWireError> {
    if bytes.len() < 4 {
        return Err(ProducerWireError::Truncated);
    }
    let length = u32::from_be_bytes(bytes[..4].try_into().expect("length checked")) as usize;
    if length > MAX_FRAME_BYTES || bytes.len() != length + 4 {
        return Err(ProducerWireError::InvalidFrameLength);
    }
    decode_body(&bytes[4..])
}

fn decode_body(body: &[u8]) -> Result<ProducerWireFrame, ProducerWireError> {
    let mut cursor = Cursor::new(body);
    if cursor.take(4)? != MAGIC {
        return Err(ProducerWireError::InvalidMagic);
    }
    let kind = cursor.u8()?;
    let frame = match kind {
        TYPE_HELLO => {
            let producer_id = ProducerId::from_bytes(cursor.fixed_16()?);
            let producer_epoch = cursor.u32()?;
            if producer_epoch == 0 {
                return Err(ProducerWireError::InvalidField("producer_epoch"));
            }
            ProducerWireFrame::Hello {
                producer_id,
                producer_epoch,
            }
        }
        TYPE_SUBMIT => {
            let sequence = cursor.u64()?;
            let count = cursor.u32()? as usize;
            if count == 0 || count > MAX_RECORDS_PER_SUBMIT {
                return Err(ProducerWireError::InvalidField("record_count"));
            }
            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                records.push(Bytes::copy_from_slice(cursor.bytes()?));
            }
            ProducerWireFrame::Submit { sequence, records }
        }
        TYPE_ACK => {
            let producer_epoch = cursor.u32()?;
            let sequence = cursor.u64()?;
            let first_offset = cursor.u64()?;
            let next_offset = cursor.u64()?;
            if producer_epoch == 0 || first_offset >= next_offset {
                return Err(ProducerWireError::InvalidField("ack"));
            }
            ProducerWireFrame::Ack {
                producer_epoch,
                sequence,
                first_offset,
                next_offset,
            }
        }
        TYPE_ERROR => {
            let producer_epoch = cursor.u32()?;
            let sequence = cursor.u64()?;
            let code = ProducerWireErrorCode::decode(cursor.u8()?)?;
            let diagnostic = cursor.bytes()?;
            if producer_epoch == 0 || diagnostic.len() > MAX_ERROR_MESSAGE_BYTES {
                return Err(ProducerWireError::InvalidField("error"));
            }
            let message = std::str::from_utf8(diagnostic)
                .map_err(|_| ProducerWireError::InvalidDiagnosticUtf8)?
                .to_owned();
            ProducerWireFrame::Error {
                producer_epoch,
                sequence,
                code,
                message,
            }
        }
        TYPE_CLOSE => ProducerWireFrame::Close,
        other => return Err(ProducerWireError::UnknownFrameType(other)),
    };
    if cursor.remaining() != 0 {
        return Err(ProducerWireError::TrailingBytes);
    }
    Ok(frame)
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_len_prefixed(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ProducerWireError> {
    let length = u32::try_from(value.len()).map_err(|_| ProducerWireError::InvalidFrameLength)?;
    push_u32(output, length);
    output.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ProducerWireError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(ProducerWireError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(ProducerWireError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ProducerWireError> {
        Ok(*self.take(1)?.first().expect("one byte requested"))
    }

    fn u32(&mut self) -> Result<u32, ProducerWireError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes requested"),
        ))
    }

    fn u64(&mut self) -> Result<u64, ProducerWireError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes requested"),
        ))
    }

    fn fixed_16(&mut self) -> Result<[u8; 16], ProducerWireError> {
        self.take(16)?
            .try_into()
            .map_err(|_| ProducerWireError::Truncated)
    }

    fn bytes(&mut self) -> Result<&'a [u8], ProducerWireError> {
        let length = self.u32()? as usize;
        if length > MAX_FRAME_BYTES {
            return Err(ProducerWireError::InvalidFrameLength);
        }
        self.take(length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer() -> ProducerId {
        ProducerId::from_bytes([7; 16])
    }

    #[test]
    fn hello_has_stable_golden_bytes() {
        let encoded = encode_producer_wire_frame(&ProducerWireFrame::Hello {
            producer_id: producer(),
            producer_epoch: 3,
        })
        .expect("encode");
        let mut expected = vec![0, 0, 0, 25, b'S', b'P', b'W', b'1', TYPE_HELLO];
        expected.extend_from_slice(&[7; 16]);
        expected.extend_from_slice(&[0, 0, 0, 3]);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn arbitrary_bytes_round_trip_in_submit() {
        let frame = ProducerWireFrame::Submit {
            sequence: 9,
            records: vec![
                Bytes::from_static(b"line\ninside"),
                Bytes::from_static(&[0, 255]),
            ],
        };
        assert_eq!(
            decode_producer_wire_frame(&encode_producer_wire_frame(&frame).expect("encode"))
                .expect("decode"),
            frame
        );
    }

    #[test]
    fn ack_and_error_round_trip() {
        for frame in [
            ProducerWireFrame::Ack {
                producer_epoch: 3,
                sequence: 11,
                first_offset: 20,
                next_offset: 22,
            },
            ProducerWireFrame::Error {
                producer_epoch: 3,
                sequence: 11,
                code: ProducerWireErrorCode::NotServing,
                message: "redirect is advisory".into(),
            },
            ProducerWireFrame::Close,
        ] {
            assert_eq!(
                decode_producer_wire_frame(&encode_producer_wire_frame(&frame).expect("encode"))
                    .expect("decode"),
                frame
            );
        }
    }

    #[test]
    fn rejects_malformed_and_trailing_inputs() {
        assert_eq!(
            decode_producer_wire_frame(&[]),
            Err(ProducerWireError::Truncated)
        );
        let mut valid = encode_producer_wire_frame(&ProducerWireFrame::Close).expect("encode");
        valid.push(0);
        assert_eq!(
            decode_producer_wire_frame(&valid),
            Err(ProducerWireError::InvalidFrameLength)
        );

        let mut body = vec![b'S', b'P', b'W', b'1', TYPE_CLOSE, 0];
        let mut framed = Vec::new();
        push_u32(&mut framed, body.len() as u32);
        framed.append(&mut body);
        assert_eq!(
            decode_producer_wire_frame(&framed),
            Err(ProducerWireError::TrailingBytes)
        );
    }

    #[test]
    fn rejects_nonzero_protocol_bounds_before_allocation() {
        assert_eq!(
            encode_producer_wire_frame(&ProducerWireFrame::Hello {
                producer_id: producer(),
                producer_epoch: 0,
            }),
            Err(ProducerWireError::InvalidField("producer_epoch"))
        );
        assert_eq!(
            encode_producer_wire_frame(&ProducerWireFrame::Submit {
                sequence: 1,
                records: Vec::new(),
            }),
            Err(ProducerWireError::InvalidField("record_count"))
        );
    }
}
