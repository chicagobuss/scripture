//! The immutable chunk — Scripture's canonical durable payload (decision 0009).
//!
//! One chunk is exactly one Holylog payload, appended to exactly one AtomicLog
//! slot. There is no descriptor object beside it and no commit flag inside it: a
//! payload cannot record its own commit. A chunk is committed iff Holylog
//! acknowledged its append.
//!
//! # Layout
//!
//! ```text
//! [ header | index | frames | trailer ]
//! ```
//!
//! The index sits **before** the frames so that one speculative range read of the
//! object's first few KiB yields the header and the index together — which
//! journals are present, and where their frames are. A reader can then fetch only
//! the frames it wants. [`decode_index`] is that path, written before range reads
//! exist so that the layout is proved to support them.
//!
//! Per-frame CRCs mean a frame fetched *alone* can be verified alone. That is
//! what makes the range-read future real rather than aspirational.
//!
//! # Canonical encoding
//!
//! Holylog's registers are write-once and single-valued, so a retry must propose
//! **byte-identical** bytes or it is corruption rather than a retry. Encoding is
//! therefore a pure function of its inputs, and a [`SealedChunk`] is immutable:
//! a retry re-sends the same buffer and never re-encodes.

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};

use crate::model::{AttributeValue, JournalId, Record, RecordOffset};

const HEADER_MAGIC: &[u8; 4] = b"SCRC";
const TRAILER_MAGIC: &[u8; 4] = b"SCRE";
const MAJOR_VERSION: u8 = 1;
const MINOR_VERSION: u8 = 0;

/// `magic(4) + major(1) + minor(1) + chunk_id(16) + cohort_id(16) +
/// generation(8) + writer_id(16) + index_offset(4) + index_len(4) +
/// frames_offset(4) + frames_len(4) + frame_count(4) + created_at(8) +
/// index_crc(4)`
const HEADER_LEN: usize = 4 + 1 + 1 + 16 + 16 + 8 + 16 + 4 + 4 + 4 + 4 + 4 + 8 + 4;

/// `journal_id(16) + base_offset(8) + record_count(4) + frame_offset(4) +
/// frame_len(4) + frame_crc(4) + producer_count(4)`, then producer ranges.
const INDEX_ENTRY_FIXED_LEN: usize = 16 + 8 + 4 + 4 + 4 + 4 + 4;

/// `producer_id(16) + epoch(4) + first_seq(8) + last_seq(8)`
const PRODUCER_RANGE_LEN: usize = 16 + 4 + 8 + 8;

/// `index_offset(4) + index_len(4) + magic(4)`
const TRAILER_LEN: usize = 4 + 4 + 4;

const STRING_TAG: u8 = 1;
const I64_TAG: u8 = 2;
const BOOL_TAG: u8 = 3;

/// A cohort: the set of policies that must age, travel, and die together.
///
/// Records may share a chunk only if they share a cohort. Proximity in time is
/// not a cohort — retention class, encryption key/tenant, placement and write
/// quorum, access boundary, and ordering owner must all match, because a chunk
/// is one blob written once to one place under one key with one lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CohortId([u8; 16]);

/// Identity of one chunk, assigned at seal and stable across retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId([u8; 16]);

/// Identity of the owner that sealed a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriterId([u8; 16]);

/// Identity of a producer, stable across its reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProducerId([u8; 16]);

macro_rules! opaque_id {
    ($name:ident, $label:literal) => {
        impl $name {
            #[doc = concat!("Constructs a ", $label, " from its durable representation.")]
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            /// Returns the durable representation.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for byte in self.0 {
                    write!(formatter, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    };
}

opaque_id!(CohortId, "cohort identity");
opaque_id!(ChunkId, "chunk identity");
opaque_id!(WriterId, "writer identity");
opaque_id!(ProducerId, "producer identity");

/// The span of one producer's sequences inside a frame.
///
/// Recorded durably so that a new owner can rebuild the dedup window from the
/// log rather than from memory (decision 0010).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerRange {
    /// The producer.
    pub producer_id: ProducerId,
    /// The producer's incarnation.
    pub producer_epoch: u32,
    /// First sequence contained in the frame.
    pub first_sequence: u64,
    /// Last sequence contained in the frame, inclusive.
    pub last_sequence: u64,
}

/// One index entry: where a journal's records live inside the chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameRef {
    /// The journal these records belong to.
    pub journal_id: JournalId,
    /// Dense offset of the frame's first record.
    pub base_offset: RecordOffset,
    /// Number of records in the frame.
    pub record_count: u32,
    /// Byte offset of the frame within the chunk.
    pub frame_offset: u32,
    /// Byte length of the frame.
    pub frame_len: u32,
    /// CRC32C of the frame's bytes, so the frame can be verified alone.
    pub frame_crc: u32,
    /// Producer sequence spans inside the frame, for dedup-window recovery.
    pub producers: Vec<ProducerRange>,
}

/// One journal's records, before sealing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// The journal these records belong to.
    pub journal_id: JournalId,
    /// Dense offset of the first record.
    pub base_offset: RecordOffset,
    /// The records, in order.
    pub records: Vec<Record>,
    /// Producer sequence spans contained here.
    pub producers: Vec<ProducerRange>,
}

/// A chunk's header fields, fixed at seal time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkHeader {
    /// Identity, stable across retries of the same chunk.
    pub chunk_id: ChunkId,
    /// The cohort every frame in this chunk belongs to.
    pub cohort_id: CohortId,
    /// The VirtualLog generation (spool epoch) that sealed it.
    pub generation: u64,
    /// The fenced owner that sealed it.
    pub writer_id: WriterId,
    /// Seal time. Part of the sealed bytes, so a retry does not change them.
    pub created_at_micros: u64,
}

/// A fully decoded chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Header fields.
    pub header: ChunkHeader,
    /// Frames, in canonical order.
    pub frames: Vec<Frame>,
}

/// Encoded, immutable chunk bytes.
///
/// A retry re-sends `bytes` unchanged. Re-encoding would be a bug: the kernel's
/// write-once register treats differing bytes at one address as corruption, not
/// as a retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedChunk {
    /// Identity, as recorded in the header.
    pub chunk_id: ChunkId,
    /// The sealed bytes. Immutable.
    pub bytes: Bytes,
}

/// Chunk encoding and decoding failures.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ChunkError {
    /// Input ended before a complete value.
    #[error("chunk is truncated")]
    Truncated,
    /// A magic marker was wrong.
    #[error("invalid chunk magic")]
    InvalidMagic,
    /// The major format is not understood.
    #[error("unsupported chunk major version {major}")]
    UnsupportedMajor {
        /// The version seen.
        major: u8,
    },
    /// The minor format is newer than this reader.
    #[error("unsupported chunk minor version {minor}")]
    UnsupportedMinor {
        /// The version seen.
        minor: u8,
    },
    /// The index's checksum did not match its bytes.
    #[error("chunk index is corrupt")]
    CorruptIndex,
    /// A frame's checksum did not match its bytes.
    #[error("chunk frame for journal {journal} is corrupt")]
    CorruptFrame {
        /// The journal whose frame failed.
        journal: JournalId,
    },
    /// Index entries were not in canonical order, or a journal repeated.
    #[error("chunk index is not canonically ordered")]
    NonCanonicalIndex,
    /// A frame's declared region does not lie inside the frame section, or two
    /// frames overlap.
    #[error("chunk frame regions are invalid")]
    InvalidFrameRegions,
    /// An attribute type tag is not understood.
    #[error("unsupported attribute type {tag}")]
    UnsupportedAttributeType {
        /// The tag seen.
        tag: u8,
    },
    /// UTF-8 text was malformed.
    #[error("chunk text is not valid UTF-8")]
    InvalidUtf8,
    /// A boolean was not encoded canonically.
    #[error("boolean attribute has invalid value {value}")]
    InvalidBool {
        /// The byte seen.
        value: u8,
    },
    /// Attribute keys were duplicated or not canonically ordered.
    #[error("attribute keys are not unique and canonically ordered")]
    NonCanonicalAttributes,
    /// A chunk with no frames carries no information and is not a valid value.
    #[error("chunk contains no frames")]
    EmptyChunk,
    /// A length or count exceeds the format's framing.
    #[error("chunk component exceeds format limits")]
    Oversized,
    /// Record offsets overflowed.
    #[error("record offset space is exhausted")]
    OffsetOverflow,
    /// Bytes remained after the declared value.
    #[error("chunk has trailing or misplaced bytes")]
    TrailingBytes,
    /// A frame declared a producer range that is not increasing.
    #[error("producer sequence range is not increasing")]
    InvalidProducerRange,
    /// Phase 1 forbids co-packing: see decision 0009's gate.
    #[error("co-packing is not permitted: this chunk has {frames} frames")]
    CoPackingForbidden {
        /// How many frames were offered.
        frames: usize,
    },
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn u32_len(value: usize) -> Result<u32, ChunkError> {
    u32::try_from(value).map_err(|_| ChunkError::Oversized)
}

fn put_len_bytes(buffer: &mut BytesMut, value: &[u8]) -> Result<(), ChunkError> {
    buffer.put_u32(u32_len(value.len())?);
    buffer.put_slice(value);
    Ok(())
}

/// Encodes one frame's records. Pure; the frame's bytes depend only on its
/// records.
fn encode_frame_records(records: &[Record]) -> Result<BytesMut, ChunkError> {
    let mut out = BytesMut::new();
    out.put_u32(u32_len(records.len())?);
    for record in records {
        out.put_u32(u32_len(record.attributes.len())?);
        // BTreeMap iterates in key order, which is the canonical order.
        for (key, value) in &record.attributes {
            put_len_bytes(&mut out, key.as_bytes())?;
            match value {
                AttributeValue::String(text) => {
                    out.put_u8(STRING_TAG);
                    put_len_bytes(&mut out, text.as_bytes())?;
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
        put_len_bytes(&mut out, &record.payload)?;
    }
    Ok(out)
}

/// Returns the exact encoded size of a chunk with these frames, without
/// encoding it.
///
/// The accumulator needs this to decide whether one more record would breach
/// `max_chunk_bytes` — a decision it must make *before* sealing.
pub fn encoded_chunk_len(frames: &[Frame]) -> Result<usize, ChunkError> {
    let mut index_len = 0_usize;
    let mut frames_len = 0_usize;
    for frame in frames {
        index_len = index_len
            .checked_add(INDEX_ENTRY_FIXED_LEN)
            .and_then(|len| len.checked_add(frame.producers.len().checked_mul(PRODUCER_RANGE_LEN)?))
            .ok_or(ChunkError::Oversized)?;
        frames_len = frames_len
            .checked_add(encode_frame_records(&frame.records)?.len())
            .ok_or(ChunkError::Oversized)?;
    }
    HEADER_LEN
        .checked_add(index_len)
        .and_then(|len| len.checked_add(frames_len))
        .and_then(|len| len.checked_add(TRAILER_LEN))
        .ok_or(ChunkError::Oversized)
}

/// Seals frames into immutable chunk bytes.
///
/// The result is a pure function of its inputs, including `header`'s
/// `chunk_id` and `created_at_micros` — which is what allows a retry to resend
/// the identical buffer.
pub fn seal_chunk(header: ChunkHeader, mut frames: Vec<Frame>) -> Result<SealedChunk, ChunkError> {
    if frames.is_empty() {
        return Err(ChunkError::EmptyChunk);
    }
    // Canonical order. Sorting here (rather than requiring the caller to) means
    // the same logical chunk always encodes to the same bytes.
    frames.sort_by(|left, right| {
        (left.journal_id, left.base_offset).cmp(&(right.journal_id, right.base_offset))
    });
    for pair in frames.windows(2) {
        if pair[0].journal_id == pair[1].journal_id {
            return Err(ChunkError::NonCanonicalIndex);
        }
    }
    for frame in &frames {
        for producer in &frame.producers {
            if producer.first_sequence > producer.last_sequence {
                return Err(ChunkError::InvalidProducerRange);
            }
        }
        frame
            .base_offset
            .checked_add(frame.records.len())
            .ok_or(ChunkError::OffsetOverflow)?;
    }

    // Two-pass: encode the frame bodies first so the index can carry their real
    // offsets, then lay the index down ahead of them.
    let bodies = frames
        .iter()
        .map(|frame| encode_frame_records(&frame.records))
        .collect::<Result<Vec<_>, _>>()?;

    let index_len: usize = frames
        .iter()
        .map(|frame| INDEX_ENTRY_FIXED_LEN + frame.producers.len() * PRODUCER_RANGE_LEN)
        .sum();
    let index_offset = HEADER_LEN;
    let frames_offset = index_offset
        .checked_add(index_len)
        .ok_or(ChunkError::Oversized)?;

    let mut index = BytesMut::with_capacity(index_len);
    let mut cursor = frames_offset;
    for (frame, body) in frames.iter().zip(&bodies) {
        index.put_slice(&frame.journal_id.as_bytes());
        index.put_u64(frame.base_offset.get());
        index.put_u32(u32_len(frame.records.len())?);
        index.put_u32(u32_len(cursor)?);
        index.put_u32(u32_len(body.len())?);
        index.put_u32(crc32c(body));
        index.put_u32(u32_len(frame.producers.len())?);
        for producer in &frame.producers {
            index.put_slice(&producer.producer_id.as_bytes());
            index.put_u32(producer.producer_epoch);
            index.put_u64(producer.first_sequence);
            index.put_u64(producer.last_sequence);
        }
        cursor = cursor
            .checked_add(body.len())
            .ok_or(ChunkError::Oversized)?;
    }
    let frames_len = cursor
        .checked_sub(frames_offset)
        .ok_or(ChunkError::Oversized)?;

    let mut out = BytesMut::with_capacity(frames_offset + frames_len + TRAILER_LEN);
    out.put_slice(HEADER_MAGIC);
    out.put_u8(MAJOR_VERSION);
    out.put_u8(MINOR_VERSION);
    out.put_slice(&header.chunk_id.as_bytes());
    out.put_slice(&header.cohort_id.as_bytes());
    out.put_u64(header.generation);
    out.put_slice(&header.writer_id.as_bytes());
    out.put_u32(u32_len(index_offset)?);
    out.put_u32(u32_len(index_len)?);
    out.put_u32(u32_len(frames_offset)?);
    out.put_u32(u32_len(frames_len)?);
    out.put_u32(u32_len(frames.len())?);
    out.put_u64(header.created_at_micros);
    out.put_u32(crc32c(&index));
    debug_assert_eq!(out.len(), HEADER_LEN);

    out.put_slice(&index);
    for body in &bodies {
        out.put_slice(body);
    }
    out.put_u32(u32_len(index_offset)?);
    out.put_u32(u32_len(index_len)?);
    out.put_slice(TRAILER_MAGIC);

    Ok(SealedChunk {
        chunk_id: header.chunk_id,
        bytes: out.freeze(),
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ChunkError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(ChunkError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(ChunkError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ChunkError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ChunkError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| ChunkError::Truncated)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, ChunkError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| ChunkError::Truncated)?,
        ))
    }

    fn id(&mut self) -> Result<[u8; 16], ChunkError> {
        self.take(16)?.try_into().map_err(|_| ChunkError::Truncated)
    }

    fn len_bytes(&mut self) -> Result<&'a [u8], ChunkError> {
        let length = usize::try_from(self.u32()?).map_err(|_| ChunkError::Oversized)?;
        self.take(length)
    }
}

/// The header plus the index — everything needed to decide which frames to
/// fetch, and where they are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkIndex {
    /// Header fields.
    pub header: ChunkHeader,
    /// One entry per frame, in canonical order.
    pub frames: Vec<FrameRef>,
}

impl ChunkIndex {
    /// Returns the entry for `journal`, if this chunk carries one.
    #[must_use]
    pub fn frame_for(&self, journal: JournalId) -> Option<&FrameRef> {
        self.frames.iter().find(|frame| frame.journal_id == journal)
    }
}

/// Decodes the header and index from the **prefix** of a chunk.
///
/// This is the range-read path: `prefix` need only contain the header and the
/// index, not the frames. It is written now, before range reads exist, because a
/// layout that claims to support them should have to prove it.
pub fn decode_index(prefix: &[u8]) -> Result<ChunkIndex, ChunkError> {
    let mut cursor = Cursor::new(prefix);
    if cursor.take(4)? != HEADER_MAGIC {
        return Err(ChunkError::InvalidMagic);
    }
    let major = cursor.u8()?;
    if major != MAJOR_VERSION {
        return Err(ChunkError::UnsupportedMajor { major });
    }
    let minor = cursor.u8()?;
    if minor > MINOR_VERSION {
        return Err(ChunkError::UnsupportedMinor { minor });
    }
    let chunk_id = ChunkId::from_bytes(cursor.id()?);
    let cohort_id = CohortId::from_bytes(cursor.id()?);
    let generation = cursor.u64()?;
    let writer_id = WriterId::from_bytes(cursor.id()?);
    let index_offset = usize::try_from(cursor.u32()?).map_err(|_| ChunkError::Oversized)?;
    let index_len = usize::try_from(cursor.u32()?).map_err(|_| ChunkError::Oversized)?;
    let frames_offset = usize::try_from(cursor.u32()?).map_err(|_| ChunkError::Oversized)?;
    let frames_len = usize::try_from(cursor.u32()?).map_err(|_| ChunkError::Oversized)?;
    let frame_count = usize::try_from(cursor.u32()?).map_err(|_| ChunkError::Oversized)?;
    let created_at_micros = cursor.u64()?;
    let index_crc = cursor.u32()?;

    if index_offset != HEADER_LEN {
        return Err(ChunkError::CorruptIndex);
    }
    let index_end = index_offset
        .checked_add(index_len)
        .ok_or(ChunkError::Oversized)?;
    if frames_offset != index_end {
        return Err(ChunkError::CorruptIndex);
    }
    let index_bytes = prefix
        .get(index_offset..index_end)
        .ok_or(ChunkError::Truncated)?;
    if crc32c(index_bytes) != index_crc {
        return Err(ChunkError::CorruptIndex);
    }

    let frames_end = frames_offset
        .checked_add(frames_len)
        .ok_or(ChunkError::Oversized)?;
    let mut index = Cursor::new(index_bytes);
    let mut frames = Vec::with_capacity(frame_count.min(1024));
    let mut previous: Option<(JournalId, RecordOffset)> = None;
    let mut region_end = frames_offset;

    for _ in 0..frame_count {
        let journal_id = JournalId::from_bytes(index.id()?);
        let base_offset = RecordOffset::new(index.u64()?);
        let record_count = index.u32()?;
        let frame_offset = index.u32()?;
        let frame_len = index.u32()?;
        let frame_crc = index.u32()?;
        let producer_count = usize::try_from(index.u32()?).map_err(|_| ChunkError::Oversized)?;

        // Canonical order, and no journal twice.
        if previous.is_some_and(|last| last >= (journal_id, base_offset)) {
            return Err(ChunkError::NonCanonicalIndex);
        }
        previous = Some((journal_id, base_offset));

        // Frames must tile the frame section in order, without gaps or overlap.
        let offset = usize::try_from(frame_offset).map_err(|_| ChunkError::Oversized)?;
        let length = usize::try_from(frame_len).map_err(|_| ChunkError::Oversized)?;
        let end = offset.checked_add(length).ok_or(ChunkError::Oversized)?;
        if offset != region_end || end > frames_end {
            return Err(ChunkError::InvalidFrameRegions);
        }
        region_end = end;

        let mut producers = Vec::with_capacity(producer_count.min(1024));
        for _ in 0..producer_count {
            let producer_id = ProducerId::from_bytes(index.id()?);
            let producer_epoch = index.u32()?;
            let first_sequence = index.u64()?;
            let last_sequence = index.u64()?;
            if first_sequence > last_sequence {
                return Err(ChunkError::InvalidProducerRange);
            }
            producers.push(ProducerRange {
                producer_id,
                producer_epoch,
                first_sequence,
                last_sequence,
            });
        }

        frames.push(FrameRef {
            journal_id,
            base_offset,
            record_count,
            frame_offset,
            frame_len,
            frame_crc,
            producers,
        });
    }

    if index.position != index_bytes.len() {
        return Err(ChunkError::CorruptIndex);
    }
    if region_end != frames_end {
        return Err(ChunkError::InvalidFrameRegions);
    }
    if frames.is_empty() {
        return Err(ChunkError::EmptyChunk);
    }

    Ok(ChunkIndex {
        header: ChunkHeader {
            chunk_id,
            cohort_id,
            generation,
            writer_id,
            created_at_micros,
        },
        frames,
    })
}

/// Decodes one frame's records from its bytes, verifying its CRC.
///
/// `frame_bytes` is exactly the region named by `entry`. A reader that fetched
/// only this range can call this without possessing the rest of the chunk —
/// which is the point of the per-frame CRC.
pub fn decode_frame(entry: &FrameRef, frame_bytes: &[u8]) -> Result<Vec<Record>, ChunkError> {
    if crc32c(frame_bytes) != entry.frame_crc {
        return Err(ChunkError::CorruptFrame {
            journal: entry.journal_id,
        });
    }
    let mut cursor = Cursor::new(frame_bytes);
    let record_count = usize::try_from(cursor.u32()?).map_err(|_| ChunkError::Oversized)?;
    if record_count != entry.record_count as usize {
        return Err(ChunkError::CorruptFrame {
            journal: entry.journal_id,
        });
    }

    let mut records = Vec::with_capacity(record_count.min(4096));
    for _ in 0..record_count {
        let attribute_count = usize::try_from(cursor.u32()?).map_err(|_| ChunkError::Oversized)?;
        let mut attributes = BTreeMap::new();
        let mut previous_key: Option<String> = None;
        for _ in 0..attribute_count {
            let key = std::str::from_utf8(cursor.len_bytes()?)
                .map_err(|_| ChunkError::InvalidUtf8)?
                .to_owned();
            if previous_key.as_ref().is_some_and(|last| last >= &key) {
                return Err(ChunkError::NonCanonicalAttributes);
            }
            previous_key = Some(key.clone());
            let tag = cursor.u8()?;
            let value_bytes = cursor.len_bytes()?;
            let value = match tag {
                STRING_TAG => AttributeValue::String(
                    std::str::from_utf8(value_bytes)
                        .map_err(|_| ChunkError::InvalidUtf8)?
                        .to_owned(),
                ),
                I64_TAG => AttributeValue::I64(i64::from_be_bytes(
                    value_bytes.try_into().map_err(|_| ChunkError::Truncated)?,
                )),
                BOOL_TAG => match value_bytes {
                    [0] => AttributeValue::Bool(false),
                    [1] => AttributeValue::Bool(true),
                    [value] => return Err(ChunkError::InvalidBool { value: *value }),
                    _ => return Err(ChunkError::Truncated),
                },
                tag => return Err(ChunkError::UnsupportedAttributeType { tag }),
            };
            if attributes.insert(key, value).is_some() {
                return Err(ChunkError::NonCanonicalAttributes);
            }
        }
        let payload = Bytes::copy_from_slice(cursor.len_bytes()?);
        records.push(Record {
            attributes,
            payload,
        });
    }

    if cursor.position != frame_bytes.len() {
        return Err(ChunkError::TrailingBytes);
    }
    Ok(records)
}

/// Decodes and fully validates a complete chunk.
pub fn decode_chunk(bytes: &Bytes) -> Result<Chunk, ChunkError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(ChunkError::Truncated);
    }
    let trailer_start = bytes.len() - TRAILER_LEN;
    let mut trailer = Cursor::new(&bytes[trailer_start..]);
    let trailer_index_offset = trailer.u32()?;
    let trailer_index_len = trailer.u32()?;
    if trailer.take(4)? != TRAILER_MAGIC {
        return Err(ChunkError::InvalidMagic);
    }

    let index = decode_index(bytes)?;

    // The trailer must agree with the header; a disagreement is corruption, not
    // a preference.
    let index_len: usize = index
        .frames
        .iter()
        .map(|frame| INDEX_ENTRY_FIXED_LEN + frame.producers.len() * PRODUCER_RANGE_LEN)
        .sum();
    if trailer_index_offset as usize != HEADER_LEN || trailer_index_len as usize != index_len {
        return Err(ChunkError::CorruptIndex);
    }

    let frames_end = index
        .frames
        .last()
        .map(|frame| frame.frame_offset as usize + frame.frame_len as usize)
        .ok_or(ChunkError::EmptyChunk)?;
    if frames_end != trailer_start {
        return Err(ChunkError::TrailingBytes);
    }

    let mut frames = Vec::with_capacity(index.frames.len());
    for entry in &index.frames {
        let start = entry.frame_offset as usize;
        let end = start + entry.frame_len as usize;
        let frame_bytes = bytes.get(start..end).ok_or(ChunkError::Truncated)?;
        let records = decode_frame(entry, frame_bytes)?;
        frames.push(Frame {
            journal_id: entry.journal_id,
            base_offset: entry.base_offset,
            records,
            producers: entry.producers.clone(),
        });
    }

    Ok(Chunk {
        header: index.header,
        frames,
    })
}
