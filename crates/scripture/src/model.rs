use std::collections::BTreeMap;
use std::fmt;

use bytes::Bytes;

/// Stable identity of one logical journal, independent of its display name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JournalId([u8; 16]);

impl JournalId {
    /// Constructs an identity from its durable 128-bit representation.
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

impl fmt::Display for JournalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Dense, consumer-visible identity of one record in a journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordOffset(u64);

impl RecordOffset {
    /// Constructs an offset.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the integer representation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn checked_add(self, count: usize) -> Option<Self> {
        u64::try_from(count)
            .ok()
            .and_then(|count| self.0.checked_add(count))
            .map(Self)
    }
}

/// Initially supported typed attribute values.
///
/// Floating-point values are finite IEEE-754 binary64 values. `NaN` and the
/// infinities do not have a useful portable data-model meaning, so the codec
/// rejects them rather than quietly persisting a non-canonical value.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    /// UTF-8 text.
    String(String),
    /// Signed 64-bit integer.
    I64(i64),
    /// Finite IEEE-754 binary64 number.
    F64(f64),
    /// UTC Unix timestamp in microseconds.
    TimestampMicros(i64),
    /// Boolean.
    Bool(bool),
}

/// One application record before or after durable encoding.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// Canonically key-ordered typed attributes.
    pub attributes: BTreeMap<String, AttributeValue>,
    /// Opaque application payload.
    pub payload: Bytes,
}

impl Record {
    /// Creates a record.
    #[must_use]
    pub fn new(
        attributes: impl IntoIterator<Item = (String, AttributeValue)>,
        payload: Bytes,
    ) -> Self {
        Self {
            attributes: attributes.into_iter().collect(),
            payload,
        }
    }
}

/// One decoded record with its stable journal offset.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalRecord {
    /// Dense record offset.
    pub offset: RecordOffset,
    /// Typed attributes.
    pub attributes: BTreeMap<String, AttributeValue>,
    /// Opaque payload.
    pub payload: Bytes,
}

/// Replaceable physical location used only to accelerate resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeHint {
    slot: u64,
    record_index: u32,
}

impl ResumeHint {
    /// Constructs a hint. Readers always validate it against durable bytes.
    #[must_use]
    pub const fn new(slot: u64, record_index: u32) -> Self {
        Self { slot, record_index }
    }

    /// Returns the underlying Holylog slot.
    #[must_use]
    pub const fn slot(self) -> u64 {
        self.slot
    }

    /// Returns the record index inside that slot's batch.
    #[must_use]
    pub const fn record_index(self) -> u32 {
        self.record_index
    }
}

/// Consumer-owned resume value identifying the next record to consume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    /// Journal this checkpoint belongs to.
    pub journal_id: JournalId,
    /// Next record to consume, never the last record already observed.
    pub next_offset: RecordOffset,
    /// Replaceable physical acceleration hint.
    pub resume_hint: Option<ResumeHint>,
}
