//! Product-facing source identities and delivered ranges.

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Stable consumer-facing Canon identity (logical stream).
///
/// Distinct from Holylog/Scripture's internal `JournalId` substrate naming.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CanonRef(String);

impl CanonRef {
    /// Constructs a Canon reference from a non-empty product id.
    pub fn new(value: impl Into<String>) -> Result<Self, TypeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(TypeError::EmptyIdentity("canon_id"));
        }
        Ok(Self(value))
    }

    /// Returns the durable string form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CanonRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Independent ordered/write-scaling lane inside a Canon.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VerseRef(String);

impl VerseRef {
    /// Constructs a Verse reference from a non-empty product id.
    pub fn new(value: impl Into<String>) -> Result<Self, TypeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(TypeError::EmptyIdentity("verse_id"));
        }
        Ok(Self(value))
    }

    /// Returns the durable string form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VerseRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Dense source offset inside one Canon/Verse lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceOffset(u64);

impl SourceOffset {
    /// Constructs an offset.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Integer form.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SourceOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Schema reference embedded in delivered batches / output commits.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaRef(String);

impl SchemaRef {
    /// Constructs a schema reference.
    pub fn new(value: impl Into<String>) -> Result<Self, TypeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(TypeError::EmptyIdentity("schema_ref"));
        }
        Ok(Self(value))
    }

    /// String form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Workload identity (stable across restarts for a binding).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkloadId(String);

impl WorkloadId {
    /// Constructs a workload id.
    pub fn new(value: impl Into<String>) -> Result<Self, TypeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(TypeError::EmptyIdentity("workload_id"));
        }
        Ok(Self(value))
    }

    /// String form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkloadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Immutable delivered source interval `[first_offset, next_offset)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRange {
    /// Canon identity.
    pub canon_id: CanonRef,
    /// Verse lane identity.
    pub verse_id: VerseRef,
    /// Inclusive start offset.
    pub first_offset: SourceOffset,
    /// Exclusive end offset.
    pub next_offset: SourceOffset,
    /// Schema governing record payloads in this range.
    pub schema_ref: SchemaRef,
    /// Ordered records covering the half-open interval.
    pub records: Vec<CanonRecord>,
}

impl SourceRange {
    /// Validates interval emptiness and record coverage.
    pub fn validate(&self) -> Result<(), TypeError> {
        if self.next_offset.get() < self.first_offset.get() {
            return Err(TypeError::InvalidRange);
        }
        let expected = self
            .next_offset
            .get()
            .saturating_sub(self.first_offset.get());
        if u64::try_from(self.records.len()).unwrap_or(u64::MAX) != expected {
            return Err(TypeError::RecordCountMismatch {
                expected,
                actual: self.records.len(),
            });
        }
        for (index, record) in self.records.iter().enumerate() {
            let want = self.first_offset.get() + u64::try_from(index).unwrap_or(u64::MAX);
            if record.offset.get() != want {
                return Err(TypeError::RecordOffsetMismatch {
                    index,
                    expected: want,
                    actual: record.offset.get(),
                });
            }
        }
        Ok(())
    }

    /// True when the interval contains no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.first_offset == self.next_offset
    }
}

/// One Canon record at a dense source offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonRecord {
    /// Dense offset inside the Verse.
    pub offset: SourceOffset,
    /// Opaque application payload (newline-JSON for the reference materializer).
    pub payload: Bytes,
}

/// Identity / range construction errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TypeError {
    /// Empty product identity string.
    #[error("empty {0}")]
    EmptyIdentity(&'static str),
    /// `next_offset` precedes `first_offset`.
    #[error("invalid source range: next_offset < first_offset")]
    InvalidRange,
    /// Record vector length does not match the half-open interval.
    #[error("record count mismatch: expected {expected}, got {actual}")]
    RecordCountMismatch {
        /// Expected count from offsets.
        expected: u64,
        /// Actual vector length.
        actual: usize,
    },
    /// Record offset does not match its position in the range.
    #[error("record offset mismatch at index {index}: expected {expected}, got {actual}")]
    RecordOffsetMismatch {
        /// Index in the range vector.
        index: usize,
        /// Expected offset.
        expected: u64,
        /// Actual offset.
        actual: u64,
    },
}
