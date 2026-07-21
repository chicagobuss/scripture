//! Read-only Canon/Verse committed-history source.
//!
//! This adapter turns durable history into bounded [`SourceRange`] values. It is
//! structurally read-only: no register CAS, reconfiguration, provisioning, or
//! repair path is exposed.
//!
//! Locating offset `a` without an index may scan from a generation base — named
//! as a future cost, not solved here.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::types::{
    CanonRecord, CanonRef, SchemaRef, SourceOffset, SourceRange, TypeError, VerseRef,
};

/// Errors from a read-only Canon history source.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CanonSourceError {
    /// Requested start is below the trim point (decision 0006 vocabulary).
    #[error("trim gap: requested first_offset {requested} but trim frontier is {trim_frontier}")]
    TrimGap {
        /// Requested first offset.
        requested: u64,
        /// Exclusive trim frontier (offsets `< trim` are gone).
        trim_frontier: u64,
    },
    /// Offset continuity failed (including across seal-and-replace).
    #[error("continuity gap at offset {offset}: {detail}")]
    ContinuityGap {
        /// First missing / discontinuous offset.
        offset: u64,
        /// Detail without payload secrets.
        detail: String,
    },
    /// Cohort frame filtering found a broken per-Verse offset chain.
    #[error("cohort offset-chain broken for verse {verse}: {detail}")]
    CohortChain {
        /// Verse being filtered.
        verse: String,
        /// Detail.
        detail: String,
    },
    /// Identity / range construction error.
    #[error(transparent)]
    Type(#[from] TypeError),
    /// Unsupported / unknown Verse.
    #[error("unknown verse `{0}`")]
    UnknownVerse(String),
}

/// Read-only port: committed Holylog/Scripture history → [`SourceRange`].
///
/// Implementations must be incapable of mutation — not merely well-behaved.
pub trait CanonHistorySource: Send + Sync {
    /// Reads the half-open Verse interval `[first_offset, next_offset)`.
    ///
    /// Rules (red-team Q1):
    /// 1. Only committed, mapped slots — durable-but-unmapped chunks are unreachable.
    /// 2. Continuity fails closed across generation boundaries.
    /// 3. Trim gaps fail closed (never silently start later).
    /// 4. Cohort filtering validates per-Verse offset chains.
    fn read_range(
        &self,
        canon_id: &CanonRef,
        verse_id: &VerseRef,
        first_offset: SourceOffset,
        next_offset: SourceOffset,
        schema_ref: &SchemaRef,
    ) -> Result<SourceRange, CanonSourceError>;
}

/// One durable chunk that may co-pack multiple Verses (cohort).
#[derive(Debug, Clone)]
pub struct CohortChunk {
    /// Chunk identity (evidence only).
    pub chunk_id: String,
    /// Whether this chunk is mapped into committed history.
    pub mapped: bool,
    /// Per-Verse declared half-open ranges inside this chunk, with payloads.
    pub verses: BTreeMap<String, CohortVerseFrame>,
}

/// One Verse frame inside a cohort chunk.
#[derive(Debug, Clone)]
pub struct CohortVerseFrame {
    /// Declared base offset for this Verse in the chunk.
    pub base_offset: u64,
    /// Ordered payloads for `[base, base+len)`.
    pub payloads: Vec<Bytes>,
}

impl CohortVerseFrame {
    /// Exclusive end offset.
    pub fn try_next_offset(&self) -> Result<u64, CanonSourceError> {
        self.base_offset
            .checked_add(u64::try_from(self.payloads.len()).unwrap_or(u64::MAX))
            .ok_or(CanonSourceError::ContinuityGap {
                offset: self.base_offset,
                detail: "cohort verse frame offset overflow".into(),
            })
    }

    /// Exclusive end offset for tests and simple callers with known-small frames.
    #[must_use]
    pub fn next_offset(&self) -> u64 {
        self.try_next_offset().unwrap_or(u64::MAX)
    }
}

/// Deterministic in-memory Canon history for adapter proofs.
///
/// Not a durability claim — a model of committed / unmapped / trimmed state.
#[derive(Debug, Clone)]
pub struct MemoryCanonSource {
    canon_id: CanonRef,
    /// Per-verse trim frontier (offsets strictly below are trimmed).
    trim_frontier: BTreeMap<String, u64>,
    /// Mapped committed records: (verse, offset) → payload.
    committed: BTreeMap<(String, u64), Bytes>,
    /// Durable but unmapped records — structurally unreachable via [`read_range`].
    unmapped: BTreeMap<(String, u64), Bytes>,
    /// Optional cohort inventory used when validating multi-Verse packing.
    cohorts: Vec<CohortChunk>,
}

impl MemoryCanonSource {
    /// Empty source for `canon_id`.
    #[must_use]
    pub fn new(canon_id: CanonRef) -> Self {
        Self {
            canon_id,
            trim_frontier: BTreeMap::new(),
            committed: BTreeMap::new(),
            unmapped: BTreeMap::new(),
            cohorts: Vec::new(),
        }
    }

    /// Sets the trim frontier for a Verse (exclusive: offsets `< frontier` are gone).
    pub fn set_trim(&mut self, verse_id: &VerseRef, frontier: u64) {
        self.trim_frontier
            .insert(verse_id.as_str().to_owned(), frontier);
    }

    /// Appends a mapped committed record.
    pub fn commit(&mut self, verse_id: &VerseRef, offset: u64, payload: impl Into<Bytes>) {
        self.committed
            .insert((verse_id.as_str().to_owned(), offset), payload.into());
    }

    /// Records a durable-but-unmapped slot (must never surface in reads).
    pub fn commit_unmapped(&mut self, verse_id: &VerseRef, offset: u64, payload: impl Into<Bytes>) {
        self.unmapped
            .insert((verse_id.as_str().to_owned(), offset), payload.into());
    }

    /// Registers a cohort chunk for per-Verse chain validation.
    pub fn add_cohort(&mut self, chunk: CohortChunk) {
        if chunk.mapped {
            for (verse, frame) in &chunk.verses {
                for (index, payload) in frame.payloads.iter().enumerate() {
                    let offset = frame.base_offset + u64::try_from(index).unwrap_or(u64::MAX);
                    self.committed
                        .entry((verse.clone(), offset))
                        .or_insert_with(|| payload.clone());
                }
            }
        }
        self.cohorts.push(chunk);
    }

    fn validate_cohort_chains(&self, verse_id: &VerseRef) -> Result<(), CanonSourceError> {
        let mut expected_next: Option<u64> = None;
        for chunk in &self.cohorts {
            let Some(frame) = chunk.verses.get(verse_id.as_str()) else {
                continue;
            };
            if !chunk.mapped {
                // Unmapped cohort frames must not participate in the chain.
                continue;
            }
            match expected_next {
                None => expected_next = Some(frame.try_next_offset()?),
                Some(want) => {
                    if frame.base_offset != want {
                        return Err(CanonSourceError::CohortChain {
                            verse: verse_id.as_str().to_owned(),
                            detail: format!(
                                "chunk {} base {} != expected {}",
                                chunk.chunk_id, frame.base_offset, want
                            ),
                        });
                    }
                    expected_next = Some(frame.try_next_offset()?);
                }
            }
        }
        Ok(())
    }
}

impl CanonHistorySource for MemoryCanonSource {
    fn read_range(
        &self,
        canon_id: &CanonRef,
        verse_id: &VerseRef,
        first_offset: SourceOffset,
        next_offset: SourceOffset,
        schema_ref: &SchemaRef,
    ) -> Result<SourceRange, CanonSourceError> {
        if canon_id != &self.canon_id {
            return Err(CanonSourceError::UnknownVerse(format!(
                "canon mismatch: requested {} have {}",
                canon_id.as_str(),
                self.canon_id.as_str()
            )));
        }
        if next_offset.get() < first_offset.get() {
            return Err(TypeError::InvalidRange.into());
        }

        let trim = self
            .trim_frontier
            .get(verse_id.as_str())
            .copied()
            .unwrap_or(0);
        if first_offset.get() < trim {
            return Err(CanonSourceError::TrimGap {
                requested: first_offset.get(),
                trim_frontier: trim,
            });
        }

        // Cohort filtering must verify the Verse's declared chains, not just drop frames.
        self.validate_cohort_chains(verse_id)?;

        let mut records = Vec::new();
        let mut cursor = first_offset.get();
        while cursor < next_offset.get() {
            let key = (verse_id.as_str().to_owned(), cursor);
            if self.unmapped.contains_key(&key) {
                return Err(CanonSourceError::ContinuityGap {
                    offset: cursor,
                    detail: "offset is durable but unmapped (cutover); refused".into(),
                });
            }
            let Some(payload) = self.committed.get(&key) else {
                return Err(CanonSourceError::ContinuityGap {
                    offset: cursor,
                    detail: "missing committed mapped record".into(),
                });
            };
            records.push(CanonRecord {
                offset: SourceOffset::new(cursor),
                payload: payload.clone(),
            });
            cursor = cursor.saturating_add(1);
        }

        let range = SourceRange {
            canon_id: canon_id.clone(),
            verse_id: verse_id.clone(),
            first_offset,
            next_offset,
            schema_ref: schema_ref.clone(),
            records,
        };
        range.validate()?;
        Ok(range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canon() -> CanonRef {
        CanonRef::new("telemetry").expect("canon")
    }
    fn verse() -> VerseRef {
        VerseRef::new("host-metrics").expect("verse")
    }
    fn schema() -> SchemaRef {
        SchemaRef::new("otel-shaped-metrics.v1").expect("schema")
    }

    #[test]
    fn same_generation_continuity() {
        let mut src = MemoryCanonSource::new(canon());
        src.commit(&verse(), 0, Bytes::from_static(b"a"));
        src.commit(&verse(), 1, Bytes::from_static(b"b"));
        src.commit(&verse(), 2, Bytes::from_static(b"c"));
        let range = src
            .read_range(
                &canon(),
                &verse(),
                SourceOffset::new(0),
                SourceOffset::new(3),
                &schema(),
            )
            .expect("read");
        assert_eq!(range.records.len(), 3);
        assert_eq!(range.records[1].payload.as_ref(), b"b");
    }

    #[test]
    fn seal_and_replace_boundary_continuity() {
        let mut src = MemoryCanonSource::new(canon());
        // Generation 0: offsets 0..2, then seal/replace continues at 2.
        src.commit(&verse(), 0, Bytes::from_static(b"g0-0"));
        src.commit(&verse(), 1, Bytes::from_static(b"g0-1"));
        src.commit(&verse(), 2, Bytes::from_static(b"g1-0"));
        src.commit(&verse(), 3, Bytes::from_static(b"g1-1"));
        let range = src
            .read_range(
                &canon(),
                &verse(),
                SourceOffset::new(1),
                SourceOffset::new(4),
                &schema(),
            )
            .expect("cross-generation");
        assert_eq!(range.records.len(), 3);
        assert_eq!(range.first_offset.get(), 1);
        assert_eq!(range.next_offset.get(), 4);
    }

    #[test]
    fn durable_but_unmapped_chunk_exclusion() {
        let mut src = MemoryCanonSource::new(canon());
        src.commit(&verse(), 0, Bytes::from_static(b"mapped"));
        // Slot 1 is durable in the store past cutover but unmapped.
        src.commit_unmapped(&verse(), 1, Bytes::from_static(b"garbage"));
        src.commit(&verse(), 2, Bytes::from_static(b"after"));
        let err = src
            .read_range(
                &canon(),
                &verse(),
                SourceOffset::new(0),
                SourceOffset::new(2),
                &schema(),
            )
            .expect_err("unmapped must fail closed");
        assert!(matches!(
            err,
            CanonSourceError::ContinuityGap { offset: 1, .. }
        ));
        // Reading only mapped prefix still works.
        let ok = src
            .read_range(
                &canon(),
                &verse(),
                SourceOffset::new(0),
                SourceOffset::new(1),
                &schema(),
            )
            .expect("mapped prefix");
        assert_eq!(ok.records.len(), 1);
    }

    #[test]
    fn trim_gap_fail_closed() {
        let mut src = MemoryCanonSource::new(canon());
        src.set_trim(&verse(), 5);
        src.commit(&verse(), 5, Bytes::from_static(b"ok"));
        let err = src
            .read_range(
                &canon(),
                &verse(),
                SourceOffset::new(3),
                SourceOffset::new(6),
                &schema(),
            )
            .expect_err("trim");
        assert!(matches!(
            err,
            CanonSourceError::TrimGap {
                requested: 3,
                trim_frontier: 5
            }
        ));
    }

    #[test]
    fn cohort_foreign_verse_filtering_checks_offset_chain() {
        let mut src = MemoryCanonSource::new(canon());
        let other = VerseRef::new("other").expect("verse");
        src.add_cohort(CohortChunk {
            chunk_id: "c1".into(),
            mapped: true,
            verses: BTreeMap::from([
                (
                    verse().as_str().to_owned(),
                    CohortVerseFrame {
                        base_offset: 0,
                        payloads: vec![Bytes::from_static(b"v0"), Bytes::from_static(b"v1")],
                    },
                ),
                (
                    other.as_str().to_owned(),
                    CohortVerseFrame {
                        base_offset: 10,
                        payloads: vec![Bytes::from_static(b"o0")],
                    },
                ),
            ]),
        });
        src.add_cohort(CohortChunk {
            chunk_id: "c2".into(),
            mapped: true,
            verses: BTreeMap::from([(
                verse().as_str().to_owned(),
                CohortVerseFrame {
                    base_offset: 2,
                    payloads: vec![Bytes::from_static(b"v2")],
                },
            )]),
        });
        let range = src
            .read_range(
                &canon(),
                &verse(),
                SourceOffset::new(0),
                SourceOffset::new(3),
                &schema(),
            )
            .expect("chained");
        assert_eq!(range.records.len(), 3);

        // Break the chain: next chunk claims base 9 instead of 3.
        let mut broken = MemoryCanonSource::new(canon());
        broken.add_cohort(CohortChunk {
            chunk_id: "c1".into(),
            mapped: true,
            verses: BTreeMap::from([(
                verse().as_str().to_owned(),
                CohortVerseFrame {
                    base_offset: 0,
                    payloads: vec![Bytes::from_static(b"v0")],
                },
            )]),
        });
        broken.add_cohort(CohortChunk {
            chunk_id: "c2".into(),
            mapped: true,
            verses: BTreeMap::from([(
                verse().as_str().to_owned(),
                CohortVerseFrame {
                    base_offset: 9,
                    payloads: vec![Bytes::from_static(b"gap")],
                },
            )]),
        });
        let err = broken
            .read_range(
                &canon(),
                &verse(),
                SourceOffset::new(0),
                SourceOffset::new(1),
                &schema(),
            )
            .expect_err("broken chain");
        assert!(matches!(err, CanonSourceError::CohortChain { .. }));
    }
}
