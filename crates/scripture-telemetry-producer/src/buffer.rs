//! Per-Verse bounded buffer with drop-oldest overflow.

use std::collections::VecDeque;

/// Record dropped from the head of a full buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedRecord {
    /// Verse that overflowed.
    pub verse: String,
    /// Sequence of the dropped envelope.
    pub seq: u64,
    /// Payload digest of the dropped line.
    pub payload_digest: String,
}

/// One buffered outbound line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedLine {
    /// Verse lane.
    pub verse: String,
    /// Monotonic seq.
    pub seq: u64,
    /// JSON line (no trailing newline).
    pub line: String,
    /// Blake3 digest of `line`.
    pub payload_digest: String,
}

/// Drop-oldest bounded buffer for one Verse.
#[derive(Debug)]
pub struct DropOldestBuffer {
    verse: String,
    max_records: usize,
    max_bytes: usize,
    records: VecDeque<BufferedLine>,
    bytes: usize,
    /// Cumulative drops.
    pub dropped_records: u64,
}

impl DropOldestBuffer {
    /// Creates an empty buffer for `verse`.
    #[must_use]
    pub fn new(verse: impl Into<String>, max_records: usize, max_bytes: usize) -> Self {
        Self {
            verse: verse.into(),
            max_records,
            max_bytes,
            records: VecDeque::new(),
            bytes: 0,
            dropped_records: 0,
        }
    }

    /// Pushes a line, dropping oldest records until the caps hold.
    pub fn push(&mut self, seq: u64, line: String, payload_digest: String) -> Vec<DroppedRecord> {
        let mut dropped = Vec::new();
        let incoming = BufferedLine {
            verse: self.verse.clone(),
            seq,
            line,
            payload_digest,
        };
        // A single record larger than the byte cap is dropped immediately
        // without evicting the existing buffer.
        if incoming.line.len() > self.max_bytes {
            self.dropped_records += 1;
            dropped.push(DroppedRecord {
                verse: incoming.verse,
                seq: incoming.seq,
                payload_digest: incoming.payload_digest,
            });
            return dropped;
        }
        while self.would_exceed(&incoming) {
            match self.records.pop_front() {
                Some(old) => {
                    self.bytes = self.bytes.saturating_sub(old.line.len());
                    self.dropped_records += 1;
                    dropped.push(DroppedRecord {
                        verse: old.verse,
                        seq: old.seq,
                        payload_digest: old.payload_digest,
                    });
                }
                None => break,
            }
        }
        self.bytes += incoming.line.len();
        self.records.push_back(incoming);
        dropped
    }

    fn would_exceed(&self, incoming: &BufferedLine) -> bool {
        if self.records.len() + 1 > self.max_records {
            return true;
        }
        self.bytes + incoming.line.len() > self.max_bytes
    }

    /// Pops the oldest pending line.
    pub fn pop_front(&mut self) -> Option<BufferedLine> {
        let line = self.records.pop_front()?;
        self.bytes = self.bytes.saturating_sub(line.line.len());
        Some(line)
    }

    /// Peeks at the oldest pending line without removing it.
    #[must_use]
    pub fn front(&self) -> Option<&BufferedLine> {
        self.records.front()
    }

    /// Current pending count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_oldest_on_record_cap() {
        let mut buffer = DropOldestBuffer::new("node-node-a", 2, 10_000);
        assert!(buffer.push(0, "a".into(), "d0".into()).is_empty());
        assert!(buffer.push(1, "b".into(), "d1".into()).is_empty());
        let dropped = buffer.push(2, "c".into(), "d2".into());
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].seq, 0);
        assert_eq!(buffer.dropped_records, 1);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.front().map(|line| line.seq), Some(1));
    }

    #[test]
    fn oversized_record_does_not_evict_buffer() {
        let mut buffer = DropOldestBuffer::new("node-node-a", 10, 4);
        assert!(buffer.push(0, "abcd".into(), "d0".into()).is_empty());
        let dropped = buffer.push(1, "too-big".into(), "d1".into());
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].seq, 1);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.front().map(|line| line.seq), Some(0));
    }
}
