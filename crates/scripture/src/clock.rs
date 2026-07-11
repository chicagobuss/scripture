use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::batch::{CodecError, encoded_batch_len, encoded_record_len};
use crate::model::Record;

/// Monotonic time source used only for batching policy.
pub trait Clock: Send + Sync {
    /// Elapsed monotonic time from an arbitrary process-local origin.
    fn now(&self) -> Duration;
}

impl<T: Clock + ?Sized> Clock for Arc<T> {
    fn now(&self) -> Duration {
        T::now(self)
    }
}

/// Production monotonic clock.
#[derive(Debug)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    /// Starts a monotonic clock at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// Deterministic monotonic clock for tests and simulations.
#[derive(Debug, Default)]
pub struct ManualClock {
    nanos: AtomicU64,
}

impl ManualClock {
    /// Creates a clock at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            nanos: AtomicU64::new(0),
        }
    }

    /// Advances the clock without sleeping.
    pub fn advance(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let _ = self
            .nanos
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(nanos))
            });
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.nanos.load(Ordering::Acquire))
    }
}

/// Upper bounds controlling one pending batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchPolicy {
    /// Maximum records in one batch.
    pub max_records: usize,
    /// Maximum encoded bytes in one batch. One oversized record is allowed so
    /// the caller can report or append it deliberately rather than deadlock.
    pub max_bytes: usize,
    /// Maximum monotonic age before the caller should flush a non-empty batch.
    pub max_age: Duration,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            max_records: 1_000,
            max_bytes: 60 * 1024,
            max_age: Duration::from_millis(100),
        }
    }
}

/// Result of trying to add a record to an accumulator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushResult {
    /// Record was staged. `should_flush` reports whether a bound is now met.
    Accepted { should_flush: bool },
    /// Existing records must be flushed before this record can be accepted.
    FlushFirst(Record),
}

/// Deterministic batching-policy state. It never acknowledges durability;
/// callers pass drained records to `JournalWriter::append_batch`.
pub struct BatchAccumulator<C> {
    policy: BatchPolicy,
    clock: C,
    records: Vec<Record>,
    encoded_length: usize,
    started_at: Option<Duration>,
}

impl<C: Clock> BatchAccumulator<C> {
    /// Creates an empty accumulator.
    #[must_use]
    pub fn new(policy: BatchPolicy, clock: C) -> Self {
        Self {
            policy,
            clock,
            records: Vec::new(),
            encoded_length: encoded_batch_len(&[]).expect("empty batch length is representable"),
            started_at: None,
        }
    }

    /// Attempts to stage one record under the count and exact encoded-byte
    /// bounds. A single oversized record is accepted into an empty batch.
    pub fn push(&mut self, record: Record) -> Result<PushResult, CodecError> {
        let candidate_length = self
            .encoded_length
            .checked_add(8)
            .and_then(|length| length.checked_add(encoded_record_len(&record).ok()?))
            .ok_or(CodecError::Oversized)?;
        let exceeds_records = self.records.len() + 1 > self.policy.max_records;
        let exceeds_bytes = candidate_length > self.policy.max_bytes;
        if !self.records.is_empty() && (exceeds_records || exceeds_bytes) {
            return Ok(PushResult::FlushFirst(record));
        }
        if self.records.is_empty() {
            self.started_at = Some(self.clock.now());
        }
        self.records.push(record);
        self.encoded_length = candidate_length;
        Ok(PushResult::Accepted {
            should_flush: self.records.len() >= self.policy.max_records
                || self.encoded_length >= self.policy.max_bytes,
        })
    }

    /// Returns whether the non-empty batch reached its monotonic age bound.
    #[must_use]
    pub fn is_due(&self) -> bool {
        self.started_at
            .is_some_and(|started| self.clock.now().saturating_sub(started) >= self.policy.max_age)
    }

    /// Returns the number of staged records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns whether no records are staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Drains the pending records for durable append.
    pub fn take(&mut self) -> Vec<Record> {
        self.started_at = None;
        self.encoded_length = encoded_batch_len(&[]).expect("empty batch length is representable");
        std::mem::take(&mut self.records)
    }
}
