use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::task::AtomicWaker;

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
#[derive(Debug, Clone)]
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

    /// Paired clock and timer sharing one origin so `now` and `sleep_until`
    /// speak the same Duration scale.
    #[must_use]
    pub fn pair() -> (Self, SystemTimer) {
        let origin = Instant::now();
        (Self { origin }, SystemTimer { origin })
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

/// A wakeable sleep until a monotonic deadline on the timer's own scale.
///
/// [`Clock::now`] alone cannot drive an age-bound flush deterministically: the
/// actor's `run` future must wake when a deadline is crossed. [`ManualTimer`]
/// shares [`ManualClock`]'s nanos; [`SystemTimer`] is for the application edge.
pub trait Timer: Send + Sync {
    /// Completes at or after `deadline` on this timer's monotonic scale.
    fn sleep_until(&self, deadline: Duration) -> BoxFuture<'static, ()>;
}

impl<T: Timer + ?Sized> Timer for Arc<T> {
    fn sleep_until(&self, deadline: Duration) -> BoxFuture<'static, ()> {
        T::sleep_until(self, deadline)
    }
}

/// Wall-clock timer for the application edge. Spawns a sleeper thread so the
/// core crate stays free of tokio. Prefer [`SystemClock::pair`] so the clock and
/// timer share one origin.
///
/// Cancellation is per sleeper: dropping one sleep future must not affect any
/// other outstanding sleep on the same timer.
#[derive(Debug, Clone)]
pub struct SystemTimer {
    origin: Instant,
}

impl SystemTimer {
    /// Starts a timer whose deadlines are measured from construction.
    ///
    /// Prefer [`SystemClock::pair`] when the same process also uses
    /// [`SystemClock::now`] values as `sleep_until` deadlines.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl Timer for SystemTimer {
    fn sleep_until(&self, deadline: Duration) -> BoxFuture<'static, ()> {
        let origin = self.origin;
        Box::pin(async move {
            let elapsed = origin.elapsed();
            if elapsed >= deadline {
                return;
            }
            let remaining = deadline - elapsed;
            let sleeper = Arc::new(Sleeper {
                done: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                waker: AtomicWaker::new(),
            });
            let thread_sleeper = Arc::clone(&sleeper);
            std::thread::spawn(move || {
                std::thread::sleep(remaining);
                if thread_sleeper.cancelled.load(Ordering::Acquire) {
                    return;
                }
                thread_sleeper.done.store(true, Ordering::Release);
                thread_sleeper.waker.wake();
            });
            SystemSleepUntil { sleeper }.await;
        })
    }
}

struct SystemSleepUntil {
    sleeper: Arc<Sleeper>,
}

impl Future for SystemSleepUntil {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.sleeper.done.load(Ordering::Acquire)
            || self.sleeper.cancelled.load(Ordering::Acquire)
        {
            return Poll::Ready(());
        }
        self.sleeper.waker.register(context.waker());
        if self.sleeper.done.load(Ordering::Acquire)
            || self.sleeper.cancelled.load(Ordering::Acquire)
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for SystemSleepUntil {
    fn drop(&mut self) {
        self.sleeper.cancelled.store(true, Ordering::Release);
        self.sleeper.waker.wake();
    }
}

struct Sleeper {
    done: AtomicBool,
    cancelled: AtomicBool,
    waker: AtomicWaker,
}

impl std::fmt::Debug for Sleeper {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Sleeper")
            .field("done", &self.done.load(Ordering::Relaxed))
            .field("cancelled", &self.cancelled.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ManualTimerInner {
    clock: Arc<ManualClock>,
    sleepers: Mutex<BTreeMap<(u64, u64), Arc<Sleeper>>>,
    next_id: AtomicU64,
}

/// Deterministic timer for tests. [`ManualTimer::advance`] moves the shared
/// [`ManualClock`] and wakes every sleeper whose deadline has passed.
#[derive(Debug, Clone)]
pub struct ManualTimer {
    inner: Arc<ManualTimerInner>,
}

impl ManualTimer {
    /// Pairs a new timer with an existing manual clock (same Duration scale).
    #[must_use]
    pub fn new(clock: Arc<ManualClock>) -> Self {
        Self {
            inner: Arc::new(ManualTimerInner {
                clock,
                sleepers: Mutex::new(BTreeMap::new()),
                next_id: AtomicU64::new(0),
            }),
        }
    }

    /// The shared manual clock this timer advances.
    #[must_use]
    pub fn clock(&self) -> &ManualClock {
        &self.inner.clock
    }

    /// Advances time and wakes sleepers whose deadlines are now due.
    pub fn advance(&self, duration: Duration) {
        self.inner.clock.advance(duration);
        self.wake_due();
    }

    /// Number of registered sleepers that have not yet completed or been
    /// cancelled. Test hook for leak regressions.
    #[must_use]
    pub fn sleeper_count(&self) -> usize {
        self.inner
            .sleepers
            .lock()
            .map(|guard| guard.len())
            .unwrap_or(0)
    }

    fn wake_due(&self) {
        let now = u64::try_from(self.inner.clock.now().as_nanos()).unwrap_or(u64::MAX);
        let Ok(mut sleepers) = self.inner.sleepers.lock() else {
            return;
        };
        while let Some(entry) = sleepers.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let sleeper = entry.remove();
            sleeper.done.store(true, Ordering::Release);
            sleeper.waker.wake();
        }
    }
}

impl Timer for ManualTimer {
    fn sleep_until(&self, deadline: Duration) -> BoxFuture<'static, ()> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let deadline_nanos = u64::try_from(deadline.as_nanos()).unwrap_or(u64::MAX);
            let now = u64::try_from(inner.clock.now().as_nanos()).unwrap_or(u64::MAX);
            if now >= deadline_nanos {
                return;
            }
            let sleeper = Arc::new(Sleeper {
                done: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                waker: AtomicWaker::new(),
            });
            let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
            let key = (deadline_nanos, id);
            {
                let Ok(mut sleepers) = inner.sleepers.lock() else {
                    return;
                };
                sleepers.insert(key, Arc::clone(&sleeper));
            }
            // Re-check after registration so an intervening advance cannot miss us.
            let now = u64::try_from(inner.clock.now().as_nanos()).unwrap_or(u64::MAX);
            if now >= deadline_nanos {
                sleeper.done.store(true, Ordering::Release);
                if let Ok(mut sleepers) = inner.sleepers.lock() {
                    sleepers.remove(&key);
                }
                return;
            }
            ManualSleepUntil {
                inner,
                key,
                sleeper,
            }
            .await;
        })
    }
}

struct ManualSleepUntil {
    inner: Arc<ManualTimerInner>,
    key: (u64, u64),
    sleeper: Arc<Sleeper>,
}

impl Future for ManualSleepUntil {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.sleeper.done.load(Ordering::Acquire)
            || self.sleeper.cancelled.load(Ordering::Acquire)
        {
            return Poll::Ready(());
        }
        self.sleeper.waker.register(context.waker());
        if self.sleeper.done.load(Ordering::Acquire)
            || self.sleeper.cancelled.load(Ordering::Acquire)
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ManualSleepUntil {
    fn drop(&mut self) {
        self.sleeper.cancelled.store(true, Ordering::Release);
        if let Ok(mut sleepers) = self.inner.sleepers.lock() {
            sleepers.remove(&self.key);
        }
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
