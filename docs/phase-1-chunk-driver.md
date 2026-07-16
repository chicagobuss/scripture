# Phase 1 — the bounded, single-owner, ack=committed chunk driver

Binding plan for the next coding task. Every API and every test is named here so
that the implementation cannot quietly widen its own scope.

Governed by decisions [0009](decisions/0009-chunk-format-and-cohorts.md) (chunk
format), [0010](decisions/0010-acknowledgement-levels-and-producer-identity.md)
(acknowledgement, producer identity), and
[0011](decisions/0011-spool-epochs-failure-and-handoff.md) (spool epochs,
failure model).

## Scope

**In:** one journal per chunk; one in-process owner; `committed`-only
acknowledgement; byte / age / record / memory / in-flight limits enforced by
reservation; producer identity and the dedup window; deterministic tests.

**Out, and enforced as out:** co-packing more than one journal per chunk
(forbidden by 0009's gate until range reads exist), any spool or WAL, any
acknowledgement level below `committed`, multi-AZ anything, service discovery,
client routing, Consul, and the network server. None of these may appear in this
phase, even behind a feature flag.

## APIs

### `scripture::chunk` — the format (0009)

```rust
pub struct CohortId([u8; 16]);
pub struct ChunkId([u8; 16]);
pub struct ChunkDigest([u8; 32]);          // BLAKE3 over the sealed bytes

pub struct FrameRef {                       // one index entry
    pub journal_id: JournalId,
    pub base_offset: RecordOffset,
    pub record_count: u32,
    pub producers: Vec<ProducerRange>,      // for dedup-window recovery (0010)
}

pub struct ProducerRange {
    pub producer_id: ProducerId,
    pub producer_epoch: u32,
    pub first_sequence: u64,
    pub last_sequence: u64,
}

pub struct Chunk {                          // decoded
    pub chunk_id: ChunkId,
    pub cohort_id: CohortId,
    pub generation: u64,
    pub writer_id: WriterId,
    pub frames: Vec<(FrameRef, Vec<Record>)>,
}

pub struct SealedChunk {                    // encoded, immutable, retry-stable
    pub chunk_id: ChunkId,
    pub digest: ChunkDigest,
    pub bytes: Bytes,
}

pub fn seal_chunk(/* header fields, frames */) -> Result<SealedChunk, ChunkError>;
pub fn decode_chunk(bytes: &Bytes) -> Result<Chunk, ChunkError>;
pub fn decode_index(prefix: &[u8]) -> Result<Vec<FrameRef>, ChunkError>;  // header+index only
pub fn encoded_chunk_len(frames: &[...]) -> Result<usize, ChunkError>;    // O(1), for the accumulator
```

`decode_index` exists now, before range reads do, because it is the function the
range-read future will call and because writing it now proves the layout supports
it.

### `scripture::chunklog` — the log surface (0009, amended)

`JournalWriter::append_batch` serializes the legacy `Batch` envelope and cannot
append chunk bytes. 0009 resolves this by making the chunk the canonical payload
and retiring `Batch`. Phase 1 therefore delivers:

```rust
/// Appends sealed chunk bytes to an AtomicLog. The only writer in a generation.
pub struct ChunkLogWriter { /* AtomicLog, per-journal next_offset, generation */ }

impl ChunkLogWriter {
    /// Appends the sealed bytes. Any non-Ok outcome POISONS (0010): no retry.
    pub async fn append(&mut self, chunk: &SealedChunk) -> Result<u64, ChunkLogError>;

    /// Rebuilds next_offset per journal AND the producer dedup window from the
    /// durable tail, bounded by the recovery-scan policy (0010).
    pub async fn recover(log: AtomicLog, cohort: CohortId, bound: RecoveryBound)
        -> Result<(Self, DedupWindow), ChunkLogError>;
}

/// Reads chunks and yields records for one journal, with trim gaps.
pub struct ChunkLogReader { /* AtomicLog, journal filter, cached chunk */ }
```

`JournalWriter`, `JournalReader`, and the `Batch` codec are **removed** in this
change, along with the transitional `AttributeValue` scalars. The raw-lines lab
and `scripture-service::JournalActor` are rewired onto the chunk path. No
production bytes exist; this is the last moment the break is free.

### `scripture::time` — the timer boundary (new; blocker)

The existing `Clock` exposes `now()` only. It **cannot wake a `run()` future when a
`ManualClock` advances**, so an age-bound flush driven by `Clock` alone is not
implementable deterministically. Phase 1 introduces the missing half:

```rust
pub trait Timer: Send + Sync {
    /// Completes at or after `deadline` on the timer's own monotonic scale.
    fn sleep_until(&self, deadline: Duration) -> BoxFuture<'static, ()>;
}

pub struct SystemTimer;                 // tokio::time::sleep_until, at the edge
pub struct ManualTimer { /* ... */ }    // advance() wakes registered sleepers
```

`ManualTimer::advance` fires the waiters whose deadlines it passes, so the entire
age-bound path is exercised with no wall clock and no `tokio::time` in the core —
the same discipline that kept the kernel runtime-free. Tokio appears only in
`SystemTimer`.

### `scripture::driver` — the chunk driver (0010, 0011)

**Ownership topology.** `submit(&self)` / `flush(&self)` / `run(self)` cannot all
hold the mutable writer. The shape is the one `scripture-service` already proved:

```rust
/// Cloneable client endpoint. Bounded channel; awaiting the send IS backpressure.
#[derive(Clone)]
pub struct ChunkDriverHandle { /* mpsc::Sender<Command> */ }

/// The single task that owns the writer, accumulator, dedup window, and
/// reservation. Run exactly once.
pub struct ChunkDriverActor { /* ChunkLogWriter, ChunkPolicy, Clock, Timer, ... */ }
```

```rust
pub struct ChunkPolicy {
    pub max_chunk_bytes: usize,        // seal at; and a HARD ceiling per chunk
    pub max_record_bytes: usize,       // HARD reject above this (0011). Must
                                       // satisfy max_record_bytes + overhead
                                       // <= max_chunk_bytes, or construction fails
    pub max_chunk_records: usize,      // seal at
    pub max_chunk_age: Duration,       // seal at (monotonic; Clock + Timer)
    pub max_buffered_bytes: usize,     // reservation ceiling, unsealed
    pub max_inflight_chunks: usize,    // pipeline depth — part of the loss budget
    pub max_uncommitted_age: Duration, // ADMISSION deadline, not a resolution
                                       // deadline (0011): stop accepting, do not
                                       // promise the in-flight window resolves
    pub recovery_scan: RecoveryBound,  // bounds the dedup-window rebuild (0010)
}

impl ChunkPolicy {
    /// Validates the policy. Fails if max_record_bytes could produce a chunk
    /// exceeding max_chunk_bytes — otherwise bytes_at_risk is not a bound.
    pub fn validate(&self) -> Result<(), PolicyError>;

    /// The HARD bytes-at-risk bound (0011). A value, not a comment.
    /// There is deliberately no `age_at_risk`: time is not bounded under
    /// provider failure, and publishing a number would be a lie.
    pub fn bytes_at_risk(&self) -> usize;
}

pub struct Submission {
    pub producer_id: ProducerId,
    pub producer_epoch: u32,
    pub sequence: u64,
    pub records: Vec<Record>,
}

pub struct Receipt {
    pub level: AckLevel,               // Phase 1: always Committed
    pub journal_id: JournalId,
    pub first_offset: RecordOffset,
    pub next_offset: RecordOffset,
    pub chunk_id: ChunkId,
    pub slot: u64,
    pub deduplicated: bool,            // true if this receipt was replayed
}

pub enum AckLevel { Accepted, Replicated, Journaled, Committed }

impl ChunkDriverHandle {
    /// Reserves, buffers, and returns a future that resolves ONLY on commit.
    /// Awaiting admission is the backpressure. Dropping the returned future
    /// abandons the receipt, never the record (0010, invariant 5).
    pub async fn submit(&self, s: Submission) -> Result<ReceiptFuture, DriverError>;

    /// Seals and appends the open chunk now, regardless of bounds.
    pub async fn flush(&self) -> Result<(), DriverError>;

    pub fn metrics(&self) -> DriverMetrics;   // bytes_at_risk, oldest_uncommitted_age,
                                              // inflight_chunks, reserved_bytes,
                                              // chunk_fill_ratio, dedup_hits
}

impl ChunkDriverActor {
    pub fn new(
        journal_id, cohort_id,
        writer: ChunkLogWriter, dedup: DedupWindow,
        policy: ChunkPolicy, clock: impl Clock, timer: impl Timer,
        queue_capacity: usize,
    ) -> (ChunkDriverHandle, Self);

    /// Rebuilds the writer and dedup window from the durable tail (0011 §3-4),
    /// bounded by the recovery-scan policy.
    pub async fn recover(log: AtomicLog, cohort_id, policy, clock, timer, bound)
        -> Result<(ChunkDriverHandle, Self), DriverError>;

    /// Drains submissions, seals on bounds (Timer drives the age bound), and
    /// appends. The append future is OWNED here and always awaited to
    /// completion — never dropped, never raced against a cancelling timeout.
    /// On any non-Ok append outcome the actor becomes Poisoned: it resolves the
    /// affected receipts as Uncertain, refuses further submissions, and exits.
    pub async fn run(self) -> Result<(), DriverError>;
}

pub enum DriverError {
    OutOfSequence { expected: u64, actual: u64 },
    FencedProducer { seen_epoch: u32, request_epoch: u32 },
    Indeterminate { producer_id: ProducerId, sequence: u64 },  // outside the window
    RecordTooLarge { bytes: usize, max: usize },               // hard reject (0011)
    /// The append outcome is unknown. NOT retryable: retrying would wedge the
    /// AtomicLog on an abandoned slot. The owner poisons and recovery decides
    /// what actually committed (0010).
    Uncertain { chunk_id: ChunkId },
    Poisoned,                                 // a prior Uncertain; nothing accepted
    Codec(ChunkError),
}
```

The actor owns the append future so it cannot be cancelled by a caller — the
cancellation that would abandon a sequencer slot and wedge the log. This is the
same shape as OpenData's `BatchWriterTask`, minus its `tokio::time::sleep` in the
core loop, which we reject on determinism grounds (`references/README.md`); the
`Timer` trait replaces it.

## Tests

Deterministic, in-memory, `ManualClock` + `InMemoryLogDrive`. No network, no
provider.

**Format (0009)**
1. `canonical_round_trip` — property: encode→decode is identity.
2. `re_encode_is_byte_identical` — property: the retry-stability requirement.
3. `arbitrary_bytes_never_panic` — property.
4. `frame_crc_mismatch_is_corruption`, `index_crc_mismatch_is_corruption`.
5. `truncated_and_trailing_bytes_rejected`.
6. `decode_index_reads_header_and_index_only` — no frame bytes touched.
7. `phase_one_encoder_emits_exactly_one_frame` — the co-packing gate, in code.

**Flush boundaries (0010)**
8. `seals_on_byte_bound` / `seals_on_record_bound` / `seals_on_age_bound`.
9. `oversized_record_is_rejected_not_sealed_alone` — an oversized record sealed
   into its own chunk would breach the published bytes-at-risk ceiling (0011).
   Reject it; a deadlock avoided by silently exceeding a durability bound is a
   worse bug than the deadlock.
10. `flush_seals_a_partial_chunk`.

**Acknowledgement (0010)**
11. `receipt_is_released_only_after_the_kernel_acknowledges` — counted through an
    instrumented log; the assertion is on the *count at the moment of release*.
12. `all_submitters_in_one_chunk_receive_receipts_with_correct_offsets`.
13. `cancelled_submitter_still_has_its_record_committed` — invariant 5.
14. `dropped_response_retry_returns_the_original_receipt` — dedup hit.
15. `duplicate_outside_the_window_is_indeterminate_not_guessed`.
16. `out_of_sequence_is_rejected_without_side_effects`.
17. `fenced_producer_epoch_is_rejected`.

**Backpressure and the loss budget (0011)**
18. `submit_blocks_when_buffered_bytes_are_exhausted` — poll-gated; asserts
    pending, not error.
19. `submit_blocks_when_inflight_chunks_are_exhausted`.
20. `reservation_is_released_only_after_commit` — the loss-budget invariant;
    asserts bytes_at_risk never exceeds the policy.
21. `bytes_at_risk_never_exceeds_the_declared_budget` — property, over generated
    submit/commit interleavings.

**Failure and recovery (0010, 0011)**
22. `failed_append_poisons_and_is_never_retried` — a retry would acquire a new
    slot while the abandoned one blocks every `complete_slot` forever. Asserts
    the driver stops, and asserts (with an instrumented log) that **no second
    append is issued**.
23. `kernel_seal_fences_the_driver_and_it_resigns` — the same poison path;
    `Sealed` differs only in what recovery finds.
24. `the_append_future_is_never_dropped` — a submitter cancelling, or `flush`
    being abandoned, must not cancel the in-flight append.
25. `recover_rebuilds_next_offset_from_the_durable_tail`.
26. `recover_rebuilds_the_dedup_window_and_absorbs_a_retry`.
26b. `recovery_scan_is_bounded_and_older_sequences_are_indeterminate` — never an
    unbounded walk of a long-lived journal.
26c. `a_durable_but_unmapped_chunk_is_not_counted_in_recovery` — invariant 7,
    over a VirtualLog cutover.

**Reference model (0011)**
27. `generated_histories_preserve_density_idempotence_and_the_loss_budget` — the
    nine properties of 0011's model, over generated operation sequences with
    injected owner death at every boundary of the state machine.

## Order of work

1. `scripture::chunk` + tests 1–7. The format is what everything depends on and
   the most expensive thing to change later.
2. The pure reference model + test 27, with a *stub* driver — including
   **uncertain append as a first-class outcome**, since that is the case the
   model exists to get right. Find the design error here, before the driver.
3. `scripture::time` (`Timer` + `ManualTimer`) — the age bound is not testable
   without it.
4. `scripture::chunklog` (`ChunkLogWriter`/`ChunkLogReader`); retire `Batch`,
   `JournalWriter`, `JournalReader`, and the transitional scalars.
5. `ChunkDriverActor` + `ChunkDriverHandle` + tests 8–26c.
6. Rewire `scripture-service::JournalActor` and the raw-lines lab onto the chunk
   path.
7. Re-measure the three-provider cost matrix with chunks and record the fill
   ratio. Chunking's whole purpose is that number.

## Definition of done

All 27 tests green under the locked gate; the three decision records unchanged or
amended with a stated reason; the co-packing gate enforced in code, not only in
prose; and a measured fill-ratio report added to the cost model. No spool, no
network, no second journal in a chunk.
