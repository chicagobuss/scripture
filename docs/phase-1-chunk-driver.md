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

### `scripture::driver` — the chunk driver (0010, 0011)

```rust
pub struct ChunkPolicy {
    pub max_chunk_bytes: usize,        // seal at
    pub max_chunk_records: usize,      // seal at
    pub max_chunk_age: Duration,       // seal at (monotonic; injected Clock)
    pub max_buffered_bytes: usize,     // reservation ceiling, unsealed
    pub max_inflight_chunks: usize,    // pipeline depth — part of the loss budget
}

impl ChunkPolicy {
    /// The declared loss budget, computed per 0011. Not a comment: a value.
    pub fn loss_budget(&self, append_latency_p99: Duration) -> LossBudget;
}

pub struct LossBudget { pub bytes_at_risk: usize, pub age_at_risk: Duration }

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

pub struct ChunkDriver { /* owns JournalWriter, accumulator, dedup window */ }

impl ChunkDriver {
    pub fn new(journal_id, cohort_id, writer: JournalWriter, policy, clock) -> Self;

    /// Reserves, buffers, and returns a future that resolves ONLY on commit.
    /// Awaiting the reservation is the backpressure. Dropping the returned
    /// future abandons the receipt, never the record (0010, invariant 5).
    pub async fn submit(&self, s: Submission) -> Result<ReceiptFuture, DriverError>;

    /// Seals and appends the open chunk now, regardless of bounds.
    pub async fn flush(&self) -> Result<(), DriverError>;

    /// Drives sealing on the age bound and appends sealed chunks. Run once.
    pub async fn run(self) -> Result<(), DriverError>;

    /// Rebuilds next_offset and the dedup window from the durable tail (0011 §3-4).
    pub async fn recover(journal_id, cohort_id, log: AtomicLog, policy, clock)
        -> Result<Self, DriverError>;

    pub fn metrics(&self) -> DriverMetrics;   // bytes_at_risk, oldest_uncommitted_age,
                                              // inflight_chunks, reserved_bytes,
                                              // chunk_fill_ratio, dedup_hits
}

pub enum DriverError {
    OutOfSequence { expected: u64, actual: u64 },
    FencedProducer { seen_epoch: u32, request_epoch: u32 },
    Indeterminate { producer_id: ProducerId, sequence: u64 },  // outside the window
    RecordTooLarge { bytes: usize, max: usize },
    Fenced,                                   // kernel Sealed: the owner resigns
    Log(AtomicLogError),
    Codec(ChunkError),
}
```

`run()` is a separate future from `submit()` so that the age bound is driven by a
task rather than by whoever happens to call in — the same shape as OpenData's
`BatchWriterTask`, minus its `tokio::time::sleep` in the core loop, which we
reject on determinism grounds (see `references/README.md`).

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
9. `oversized_single_record_seals_alone_rather_than_deadlocking`.
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

**Failure and recovery (0011)**
22. `failed_append_retries_the_identical_bytes` — the kernel must see an
    idempotent write, not a conflict.
23. `kernel_seal_fences_the_driver_and_it_resigns` — no retry into a sealed
    generation.
24. `recover_rebuilds_next_offset_from_the_durable_tail`.
25. `recover_rebuilds_the_dedup_window_and_absorbs_a_retry`.
26. `a_durable_but_unmapped_chunk_is_not_counted_in_recovery` — invariant 7,
    over a VirtualLog cutover.

**Reference model (0011)**
27. `generated_histories_preserve_density_idempotence_and_the_loss_budget` — the
    nine properties of 0011's model, over generated operation sequences with
    injected owner death at every boundary of the state machine.

## Order of work

1. `scripture::chunk` + tests 1–7. The format is the thing everything else
   depends on and the thing that is most expensive to change later.
2. The reference model + test 27, with a *stub* driver. It should be possible to
   find a design error here before writing the real driver.
3. `ChunkDriver` + tests 8–26.
4. Wire `scripture-service`'s `JournalActor` onto the driver (it already has the
   right shape: bounded submission, ack future, terminal failure).
5. Re-measure the three-provider cost matrix with chunks and record the fill
   ratio. Chunking's whole purpose is that number.

## Definition of done

All 27 tests green under the locked gate; the three decision records unchanged or
amended with a stated reason; the co-packing gate enforced in code, not only in
prose; and a measured fill-ratio report added to the cost model. No spool, no
network, no second journal in a chunk.
