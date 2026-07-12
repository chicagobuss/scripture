# Decision: the immutable chunk — format, cohorts, and the co-packing gate

- Status: accepted (design); Phase 1 binds the single-journal subset
- Date: 2026-07-12
- Layer: envelope/format, write path
- Obligation basis: 1, 9, 10; supersedes the "segments/compaction" line of the
  2026-07-11 audit
- Related: 0010 (acknowledgement), 0011 (spool epochs), holylog 0002 (trim/
  reclamation), holylog 0005 (VirtualLog cutover)

## Context

Scripture's canonical history must not begin as many tiny objects that a later
compactor is required to repair. A writer aggregates records into a
self-contained **immutable chunk** and appends that chunk once through Holylog.

## Decision

### The chunk is the payload

**One chunk is exactly one Holylog payload, appended to exactly one AtomicLog
slot.** There is no descriptor object beside it, no manifest object pointing at
it, and no mutable metadata anywhere in the data plane. The kernel already
guarantees what a descriptor would have to reinvent: ordering, single-value
immutability, and a durable commit point.

**A chunk carries no commit flag, and cannot.** A payload cannot record its own
commit — the statement would have to be written before the fact it asserts. The
commit rule is therefore external and total:

> A chunk is **committed** iff Holylog acknowledged its append at slot `S`. It is
> **visible** iff `S` is below a reader's checked tail. Its records' offsets are
> what the chunk itself declares.

A chunk that is durable in the object store but whose slot lies at or above a
VirtualLog cutover boundary is **not committed and never will be** — it is
unmapped and unreachable (holylog 0005). This is load-bearing for retry
semantics; see 0010.

### Layout: index before frames

```text
+-- ChunkHeader (fixed size) ----------------------------------+
|  magic "SCRC" | major | minor                                |
|  chunk_id            (16B, assigned at seal, stable on retry)|
|  cohort_id           (16B)                                   |
|  generation          (u64, the VirtualLog generation/epoch)  |
|  writer_id           (16B, the fenced owner that sealed it)  |
|  index_offset (u32) | index_len (u32)                        |
|  frames_offset(u32) | frames_len(u32)                        |
|  frame_count  (u32) | created_at_micros (u64)                |
|  index_crc32c (u32)                                          |
+-- Index (sorted by journal_id, then base_offset) ------------+
|  [ journal_id(16B) | base_offset(u64) | record_count(u32)    |
|    frame_offset(u32) | frame_len(u32) | frame_crc32c(u32) ]  |
+-- Frames (record data, one region per index entry) ----------+
+-- Trailer: index_offset | index_len | magic "SCRE" ----------+
```

The index sits **before** the frames, and the header is fixed-size and first.
A reader that speculatively range-reads the first few KiB of the object obtains
the header *and* the index in **one request**, learns which journals the chunk
contains and where their frames are, and then fetches only the frames it wants.
A footer-only index would cost a second round trip (read the trailer, then read
the index). The chunk is assembled in memory before sealing, so writing the
index first costs nothing.

The trailer is an integrity anchor, not the primary path.

### Canonical encoding is mandatory

Holylog's registers are write-once and single-valued: a retry must propose
**byte-identical** bytes or it is corruption, not a retry. Therefore:

- The encoding is a pure, deterministic function of (header fields, ordered
  frames). Index entries are sorted by `(journal_id, base_offset)`.
- `chunk_id` and `created_at_micros` are fixed **at seal time** and are part of
  the sealed bytes. A retry re-sends the *same buffer*, never a re-encode.
- A sealed chunk is an immutable `Bytes`. Nothing may mutate it, including a
  retry path.

Property-tested round-trip and byte-identical re-encode, as in decision 0001.

### Integrity

- `frame_crc32c` per frame — so a reader that range-reads **one frame** can
  verify it without possessing the rest of the chunk. This is what makes the
  range-read future real rather than aspirational.
- `index_crc32c` over the index — so a reader that range-reads only the
  header+index can trust the offsets it is about to seek to.

**CRC-32C means Castagnoli, not CRC-32/IEEE.** The two are trivially confused —
the first implementation of this codec named the field `crc32c` while computing
IEEE — and a reader that computes the wrong polynomial rejects every valid chunk.
Castagnoli is what storage formats use (Parquet, ext4, Btrfs, iSCSI): better
error detection over the short spans we checksum, and hardware-accelerated on x86
and ARM. A known-answer test against the standard `"123456789"` vector pins it, so
the format's name and its algorithm cannot drift apart again.
- The chunk's content digest (BLAKE3-256 over the sealed bytes) is **not stored
  inside the chunk** (it cannot be, self-reference), but is computed at seal and
  carried in receipts and in the producer dedup window (0010). It is the natural
  identity for deduplication and for a future content-addressed lakehouse
  projection.

### Cohorts: what may be packed together

Records may share a chunk **iff they share a cohort**. A `CohortId` fixes every
policy that must age, travel, and die together:

| Dimension | Why it must match |
|---|---|
| retention / lifecycle class | mixed retention makes provider lifecycle deletion unsafe — the object cannot expire on two schedules |
| encryption key / tenant | a chunk is one blob; one key decrypts all of it |
| object-store placement + write quorum | the chunk is written once, to one place, at one quorum |
| access-control / compliance boundary | a reader who can fetch the object can fetch every frame in it |
| ordering owner (spool cell / generation) | offsets in a chunk are allocated by one fenced owner (0011) |

Proximity in time is **not** a cohort. The cohort, not the journal name, is the
scheduling and reclamation unit — which is what lets sealed generation prefixes
be reclaimed by provider lifecycle rules instead of DELETE loops (holylog 0002).

### Per-journal dense offsets under co-packing

Each frame declares `(journal_id, base_offset, record_count)`. For a journal `J`,
the frames for `J` across the cohort's chunks, in slot order, must have
contiguous offset ranges: `base_{n+1} == base_n + count_n`. The single fenced
owner per generation is the sole allocator (0011), so co-packing changes *what
object a record lands in*, never *what offset it receives*.

## The co-packing gate — the most important line in this record

**Holylog has no range read.** `LogDrive::read(address)` returns the entire
opaque value; quorum reads compare whole values; the object-store adapter's read
is a whole-object GET. This is not an oversight — it is the model, and decision
holylog 0002 already priced reads per entry on that basis.

Therefore, **today**, a reader of a sparse journal inside a co-packed 64 MiB
chunk would download **64 MiB to obtain its 1 KiB frame.** Co-packing would
convert a sparse reader's cost from *O(its own data)* to *O(all data in the
cohort)*. That is not a tuning regression; it is a different asymptote, and it
would make co-packing strictly worse than the tiny objects it is meant to
replace.

**So: co-packing (more than one journal per chunk) is FORBIDDEN until a
range-read capability exists and is attested.** Phase 1 chunks contain exactly
one journal. The format above is defined for the general case so that enabling
co-packing later is a policy change and not a format break — the index, the
per-frame CRCs, and the multi-frame layout all exist now precisely so that the
day range reads land, no bytes need rewriting.

The range-read capability, when specified, is an **adapter** capability
(`holylog-object-store` gains a range GET keyed by slot), not a change to the
kernel's `LogDrive` contract. It will need its own decision, including:

- quorum semantics: a range read cannot compare whole values, so a quorum range
  read must compare digests or be restricted to single-drive deployments;
- capability attestation per backend, exactly as conditional writes were;
- a cost model term, since a range GET is billed like a GET but transfers less.

## The chunk supersedes the batch envelope (amended 2026-07-12)

Decision 0001 defined a `Batch` envelope — one journal, a base offset, records, a
footer index — and `JournalWriter::append_batch` serializes *that* format and
nothing else. A chunk driver cannot hand `SealedChunk::bytes` to it.

There are three ways out, and only one is honest:

1. **Double-wrap** a chunk inside a legacy batch. Rejected: two envelopes, two
   checksums, two version fields, and a canonical format that is a lie about
   itself.
2. **Keep both formats** and choose per journal. Rejected: two decoders, two
   recovery paths, two corruption models, forever.
3. **The chunk *is* the canonical payload.** Accepted.

**Decision 0009 supersedes decision 0001's `Batch` as Scripture's durable payload
format.** A single-frame chunk is the direct successor to a batch — it carries the
same `(journal_id, base_offset, records)` and adds cohort, generation, writer
identity, producer ranges, and per-frame integrity. Phase 1 ships
`ChunkLogWriter` / `ChunkLogReader` over `AtomicLog` and **retires** the `Batch`
codec, `JournalWriter`, and `JournalReader`.

This is a format break with no cost, and it is the last moment it will be free:
**no production bytes exist.** Making it now is cheaper than carrying two
envelopes for the life of the system.

**Scope (amended 2026-07-12): the transitional `AttributeValue` scalars stay.**
Replacing the envelope does not require changing the record's type policy — the
chunk represents `string | i64 | bool` attributes exactly as the batch did. The
opaque-core migration is a *separate* question with a separate rationale, and
folding it into a format replacement would make two independently reviewable
decisions into one unreviewable commit. 0001's transitional note stands; retiring
the scalars gets its own change.

The raw-lines lab listener and `scripture-service`'s `JournalActor` are rewired
onto the chunk path in the same change. Neither has durable data.

## Deployment profiles: single-node is a first-class profile

Nothing in this record, or in 0010 and 0011, requires more than one node.

| Profile | Owner | Fencing | What you get | What you give up |
|---|---|---|---|---|
| **single-node** | one process | none needed (there is no contender) | the full contract: chunks, dense offsets, `committed` acks, producer idempotence, trim, recovery from durable bytes | **high availability.** If the process is down, writes stop |
| **fenced multi-node** | one at a time, elected | VirtualLog generation + conditional register (0011) | the above, plus failover | an attested register, and the operational footprint that comes with it |

The single-node profile is not a degraded mode or a toy: it is the same code,
the same format, and the same durability, because durability comes from the
object store rather than from the fleet. A VirtualLog is *optional* — a
`ChunkLogWriter` sits directly on an `AtomicLog`, and a deployment that never
reconfigures never needs a register.

What a single node loses is precisely availability: no successor can take over,
so an outage stops writes until the process returns — and, because the process
was the only owner, it can simply resume (its next `recover` rebuilds state from
durable bytes). It cannot lose committed data, because committed means the object
store has it.

This is the profile Phase 1 implements. Fencing is what the *second* node costs,
and it is charged only to deployments that want one.

## Correctness

Immutability and single-value: a chunk is sealed to bytes before its append, and
retries resend the same bytes, so Holylog's write-once register sees an
idempotent retry rather than a conflict.

Ordering: Holylog's slot order totally orders chunks in a generation; the frames
within a chunk carry explicit per-journal offset ranges; the fenced owner is the
only allocator. Therefore per-journal record order is the slot order of the
chunks containing that journal, and it is dense.

Corruption: a frame whose CRC fails is persisted-state corruption and is reported
as such — never skipped, never "best-effort" decoded.

## Deterministic validation

- Canonical round-trip and byte-identical re-encode (property test).
- Arbitrary-bytes decode never panics (fuzz-shaped property test).
- Index/frame CRC mismatch is rejected as corruption.
- Truncated, oversized, misordered-index, duplicate-journal-frame, and
  overlapping-frame-range chunks are rejected.
- Dense-offset continuity across a generated history of chunks for one journal.
- Co-packed dense-offset continuity per journal (model-level, ahead of the gate).
- A Phase-1 encoder never emits more than one frame (the gate, enforced in code
  and asserted).

## Cost and observability

Measured on three providers (GCS, R2, S3): one PUT per chunk, and the metadata
registers (seal, trim) cost more requests than the data at small chunk sizes.
Chunking's entire purpose is to move the ratio: one PUT per *chunk* instead of
one per batch, with a fill ratio that is now an explicit, reported metric rather
than a hidden compaction debt.

Required metrics from day one: chunk fill ratio, chunk bytes, records per chunk,
time-to-seal, and — for the gate — bytes downloaded per record read.

## Alternatives and consequences

**A descriptor object beside the chunk** was rejected: it doubles the PUT count,
introduces a two-object commit with no atomicity, and reinvents ordering that
Holylog already provides.

**A footer-only index** was rejected: it costs a second round trip on the read
path the format exists to make cheap.

**Compaction / consolidated segment files** (the 2026-07-11 audit's finding C)
are **superseded by this record.** That finding correctly identified the cost of
one object per slot and proposed consolidating slots into segment files
afterwards. The chunk does the consolidation *at write time*, on the hot path,
so the final-sized object is the first and only object. There is no compactor,
no rewrite, and no window in which the log is expensive-but-not-yet-repaired.
Compaction may return later as an *optional* analytics/layout optimization; it
must never become a prerequisite for retention, durability, or ordinary replay.

Consequence: sparse cohorts will produce underfilled chunks on the age bound.
That cost is visible (fill ratio) and paid at write time, rather than deferred
into a compaction obligation. That is the trade this record makes deliberately.
