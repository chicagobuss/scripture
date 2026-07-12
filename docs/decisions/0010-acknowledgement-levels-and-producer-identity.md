# Decision: acknowledgement levels, producer identity, and retry semantics

- Status: accepted (design); Phase 1 binds `committed`-only
- Date: 2026-07-12
- Layer: write path, consumer-visible contract
- Obligation basis: 2, 5, 9
- Related: 0009 (chunk), 0011 (spool epochs), holylog 0001/0005 (seal, cutover)

## Context

An acknowledgement is a promise about survival. The failure mode this record
exists to prevent is a promise that is *nearly* true — a level that means
"probably durable" and is read by an operator as "durable".

## Decision

### Four levels, four distinct meanings

| Level | Means | Survives | May be called durable |
|---|---|---|---|
| `accepted` | one node has the bytes in memory | nothing | **no** |
| `replicated` | a memory quorum on independent hosts has it | a process/host loss | **no** |
| `journaled` | a local-disk quorum in one cell has fsynced it | a node loss within the cell | **only within the declared spool failure domain** |
| `committed` | the containing chunk is acknowledged by Holylog's object-store write quorum | whatever the object store survives | **yes** |

**No API may describe `replicated` as durable.** A memory quorum is an
availability mechanism.

A receipt reports the **achieved** level (never merely the requested one), plus
`producer_id`, `sequence`, `generation`, `chunk_id`, and — at `committed` — the
`journal_id`, `first_offset`, `next_offset`, and `slot`.

### Deployment profiles declare which levels exist

| Profile | Levels offered | Requires |
|---|---|---|
| **object-commit** (baseline, Phase 1) | `committed` only | nothing beyond the kernel |
| local-spool | `journaled`, `committed` | a WAL quorum **and** the WAL handoff protocol (0011) |
| memory-spool | `replicated`, `journaled`, `committed` | the above, plus an explicit non-durability disclosure |

A profile must publish its **loss budget** (0011). A profile that offers a level
below `committed` without a loss budget is invalid configuration and must fail to
construct.

`accepted` is an internal state. It is never a client-visible durability claim in
any profile.

**`journaled` is a lie without a handoff protocol.** If a cell is fenced while
holding `journaled`-but-uncommitted data, that data can never reach the log
through the sealed generation — so the promise is broken unless the WAL is
replayed into the successor. 0011 specifies that protocol and gates the level on
it. Until it exists, only `committed` may be offered. This is why Phase 1 is
`committed`-only, and it is a correctness constraint, not a scoping preference.

### Producer identity and idempotent retry

Every submission carries `(producer_id, producer_epoch, sequence)`:

- `producer_id` — stable across reconnects (16 bytes).
- `producer_epoch` — incremented when a producer restarts and re-registers;
  fences a zombie producer instance.
- `sequence` — strictly increasing per `(producer_id, producer_epoch, journal)`,
  starting at 0.

**The durable dedup key is `(producer_id, producer_epoch, journal)`** — not
`(producer_id, journal)`. The owner keeps, per journal:

- `highest_epoch[producer_id]` — the greatest epoch ever admitted; and
- a bounded **dedup window** keyed by `(producer_id, producer_epoch)`, holding the
  highest committed sequence and a bounded map from recent sequence → assigned
  offsets.

**Epoch admission rules:**

| Condition | Outcome |
|---|---|
| `epoch > highest_epoch` | admit; record the new `highest_epoch`; the expected initial sequence for that epoch is **0** |
| `epoch == highest_epoch` | admit; sequence rules below apply |
| `epoch < highest_epoch` | `FencedProducer` — a zombie instance. Reject, no side effects |

**A retry preserves its original `(epoch, sequence)`.** This is a contract on the
producer, and it is the only thing that makes a retry a *retry*. A producer that
restarts, bumps its epoch, and re-sends the same logical event under the new
epoch has **created a new identity**: the owner cannot know the two are the same
event, and it will commit both. Such a producer gets at-least-once and must be
told so — exactly-once across a producer restart requires an
application-level idempotency key, which Scripture does not invent for it.

**Recovery scan is bounded — and the bound limits what we can know about
producers, not just about sequences (amended 2026-07-12).**

```rust
pub struct RecoveryBound {
    pub max_chunks: usize,        // stop after this many chunks
    pub max_encoded_bytes: usize, // or after this many bytes
}
```

Both limits are enforced: the scan walks the sealed predecessor's tail backwards
and stops before the next chunk would exceed **either** budget. (They are
different units and neither substitutes for the other.)

The consequence runs deeper than sequence numbers. `highest_epoch[producer_id]`
has the same persistence problem: **a bounded scan cannot reconstruct a global
fact.** If producer `P`'s newest chunk lies before the recovery window, the new
owner does not know `P` exists, and an arriving `(P, epoch, 0)` is
indistinguishable from a brand-new producer and from a zombie that has been
asleep for a week. Admitting it and *later* claiming `FencedProducer` semantics
would be a promise the owner cannot keep.

Therefore:

- Producer epoch and dedup knowledge is retained **only within the configured
  recovery window**, unless a separate durable producer registry is introduced
  (which would be its own decision, with its own storage and cost).
- A producer absent from the recovered window yields **`IndeterminateProducer`**,
  not automatic admission and not automatic fencing. The owner says "I cannot
  know", which is the truth.
- **A genuinely new logical producer uses a new `producer_id`.** The owner must
  never infer that a reused ID is new. A producer that lost its session state
  needs an explicit re-registration policy at the application layer; Scripture
  will not guess on its behalf.
- Sequences older than the scanned boundary yield `Indeterminate`, not a guess.

The bound is a published policy value and appears alongside the loss budget
(0011). It is the price of not carrying an unbounded producer registry, and it is
stated rather than hidden.

Submission handling:

| Condition | Outcome |
|---|---|
| `sequence == last_committed + 1` | accept, allocate offsets |
| `sequence <= last_committed`, in window | **duplicate** — return the *original* receipt (same offsets). Idempotent. |
| `sequence <= last_committed`, outside window | `Indeterminate` — the producer must reconcile by reading the log. Honest; never guessed. |
| `sequence > last_committed + 1` | `OutOfSequence` — a gap. Reject; the producer must resync. |
| `producer_epoch` < the highest seen | `FencedProducer` — a zombie instance. Reject. |

**The dedup window is recovered from the log, not from memory.** Each frame
records, per submission, `(producer_id, producer_epoch, sequence, first_record,
record_count)` — a record **span**, not a sequence range (0009). A new owner
rebuilds the window by scanning the tail of the sealed predecessor and can then
answer a retry with the *original offsets*, because the span says exactly where
that submission landed. A sequence range would say only that the sequence
committed, which is not a receipt.

A window that cannot be rebuilt (because the required chunks were trimmed or lie
outside the recovery bound) yields `Indeterminate` for those sequences — which is
correct, not a degradation.

This is the mechanism the fleet gate names as "Scripture producer idempotence."

### What a dropped request or response means

| Event | Producer sees | Truth | Resolution |
|---|---|---|---|
| request lost | timeout | nothing happened | retry same `(pid, epoch, seq)` → accepted normally |
| response lost, chunk **committed** | timeout | the record is in the log | retry → dedup window returns the **original** receipt |
| response lost, chunk **failed** | timeout | nothing is in the log | retry → accepted normally |
| response lost, chunk **durable but uncommitted** (kernel `Sealed`; the slot is at/above a cutover boundary) | timeout | the bytes exist in the object store but are **unmapped and unreachable forever** (holylog 0005) | retry → lands in the successor generation. The stranded copy is invisible, so there is **no duplicate** |

That last row is the subtle one, and it is why 0009 forbids a commit flag inside
the chunk: the durable-but-excluded chunk *looks* committed from the object
store's point of view and is not. Only the kernel's mapping decides, and the
kernel already excludes it — proven by holylog's
`post_seal_zombie_is_not_mapped_after_cutover`.

The dedup window handles the steady-state duplicate; the VirtualLog cutover
handles the crash-time one. **Both mechanisms are required** — neither covers the
other's case.

## Acknowledgement state machine

States of one submission inside the owner:

```text
              submit()
                 │
                 ▼
          ┌─────────────┐   reservation denied / limits exceeded
          │  Reserved   │──────────────────────────────► Rejected(Backpressure)
          └─────┬───────┘
                │ bytes admitted to the open chunk
                ▼
          ┌─────────────┐   dedup hit (seq <= last, in window)
          │   Buffered  │──────────────────────────────► Committed(original receipt)
          └─────┬───────┘
                │ chunk seals (bytes | age | records | flush)
                ▼
          ┌─────────────┐
          │   Sealed    │  bytes are final and immutable; retries reuse them
          └─────┬───────┘
                │ Holylog append issued
                ▼
          ┌─────────────┐
          │  Appending  │  the append future is OWNED and AWAITED; never dropped
          └──┬───────┬──┘
             │       └── any non-Ok outcome ─────────► Uncertain ─► owner poisons,
             │           (error, timeout, cancel,                   stops accepting,
             │            kernel Sealed)                            recovers (0011)
             ▼
        append acknowledged
             │
             ▼
       ┌─────────────┐
       │  Committed  │  receipt released to every submitter in the chunk
       └─────┬───────┘
             │ reservation released
             ▼
          (steady state)
```

### There is no retryable append failure (amended 2026-07-12)

An earlier draft of this record had `Appending` fall back to
`Failed(retryable) → re-append the same bytes`. **That is unsound, and it would
deadlock.**

`AtomicLog::append` acquires a slot, *then* writes, *then* completes the slot:

```rust
let address = self.sequencer.acquire_slot().await?;
self.drive.write(address, value).await?;      // an error here abandons the slot
self.sequencer.complete_slot(address).await?; // never reached
```

If the write fails or the future is cancelled after the slot was acquired, that
slot is **allocated and never completed**. The sequencer's completed tail cannot
advance past it, so *every subsequent* `complete_slot` — including the retry's —
blocks forever. A retry would not merely risk a duplicate; it would **hang the
driver permanently on the first transient upload error.** This is the kernel's
documented intentional wedging (holylog `docs/atomic_log.md`), and it is the same
reason `JournalWriter` poisons after an uncertain append (decision 0003). The
draft state machine contradicted a decision this repo had already made.

**The rule, therefore:**

> **Every non-`Ok` outcome of `AtomicLog::append` — error, timeout, cancellation,
> or `Sealed` — is `Uncertain`. The driver poisons: it stops accepting
> submissions, it does **not** retry into the same AtomicLog, and it enters the
> recovery/reconfiguration path of 0011.**

Two corollaries the implementation must honour:

1. **The append future is owned by the driver and always awaited to completion.**
   It is never dropped, never raced against a timeout that abandons it, and never
   placed in a `select!` that can cancel it. Cancelling it is what creates the
   wedge.
2. **`Sealed` is not the only uncertain case.** It is merely the one where we also
   know *why*. A network error and a fence are handled identically by the driver;
   they differ only in what recovery finds.

**Kernel gap (not a Phase-1 dependency).** A retry could be made safe for the
*pre-acquire* case if `AtomicLog::append` classified its failure phase — "no slot
was acquired" versus "a slot was acquired and its fate is unknown". The first is
cleanly retryable; the second never is. No such classification exists today, and
Phase 1 does not assume one. It is recorded in `docs/kernel-gap-report.md` as a
proposal, because it would convert the common transient-error case from
"poison and recover" into "retry and continue" — which is worth real money at
scale.

Invariants over this machine, and they are the properties the model in 0011 must
check:

1. **A receipt is released only from `Committed`.** No path releases a receipt
   from `Buffered`, `Sealed`, or `Appending` in the object-commit profile.
2. **Reserved bytes are released only after `Committed` or a terminal failure.**
   Never on `Sealed`, or the loss budget becomes unbounded.
3. **`Sealed` bytes never change.** A retry after a kernel error re-appends the
   identical buffer, which the kernel sees as an idempotent write.
4. **`Indeterminate` never becomes `Committed`.** The owner that observes a fence
   resigns; it does not retry into a sealed generation.
5. **Cancellation of a submitter's future does not cancel the chunk.** The bytes
   are already collective. The submitter loses its receipt, not its record — a
   record that reaches `Committed` is in the log regardless of who is still
   waiting.

Point 5 is deliberate and is the opposite of the kernel's `JournalWriter`, whose
cancellation poisons the writer (scripture 0003). At the chunk driver, a
submission is one of many in a shared chunk and cannot be individually withdrawn
once buffered. The API must say so.

## Correctness

`committed` inherits the kernel's guarantee exactly: Qw durable copies,
address-ordered completion, and a seal check after ordered completion — nothing
weaker, and nothing renamed.

Idempotence: a record identified by `(producer_id, producer_epoch, sequence)`
appears at most once in the visible log. Steady-state duplicates are absorbed by
the dedup window; crash-time duplicates cannot occur because the indeterminate
copy is excluded by the cutover boundary.

Ordering: per `(producer, journal)`, records appear in sequence order, because
the owner rejects gaps and allocates offsets in submission order under one
fenced epoch.

## Deterministic validation

- A receipt is never released before the kernel acknowledges (counted through an
  instrumented log).
- Duplicate `sequence` in-window returns byte-identical offsets to the original.
- Duplicate out-of-window returns `Indeterminate`, never a guessed offset.
- `OutOfSequence` and `FencedProducer` are rejected without side effects.
- A dropped-response retry after commit yields exactly one visible record.
- A submitter that cancels still has its record committed, and the chunk is
  unaffected.
- The dedup window rebuilt from a log tail equals the window in memory before a
  simulated owner restart.

## Cost and observability

`committed` costs exactly one chunk PUT (plus the kernel's seal GET per append —
irreducible, per holylog 0006). Levels below `committed` trade requests for
latency and buy a loss budget; the budget is the cost, and it is denominated in
bytes, age, and in-flight chunks (0011).

Required metrics: receipts by achieved level, dedup hits, `Indeterminate` count,
`OutOfSequence` count, and oldest uncommitted age.

## Alternatives and consequences

Acknowledging at `Sealed` (bytes final, append in flight) was rejected: it is the
exact shape of a promise that is nearly true, and it would make the loss budget
unbounded in upload latency.

A monotonic global sequence per producer (rather than per producer *per journal*)
was rejected: it couples journals that have no ordering relationship and would
make a slow journal stall a fast one.

Consequence: producers must carry identity and sequence to get idempotence. A
producer that will not is offered at-least-once and must be told so plainly.
