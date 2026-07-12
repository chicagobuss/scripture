# Decision: spool epochs, failure semantics, and handoff

- Status: accepted (design); **no implementation in Phase 1**
- Date: 2026-07-12
- Layer: write path, operations
- Obligation basis: 2, 6, 15
- Related: 0009 (chunk), 0010 (acknowledgement), holylog 0004/0005 (VirtualLog)

## Context

A spool holds records that have been acknowledged below `committed` and have not
yet reached the object store. Everything hard about the data plane lives in the
window between those two facts.

## Decision

### The spool epoch **is** the VirtualLog generation

We do not invent a second fencing mechanism. The kernel already has one, it is
proved against the paper, and it is attested on three clouds.

```text
spool epoch  ≡  VirtualLog generation
fenced owner ≡  the sole writer to that generation's AtomicLog
handoff      ≡  seal-and-replace reconfiguration
```

A successor becomes the owner by **sealing the predecessor's generation and
publishing its own** (holylog 0005). The consequences fall out for free:

- the old owner learns it is fenced because its next append returns `Sealed` —
  it does not need to be told, and it cannot be tricked by a stale health check;
- an append that raced the seal is durable-but-unmapped and therefore invisible
  (holylog 0005), so it cannot become a duplicate;
- exactly one successor wins, because publication is a compare-and-swap on an
  attested conditional register.

**Service discovery is not fencing.** A health check says who *appears* alive. The
register says who *may allocate offsets*. Nothing in the discovery layer may ever
authorize a write. (This is the one line of this record most likely to be quietly
violated by a future implementation, so it is stated as a prohibition.)

### Fencing rules before a successor allocates

A successor MUST, in this order:

1. **Seal** the predecessor's generation. (Idempotent; safe to repeat.)
2. **Determine the boundary** from the sealed generation's tail.
3. **Rebuild per-journal `next_offset`** by reading the sealed tail's chunks
   (scripture 0003 `recover`, extended to chunks) — including any zombie chunks
   below the boundary, which *are* committed and whose offsets *are* allocated.
4. **Rebuild the producer dedup window** from the same tail scan (0010).
5. **Publish** its generation via CAS. If the CAS loses, it is not the owner and
   must discard everything above and start over.
6. Only now may it allocate an offset.

A successor that allocates before step 5 has produced offsets it may not own. The
only thing preventing this is the discipline of the code, so the model in the
test plan below checks it explicitly.

### Owner death at every boundary

`R` = reserved, `B` = buffered, `S` = sealed bytes, `A` = append issued,
`C` = kernel acknowledged, `K` = receipt released.

| Owner dies at | Producer saw | Record's fate | Successor must |
|---|---|---|---|
| before `R` | timeout | nothing | nothing |
| `R`, before `B` | timeout | nothing | nothing |
| `B`, before `S` | timeout | **lost** (buffered only) — within the loss budget | nothing; the producer retries and is accepted normally |
| `S`, before `A` | timeout | **lost** — sealed bytes were never appended | nothing; producer retries |
| `A`, before `C` | timeout | **indeterminate**: the append may or may not be durable | seal, then look. Below the boundary ⇒ committed, and the dedup window absorbs the retry. At/above ⇒ unmapped, invisible; the retry is the only copy |
| `C`, before `K` | timeout | **committed** — the record is in the log | nothing; the retry hits the rebuilt dedup window and gets the *original* offsets |
| after `K` | receipt | committed | nothing |

The only rows that lose data are the ones where nothing durable was ever written,
and they are exactly the loss budget. The only genuinely subtle row is `A`→`C`,
and it is subtle only because the object store's view ("the bytes are there") and
the log's view ("that slot is unmapped") disagree — which is why 0009 forbids a
commit flag in the chunk and 0010 makes the kernel's mapping the sole arbiter.

### AZ unavailable versus AZ permanently lost — and the trap

These are **not** the same event, and the difference is invisible at the moment
you must act.

- **Unavailable**: the cell may return with its WAL intact.
- **Permanently lost**: it will not.

A successor cannot distinguish them, and **must not guess**. What it *can* do is
fence — and fencing is the trap:

> The moment a successor seals the predecessor's generation, any `journaled`
> data still sitting in the unavailable cell's WAL **can never be committed
> through that generation.** It was acknowledged as durable. It is now
> unreachable. The `journaled` promise is broken by the very act that makes
> progress safe.

So a `journaled` acknowledgement forces an explicit, configured choice, and there
is no third option:

| Policy | On cell unavailability | Cost |
|---|---|---|
| **`block`** | the journal stops accepting writes until the cell returns and drains its WAL into the current generation | availability loss, bounded by the cell's outage |
| **`declare-lost`** | fence immediately, report every `journaled`-but-uncommitted record in that cell as **lost**, by producer and sequence | durability loss, bounded by the loss budget |

There is no policy that is both available and durable here, because the data
exists in exactly one place. Choosing `declare-lost` and calling the level
"durable" would be the dishonesty this decision exists to prevent.

**A further consequence, which is easy to miss:** if a stranded WAL is later
drained into a *newer* generation, its records receive **new, later offsets** —
after records the producer sent afterwards and that committed in the meantime.
That reorders a producer's own stream. Therefore the drain protocol must either
(a) run under `block`, so nothing overtook the stranded records, or (b) reject
the stranded records outright. Draining a stale WAL into a live generation
**must not** be implemented as a background best-effort catch-up; it silently
reorders, and it will not be caught by any test that does not look for it.

`replicated` (memory quorum) is strictly worse: it survives a host, not a cell,
and a cell-wide event loses it entirely.

### The loss budget, calculated

The loss budget is the maximum acknowledged-but-uncommitted data a profile may
hold. It must be **enforced by reservation**, not merely documented.

**Bytes are a hard bound. Time is not — and the record must not pretend
otherwise (amended 2026-07-12).**

```text
# HARD BOUND, enforced by reservation. No admission may exceed it.
bytes_at_risk = max_buffered_bytes                       # not yet sealed
              + max_inflight_chunks × max_chunk_bytes    # sealed, append in flight
```

This is a true maximum only if **no chunk can exceed `max_chunk_bytes`**.
Therefore a hard `max_record_bytes` is mandatory, with
`max_record_bytes + chunk_overhead <= max_chunk_bytes`, and a record above it is
**rejected** (`RecordTooLarge`) rather than sealed into an oversized chunk of its
own. An earlier draft allowed an oversized record to "seal alone rather than
deadlock" — that avoids a deadlock by silently breaching the published
bytes-at-risk ceiling, which is worse. Reject it and say why.

**Pipeline depth is in the bound.** "We can lose at most one chunk" is true *only*
when `max_inflight_chunks == 1`. A profile that pipelines four chunks to hide
upload latency has quadrupled its exposure, and the sentence that used to be true
is now false by a factor of four. This is the easiest place for an implementation
to quietly distort the contract, which is why it is a formula and not a slogan.

**Time is not a hard bound, and the honest statement is uncomfortable:**

```text
# NOT A BOUND. p99 is a measurement; a provider outage exceeds it arbitrarily.
age_at_risk ≠ max_chunk_age + append_latency_p99 × max_inflight_chunks
```

An in-flight append can hang for as long as the provider is unwell. Nothing
Scripture does can bound that, because it cannot un-issue an append and it cannot
learn the outcome. What it *can* bound is **admission**:

- `max_chunk_age` bounds the time from a record's acceptance to its **seal**.
- `max_uncommitted_age` is an **admission deadline**, not a resolution deadline:
  when the oldest uncommitted chunk exceeds it, the driver **stops accepting new
  submissions** and raises `oldest_uncommitted_age` on its metrics. It does not,
  and cannot, promise the in-flight data resolves by then.

So the published contract is: **bounded bytes at risk, bounded admission,
unbounded resolution time under provider failure.** A profile that advertises a
time-bounded loss window is lying about a dependency it does not control. If an
operator needs a resolution deadline, the only honest mechanism is to fail the
in-flight window explicitly — which, per the state machine (0010), means the
driver poisons and recovery decides what was committed. That is a *loss
declaration*, not a timeout.

A submission that would exceed `bytes_at_risk`, or that arrives after the
admission deadline has tripped, **blocks** (backpressure). It is never
accepted-and-then-dropped. A spool fleet must never become an unbounded shared
heap.

## Correctness

Fencing safety reduces to the kernel's, which is proved and attested: at most one
generation is active; the loser of a CAS never allocates; a fenced owner's writes
fail closed.

Offset density across handoff holds because the successor rebuilds `next_offset`
from durable bytes below the boundary before allocating, and the predecessor
cannot append after the seal.

Idempotence across handoff holds because the dedup window is rebuilt from the
same durable bytes (0010).

No record is both committed and reported lost: `declare-lost` may only name
records that are *not* below the boundary of the sealed generation, and that set
is exactly what a tail scan determines.

## Deterministic validation — the reference model

A pure in-memory model, no network, no server, no object store. It exists to be
wrong cheaply.

**Model state.** Cohorts → journals; an owner with `(generation, next_offset per
journal, dedup window, open chunk, in-flight chunks, reservation)`; a log of
committed chunks; a set of durable-but-unmapped chunks; producers with
`(id, epoch, next_sequence)`.

**Generated operations.** submit / seal / append-succeeds / append-fails /
append-is-fenced / owner-dies / successor-recovers / producer-retries /
producer-restarts (epoch bump) / trim / cell-unavailable / cell-lost.

**Properties checked over every generated history:**

1. **Density** — for each journal, committed offsets form a gapless prefix from 0.
2. **No duplicates** — each `(producer_id, epoch, sequence)` appears at most once
   in the visible log.
3. **No loss beyond budget** — the set of acknowledged-but-not-committed records
   never exceeds `bytes_at_risk` / `age_at_risk`.
4. **Receipt soundness** — every released receipt names a record that is visible
   in the log at exactly the offsets the receipt claims.
5. **Per-producer order** — for each `(producer, journal)`, committed sequences
   are increasing in offset order.
6. **Fencing** — no two generations allocate the same offset; a successor never
   allocates before its CAS wins.
7. **Excluded ≠ committed** — a durable-but-unmapped chunk is never read, never
   deduped against, and never counted in `next_offset`.
8. **Cohort integrity** — no chunk contains frames from two cohorts.
9. **Loss honesty** — every record reported lost is genuinely absent from the
   visible log, and no committed record is ever reported lost.

Properties 3, 7, and 9 are the ones a plausible-looking implementation will
violate, and they are the reason this model must exist before the network code
does.

## Cost and observability

Required metrics, and they are the operator's whole view of risk:
`bytes_at_risk`, `oldest_uncommitted_age`, `inflight_chunks`, `reserved_bytes`,
`chunk_fill_ratio`, `commit_lag`, and — if any level below `committed` is
offered — `records_lost` broken down by producer.

## Operational footprint (obligation 15)

Phase 1 adds **nothing**: the chunk driver is an in-process library object over
the existing kernel. Object storage stays delegated; the sequencer stays
soft-state; there is no new stateful tier.

A spool cell, when it arrives, is the family's **first self-hosted stateful
component** (local WAL). Obligation 15 requires that to be justified in its own
record, and the justification must survive this one's finding: a same-AZ cell
buys availability, not durability, and pays for it with a loss budget and a
blocking-or-losing choice on cell failure. That is a real trade, and it may be
the right one — but `committed` remains the only level that needs no such
argument.

## Alternatives and consequences

**A separate spool-ownership register with its own fencing** was rejected: it
would be a second source of write authority, and two authorities that can
disagree are worse than one that can be slow.

**Health-check-based ownership** was rejected outright and is now a prohibition.

**Best-effort background WAL drain into a live generation** was rejected: it
silently reorders a producer's stream, and no test that is not looking for it
will catch it.

Consequence: multi-AZ spool quorum is the *only* configuration that makes a
pre-commit acknowledgement survive an AZ loss, and it costs cross-AZ traffic on
the hot path. Same-AZ cells are an availability feature. Saying so plainly costs
us a marketing line and buys us the ability to defend the system to the people
who wrote the paper.
