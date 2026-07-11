# Decision: one in-process writer with durable ordered acknowledgements

- Status: accepted
- Date: 2026-07-11
- Layer: write path
- Obligation basis: 2

## Context

Scripture needs batching and dense offset allocation without pretending that
Holylog's conflict detection is distributed writer exclusion.

## Decision

v0 supports one non-cloneable, in-process `JournalWriter` per journal. Multiple
independent writers are an invalid configuration. The implementation should
make accidental local duplication difficult through ownership, but provides no
cross-process lease, restart, or failover guarantee.

An append acknowledges only after Holylog acknowledges its batch: Qw durable
copies, address-ordered completion, and the final seal check. Buffered records
are never reported durable. Batching is bounded by record count, encoded bytes,
and monotonic elapsed age. Scripture owns its small injectable timer
abstraction; it does not wait for Holylog's unrelated hedging timer.

The first v0 writer is deliberately sequential: `append_batch(&mut self)`
awaits one Holylog append before accepting the next. It does not yet pipeline
up to K or expose per-record acknowledgement futures. Per-journal throughput is
therefore capped at one batch per durable-append round trip. Pipelining needs a
separate async-driver decision and must not be assumed by v0 cost estimates.

Kernel conflicts are diagnostic only. Identical bytes at the same slot can be
accepted idempotently and collapse two logical writes, so conflict detection
must never be described as fencing. Production fencing is gated on VirtualLog
and a conditional-register decision.

A sealed append may return an error after its bytes became durable (Holylog's
documented zombie-append behavior). The writer does not advance its in-memory
offset on that error, but fenced recovery must count the durable zombie range
as allocated. Consequently, errored records may appear in the log and a retry
in a successor generation may occupy different offsets, producing
at-least-once duplicates across generation replacement.

## Correctness

The writer allocates one dense record range per batch and advances durable
state only after `AtomicLog::append` succeeds. A failed or sealed append does
not produce successful record acknowledgements.

## Kafka-mappability

Durable ordered acknowledgement is at least as strong as the intended future
gateway's acknowledged-produce subset. Buffered-only acknowledgement is absent.

## Deterministic validation

Test record/byte/age flush boundaries with a manual monotonic clock, failed and
sealed appends, offset overflow, retry bytes, and ordered acknowledgement under
out-of-order storage completion.

## Cost and observability

Batch bounds control PUT and byte amplification. Report records per batch,
encoded bytes, flush reason, append latency, and failed/unacknowledged batches.

## Operational footprint

Writer and timer are in-process. No register or agent.

## Alternatives and consequences

A writer lease was deferred to avoid duplicating VirtualLog fencing. A future
multi-writer design must couple slot and record-range allocation.
