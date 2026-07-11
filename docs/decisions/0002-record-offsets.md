# Decision: dense record offsets with physical resume hints

- Status: accepted
- Date: 2026-07-11
- Layer: envelope/format
- Obligation basis: 1

## Context

One Holylog slot contains a batch, but consumers need a stable identity for
each record that remains meaningful across Loglet generations.

## Decision

`RecordOffset(u64)` is the public, dense record identity. A batch stores its
base offset, and record `i` has `base + i`. Physical `(generation, slot,
record_index)` locations are implementation details and may appear only as
validated, replaceable resume hints.

The v0 writer is the sole range allocator and advances by the number of records
in each acknowledged batch. This is a v0 mechanism, not a permanent claim that
dense offsets inherently require one writer. A future coordinator may allocate
an ordered `(slot, record range)` pair atomically.

Writer restart and generation replacement must recover the next offset from a
fenced final boundary. Until VirtualLog exists, v0 makes no cross-process
restart or failover claim.

## Correctness

Offsets are assigned once, increase without gaps for acknowledged records, and
are checked for overflow before append. They are embedded in immutable batch
bytes rather than inferred from mutable metadata.

## Kafka-mappability

The dense `u64` is directly mappable to a Kafka partition offset for the
constrained future gateway surface.

## Deterministic validation

Generate batch sizes and verify dense ranges, overflow rejection, resume-hint
validation, and generation-boundary recovery once VirtualLog exists.

## Cost and observability

Eight base-offset bytes per batch; no additional request in v0.

## Operational footprint

The v0 allocator is in-process and soft-state. A future shared allocator needs
its own decision.

## Alternatives and consequences

Public `(slot, index)` offsets were rejected because they expose kernel layout
and complicate reconfiguration. Deriving cumulative offsets by rescanning all
prior batches was rejected as the normal path.
