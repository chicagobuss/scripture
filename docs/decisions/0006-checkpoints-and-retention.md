# Decision: consumer-owned next-offset checkpoints and manual retention

- Status: accepted
- Date: 2026-07-11
- Layer: consumer state | retention
- Obligation basis: 5, 7

## Context

v0 needs resumable reads and trim exposure without claiming coordinated
delivery or introducing another durable state service.

## Decision

A checkpoint identifies the journal and the **next record to consume**. It may
include a physical resume hint, which is validated and replaceable. Checkpoint
storage and the point at which application processing advances it belong to
the consumer. At-least-once is the only claimed delivery guarantee.

Retention is a manual trim operation under one retention authority per
journal. There is no automatic policy or physical reclamation in Scripture v0.

## Correctness

The next-offset convention removes last-read/last-processed ambiguity from the
serialized value. A bad hint cannot change the semantic offset. Trim gaps are
reported to the application, which decides whether data loss is acceptable.

## Kafka-mappability

The next record offset maps naturally to committed consumer offsets. Consumer
groups and queue acknowledgements remain absent.

## Deterministic validation

Test checkpoint round trips, identity mismatch, valid/invalid hints, replay
after processing failure, and checkpoints below trim.

## Cost and observability

Scripture incurs no checkpoint-storage request in v0. Applications must count
their chosen store. Manual trim has the kernel trim-check/metadata cost and no
retained-byte savings until physical GC exists.

## Operational footprint

Consumer-owned storage; no Scripture service.

## Alternatives and consequences

Server-owned checkpoints, groups, exactly-once claims, and retention policy
engines are deferred.
