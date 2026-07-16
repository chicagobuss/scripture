# Decision: client-direct pull reader with explicit trim gaps

- Status: accepted
- Date: 2026-07-11
- Layer: read path
- Obligation basis: 3

## Context

v0 needs the smallest honest read path over Loglet `check_tail` and
`read_next`, including visible request costs and lagging-consumer behavior.

## Decision

`JournalReader` is client-direct and pull-based. Tail refresh is an explicit
`refresh_tail` operation, so the caller controls and can observe poll cadence.
`read_next` never polls implicitly; it returns `CaughtUp` at the last checked
tail. The reader fetches whole batches, validates journal identity and dense
offset continuity, and yields records in order. There is no agent,
notification channel, filtering, or consumer group in v0.

A logical trim is returned as a typed gap containing the requested slot and
new start slot. It is never silently skipped. The first surviving batch
establishes the corresponding next `RecordOffset`.

## Correctness

Reads stay below a previously checked tail and rely on Holylog's immutable
point reads. Batch identity and base-offset checks detect namespace mixups and
sequence discontinuities.

## Kafka-mappability

Ordered pull reads over dense offsets map to a constrained fetch path.

## Deterministic validation

Test open/sealed reads, multiple records per batch, polling without progress,
trim gaps, corrupt/wrong-journal batches, discontinuous offsets, and checkpoints
with invalid hints.

## Cost and observability

Each tail refresh incurs the kernel tail/seal work; each new slot read incurs
Qr whole-value reads. Track polls, empty polls, slots and bytes read.

## Operational footprint

Client library only.

## Alternatives and consequences

Push notifications and shared read caching are deferred. The API must not make
correctness depend on a future notification hint.
