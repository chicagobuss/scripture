# Decision: versioned canonical envelope and self-contained batches

- Status: accepted
- Date: 2026-07-11
- Layer: envelope/format
- Obligation basis: 1, 10; layout hooks for 13 and 14

## Context

Holylog stores opaque immutable bytes. Scripture must define durable record and
batch bytes before product data is written, while avoiding claims that the
kernel can perform partial reads that its public API does not expose.

## Decision

Every batch is a canonical, self-contained value with a magic number, major and
minor format versions, `JournalId`, dense base `RecordOffset`, record count,
record section, footer index, footer length, and footer magic.

Attributes are canonically ordered by key. Initially supported scalar values
are UTF-8 string, signed 64-bit integer, and boolean. The wire encoding uses a
type tag and length-delimited value so later scalar types need not change the
outer framing. Duplicate attribute keys, malformed values, non-canonical
ordering, trailing bytes, and unknown major versions are rejected.

The footer records each record's byte offset. In v0 this accelerates in-memory
decoding only. Holylog's `LogDrive::read` returns a whole value, so Scripture
does not claim suffix-range reads, pruning, or record-level object-store I/O.

## Correctness

Canonical encoding ensures that an identical logical retry proposes identical
bytes to Holylog's single-value register. The journal identity and base offset
bind decoded bytes to their logical stream and ordered record range.

## Kafka-mappability

Dense record offsets and ordered batches admit a future topic-partition
translation. Typed attributes and the footer are additive native metadata.

## Deterministic validation

Property-test canonical round trips, malformed/truncated inputs, unknown
versions and types, duplicate/out-of-order keys, offset overflow, journal
identity mismatch, and corrupted footer offsets.

## Cost and observability

Header, per-attribute framing, and footer bytes contribute to uploaded and
retained bytes per batch. The footer currently saves no GET requests.

## Operational footprint

Pure codec; no component or service.

## Alternatives and consequences

A separate manifest object is deferred because it adds a PUT and association
protocol per batch. Partial reads require a later Holylog capability and quorum
correctness decision. A future major format may change physical layout.
