# Decision: stable journal identity without a v0 directory

- Status: accepted
- Date: 2026-07-11
- Layer: metadata
- Obligation basis: 4

## Context

VirtualLog membership and its MetaStore do not exist yet. A directory service
would prematurely choose their metadata and failure model.

## Decision

v0 journals are operator-configured handles, not directory-managed named
resources. Every journal has a stable 128-bit `JournalId`, independent of its
optional display name and physical prefixes. Batches and checkpoints carry the
identity. Namespace non-overlap for data, seal, and trim storage remains an
explicit caller precondition.

Create/delete, discovery, name reuse, tenancy, logical-stream multiplexing,
and generation membership are deferred to a later directory/MetaStore record.

## Correctness

Readers reject batches and checkpoints for another journal, preventing an
operator from silently reusing a checkpoint with a different namespace.

## Kafka-mappability

A future directory can map topic-partition names to stable journal identities.

## Deterministic validation

Test identity round trips and wrong-journal batch/checkpoint rejection.

## Cost and observability

Sixteen identity bytes per batch and checkpoint; no metadata requests in v0.

## Operational footprint

Operator configuration only.

## Alternatives and consequences

A conditional-register directory is deferred until it can share the
VirtualLog MetaStore design rather than becoming a second authority.
