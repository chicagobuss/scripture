# Decision: publish backend-neutral operation and byte formulas with v0

- Status: accepted
- Date: 2026-07-11
- Layer: operations
- Obligation basis: 9

## Context

Object-store-native messaging trades local broker complexity for visible
request, byte, latency, and idle-poll costs. These cannot remain anecdotal.

## Decision

`docs/cost-model.md` defines backend-neutral formulas for append, read,
tail-poll, idle subscriber, trim metadata, and retained bytes. Provider price
tables remain inputs outside the model.

Counters are labeled by scope. The Protoscripture numbers are deterministic
in-memory QuorumLogDrive data-plane counters, not end-to-end provider cost.
Provider-realistic experiments must include durable seal/trim metadata and the
poll cadence.

## Correctness

No correctness guarantee depends on a cost optimization. Logical operations
and physical amplification are kept distinct.

## Kafka-mappability

No protocol effect.

## Deterministic validation

Use algebraic examples and metric assertions for known append/read histories;
later compare formulas with backend adapter counters.

## Cost and observability

This decision defines the accounting surface itself: logical records/batches,
Qw writes, Qr reads, tail/seal operations, bytes uploaded/downloaded/retained,
poll cadence, and metadata operations.

## Operational footprint

Documentation and counters only.

## Alternatives and consequences

Bundled provider prices were rejected because they age independently. A later
agent/cache tier must add its compute and amplification terms explicitly.
