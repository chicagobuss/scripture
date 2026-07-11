# Decision: bounded single-writer service boundary and raw-lines lab adapter

- Status: accepted
- Date: 2026-07-11
- Layer: service / transport
- Obligation basis: 2, 3, 5, 8, 15

## Context

The library writer is intentionally `&mut self`, sequential, and not a
distributed exclusion mechanism. Network protocol experiments need concurrent
producers without letting each transport independently allocate offsets or
invent acknowledgement semantics.

## Decision

`scripture-service` provides a bounded `JournalHandle` and one `JournalActor`
per journal. The actor exclusively owns the existing `JournalWriter`.
`submit(records)` waits for bounded-channel capacity, then returns an
acknowledgement future. That future succeeds only after the actor receives a
Holylog acknowledgement for the containing batch. v1 keeps pipeline depth one
and emits one batch per submission; batching is an actor-internal future
change, never a transport concern.

The actor has terminal failure behavior. On a kernel failure (or offset-space
exhaustion) it returns the mapped error for that request and returns the same
terminal error to every later queued submission. It does not leave
acknowledgement futures pending. `AtomicLogError::Sealed` maps to `Sealed {
slot }`; other kernel failures map to `Unavailable`. Deterministic bad input
(such as a non-finite float) is rejected only for that request and does not
wedge the actor. An unavailable or sealed request may still have durable zombie
bytes, so an error is never evidence that its records are absent.

`scriptured` supplies only a lab-grade raw-lines TCP adapter: one newline
delimited byte line maps to one record and, in FIFO line order, receives
`OK <first-offset> <next-offset>`. The listener configuration is fixed for a
connection. A malformed/oversized line receives `ERR` and closes; a disconnect
before `OK` leaves an unacknowledged suffix with unknown outcome and callers
must retry at-least-once.

## Correctness

All transports share one local allocator and one total order per actor. They
cannot falsely acknowledge buffered data, and actor failure cannot strand
callers behind an unreported kernel error. Cross-transport ordering is simply
the actor's arrival order; no fairness claim is made.

This is a **singleton soft-state ingest component per journal**. It has no
cross-process writer fencing, failover, or crash-restart guarantee. The
library's same-live-log `JournalWriter::recover` helper is not a substitute for
such a protocol. A service crash therefore makes the journal unappendable
until an operator establishes the next safe generation.

## Kafka-mappability

The acknowledged-produce subset maps to `submit` plus its durable future. The
raw text protocol does not attempt Kafka wire compatibility and carries no
consumer state.

## Deterministic validation

Tests prove dense acknowledgement ranges under concurrent submitters, terminal
sealed behavior for current and queued acknowledgements, and an actual
loopback TCP history with two raw lines, FIFO acknowledgements, and durable
readback.

## Cost and observability

The queue bounds process memory but does not change Holylog PUT amplification.
At depth one, one submitted raw line means one durable batch and its final seal
read. Report queue saturation, pending submissions, terminal transitions,
unacknowledged disconnects, records per durable batch, and append latency.
Tail-cache amortization is deferred; this write-only adapter does not poll or
serve consumers.

## Operational footprint

One process-local actor task and bounded queue per configured journal. The
raw-lines adapter opens one TCP connection task. No schema registry, directory,
authorization layer, durable service metadata, HTTP endpoint, or listener
supervisor is included.

## Alternatives and consequences

Letting a TCP handler own a writer would make every connection a competing
allocator; rejected. Returning HTTP-style accepted offsets before durability
would be incoherent because offsets become meaningful only at durable append;
rejected. A production listener remains gated on fenced recovery/VirtualLog,
schema registration, quotas, observability, batching, and a tail cache.
