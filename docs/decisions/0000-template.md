# Decision: concise title

- Status: proposed
- Date: YYYY-MM-DD
- Layer: envelope/format | write path | read path | consumer state | metadata | retention | operations
- Obligation basis: which numbered Scripture design obligation(s) this answers

## Context

What concrete problem or uncertainty requires a decision? State the current
assumptions and the smallest affected surface.

## Decision

What will Scripture do? Separate mechanism from policy and identify any
intentionally unsupported behavior.

## Correctness

- Which kernel (holylog) guarantees does this rely on, exactly?
- Which caller obligations does it create or discharge?
- What safety and liveness properties result?

## Kafka-mappability

Does this preserve ordered named streams, dense consumer-visible offsets (or a
stable translation), and consumer checkpoints? If the semantics are richer
than a gateway could translate, say so explicitly and confirm the constrained
subset still exists.

## Deterministic validation

List executable histories, schedules, failures, and replay artifacts required
before acceptance. Distinguish targeted examples from generated evidence.

## Cost and observability

Account for requests, bytes, storage duration, amplification, polling, and
idle costs, in the published cost-model units. Name the cost-model line each
API choice changes.

## Operational footprint

Classify any new component as delegated, small pluggable register, soft-state,
or stateless. A new self-hosted stateful component requires its own decision
record.

## Alternatives and consequences

What credible alternatives were rejected, and what future work or migration
cost does this decision create?
