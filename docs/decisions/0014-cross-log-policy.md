# Decision: cross-log policy boundaries (tdv2)

- Status: proposed
- Date: 2026-07-20
- Layer: metadata | operations
- Obligation basis: multi-log governance from trinity-data-view-2; Scripture shard simplification plan

## Context

Scripture is converging on one authoritative VirtualLog root per data shard and a
separate consolidated control plane for fleet/config state. Cross-log references
are tempting as a general authorization mechanism, but tdv2 review showed witness
embedding is only safe for monotonic epoch fencing unless the target log carries
replicated source state.

## Decision

Define two log classes for the near-term architecture:

1. **Data-shard log** — append-critical producer path; synchronous with exactly one
   log per shard; owns dense offsets, event-id dedup, and immutable blob refs via
   the deterministic shard reducer.
2. **Control log** — low-volume fleet, group, and configuration metadata; may lag
   the data plane; never grants write authority by itself.

Cross-log operations are classified explicitly:

| Concern | Atomicity required? | Mechanism |
| --- | --- | --- |
| Writer fencing / serving epoch | Yes (one root) | SCAR in VirtualLog application fence |
| Producer retry dedup | No (shard-local) | Shard reducer event-id set |
| Stream migration | Yes (two-phase) | Seal predecessor generation, provision successor, root CAS — not witness-only |
| Retention vs consumer progress | No (derived) | Checkpoints on data shard; control log holds policy hints only |
| Fleet/group membership | No (eventual) | Control log records; data path reads cached materialization |

Witness bytes may fence **monotonic epoch** only. They must not authorize append,
offset assignment, or blob admission without a fresh root observation on the target
data shard.

## Correctness

- Relies on Holylog VirtualLog root CAS and generation cutover for fencing.
- Shard reducer remains pure and replayable; cross-log coupling stays outside it.
- Unsafe witness passing is excluded from the first reducer rather than hidden
  behind convenience APIs.

## Deterministic validation

Before deleting legacy Canon-fence transition paths:

- Root-fence and legacy Canon-fence recovery must reach the same serving decision
  on recorded traces.
- Shard reducer tests must reject stale epochs, duplicate insertion, and
  out-of-sequence events under replay.
- Holylog correctness harness must model effect-vs-completion loss at register and
  LogDrive boundaries (`StoragePhase`).

## Alternatives and consequences

- **One log per feature** — rejected; proliferation cost and unsafe cross-log
  coupling dominate.
- **Generic Holylog SMR kernel** — deferred; kernel stays at VirtualLog until
  multiple consumers prove a stable abstraction.
