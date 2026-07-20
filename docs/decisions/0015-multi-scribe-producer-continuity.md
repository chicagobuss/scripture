# Decision: multi-scribe continuity via producer-edge outbox

- Status: accepted (lab proof)
- Date: 2026-07-20
- Layer: write path | operations
- Obligation basis: foundation producer continuity across a Scribe fleet

## Context

The existing HA path is single-writer drain→seal→replace with an explicit
scribe-side loss budget for buffered-but-uncommitted work. That cannot satisfy
"keep producing through a rolling restart with no dropped locally durable
records."

## Decision

Ship a **different** continuity design alongside the legacy path:

1. **Active-active Scribes** serve disjoint Verses concurrently.
2. Producers admit into a **ContinuityOutbox** (local-durable) before routing.
3. Route / Scribe unavailability retains pending work and retries after promote.
4. Rolling restart crashes a serving Scribe and promotes a successor; the
   outbox absorbs the gap.
5. Success means every locally durable identity eventually receives a committed
   receipt — zero drop of outbox-admitted work.

This is implemented as campaign Composition scenario
`multi-scribe-rolling-restart` and `scripture_producer::ContinuityOutbox`.

## Correctness

- Write authority remains the VirtualLog root / Serving Authority gate.
- Outbox never grants writes; retries are at-least-once by stable event identity.
- Unrelated Verses continue serving during another Verse's restart.

## Deterministic validation

`cargo test -p scripture-campaign multi_scribe` runs three concurrent Verses,
continuous produce (≥600 admissions), one rolling restart pass per Verse, and
asserts `local_durable == committed` with `pending == 0`.

## Non-claims

- Does not replace drain→seal→replace for lost-sequencer recovery.
- In-memory outbox is lab-grade; production requires a fsynced store.
- Does not yet prove multi-process / multi-pod placement.
