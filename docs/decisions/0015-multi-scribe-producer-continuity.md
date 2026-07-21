# Decision: multi-scribe continuity via producer-edge outbox

- Status: accepted (lab proof + automatic rejoin + active-active release proof)
- Date: 2026-07-20 (updated 2026-07-21)
- Layer: write path | operations
- Obligation basis: foundation producer continuity across a Scribe fleet

## Context

The existing HA path is single-writer drain→seal→replace with an explicit
scribe-side loss budget for buffered-but-uncommitted work. That cannot satisfy
"keep producing through a rolling restart with no dropped locally durable
records."

## Decision

Ship a **different** continuity design alongside the legacy path:

1. Fleet members share one `(Canon, Verse)` and start with the same normal
   lifecycle (`scripture scribe run`). There is no standby role and no operator
   promote step in the preferred path.
2. Producers admit into a **ContinuityOutbox** only after append+fsync
   (local-durable); progress frames are synced on Canon commit.
3. Route / Scribe unavailability retains pending work and retries after the
   lawful successor is published by the durable root CAS.
4. Peer unreachability may **arm** recovery; the conditional VirtualLog root
   remains the only write-authority grant.
5. Success means every locally durable identity eventually receives a committed
   receipt — zero drop of outbox-admitted work.
6. **Active-active** (same Canon, disjoint Verses on concurrent Scribes) is a
   producer-visible property proven hermetically: multi-bootstrap route refresh
   through a Verse promotion under traffic, no stale committed receipt, sibling
   Verse isolation, and contiguous consumer history.

Implemented as:

- `scripture::ContinuityOutbox` (`spool/continuity.rs`)
- `scripture_runtime::ScribeLifecycle` / `scripture scribe run`
- Hermetic same-Verse rejoin: `cargo test -p scripture-runtime --test scribe_rejoin`
- Hermetic active-active release proof:
  `cargo test -p scripture-runtime --test active_active_release`

## Correctness

- Write authority remains the VirtualLog root / Serving Authority gate.
- Outbox never grants writes; retries are at-least-once by stable event identity.
- Returning former writers rejoin as healthy non-writers until the root authorizes
  them again.
- A bootstrap / directory route is never a write grant.

## Non-claims

- Does not prove unbounded distributed liveness from a bounded peer-grace arm.
- Does not yet prove multi-process / multi-pod placement beyond hermetic tests.
- Local-disk outbox survival is under that producer's disk assumptions only.
- Hermetic active-active is authoritative for release; a real k0s drill remains
  optional follow-on capacity evidence, not a substitute for this gate.
