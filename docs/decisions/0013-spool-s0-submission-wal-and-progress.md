# Decision: S0 submission WAL, progress evidence, and fail-closed recovery

- Status: accepted (S1a implementation gate)
- Date: 2026-07-14
- Layer: write path, operations
- Obligation basis: 2, 6, 15
- Related: 0010 (acknowledgement), 0011 (spool epochs), design proposal
  `scripture/design-proposal-spool-code-structure` v2; Holylog Decision 0012 remains
  open and is **out of scope** here

## Context

Phase 1 is committed-only. Introducing a local submission WAL without a settled
receipt/progress contract would reopen the `Journaled` promise of 0010 on a
single disk, or silently resume an old VirtualLog generation after restart
(forbidden by 0011). This record is the S0 gate for S1a/S1b-lite: durable local
evidence and operator-readable classification only.

## Decision

### One submission → one committed receipt future

A submission requests and receives **one** receipt future. That future resolves
at most once, and only to a terminal committed outcome or a typed error. There
is no multi-stage receipt, no public sub-committed acknowledgement, and no
`Journaled` emission from a single-node WAL.

`AckLevel` remains `#[non_exhaustive]` (already landed). A public level below
`Committed` requires its own decision and a disk-quorum + handoff contract.

### WAL frames carry Submission identity only

Each submission frame stores exactly:

`(producer_id, producer_epoch, sequence, journal_id, records)`

Frames MUST NOT store chunk placement, Loglet address, Canon revision, offsets,
or seal metadata. Chunk formation stays after the WAL, on the existing driver
path, with an unchanged submission identity.

### S1a durability ordering (synchronous)

Default S1a is **append + sync before forward**. There is no implicit group
commit and no delayed fsync window. The cell sequence is:

1. reserve against physical spool limits (reject/block **before** accept);
2. append the submission frame and sync;
3. forward the unchanged submission to the existing driver/service boundary;
4. await that path's committed receipt;
5. append + sync a **progress** frame for the same submission identity;
6. resolve the **same** committed receipt to the caller.

A remote committed success without a durable local progress frame is classified
`PendingUnclassified` on restart — never assumed committed or auto-replayable.

### Progress evidence: per-submission frames, not a contiguous watermark

S1a records durable commit evidence as an explicit **progress frame** naming the
submission identity. A contiguous watermark is deferred until a single-writer
in-order proof and GC policy exist. Progress frames are append-only; S1a
**deletes nothing** merely because a process believes it committed (no segment
retirement).

### One process owns one spool directory

A spool directory has at most one live process owner. Exclusive ownership is
taken with a PID lock file: a live peer fails closed; a lock left by a dead
process may be reclaimed so crash restart can classify without serving. This is
**local-host best effort** (PID reuse is a known weakness) and must not be
treated as a portable lock proof or shared-filesystem coordination. A shared
filesystem is not a coordination mechanism and is not a failure domain.

### Fail-closed frame and IO outcomes

| Condition | Handling |
|---|---|
| Terminal truncated / CRC-invalid frame | `TornTerminalFrame`; prior valid history retained |
| CRC / decode failure in the middle | `CorruptHistory`; refuse Serving |
| Disk full / fsync error | typed IO/capacity error; never success |
| Duplicate submission identity in WAL | reject on append when Serving; `CorruptHistory` if found on scan |
| Progress without a prior submission | `CorruptHistory` |
| Remote commit, missing local progress | `PendingUnclassified` |
| Submission + matching progress | `CommittedLocally` |

### Restart never reopens the old generation for writes

On open, the cell scans and classifies. Any non-empty history lands in
`RecoveryRequired`: the cell may report classification counts, but it must not
`submit`, reattach the old open generation, mutate Canon, or replay. Fresh empty
directories may enter `Serving`. This is local durable evidence, not HA recovery.
Decision 0012 provisioning / seal-and-replace remains a separate gate.

### Honest durability boundary

One local spool disk is **not** a disk quorum, peer-memory replication, or a
cross-failure-domain promise. It does not satisfy 0010's `journaled` level.

## Correctness

- Relies on Holylog/object-store commit for public durability; the WAL only
  records what was submitted and what this process locally witnessed as committed.
- Caller obligation: a dropped receipt future after admission must not be read as
  success; S1a never resolves success without driver commit **and** progress sync.
  Progress completion is owned by the cell completer (`SpoolCell::run`), not by
  polling the caller future.
- After a durable submission WAL frame, forward / receipt / progress failure
  poisons the live cell: later admissions are refused for this process lifetime.
- Safety: no auto-resubmit; no serving after non-empty restart; no public
  below-Committed ack.
- Liveness: Serving path may block on reservation; RecoveryRequired requires
  operator / successor protocol (out of S1a scope).

## Kafka-mappability

Unchanged: dense offsets and checkpoints still come only from committed chunks.

## Deterministic validation

Required by the S1a work package: frame round-trip and CRC32C known vector;
corrupt/truncated inputs; WAL-before-forward ordering; no success before progress;
limit enforcement; drop-does-not-cancel; FileSpoolStorage reconstruction;
fleet-lab `--spool-dir` crash restart → `RecoveryRequired` and zero new writes.

## Cost and observability

Local fsync per submission and per progress frame (two syncs in the happy path).
Status surfaces classification counts only — never remote secrets or raw payloads.

## Operational footprint

Self-hosted local state under an operator-chosen directory. Soft-state discovery
and peer replication are not introduced.

## Alternatives and consequences

- **Contiguous watermark only** — rejected for S1a because pipelined commits can
  make the watermark incomplete without per-identity evidence.
- **Public `Journaled` from one fsync** — rejected (0010/0011); needs quorum +
  handoff.
- **Auto-replay on restart** — rejected; would reopen a dead generation.
- **Segment GC in S1a** — deferred until retirement is provably safe.
