# Kernel gap report — protoscripture spike, 2026-07-11

Findings from building and running the first consumer of holylog's public API
(`crates/protoscripture` against holylog `5b222b5`). "Gap" here means friction
a real consumer would hit, not a defect; each item names where it should be
addressed.

## Confirmed working from the consumer's seat

- The full Loglet lifecycle composes from public API alone: append over a
  three-replica `QuorumLogDrive`, checked-tail reads via `read_next`, logical
  `prefix_trim` with the deterministic `Trimmed { trim_point }` error letting
  a lagging consumer skip forward cleanly, sealing, and post-seal recovery
  reads. No private interfaces were needed.
- The batching boundary holds: payloads are opaque `Bytes` end to end, no size
  ceiling interfered, and the kernel imposed no record/flush concepts on the
  envelope or codec.
- `QuorumMetrics` gave per-run figures with zero effort: the spike's
  3 appends / 5 batch reads / 1 trim / 1 seal produced 6 replica writes,
  10 replica reads, 2 tail queries, 606 bytes up, 1010 bytes down.
  **Label these precisely: they are deterministic in-memory QuorumLogDrive
  data-plane operation counters, not end-to-end object-store cost data.**
  The run used `InMemorySeal` and `InMemoryTrimPoint`, so the durable seal
  GET that every canonical `check_tail` (and acknowledged append) performs is
  absent, and no idle polling loop was modeled. The first provider-realistic
  cost experiment needs durable metadata objects, a defined polling cadence,
  and metadata plus data-plane operations in one ledger (obligations 3 and 9).

## Gaps

1. **No exported deterministic in-memory LogDrive.** ~~Every consumer rebuilds
   the same wrapper around `ReferenceLogDrive`.~~ **Addressed** at holylog
   `a6b4660`: `holylog::memory::InMemoryLogDrive` is public; the spike now
   uses it. Fault-injection hooks remain test infrastructure per the roadmap.

2. **AtomicLog assembly is verbose and K appears twice.** **Addressed** at
   holylog `a6b4660`: `AtomicLog::builder(drive, k)` supplies coherent
   in-memory defaults (the default sequencer is constructed from the builder's
   K); explicit sequencers still fail construction on mismatch.

3. **Construction error types don't compose.** **Resolved by policy** per
   review: `QuorumError` is a construction/configuration error and must not be
   flattened into runtime `DriveError`. The consumer keeps a typed setup error
   (see the spike's `SetupError`); the builder removes most of the wiring that
   made this annoying.

4. **Every tail poll costs a sequencer call plus a seal read.** `check_tail`
   is the only way to learn about new entries, so a polling consumer pays that
   pair per poll (a real GET against durable seal storage). Not a kernel
   defect — the kernel is honest about it — but it makes obligation 3's
   read-path fork and obligation 9's $/idle-subscriber line very concrete.
   → Scripture design (obligations 3 and 9).

5. **Per-entry reads price the batch as the unit.** Each `read_next` is one
   quorum point read (Qr replica GETs). At batch granularity that is fine; at
   record granularity it would be pathological. This confirms obligation 1's
   framing: consumer-visible offsets must address records *within* batches
   without per-record kernel reads (slot + intra-batch index, manifests).
   → Scripture design (obligation 1).

## Explicitly out of the spike's scope

Single-threaded deterministic execution only: no concurrency, no fault
injection, no real object storage, no durability claims. Those are covered by
holylog's own roadmap (scripted substrate, history checker) and by Scripture's
obligations, in that order.
