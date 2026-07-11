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
- `QuorumMetrics` gave per-run logical cost numbers with zero effort: the
  spike's 3 appends / 5 batch reads / 1 trim / 1 seal produced 6 replica
  writes, 10 replica reads, 2 tail queries, 606 bytes up, 1010 bytes down.
  This is the raw material obligation 9's cost model needs.

## Gaps

1. **No exported deterministic in-memory LogDrive.** Every consumer (and every
   holylog integration test) rebuilds the same ~50-line wrapper around
   `ReferenceLogDrive`. Already on holylog's Milestone 1 "next" list as
   reusable test infrastructure; the spike confirms it should be a public,
   documented kernel offering (a `holylog::memory` module or feature), ideally
   with the fault-injection hooks the scripted test drive already has.
   → holylog.

2. **AtomicLog assembly is verbose and K appears twice.** Wiring
   drive + sequencer + seal + trim + K (with K repeated into the sequencer) is
   five lines of `Arc::new` per log. The construction-time K-coherence check
   catches mistakes, but a small builder (or a config struct that constructs
   the in-memory components by default) would make the common case harder to
   get wrong. Matters more once Scripture multiplexes many logs. → holylog,
   minor.

3. **Construction error types don't compose.** `QuorumLogDrive::new` returns
   `QuorumError`, which has no path into `AtomicLogError`; the spike had to
   wrap it as `DriveError::backend` manually. Runtime composition is clean
   (the quorum *is* a `LogDrive`); only constructor errors clash. A
   `From<QuorumError> for DriveError` (or a shared construction error) would
   remove the wart. → holylog, minor.

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
