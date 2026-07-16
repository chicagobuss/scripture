//! A pure reference model of the chunk driver (Phase 1, step 2).
//!
//! No async, no I/O, no clock — a state machine that a generated *semantic
//! schedule* drives. It exists to be wrong cheaply: a design error found here
//! costs an afternoon, and the same error found in the actor costs a rewrite.
//!
//! The schedule names **protocol boundaries**, never anonymous futures. A
//! minimized counterexample must be able to say *which protocol action* went
//! wrong, and `Fail(operation 3)` cannot.
//!
//! Note what the schedule deliberately **cannot** do: cancel an in-flight
//! append. The actor owns and awaits that future by contract (scripture 0010),
//! precisely because cancelling it abandons a sequencer slot and wedges the log
//! forever. A harness that could cancel it would be testing a system we have
//! promised not to build.

#![allow(dead_code, unreachable_pub)]

use std::collections::BTreeMap;

use scripture::{
    ChunkId, Effect, Event, Ledger, ProducerId, RecordOffset, RejectReason, TerminalOutcome,
};

/// `(highest committed sequence, sequence -> the offsets it received)`.
type DedupEntry = (u64, BTreeMap<u64, (RecordOffset, u32)>);
/// The dedup window, keyed by the full submission identity's producer part.
type DedupWindow = BTreeMap<(ProducerId, u32), DedupEntry>;

/// The bounds a profile publishes and the driver enforces by reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub max_chunk_records: u32,
    pub max_chunk_bytes: usize,
    pub max_record_bytes: usize,
    pub max_buffered_bytes: usize,
    pub max_inflight_chunks: usize,
    /// The bounded recovery scan (scripture 0010).
    pub recovery_scan_chunks: usize,
}

impl Policy {
    /// The hard bytes-at-risk bound. There is deliberately no `age_at_risk`:
    /// time is not bounded under provider failure, and publishing a number
    /// would be a lie (scripture 0011).
    pub fn bytes_at_risk(&self) -> usize {
        self.max_buffered_bytes + self.max_inflight_chunks * self.max_chunk_bytes
    }
}

/// One producer submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Submission {
    pub producer_id: ProducerId,
    pub producer_epoch: u32,
    pub sequence: u64,
    pub records: u32,
    pub bytes: usize,
}

/// A submission as it sits inside a chunk, with the offsets it was allocated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placed {
    pub submission: Submission,
    pub first_offset: RecordOffset,
}

/// A chunk of placed submissions. Immutable once sealed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub chunk_id: ChunkId,
    pub generation: u64,
    pub base_offset: RecordOffset,
    pub placed: Vec<Placed>,
    pub bytes: usize,
}

impl Chunk {
    pub fn records(&self) -> u32 {
        self.placed.iter().map(|p| p.submission.records).sum()
    }
}

/// What a waiter was told, if anything yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waiter {
    Pending,
    Receipt {
        first_offset: RecordOffset,
        records: u32,
    },
    Terminal(TerminalOutcome),
}

/// The durable log: chunks the kernel acknowledged, plus chunks that are durable
/// in the object store yet unmapped by a cutover and therefore invisible.
#[derive(Debug, Default, Clone)]
pub struct DurableLog {
    pub committed: Vec<Chunk>,
    pub excluded: Vec<Chunk>,
    pub sealed: bool,
    pub generation: u64,
}

impl DurableLog {
    /// Records visible in the log, in offset order.
    pub fn visible_submissions(&self) -> Vec<Placed> {
        let mut out: Vec<Placed> = self
            .committed
            .iter()
            .flat_map(|chunk| chunk.placed.iter().copied())
            .collect();
        out.sort_by_key(|p| p.first_offset.get());
        out
    }
}

/// What the driver knows about one producer, within its bounded window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProducerState {
    epoch: u32,
    highest_sequence: Option<u64>,
}

/// The driver.
#[derive(Debug, Clone)]
pub struct Driver {
    policy: Policy,
    generation: u64,
    next_offset: RecordOffset,
    open: Vec<Placed>,
    open_bytes: usize,
    inflight: Vec<Chunk>,
    sealed_not_appended: Vec<Chunk>,
    /// `(producer, epoch)` → highest committed sequence and its offsets.
    dedup: DedupWindow,
    /// The highest sequence ADMITTED, which is not the highest COMMITTED.
    ///
    /// A producer must be able to pipeline: submit seq 1 while seq 0 is still
    /// in flight. Gating admission on the committed sequence would cap every
    /// producer at one submission per round trip and destroy the reason chunks
    /// exist. Recovery rebuilds this from the committed window, because the
    /// in-flight ones are exactly what a crash loses.
    admitted_seq: BTreeMap<(ProducerId, u32), u64>,
    known_producers: BTreeMap<ProducerId, u32>,
    waiters: BTreeMap<(ProducerId, u32, u64), Waiter>,
    poisoned: bool,
    next_chunk: u8,
}

/// The whole world: driver plus durable log plus the ledger.
#[derive(Debug, Clone)]
pub struct World {
    pub driver: Driver,
    pub log: DurableLog,
    pub ledger: Ledger,
    /// Submissions the caller was told were admitted (so we can check none hang).
    pub admitted: Vec<Submission>,
}

/// A semantic schedule decision. Every variant names a protocol boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Offer a submission to the driver.
    Submit(Submission),
    /// Seal the open chunk now, regardless of bounds (an explicit flush).
    SealOpenChunk,
    /// The kernel acknowledged the oldest in-flight append.
    AppendAcknowledged,
    /// The append's outcome is unknown — an error, a timeout, or a fence. The
    /// driver must poison and must never retry.
    AppendUncertain,
    /// A cutover excluded the oldest in-flight chunk: it is durable in the
    /// object store but at or above the boundary, so it is unmapped forever.
    AppendExcludedByCutover,
    /// Issue an append for the oldest sealed-but-unappended chunk.
    IssueAppend,
    /// The owner dies. A successor recovers from durable bytes.
    OwnerCrashAndRecover,
}

impl World {
    pub fn new(policy: Policy) -> Self {
        Self {
            driver: Driver {
                policy,
                generation: 0,
                next_offset: RecordOffset::new(0),
                open: Vec::new(),
                open_bytes: 0,
                inflight: Vec::new(),
                sealed_not_appended: Vec::new(),
                dedup: BTreeMap::new(),
                admitted_seq: BTreeMap::new(),
                known_producers: BTreeMap::new(),
                waiters: BTreeMap::new(),
                poisoned: false,
                next_chunk: 0,
            },
            log: DurableLog::default(),
            ledger: Ledger::new(),
            admitted: Vec::new(),
        }
    }

    /// Bytes the driver has accepted responsibility for and not yet committed.
    /// This is the loss budget, and it is enforced, not documented.
    pub fn bytes_at_risk(&self) -> usize {
        self.driver.open_bytes
            + self
                .driver
                .inflight
                .iter()
                .chain(&self.driver.sealed_not_appended)
                .map(|chunk| chunk.bytes)
                .sum::<usize>()
    }

    /// Runs the driver to quiescence: seal what is open, append what is sealed,
    /// acknowledge what is in flight.
    ///
    /// This is the **fairness assumption** made explicit. The liveness claim in
    /// scripture 0011 is conditional — *if the owner remains alive and each
    /// storage operation eventually resolves*, every admitted submission
    /// resolves. A schedule that simply stops mid-flight has not violated that
    /// claim; it has merely ended. Asserting liveness without this would be
    /// asserting something we never promised.
    pub fn quiesce(&mut self) {
        if self.driver.poisoned {
            return;
        }
        self.seal_open_chunk();
        for _ in 0..64 {
            if self.driver.sealed_not_appended.is_empty() && self.driver.inflight.is_empty() {
                break;
            }
            self.issue_append();
            self.append_acknowledged();
        }
    }

    pub fn step(&mut self, decision: Decision) {
        match decision {
            Decision::Submit(submission) => self.submit(submission),
            Decision::SealOpenChunk => self.seal_open_chunk(),
            Decision::IssueAppend => self.issue_append(),
            Decision::AppendAcknowledged => self.append_acknowledged(),
            Decision::AppendUncertain => self.append_uncertain(),
            Decision::AppendExcludedByCutover => self.append_excluded(),
            Decision::OwnerCrashAndRecover => self.crash_and_recover(),
        }
    }

    fn submit(&mut self, submission: Submission) {
        let driver = &self.driver;

        if driver.poisoned {
            self.ledger.event(Event::SubmissionRejected {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
                reason: RejectReason::DriverStopped,
            });
            return;
        }
        if submission.bytes > driver.policy.max_record_bytes {
            self.ledger.event(Event::SubmissionRejected {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
                reason: RejectReason::RecordTooLarge,
            });
            return;
        }

        // Epoch admission (scripture 0010).
        match driver.known_producers.get(&submission.producer_id).copied() {
            Some(highest) if submission.producer_epoch < highest => {
                self.ledger.event(Event::SubmissionRejected {
                    producer_id: submission.producer_id,
                    sequence: submission.sequence,
                    reason: RejectReason::FencedProducer,
                });
                return;
            }
            _ => {}
        }

        let key = (submission.producer_id, submission.producer_epoch);

        // Dedup: a duplicate is answered with the ORIGINAL offsets, never a
        // guess. This is the whole point of storing the record span.
        if let Some((highest, window)) = driver.dedup.get(&key)
            && submission.sequence <= *highest
        {
            if let Some((first_offset, records)) = window.get(&submission.sequence).copied() {
                self.ledger.event(Event::SubmissionDeduplicated {
                    producer_id: submission.producer_id,
                    producer_epoch: submission.producer_epoch,
                    sequence: submission.sequence,
                    first_offset,
                });
                self.ledger.event(Event::ReceiptReleased {
                    producer_id: submission.producer_id,
                    producer_epoch: submission.producer_epoch,
                    sequence: submission.sequence,
                    first_offset,
                    records,
                });
                return;
            }
            // Outside the window: we cannot know. Say so.
            self.ledger.event(Event::SubmissionRejected {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
                reason: RejectReason::IndeterminateProducer,
            });
            return;
        }

        // A retry of a submission that is admitted but not yet committed. Its
        // offsets are allocated and its waiter is pending, so admitting a second
        // copy would duplicate the record. Do not admit; the original resolves
        // and the retry shares its fate. (The real driver joins the caller to the
        // original receipt future.)
        if let Some(highest_admitted) = driver.admitted_seq.get(&key).copied()
            && submission.sequence <= highest_admitted
        {
            return;
        }

        // Sequence gap — measured against the highest ADMITTED sequence, not the
        // highest committed one, so a producer may pipeline.
        let expected = driver
            .admitted_seq
            .get(&key)
            .map(|highest| highest.saturating_add(1))
            .or_else(|| {
                driver
                    .dedup
                    .get(&key)
                    .map(|(highest, _)| highest.saturating_add(1))
            })
            .unwrap_or(0);
        if submission.sequence != expected {
            self.ledger.event(Event::SubmissionRejected {
                producer_id: submission.producer_id,
                sequence: submission.sequence,
                reason: RejectReason::OutOfSequence,
            });
            return;
        }

        // Backpressure: never accept beyond the budget. A submission that would
        // breach it blocks; in the model, it is simply not admitted.
        let policy = self.driver.policy;
        let would_be_at_risk = self.bytes_at_risk() + submission.bytes;
        let pipeline_full = self.driver.inflight.len() + self.driver.sealed_not_appended.len()
            >= policy.max_inflight_chunks;
        let open_would_overflow =
            self.driver.open_bytes + submission.bytes > policy.max_chunk_bytes;
        if would_be_at_risk > policy.bytes_at_risk() || (pipeline_full && open_would_overflow) {
            // Not admitted: nothing reserved, nothing promised.
            return;
        }

        // Admit.
        let first_offset = self.driver.next_offset;
        let placed = Placed {
            submission,
            first_offset,
        };
        self.driver.next_offset = first_offset
            .checked_add(submission.records as usize)
            .expect("offset space");
        self.driver.open.push(placed);
        self.driver.open_bytes += submission.bytes;
        self.driver.admitted_seq.insert(key, submission.sequence);
        self.driver.known_producers.insert(
            submission.producer_id,
            submission.producer_epoch.max(
                self.driver
                    .known_producers
                    .get(&submission.producer_id)
                    .copied()
                    .unwrap_or(0),
            ),
        );
        self.driver.waiters.insert(
            (
                submission.producer_id,
                submission.producer_epoch,
                submission.sequence,
            ),
            Waiter::Pending,
        );
        self.admitted.push(submission);
        self.ledger.event(Event::SubmissionAdmitted {
            producer_id: submission.producer_id,
            producer_epoch: submission.producer_epoch,
            sequence: submission.sequence,
            records: submission.records,
        });

        // Seal on bounds.
        let records: u32 = self.driver.open.iter().map(|p| p.submission.records).sum();
        if records >= self.driver.policy.max_chunk_records
            || self.driver.open_bytes >= self.driver.policy.max_chunk_bytes
        {
            self.seal_open_chunk();
        }
    }

    fn seal_open_chunk(&mut self) {
        if self.driver.open.is_empty() || self.driver.poisoned {
            return;
        }
        let placed = std::mem::take(&mut self.driver.open);
        let bytes = std::mem::take(&mut self.driver.open_bytes);
        let base_offset = placed[0].first_offset;
        let chunk_id = ChunkId::from_bytes([self.driver.next_chunk; 16]);
        self.driver.next_chunk = self.driver.next_chunk.wrapping_add(1);

        let chunk = Chunk {
            chunk_id,
            generation: self.driver.generation,
            base_offset,
            placed,
            bytes,
        };
        self.ledger.event(Event::ChunkSealed {
            chunk_id,
            records: chunk.records(),
            bytes,
        });
        self.driver.sealed_not_appended.push(chunk);
    }

    fn issue_append(&mut self) {
        if self.driver.poisoned || self.driver.sealed_not_appended.is_empty() {
            return;
        }
        if self.driver.inflight.len() >= self.driver.policy.max_inflight_chunks {
            return;
        }
        let chunk = self.driver.sealed_not_appended.remove(0);
        self.ledger.event(Event::AppendIssued {
            chunk_id: chunk.chunk_id,
        });
        self.ledger
            .effect(scripture::CostScope::Adapter, Effect::DataPut);
        // The kernel reads the seal on every acknowledged append (holylog 0006).
        self.ledger
            .effect(scripture::CostScope::Adapter, Effect::SealGet);
        self.driver.inflight.push(chunk);
    }

    fn append_acknowledged(&mut self) {
        if self.driver.inflight.is_empty() {
            return;
        }
        let chunk = self.driver.inflight.remove(0);
        let slot = self.log.committed.len() as u64;

        self.ledger.event(Event::AppendAcknowledged {
            chunk_id: chunk.chunk_id,
            slot,
        });
        self.ledger.event(Event::ChunkVisible {
            chunk_id: chunk.chunk_id,
            slot,
        });
        self.ledger
            .effect(scripture::CostScope::Logical, Effect::ChunkCommitted);

        // Release receipts and record the dedup window, with the exact offsets.
        for placed in &chunk.placed {
            let key = (
                placed.submission.producer_id,
                placed.submission.producer_epoch,
            );
            let entry = self
                .driver
                .dedup
                .entry(key)
                .or_insert((placed.submission.sequence, BTreeMap::new()));
            entry.0 = entry.0.max(placed.submission.sequence);
            entry.1.insert(
                placed.submission.sequence,
                (placed.first_offset, placed.submission.records),
            );

            self.driver.waiters.insert(
                (
                    placed.submission.producer_id,
                    placed.submission.producer_epoch,
                    placed.submission.sequence,
                ),
                Waiter::Receipt {
                    first_offset: placed.first_offset,
                    records: placed.submission.records,
                },
            );
            self.ledger.event(Event::ReceiptReleased {
                producer_id: placed.submission.producer_id,
                producer_epoch: placed.submission.producer_epoch,
                sequence: placed.submission.sequence,
                first_offset: placed.first_offset,
                records: placed.submission.records,
            });
        }

        self.log.committed.push(chunk);
    }

    /// The append's outcome is unknown. The driver poisons and drains.
    ///
    /// **Every in-flight append becomes uncertain, not just this one.** The
    /// kernel completes appends in address order (`complete_slot` waits for all
    /// earlier slots), so if this chunk's slot is abandoned, no later chunk's
    /// append can *ever* acknowledge — it blocks forever behind the hole.
    ///
    /// The model learned this the hard way: allowing a later in-flight append to
    /// acknowledge after an earlier one went uncertain produced a permanent gap
    /// in the offset space. The kernel's ordered completion is therefore
    /// load-bearing for the driver's density invariant, and pipeline depth is
    /// not just a loss-budget multiplier — an uncertain append makes the **whole
    /// pipeline** uncertain.
    fn append_uncertain(&mut self) {
        if self.driver.inflight.is_empty() {
            return;
        }
        let inflight = std::mem::take(&mut self.driver.inflight);
        for chunk in &inflight {
            self.ledger.event(Event::AppendUncertain {
                chunk_id: chunk.chunk_id,
            });
        }
        self.ledger.event(Event::OwnerPoisoned);
        self.driver.poisoned = true;

        for chunk in &inflight {
            for placed in &chunk.placed {
                self.resolve_terminal(placed, TerminalOutcome::Uncertain);
            }
        }
        // Everything else the driver accepted was never appended, and it KNOWS
        // that. Those callers get a distinct, cleanly retryable outcome.
        self.drain_not_written();
    }

    /// A cutover excluded the chunk: durable in the store, unmapped forever.
    ///
    /// Every in-flight chunk is affected, for the same ordered-completion reason
    /// as [`Self::append_uncertain`].
    fn append_excluded(&mut self) {
        if self.driver.inflight.is_empty() {
            return;
        }
        let inflight = std::mem::take(&mut self.driver.inflight);
        for chunk in &inflight {
            self.ledger.event(Event::ChunkExcluded {
                chunk_id: chunk.chunk_id,
            });
            self.ledger.event(Event::AppendUncertain {
                chunk_id: chunk.chunk_id,
            });
        }
        self.ledger.event(Event::OwnerPoisoned);
        self.driver.poisoned = true;

        for chunk in &inflight {
            for placed in &chunk.placed {
                self.resolve_terminal(placed, TerminalOutcome::Uncertain);
            }
        }
        self.drain_not_written();
        for chunk in inflight {
            self.log.excluded.push(chunk);
        }
    }

    fn resolve_terminal(&mut self, placed: &Placed, outcome: TerminalOutcome) {
        self.driver.waiters.insert(
            (
                placed.submission.producer_id,
                placed.submission.producer_epoch,
                placed.submission.sequence,
            ),
            Waiter::Terminal(outcome),
        );
        self.ledger.event(Event::WaiterFailed {
            producer_id: placed.submission.producer_id,
            producer_epoch: placed.submission.producer_epoch,
            sequence: placed.submission.sequence,
            outcome,
        });
    }

    /// Everything buffered or sealed-but-never-appended is provably not in the
    /// log. Say so, rather than making the caller reconcile.
    fn drain_not_written(&mut self) {
        let open = std::mem::take(&mut self.driver.open);
        let sealed = std::mem::take(&mut self.driver.sealed_not_appended);
        self.driver.open_bytes = 0;
        for placed in open {
            self.resolve_terminal(&placed, TerminalOutcome::NotWritten);
        }
        for chunk in sealed {
            for placed in &chunk.placed {
                self.resolve_terminal(placed, TerminalOutcome::NotWritten);
            }
        }
    }

    /// The owner dies. A successor seals, recovers from durable bytes, and takes
    /// over. Anything in flight becomes uncertain.
    fn crash_and_recover(&mut self) {
        // In-flight appends have unknown outcomes; the model resolves them as
        // uncertain, and their chunks are neither committed nor excluded here —
        // the successor's scan of durable bytes decides.
        let inflight = std::mem::take(&mut self.driver.inflight);
        for chunk in &inflight {
            for placed in &chunk.placed {
                self.resolve_terminal(placed, TerminalOutcome::Uncertain);
            }
        }
        self.drain_not_written();

        self.log.sealed = true;
        self.log.generation += 1;
        let boundary = self.log.committed.len() as u64;
        self.ledger.event(Event::MembershipPublished {
            generation: self.log.generation,
            boundary,
        });

        // Recover: rebuild next_offset and the dedup window from the durable,
        // VISIBLE bytes. Excluded chunks are not counted — they are not
        // committed and never will be.
        let bound = self.driver.policy.recovery_scan_chunks;
        let visible = &self.log.committed;
        let scanned: Vec<&Chunk> = visible.iter().rev().take(bound).collect();
        self.ledger
            .effect(scripture::CostScope::Adapter, Effect::TailScan);
        for _ in &scanned {
            self.ledger
                .effect(scripture::CostScope::Adapter, Effect::DataGet);
        }

        let next_offset = visible
            .iter()
            .flat_map(|chunk| chunk.placed.iter())
            .map(|p| {
                p.first_offset
                    .checked_add(p.submission.records as usize)
                    .expect("offset space")
            })
            .max()
            .unwrap_or(RecordOffset::new(0));

        let mut dedup: DedupWindow = BTreeMap::new();
        let mut known: BTreeMap<ProducerId, u32> = BTreeMap::new();
        for chunk in scanned.iter().rev() {
            for placed in &chunk.placed {
                let key = (
                    placed.submission.producer_id,
                    placed.submission.producer_epoch,
                );
                let entry = dedup
                    .entry(key)
                    .or_insert((placed.submission.sequence, BTreeMap::new()));
                entry.0 = entry.0.max(placed.submission.sequence);
                entry.1.insert(
                    placed.submission.sequence,
                    (placed.first_offset, placed.submission.records),
                );
                let highest = known
                    .entry(placed.submission.producer_id)
                    .or_insert(placed.submission.producer_epoch);
                *highest = (*highest).max(placed.submission.producer_epoch);
            }
        }

        // Admitted state is rebuilt from what actually committed: the in-flight
        // submissions are precisely what the crash lost.
        let admitted_seq = dedup
            .iter()
            .map(|(key, (highest, _))| (*key, *highest))
            .collect();

        self.driver.generation = self.log.generation;
        self.driver.next_offset = next_offset;
        self.driver.dedup = dedup;
        self.driver.admitted_seq = admitted_seq;
        self.driver.known_producers = known;
        self.driver.poisoned = false;
        self.driver.open.clear();
        self.driver.open_bytes = 0;
        self.driver.sealed_not_appended.clear();
        self.log.sealed = false;

        self.ledger.event(Event::OwnerRecovered {
            next_offset,
            scanned_chunks: scanned.len(),
        });
    }
}
