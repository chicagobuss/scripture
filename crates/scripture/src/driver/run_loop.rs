//! Command/timer select loop, sealing, append, and poison drain.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::StreamExt;
use futures::future::{self, Either};

use crate::chunk::{ChunkHeader, ChunkId, Frame, SubmissionRef, seal_single_frame_chunk};
use crate::trace::{Effect, Event, TerminalOutcome};

use super::state::{Command, SealedWork};
use super::{AckLevel, ChunkDriverActor, DriverError, Receipt};

impl<C: crate::clock::Clock, T: crate::clock::Timer> ChunkDriverActor<C, T> {
    /// Drains commands, seals on bounds, and owns every append future.
    pub async fn run(mut self) -> Result<(), DriverError> {
        loop {
            if self.poisoned {
                self.poison_blocked();
                while let Some(command) = self.commands.next().await {
                    self.reject_command(command, DriverError::Poisoned);
                }
                return Ok(());
            }

            // Depth one: if a sealed chunk is waiting, append it before taking
            // more seal decisions. Commands may still queue in the channel.
            if self.pending_append.is_some() {
                self.append_pending().await?;
                continue;
            }

            if self.age_due() {
                self.clear_age_sleep();
                self.seal_open();
                continue;
            }

            self.refresh_age_sleep();

            let next_command = self.commands.next();
            let command = if let Some(sleep) = self.age_sleep.take() {
                match future::select(next_command, sleep).await {
                    Either::Left((command, sleep)) => {
                        self.age_sleep = Some(sleep);
                        command
                    }
                    Either::Right(((), _command)) => {
                        self.age_sleep_deadline = None;
                        if self.age_due() {
                            self.seal_open();
                        }
                        continue;
                    }
                }
            } else {
                next_command.await
            };

            let Some(command) = command else {
                // All handles dropped: flush remaining work then exit.
                self.clear_age_sleep();
                if self.open.is_some() {
                    self.seal_open();
                }
                if self.pending_append.is_some() {
                    self.append_pending().await?;
                }
                while let Some(blocked) = self.blocked.pop_front() {
                    let _ = blocked.admission.send(Err(DriverError::Unavailable));
                }
                return Ok(());
            };
            self.handle_command(command).await?;
        }
    }

    fn age_deadline(&self) -> Option<Duration> {
        let open = self.open.as_ref()?;
        if open.placed.is_empty() {
            return None;
        }
        Some(
            open.started_at
                .checked_add(self.policy.max_chunk_age)
                .unwrap_or(Duration::MAX),
        )
    }

    fn age_due(&self) -> bool {
        self.open.as_ref().is_some_and(|open| {
            !open.placed.is_empty()
                && self.clock.now().saturating_sub(open.started_at) >= self.policy.max_chunk_age
        })
    }

    pub(super) fn clear_age_sleep(&mut self) {
        self.age_sleep = None;
        self.age_sleep_deadline = None;
    }

    fn refresh_age_sleep(&mut self) {
        let deadline = self.age_deadline();
        if self.age_sleep_deadline == deadline {
            if deadline.is_some() && self.age_sleep.is_none() {
                // Previously completed or cleared; recreate for the same deadline.
                if let Some(deadline) = deadline {
                    self.age_sleep = Some(self.timer.sleep_until(deadline));
                }
            }
            return;
        }
        self.age_sleep = None;
        self.age_sleep_deadline = deadline;
        if let Some(deadline) = deadline {
            self.age_sleep = Some(self.timer.sleep_until(deadline));
        }
    }

    async fn handle_command(&mut self, command: Command) -> Result<(), DriverError> {
        match command {
            Command::Submit {
                submission,
                admission,
            } => {
                self.admit(submission, admission);
                Ok(())
            }
            Command::Flush { responder } => {
                if self.poisoned {
                    let _ = responder.send(Err(DriverError::Poisoned));
                    return Ok(());
                }
                if self.open.is_some() {
                    self.clear_age_sleep();
                    self.seal_open();
                }
                if self.pending_append.is_some() {
                    self.append_pending().await?;
                }
                if self.poisoned {
                    let _ = responder.send(Err(DriverError::Poisoned));
                } else {
                    let _ = responder.send(Ok(()));
                }
                Ok(())
            }
        }
    }

    fn reject_command(&mut self, command: Command, error: DriverError) {
        match command {
            Command::Submit { admission, .. } => {
                let _ = admission.send(Err(error));
            }
            Command::Flush { responder } => {
                let _ = responder.send(Err(error));
            }
        }
    }

    pub(super) fn poison_blocked(&mut self) {
        while let Some(blocked) = self.blocked.pop_front() {
            let _ = blocked.admission.send(Err(DriverError::Poisoned));
        }
    }
    pub(super) fn seal_open(&mut self) {
        let Some(open) = self.open.take() else {
            return;
        };
        self.clear_age_sleep();
        if open.placed.is_empty() {
            return;
        }
        let base_offset = open.placed[0].first_offset;
        let mut records = Vec::new();
        let mut submissions = Vec::new();
        let mut first_record = 0_u32;
        for placed in &open.placed {
            let count = u32::try_from(placed.submission.records.len()).unwrap_or(u32::MAX);
            submissions.push(SubmissionRef {
                producer_id: placed.submission.producer_id,
                producer_epoch: placed.submission.producer_epoch,
                sequence: placed.submission.sequence,
                first_record,
                record_count: count,
            });
            records.extend(placed.submission.records.iter().cloned());
            first_record = first_record.saturating_add(count);
        }
        let chunk_id = ChunkId::from_bytes({
            let mut bytes = [0_u8; 16];
            bytes[..8].copy_from_slice(&self.next_chunk.to_be_bytes());
            self.next_chunk = self.next_chunk.wrapping_add(1);
            bytes
        });
        let created_at_micros = u64::try_from(self.clock.now().as_micros()).unwrap_or(u64::MAX);
        let sealed_at = self.clock.now();
        let sealed = match seal_single_frame_chunk(
            ChunkHeader {
                chunk_id,
                cohort_id: self.cohort_id,
                generation: self.generation,
                writer_id: self.writer_id,
                created_at_micros,
            },
            vec![Frame {
                journal_id: self.journal_id,
                base_offset,
                records,
                submissions,
            }],
        ) {
            Ok(sealed) => sealed,
            Err(error) => {
                for placed in open.placed {
                    for waiter in placed.waiters {
                        let _ = waiter.send(Err(DriverError::Codec(error.clone())));
                    }
                }
                self.publish_reserved();
                self.drain_blocked();
                return;
            }
        };
        let bytes = sealed.bytes.len();
        self.ledger.event(Event::ChunkSealed {
            chunk_id,
            records: first_record,
            bytes,
        });
        self.pending_append = Some(SealedWork {
            sealed,
            placed: open.placed,
            encoded_bytes: open.encoded_bytes,
            sealed_at,
        });
        self.publish_reserved();
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.inflight_chunks = 1;
            metrics.bytes_at_risk = self.policy.bytes_at_risk();
        }
    }

    pub(super) fn publish_reserved(&mut self) {
        let open_bytes = self.open.as_ref().map_or(0, |open| open.encoded_bytes);
        let pending_bytes = self
            .pending_append
            .as_ref()
            .map_or(0, |pending| pending.encoded_bytes);
        self.reserved_bytes = open_bytes + pending_bytes;
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.reserved_bytes = self.reserved_bytes;
            metrics.bytes_at_risk = self.policy.bytes_at_risk();
        }
    }

    async fn append_pending(&mut self) -> Result<(), DriverError> {
        let Some(pending) = self.pending_append.take() else {
            return Ok(());
        };
        self.ledger.event(Event::AppendIssued {
            chunk_id: pending.sealed.chunk_id,
        });
        match self.writer.append(&pending.sealed).await {
            Ok(ack) => {
                self.ledger.event(Event::AppendAcknowledged {
                    chunk_id: pending.sealed.chunk_id,
                    slot: ack.slot,
                });
                self.ledger
                    .effect(crate::trace::CostScope::Logical, Effect::ChunkCommitted);
                for placed in pending.placed {
                    let records = placed.submission.records.len() as u32;
                    let next_offset = placed
                        .first_offset
                        .checked_add(placed.submission.records.len())
                        .unwrap_or(placed.first_offset);
                    let key = (
                        placed.submission.producer_id,
                        placed.submission.producer_epoch,
                    );
                    let entry = self
                        .dedup
                        .entry(key)
                        .or_insert((placed.submission.sequence, BTreeMap::new()));
                    entry.0 = entry.0.max(placed.submission.sequence);
                    entry.1.insert(
                        placed.submission.sequence,
                        (
                            placed.first_offset,
                            records,
                            pending.sealed.chunk_id,
                            ack.slot,
                            self.generation,
                        ),
                    );
                    self.ledger.event(Event::ReceiptReleased {
                        producer_id: placed.submission.producer_id,
                        producer_epoch: placed.submission.producer_epoch,
                        sequence: placed.submission.sequence,
                        first_offset: placed.first_offset,
                        records,
                    });
                    let receipt = Receipt {
                        level: AckLevel::Committed,
                        journal_id: self.journal_id,
                        first_offset: placed.first_offset,
                        next_offset,
                        chunk_id: pending.sealed.chunk_id,
                        slot: ack.slot,
                        canon_revision: self.generation,
                        deduplicated: false,
                    };
                    for waiter in placed.waiters {
                        let _ = waiter.send(Ok(receipt.clone()));
                    }
                }
                self.publish_reserved();
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.inflight_chunks = 0;
                    metrics.committed_chunks = metrics.committed_chunks.saturating_add(1);
                }
                self.drain_blocked();
                Ok(())
            }
            Err(_) => {
                self.ledger.event(Event::AppendUncertain {
                    chunk_id: pending.sealed.chunk_id,
                });
                self.ledger.event(Event::OwnerPoisoned);
                self.poisoned = true;
                for placed in pending.placed {
                    self.ledger.event(Event::WaiterFailed {
                        producer_id: placed.submission.producer_id,
                        producer_epoch: placed.submission.producer_epoch,
                        sequence: placed.submission.sequence,
                        outcome: TerminalOutcome::Uncertain,
                    });
                    for waiter in placed.waiters {
                        let _ = waiter.send(Err(DriverError::Uncertain {
                            chunk_id: pending.sealed.chunk_id,
                        }));
                    }
                }
                if let Some(open) = self.open.take() {
                    self.clear_age_sleep();
                    for placed in open.placed {
                        self.ledger.event(Event::WaiterFailed {
                            producer_id: placed.submission.producer_id,
                            producer_epoch: placed.submission.producer_epoch,
                            sequence: placed.submission.sequence,
                            outcome: TerminalOutcome::NotWritten,
                        });
                        for waiter in placed.waiters {
                            let _ = waiter.send(Err(DriverError::NotWritten));
                        }
                    }
                }
                self.poison_blocked();
                self.reserved_bytes = 0;
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.poisoned = true;
                    metrics.inflight_chunks = 0;
                    metrics.reserved_bytes = 0;
                    metrics.bytes_at_risk = self.policy.bytes_at_risk();
                }
                // Do not return Err: run must continue into the poisoned drain loop
                // so later Submit callers observe Poisoned, not Unavailable.
                Ok(())
            }
        }
    }
}
