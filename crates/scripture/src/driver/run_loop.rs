//! Command/timer select loop, sealing, append, and poison drain.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::StreamExt;
use futures::future::{self, Either};

use crate::blob_sink::BlobSinkSubmit;
use crate::chunk::{ChunkHeader, ChunkId, Frame, SubmissionRef, seal_single_frame_chunk};
use crate::trace::{Effect, Event, TerminalOutcome};

use super::state::{Command, PendingAppend, SealedWork};
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
            // more seal decisions. Shared-sink pending stays non-blocking so cut
            // seal/append commands can be handled while envelopes buffer.
            if matches!(self.pending_append, Some(PendingAppend::DepthOne(_))) {
                self.append_depth_one().await?;
                continue;
            }

            if matches!(
                self.pending_append,
                Some(PendingAppend::BlobSink {
                    enqueued: false,
                    ..
                })
            ) {
                self.try_enqueue_blob_sink().await?;
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
                if matches!(self.pending_append, Some(PendingAppend::DepthOne(_))) {
                    self.append_depth_one().await?;
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
            if deadline.is_some()
                && self.age_sleep.is_none()
                && let Some(deadline) = deadline
            {
                self.age_sleep = Some(self.timer.sleep_until(deadline));
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
                if matches!(self.pending_append, Some(PendingAppend::DepthOne(_))) {
                    self.append_depth_one().await?;
                }
                if self.poisoned {
                    let _ = responder.send(Err(DriverError::Poisoned));
                } else {
                    let _ = responder.send(Ok(()));
                }
                Ok(())
            }
            Command::BlobSinkSeal {
                envelope,
                responder,
            } => {
                let result = self.seal_envelope(&envelope);
                let _ = responder.send(result);
                Ok(())
            }
            Command::BlobSinkAppendRefs { items, responder } => {
                let result = self.append_blob_sink_refs(items).await;
                let _ = responder.send(result);
                Ok(())
            }
            Command::BlobSinkCommitted {
                chunk_id,
                ack,
                responder,
            } => {
                let result = self.finish_blob_sink_committed(chunk_id, ack);
                let _ = responder.send(result);
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
            Command::BlobSinkSeal { responder, .. } => {
                let _ = responder.send(Err(error));
            }
            Command::BlobSinkAppendRefs { responder, .. } => {
                let _ = responder.send(Err(error));
            }
            Command::BlobSinkCommitted { responder, .. } => {
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

        if self.blob_sink.is_some() {
            let verse_key = self
                .blob_verse_key
                .clone()
                .unwrap_or_else(|| "unknown".to_owned());
            let envelope = crate::blob_sink::PendingBlobEnvelope {
                verse_key,
                chunk_id,
                base_offset,
                journal_id: self.journal_id,
                cohort_id: self.cohort_id,
                records,
                submissions,
            };
            let bytes = match crate::chunk::encoded_chunk_len(&[Frame {
                journal_id: envelope.journal_id,
                base_offset: envelope.base_offset,
                records: envelope.records.clone(),
                submissions: envelope.submissions.clone(),
            }]) {
                Ok(len) => len,
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
            self.ledger.event(Event::ChunkSealed {
                chunk_id,
                records: first_record,
                bytes,
            });
            self.pending_append = Some(PendingAppend::BlobSink {
                envelope,
                placed: open.placed,
                encoded_bytes: open.encoded_bytes,
                enqueued: false,
            });
            self.publish_reserved();
            if let Ok(mut metrics) = self.metrics.lock() {
                metrics.inflight_chunks = 1;
                metrics.bytes_at_risk = self.policy.bytes_at_risk();
            }
            return;
        }

        let created_at_micros = u64::try_from(self.clock.now().as_micros()).unwrap_or(u64::MAX);
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
        self.pending_append = Some(PendingAppend::DepthOne(SealedWork {
            sealed,
            placed: open.placed,
            encoded_bytes: open.encoded_bytes,
            sealed_at: self.clock.now(),
        }));
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
            .map_or(0, PendingAppend::encoded_bytes);
        self.reserved_bytes = open_bytes + pending_bytes;
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.reserved_bytes = self.reserved_bytes;
            metrics.bytes_at_risk = self.policy.bytes_at_risk();
        }
    }

    async fn append_depth_one(&mut self) -> Result<(), DriverError> {
        let Some(PendingAppend::DepthOne(pending)) = self.pending_append.take() else {
            return Ok(());
        };
        self.ledger.event(Event::AppendIssued {
            chunk_id: pending.sealed.chunk_id,
        });
        let append_result = if let Some(config) = self.dataref_blobs.as_ref() {
            crate::blob_store::commit_sealed_as_data_ref(
                &mut self.writer,
                config.store.as_ref(),
                &config.blob_prefix,
                &pending.sealed,
            )
            .await
        } else {
            self.writer.append(&pending.sealed).await
        };
        self.finish_depth_one_append(pending, append_result).await
    }

    async fn try_enqueue_blob_sink(&mut self) -> Result<(), DriverError> {
        let Some(PendingAppend::BlobSink {
            envelope,
            encoded_bytes,
            enqueued: false,
            ..
        }) = self.pending_append.as_ref()
        else {
            return Ok(());
        };
        let chunk_id = envelope.chunk_id;
        self.ledger.event(Event::AppendIssued { chunk_id });
        let Some(sink) = self.blob_sink.clone() else {
            return Err(DriverError::Invariant(
                "blob sink pending without a configured sink".into(),
            ));
        };
        let (completion_tx, _completion_rx) = futures::channel::oneshot::channel();
        let submit = BlobSinkSubmit {
            envelope: envelope.clone(),
            encoded_bytes: *encoded_bytes,
            completion: completion_tx,
        };
        match sink.submit(submit).await {
            Ok(()) => {
                if let Some(PendingAppend::BlobSink { enqueued, .. }) = &mut self.pending_append {
                    *enqueued = true;
                }
                Ok(())
            }
            Err(DriverError::BlobSinkBufferFull) => {
                std::thread::yield_now();
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                let pending = self.pending_append.take();
                if let Some(PendingAppend::BlobSink { placed, .. }) = pending {
                    for placed in placed {
                        for waiter in placed.waiters {
                            let _ = waiter.send(Err(DriverError::Invariant(message.clone())));
                        }
                    }
                }
                self.publish_reserved();
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.inflight_chunks = 0;
                }
                Ok(())
            }
        }
    }

    fn seal_envelope(
        &mut self,
        envelope: &crate::blob_sink::PendingBlobEnvelope,
    ) -> Result<crate::chunk::SealedChunk, DriverError> {
        let created_at_micros = u64::try_from(self.clock.now().as_micros()).unwrap_or(u64::MAX);
        seal_single_frame_chunk(
            ChunkHeader {
                chunk_id: envelope.chunk_id,
                cohort_id: envelope.cohort_id,
                generation: self.generation,
                writer_id: self.writer_id,
                created_at_micros,
            },
            vec![Frame {
                journal_id: envelope.journal_id,
                base_offset: envelope.base_offset,
                records: envelope.records.clone(),
                submissions: envelope.submissions.clone(),
            }],
        )
        .map_err(DriverError::from)
    }

    async fn append_blob_sink_refs(
        &mut self,
        items: Vec<crate::blob_sink::BlobSinkAppendItem>,
    ) -> Result<crate::chunklog::ChunkAppendAck, DriverError> {
        if items.is_empty() {
            return Err(DriverError::Invariant(
                "append_blob_sink_refs requires at least one item".into(),
            ));
        }
        let refs: Vec<(&crate::chunk::SealedChunk, &crate::dataref::DataRef)> = items
            .iter()
            .map(|item| (&item.sealed, &item.data_ref))
            .collect();
        match refs.len() {
            1 => self
                .writer
                .append_data_ref(refs[0].0, refs[0].1)
                .await
                .map_err(DriverError::from),
            _ => self
                .writer
                .append_reference_batch(&refs)
                .await
                .map_err(DriverError::from),
        }
    }

    fn finish_blob_sink_committed(
        &mut self,
        chunk_id: ChunkId,
        ack: crate::chunklog::ChunkAppendAck,
    ) -> Result<(), DriverError> {
        let Some(PendingAppend::BlobSink { placed, .. }) = self.pending_append.take() else {
            return Err(DriverError::Invariant(
                "blob sink committed without pending work".into(),
            ));
        };
        if ack.chunk_id != chunk_id {
            return Err(DriverError::Invariant(
                "blob sink committed chunk_id mismatch".into(),
            ));
        }
        self.ledger.event(Event::AppendAcknowledged {
            chunk_id,
            slot: ack.slot,
        });
        self.ledger
            .effect(crate::trace::CostScope::Logical, Effect::ChunkCommitted);
        self.release_receipts_for_placed(placed, chunk_id, ack);
        self.publish_reserved();
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.inflight_chunks = 0;
            metrics.committed_chunks = metrics.committed_chunks.saturating_add(1);
        }
        self.drain_blocked();
        Ok(())
    }

    async fn finish_depth_one_append(
        &mut self,
        pending: SealedWork,
        append_result: Result<crate::chunklog::ChunkAppendAck, crate::chunklog::ChunkLogError>,
    ) -> Result<(), DriverError> {
        match append_result {
            Ok(ack) => {
                self.ledger.event(Event::AppendAcknowledged {
                    chunk_id: pending.sealed.chunk_id,
                    slot: ack.slot,
                });
                self.ledger
                    .effect(crate::trace::CostScope::Logical, Effect::ChunkCommitted);
                self.release_receipts_for_placed(pending.placed, pending.sealed.chunk_id, ack);
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
                Ok(())
            }
        }
    }

    fn release_receipts_for_placed(
        &mut self,
        placed: Vec<super::state::PlacedSubmission>,
        chunk_id: ChunkId,
        ack: crate::chunklog::ChunkAppendAck,
    ) {
        for placed in placed {
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
                super::state::DedupReceipt {
                    first_offset: placed.first_offset,
                    record_count: records,
                    chunk_id,
                    slot: ack.slot,
                    canon_revision: self.generation,
                    submission_digest: crate::model::canonical_records_digest(
                        &placed.submission.records,
                    ),
                },
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
                chunk_id,
                slot: ack.slot,
                canon_revision: self.generation,
                deduplicated: false,
            };
            for waiter in placed.waiters {
                let _ = waiter.send(Ok(receipt.clone()));
            }
        }
    }
}
