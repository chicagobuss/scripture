//! Scribe-level shared blob sink: one [`BlobWriter`] fed by every assignment.
//!
//! Everything buffered here is pre-acknowledgement. A cut PUTs one object, then
//! each Verse commits references under its own fence. A stalled or refused append
//! for one Verse must not block siblings in the same blob.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::SinkExt;
use futures::StreamExt;
use futures::channel::{mpsc, oneshot};
use object_store::ObjectStore;
use scripture::{
    BlobCommitSink, BlobSinkAppendItem, BlobSinkSubmit, ChunkDriverHandle, DriverError,
    PendingBlobEnvelope,
};
use std::sync::Mutex;
use tokio::task::JoinHandle;

use crate::blob_writer::{
    BlobEnvelope, BlobWriter, BlobWriterConfig, BlobWriterError, CutPlan, DataRefAppendTarget,
    VerseSealer, commit_cut_plan,
};

/// Configuration for one shared Scribe blob sink.
#[derive(Debug, Clone)]
pub struct SharedBlobSinkConfig {
    /// Writer cut policy (size / linger).
    pub writer: BlobWriterConfig,
    /// Scribe-wide pre-ack buffer ceiling.
    pub max_buffer_bytes: usize,
    /// Per-assignment fair share of the buffer (hot Verse cannot starve siblings).
    pub per_assignment_max_bytes: usize,
}

impl SharedBlobSinkConfig {
    /// Builds fair-share limits for `assignment_count` serving assignments.
    #[must_use]
    pub fn fair_for_assignments(
        writer: BlobWriterConfig,
        max_buffer_bytes: usize,
        assignment_count: usize,
    ) -> Self {
        let count = assignment_count.max(1);
        Self {
            writer,
            max_buffer_bytes,
            per_assignment_max_bytes: max_buffer_bytes / count,
        }
    }
}

/// Live observation of the shared buffer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SharedBlobSinkMetrics {
    /// Bytes currently buffered pre-ack.
    pub buffered_bytes: usize,
    /// High-water buffered bytes since start.
    pub buffered_bytes_high_water: usize,
    /// Completed shared-blob cuts.
    pub cuts: usize,
}

struct PendingCompletion {
    chunk_id: scripture::ChunkId,
    verse_key: String,
    encoded_bytes: usize,
    completion: oneshot::Sender<Result<scripture::ChunkAppendAck, DriverError>>,
}

struct DriverSealer {
    handle: ChunkDriverHandle,
}

#[async_trait]
impl VerseSealer for DriverSealer {
    async fn seal(
        &mut self,
        envelope: &BlobEnvelope,
    ) -> Result<scripture::SealedChunk, BlobWriterError> {
        self.handle
            .blob_sink_seal(pending_from_runtime(envelope))
            .await
            .map_err(blob_writer_error_from_driver)
    }
}

struct DriverAppendTarget {
    handle: ChunkDriverHandle,
}

#[async_trait]
impl DataRefAppendTarget for DriverAppendTarget {
    async fn append_data_ref(
        &mut self,
        sealed: &scripture::SealedChunk,
        data_ref: &scripture::DataRef,
    ) -> Result<scripture::ChunkAppendAck, BlobWriterError> {
        self.handle
            .blob_sink_append_refs(vec![BlobSinkAppendItem {
                sealed: sealed.clone(),
                data_ref: data_ref.clone(),
            }])
            .await
            .map_err(blob_writer_error_from_driver)
    }

    async fn append_data_refs(
        &mut self,
        items: &[(&scripture::SealedChunk, &scripture::DataRef)],
    ) -> Result<scripture::ChunkAppendAck, BlobWriterError> {
        let batch = items
            .iter()
            .map(|(sealed, data_ref)| BlobSinkAppendItem {
                sealed: (*sealed).clone(),
                data_ref: (*data_ref).clone(),
            })
            .collect();
        self.handle
            .blob_sink_append_refs(batch)
            .await
            .map_err(blob_writer_error_from_driver)
    }
}

/// Shared Scribe blob sink handle.
#[derive(Clone)]
pub struct SharedBlobSink {
    inner: Arc<SharedBlobSinkInner>,
}

struct SharedBlobSinkInner {
    submit_tx: mpsc::Sender<BlobSinkSubmit>,
    drivers: Mutex<BTreeMap<String, ChunkDriverHandle>>,
    buffered_bytes: AtomicUsize,
    buffered_high_water: AtomicUsize,
    assignment_buffered: Mutex<HashMap<String, usize>>,
    cuts: AtomicUsize,
    max_buffer_bytes: usize,
    per_assignment_max_bytes: usize,
    linger_poll: Duration,
}

impl SharedBlobSink {
    /// Spawns the sink task and returns a handle for assignment drivers.
    pub fn spawn(
        store: Arc<dyn ObjectStore>,
        config: SharedBlobSinkConfig,
    ) -> Result<Self, BlobWriterError> {
        let linger_poll = config.writer.max_linger / 4 + Duration::from_millis(1);
        let writer = BlobWriter::new(config.writer)?;
        let (submit_tx, submit_rx) = mpsc::channel(256);
        let inner = Arc::new(SharedBlobSinkInner {
            submit_tx,
            drivers: Mutex::new(BTreeMap::new()),
            buffered_bytes: AtomicUsize::new(0),
            buffered_high_water: AtomicUsize::new(0),
            assignment_buffered: Mutex::new(HashMap::new()),
            cuts: AtomicUsize::new(0),
            max_buffer_bytes: config.max_buffer_bytes,
            per_assignment_max_bytes: config.per_assignment_max_bytes,
            linger_poll,
        });
        let task_inner = Arc::clone(&inner);
        let _task: JoinHandle<()> = tokio::spawn(async move {
            run_sink_task(store, writer, submit_rx, task_inner).await;
        });
        Ok(Self { inner })
    }

    /// Registers one assignment driver for seal/append during cuts.
    pub fn register_driver(&self, verse_key: impl Into<String>, handle: ChunkDriverHandle) {
        self.inner
            .drivers
            .lock()
            .expect("drivers")
            .insert(verse_key.into(), handle);
    }

    /// Snapshot of buffer accounting for status probes.
    #[must_use]
    pub fn metrics(&self) -> SharedBlobSinkMetrics {
        SharedBlobSinkMetrics {
            buffered_bytes: self.inner.buffered_bytes.load(Ordering::Relaxed),
            buffered_bytes_high_water: self.inner.buffered_high_water.load(Ordering::Relaxed),
            cuts: self.inner.cuts.load(Ordering::Relaxed),
        }
    }

    fn try_reserve_buffer(&self, verse_key: &str, bytes: usize) -> bool {
        let total = self.inner.buffered_bytes.load(Ordering::Relaxed);
        if total.saturating_add(bytes) > self.inner.max_buffer_bytes {
            return false;
        }
        let mut per_assignment = self.inner.assignment_buffered.lock().expect("assignment");
        let used = per_assignment.get(verse_key).copied().unwrap_or(0);
        if used.saturating_add(bytes) > self.inner.per_assignment_max_bytes {
            return false;
        }
        per_assignment.insert(verse_key.to_owned(), used.saturating_add(bytes));
        let next_total = total.saturating_add(bytes);
        self.inner
            .buffered_bytes
            .store(next_total, Ordering::Relaxed);
        let high = self.inner.buffered_high_water.load(Ordering::Relaxed);
        if next_total > high {
            self.inner
                .buffered_high_water
                .store(next_total, Ordering::Relaxed);
        }
        true
    }

    fn release_buffer(&self, verse_key: &str, bytes: usize) {
        let mut per_assignment = self.inner.assignment_buffered.lock().expect("assignment");
        if let Some(used) = per_assignment.get_mut(verse_key) {
            *used = used.saturating_sub(bytes);
        }
        self.inner
            .buffered_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }
}

impl BlobCommitSink for SharedBlobSink {
    fn register_driver(&self, verse_key: &str, handle: ChunkDriverHandle) {
        SharedBlobSink::register_driver(self, verse_key, handle);
    }

    fn submit(
        self: Arc<Self>,
        item: BlobSinkSubmit,
    ) -> Pin<Box<dyn Future<Output = Result<(), DriverError>> + Send>> {
        Box::pin(async move {
            let encoded = item.encoded_bytes;
            let verse_key = item.envelope.verse_key.clone();
            if !self.try_reserve_buffer(&verse_key, encoded) {
                return Err(DriverError::BlobSinkBufferFull);
            }
            match self.inner.submit_tx.clone().send(item).await {
                Ok(()) => Ok(()),
                Err(_send_err) => {
                    self.release_buffer(&verse_key, encoded);
                    Err(DriverError::Unavailable)
                }
            }
        })
    }
}

async fn run_sink_task(
    store: Arc<dyn ObjectStore>,
    mut writer: BlobWriter,
    mut submit_rx: mpsc::Receiver<BlobSinkSubmit>,
    inner: Arc<SharedBlobSinkInner>,
) {
    let mut linger_tick = tokio::time::interval(inner.linger_poll);
    linger_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut completions: VecDeque<PendingCompletion> = VecDeque::new();

    loop {
        tokio::select! {
            item = submit_rx.next() => {
                let Some(item) = item else { break };
                let verse_key = item.envelope.verse_key.clone();
                let chunk_id = item.envelope.chunk_id;
                let encoded = item.encoded_bytes;
                let completion = item.completion;
                let runtime_envelope = runtime_from_pending(&item.envelope);
                completions.push_back(PendingCompletion {
                    chunk_id,
                    verse_key: verse_key.clone(),
                    encoded_bytes: encoded,
                    completion,
                });
                match writer.push(runtime_envelope) {
                    Ok(Some(plan)) => {
                        commit_plan(&store, &inner, plan, &mut completions).await;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        fail_tail_completion(&inner, &mut completions, error).await;
                    }
                }
            }
            _ = linger_tick.tick() => {
                match writer.poll_linger() {
                    Ok(Some(plan)) => {
                        commit_plan(&store, &inner, plan, &mut completions).await;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        fail_all_completions(&inner, &mut completions, error).await;
                    }
                }
            }
        }
    }
}

async fn commit_plan(
    store: &Arc<dyn ObjectStore>,
    inner: &SharedBlobSinkInner,
    plan: CutPlan,
    completions: &mut VecDeque<PendingCompletion>,
) {
    let drivers = inner.drivers.lock().expect("drivers").clone();
    let mut sealers: BTreeMap<String, Box<dyn VerseSealer>> = BTreeMap::new();
    let mut targets: BTreeMap<String, Box<dyn DataRefAppendTarget>> = BTreeMap::new();
    for verse_key in plan
        .envelopes
        .iter()
        .map(|env| env.verse_key.clone())
        .collect::<Vec<_>>()
    {
        if let Some(handle) = drivers.get(&verse_key) {
            sealers.insert(
                verse_key.clone(),
                Box::new(DriverSealer {
                    handle: handle.clone(),
                }),
            );
            targets.insert(
                verse_key,
                Box::new(DriverAppendTarget {
                    handle: handle.clone(),
                }),
            );
        }
    }
    drop(drivers);

    match commit_cut_plan(store, &plan, &mut sealers, &mut targets).await {
        Ok(outcomes) => {
            inner.cuts.fetch_add(1, Ordering::Relaxed);
            for outcome in outcomes {
                match outcome.result {
                    Ok(ack) => {
                        let handle = inner
                            .drivers
                            .lock()
                            .expect("drivers")
                            .get(&outcome.verse_key)
                            .cloned();
                        if let Some(handle) = handle {
                            let _ = handle.blob_sink_committed(outcome.chunk_id, ack).await;
                        }
                        notify_chunk_success(
                            inner,
                            &outcome.verse_key,
                            outcome.chunk_id,
                            ack,
                            completions,
                        )
                        .await;
                    }
                    Err(error) => {
                        notify_chunk_failure(
                            inner,
                            &outcome.verse_key,
                            outcome.chunk_id,
                            completions,
                            blob_writer_error_to_driver(error),
                        )
                        .await;
                    }
                }
            }
        }
        Err(error) => {
            fail_all_completions(inner, completions, error).await;
        }
    }
}

async fn notify_chunk_success(
    inner: &SharedBlobSinkInner,
    verse_key: &str,
    chunk_id: scripture::ChunkId,
    ack: scripture::ChunkAppendAck,
    completions: &mut VecDeque<PendingCompletion>,
) {
    let mut remaining = VecDeque::new();
    while let Some(pending) = completions.pop_front() {
        if pending.chunk_id == chunk_id {
            inner.release_buffer_internal(&pending.verse_key, pending.encoded_bytes);
            let _ = pending.completion.send(Ok(ack));
        } else {
            remaining.push_back(pending);
        }
    }
    *completions = remaining;
    let _ = verse_key;
}

async fn notify_chunk_failure(
    inner: &SharedBlobSinkInner,
    verse_key: &str,
    chunk_id: scripture::ChunkId,
    completions: &mut VecDeque<PendingCompletion>,
    error: DriverError,
) {
    let mut remaining = VecDeque::new();
    while let Some(pending) = completions.pop_front() {
        if pending.chunk_id == chunk_id {
            inner.release_buffer_internal(&pending.verse_key, pending.encoded_bytes);
            let _ = pending
                .completion
                .send(Err(DriverError::Invariant(error.to_string())));
        } else {
            remaining.push_back(pending);
        }
    }
    *completions = remaining;
    let _ = verse_key;
}

async fn fail_tail_completion(
    inner: &SharedBlobSinkInner,
    completions: &mut VecDeque<PendingCompletion>,
    error: BlobWriterError,
) {
    if let Some(pending) = completions.pop_back() {
        inner.release_buffer_internal(&pending.verse_key, pending.encoded_bytes);
        let _ = pending
            .completion
            .send(Err(blob_writer_error_to_driver(error)));
    }
}

async fn fail_all_completions(
    inner: &SharedBlobSinkInner,
    completions: &mut VecDeque<PendingCompletion>,
    error: BlobWriterError,
) {
    let message = blob_writer_error_to_driver(error).to_string();
    while let Some(pending) = completions.pop_front() {
        inner.release_buffer_internal(&pending.verse_key, pending.encoded_bytes);
        let _ = pending
            .completion
            .send(Err(DriverError::Invariant(message.clone())));
    }
}

impl SharedBlobSinkInner {
    fn release_buffer_internal(&self, verse_key: &str, bytes: usize) {
        let mut per_assignment = self.assignment_buffered.lock().expect("assignment");
        if let Some(used) = per_assignment.get_mut(verse_key) {
            *used = used.saturating_sub(bytes);
        }
        self.buffered_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
}

fn runtime_from_pending(envelope: &PendingBlobEnvelope) -> BlobEnvelope {
    BlobEnvelope {
        verse_key: envelope.verse_key.clone(),
        chunk_id: envelope.chunk_id,
        base_offset: envelope.base_offset,
        journal_id: envelope.journal_id,
        cohort_id: envelope.cohort_id,
        records: envelope.records.clone(),
        submissions: envelope.submissions.clone(),
    }
}

fn pending_from_runtime(envelope: &BlobEnvelope) -> PendingBlobEnvelope {
    PendingBlobEnvelope {
        verse_key: envelope.verse_key.clone(),
        chunk_id: envelope.chunk_id,
        base_offset: envelope.base_offset,
        journal_id: envelope.journal_id,
        cohort_id: envelope.cohort_id,
        records: envelope.records.clone(),
        submissions: envelope.submissions.clone(),
    }
}

fn blob_writer_error_from_driver(error: DriverError) -> BlobWriterError {
    BlobWriterError::Invariant(error.to_string())
}

fn blob_writer_error_to_driver(error: BlobWriterError) -> DriverError {
    DriverError::Invariant(error.to_string())
}
