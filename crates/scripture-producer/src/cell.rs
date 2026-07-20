//! Spool cell: reserve → WAL sync → forward → cell-owned progress → receipt.
//!
//! Completions are driven by [`SpoolCell::run`], not by polling
//! [`SpoolReceiptFuture`]. Dropping a caller receipt never cancels progress
//! attempts and never yields false success.
//!
//! Concurrent admission is serialized on [`CellShared::admission`]: duplicate
//! identity, lifecycle reservation, completion-queue slot, and WAL append/sync
//! happen atomically relative to other submits. The admission lock is never held
//! across `forward` or receipt awaiting.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::oneshot;

use scripture::driver::{DriverError, Receipt, ReceiptFuture, Submission};
use scripture::model::JournalId;

use super::frame::SpoolFrame;
use super::progress::ProgressIdentity;
use super::recovery::{RecoveryReport, scan_and_classify};
use super::storage::{
    SpoolConfig, SpoolError, SpoolPoisonCause, SpoolStorage, encoded_frame_bytes,
};

/// Serving vs fail-closed recovery / poison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolCellState {
    /// Fresh empty spool; submits allowed.
    Serving,
    /// Terminal for this process lifetime; submissions refused.
    Poisoned {
        /// Why admission is blocked.
        cause: SpoolPoisonCause,
    },
    /// Non-empty or corrupt history; submits forbidden.
    RecoveryRequired(RecoveryReport),
}

struct CellShared<S> {
    journal_id: JournalId,
    config: SpoolConfig,
    /// Serializes identity / reservation / completion slot / WAL (not forward).
    admission: Mutex<()>,
    storage: Mutex<S>,
    state: Mutex<SpoolCellState>,
    known: Mutex<BTreeSet<ProgressIdentity>>,
    reserved_bytes: Mutex<usize>,
    reserved_frames: Mutex<usize>,
    /// Completions reserved from admit until [`complete_work`] finishes.
    inflight_completions: Mutex<usize>,
}

/// What the completer receives after admit reserved a completion-queue slot.
enum DriverDelivery {
    /// Admit aborted before a durable WAL frame; bytes already released by admit.
    Canceled,
    /// Durable WAL exists; forward failed.
    ForwardFailed(SpoolError),
    /// Durable WAL exists; await this receipt then write progress.
    Forwarded(ReceiptFuture),
}

struct CompletionWork {
    identity: ProgressIdentity,
    progress_bytes: usize,
    driver: oneshot::Receiver<DriverDelivery>,
    reply: oneshot::Sender<Result<Receipt, SpoolError>>,
}

/// Cloneable admit surface. Completions require a paired [`SpoolCell::run`].
pub struct SpoolCellHandle<S> {
    shared: Arc<CellShared<S>>,
    work_tx: mpsc::Sender<CompletionWork>,
}

impl<S> Clone for SpoolCellHandle<S> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            work_tx: self.work_tx.clone(),
        }
    }
}

/// Owns the completion queue. Spawn [`Self::run`] for the lifetime of the cell.
pub struct SpoolCell<S> {
    shared: Arc<CellShared<S>>,
    work_rx: mpsc::Receiver<CompletionWork>,
}

impl<S: SpoolStorage + 'static> SpoolCell<S> {
    /// Opens storage, scans, and returns a handle plus a completer to run.
    ///
    /// Serving only when clean+empty. Callers must spawn [`Self::run`] before
    /// awaiting receipts (and should keep it running while handles are live).
    pub fn open(
        journal_id: JournalId,
        config: SpoolConfig,
        mut storage: S,
    ) -> Result<(SpoolCellHandle<S>, Self), SpoolError> {
        config.validate()?;
        let report = scan_and_classify(&storage)?;
        let state = if report.recovery_required() {
            SpoolCellState::RecoveryRequired(report)
        } else {
            SpoolCellState::Serving
        };
        storage.set_faults(Default::default());
        let queue_capacity = config.max_inflight_completions;
        let shared = Arc::new(CellShared {
            journal_id,
            config,
            admission: Mutex::new(()),
            storage: Mutex::new(storage),
            state: Mutex::new(state),
            known: Mutex::new(BTreeSet::new()),
            reserved_bytes: Mutex::new(0),
            reserved_frames: Mutex::new(0),
            inflight_completions: Mutex::new(0),
        });
        // Buffer matches the documented limit; the inflight counter is authoritative
        // while a work item is processing (channel slot may already be free).
        let (work_tx, work_rx) = mpsc::channel(queue_capacity);
        Ok((
            SpoolCellHandle {
                shared: Arc::clone(&shared),
                work_tx,
            },
            Self { shared, work_rx },
        ))
    }

    /// Drives receipt → progress → resolve for every admitted submission.
    ///
    /// Independent of whether callers keep [`SpoolReceiptFuture`]s. Returns when
    /// all handles are dropped and the work queue is closed.
    pub async fn run(mut self) {
        while let Some(work) = self.work_rx.next().await {
            complete_work(&self.shared, work).await;
        }
    }
}

struct AdmittedForward {
    driver_tx: oneshot::Sender<DriverDelivery>,
    receipt: SpoolReceiptFuture,
}

impl<S: SpoolStorage + 'static> SpoolCellHandle<S> {
    /// Snapshot of cell state.
    #[must_use]
    pub fn state(&self) -> SpoolCellState {
        self.shared
            .state
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or(SpoolCellState::Poisoned {
                cause: SpoolPoisonCause::ProgressFailed,
            })
    }

    /// Recovery report when not Serving.
    #[must_use]
    pub fn recovery_report(&self) -> Option<RecoveryReport> {
        match self.state() {
            SpoolCellState::RecoveryRequired(report) => Some(report),
            _ => None,
        }
    }

    /// True when submits are allowed.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        matches!(self.state(), SpoolCellState::Serving)
    }

    /// Reserve → WAL sync → `forward` → cell-owned completion.
    pub async fn submit_forwarded<F, Fut>(
        &self,
        submission: Submission,
        forward: F,
    ) -> Result<SpoolReceiptFuture, SpoolError>
    where
        F: FnOnce(Submission) -> Fut,
        Fut: Future<Output = Result<ReceiptFuture, SpoolError>>,
    {
        if submission.records.is_empty() {
            return Err(SpoolError::Forward(
                DriverError::EmptySubmission.to_string(),
            ));
        }

        let admitted = self.admit_wal_and_queue(submission.clone())?;
        let AdmittedForward { driver_tx, receipt } = admitted;

        match forward(submission).await {
            Ok(inner) => {
                let _ = driver_tx.send(DriverDelivery::Forwarded(inner));
                Ok(receipt)
            }
            Err(error) => {
                // The submission frame is already durable.  Poison before
                // returning so a later queue turn cannot admit a new write
                // after this known post-WAL failure.
                poison_shared(&self.shared, SpoolPoisonCause::ForwardFailed);
                let _ = driver_tx.send(DriverDelivery::ForwardFailed(SpoolError::Forward(
                    error.to_string(),
                )));
                Err(error)
            }
        }
    }

    /// Convenience forward through a [`scripture::driver::ChunkDriverHandle`].
    pub async fn submit(
        &self,
        driver: &scripture::driver::ChunkDriverHandle,
        submission: Submission,
    ) -> Result<SpoolReceiptFuture, SpoolError> {
        self.submit_forwarded(submission, |submission| async move {
            driver
                .submit(submission)
                .await
                .map_err(|error| SpoolError::Forward(error.to_string()))
        })
        .await
    }

    /// Identity + reservation + completion slot + WAL under [`CellShared::admission`].
    fn admit_wal_and_queue(&self, submission: Submission) -> Result<AdmittedForward, SpoolError> {
        let _admit = self
            .shared
            .admission
            .lock()
            .map_err(|_| SpoolError::Io(std::io::Error::other("spool lock poisoned")))?;

        self.ensure_serving()?;

        let identity = ProgressIdentity {
            journal_id: self.shared.journal_id,
            producer_id: submission.producer_id,
            producer_epoch: submission.producer_epoch,
            sequence: submission.sequence,
        };

        let frame = SpoolFrame::Submission {
            journal_id: self.shared.journal_id,
            submission,
        };
        let progress = SpoolFrame::Progress(identity);
        let encoded = encoded_frame_bytes(&frame)?;
        let progress_encoded = encoded_frame_bytes(&progress)?;
        if encoded.len() > self.shared.config.max_frame_bytes
            || progress_encoded.len() > self.shared.config.max_frame_bytes
        {
            return Err(SpoolError::CapacityExceeded);
        }
        let lifecycle_bytes = encoded
            .len()
            .checked_add(progress_encoded.len())
            .ok_or(SpoolError::CapacityExceeded)?;

        {
            let known = self
                .shared
                .known
                .lock()
                .map_err(|_| SpoolError::Io(std::io::Error::other("spool lock poisoned")))?;
            if known.contains(&identity) {
                return Err(SpoolError::DuplicateIdentity);
            }
        }

        self.reserve(lifecycle_bytes, 2)?;
        self.reserve_completion()?;

        let (driver_tx, driver_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        let mut work_tx = self.work_tx.clone();
        if work_tx
            .try_send(CompletionWork {
                identity,
                progress_bytes: progress_encoded.len(),
                driver: driver_rx,
                reply: reply_tx,
            })
            .is_err()
        {
            self.release_completion();
            self.release(lifecycle_bytes, 2);
            return Err(SpoolError::CapacityExceeded);
        }

        {
            let mut known = self
                .shared
                .known
                .lock()
                .map_err(|_| SpoolError::Io(std::io::Error::other("spool lock poisoned")))?;
            known.insert(identity);
        }

        let wal_result = (|| {
            let mut storage = self
                .shared
                .storage
                .lock()
                .map_err(|_| SpoolError::Io(std::io::Error::other("spool lock poisoned")))?;
            storage.append_frame(&frame)?;
            storage.sync()?;
            Ok::<(), SpoolError>(())
        })();
        if let Err(error) = wal_result {
            let _ = driver_tx.send(DriverDelivery::Canceled);
            {
                let mut known =
                    self.shared.known.lock().map_err(|_| {
                        SpoolError::Io(std::io::Error::other("spool lock poisoned"))
                    })?;
                known.remove(&identity);
            }
            self.release(lifecycle_bytes, 2);
            // Completion slot released by completer on Canceled.
            return Err(error);
        }

        // Submission durable: keep progress reservation only.
        self.release(encoded.len(), 1);

        Ok(AdmittedForward {
            driver_tx,
            receipt: SpoolReceiptFuture { receiver: reply_rx },
        })
    }

    fn ensure_serving(&self) -> Result<(), SpoolError> {
        match self.state() {
            SpoolCellState::Serving => Ok(()),
            SpoolCellState::RecoveryRequired(_) => Err(SpoolError::RecoveryRequired),
            SpoolCellState::Poisoned { cause } => Err(SpoolError::Poisoned { cause }),
        }
    }

    fn reserve(&self, bytes: usize, frames: usize) -> Result<(), SpoolError> {
        let mut reserved_bytes = self
            .shared
            .reserved_bytes
            .lock()
            .map_err(|_| SpoolError::Io(std::io::Error::other("spool lock poisoned")))?;
        let mut reserved_frames = self
            .shared
            .reserved_frames
            .lock()
            .map_err(|_| SpoolError::Io(std::io::Error::other("spool lock poisoned")))?;
        let storage = self
            .shared
            .storage
            .lock()
            .map_err(|_| SpoolError::Io(std::io::Error::other("spool lock poisoned")))?;
        let next_bytes = storage
            .used_bytes()
            .saturating_add(*reserved_bytes)
            .saturating_add(bytes);
        let next_frames = storage
            .frame_count()
            .saturating_add(*reserved_frames)
            .saturating_add(frames);
        if next_bytes > self.shared.config.max_wal_bytes
            || next_frames > self.shared.config.max_frames
        {
            return Err(SpoolError::CapacityExceeded);
        }
        *reserved_bytes = reserved_bytes.saturating_add(bytes);
        *reserved_frames = reserved_frames.saturating_add(frames);
        Ok(())
    }

    fn release(&self, bytes: usize, frames: usize) {
        if let Ok(mut reserved_bytes) = self.shared.reserved_bytes.lock() {
            *reserved_bytes = reserved_bytes.saturating_sub(bytes);
        }
        if let Ok(mut reserved_frames) = self.shared.reserved_frames.lock() {
            *reserved_frames = reserved_frames.saturating_sub(frames);
        }
    }

    fn reserve_completion(&self) -> Result<(), SpoolError> {
        let mut inflight = self
            .shared
            .inflight_completions
            .lock()
            .map_err(|_| SpoolError::Io(std::io::Error::other("spool lock poisoned")))?;
        if *inflight >= self.shared.config.max_inflight_completions {
            return Err(SpoolError::CapacityExceeded);
        }
        *inflight = inflight.saturating_add(1);
        Ok(())
    }

    fn release_completion(&self) {
        release_completion_shared(&self.shared);
    }
}

async fn complete_work<S: SpoolStorage>(shared: &Arc<CellShared<S>>, work: CompletionWork) {
    let CompletionWork {
        identity,
        progress_bytes,
        driver,
        reply,
    } = work;

    let delivery = match driver.await {
        Ok(delivery) => delivery,
        Err(_) => DriverDelivery::Canceled,
    };

    let outcome = match delivery {
        DriverDelivery::Canceled => {
            // Admit already released lifecycle reservations and removed identity.
            Err(SpoolError::Unavailable)
        }
        DriverDelivery::ForwardFailed(error) => {
            release_shared(shared, progress_bytes, 1);
            poison_shared(shared, SpoolPoisonCause::ForwardFailed);
            Err(error)
        }
        DriverDelivery::Forwarded(inner) => match inner.await {
            Err(error) => {
                release_shared(shared, progress_bytes, 1);
                poison_shared(shared, SpoolPoisonCause::ReceiptFailed);
                Err(SpoolError::Forward(error.to_string()))
            }
            Ok(receipt) => {
                let persist = (|| {
                    let mut storage = shared.storage.lock().map_err(|_| {
                        SpoolError::Io(std::io::Error::other("spool lock poisoned"))
                    })?;
                    storage.append_frame(&SpoolFrame::Progress(identity))?;
                    storage.sync()?;
                    Ok::<(), SpoolError>(())
                })();
                match persist {
                    Ok(()) => {
                        release_shared(shared, progress_bytes, 1);
                        Ok(receipt)
                    }
                    Err(SpoolError::Io(_)) | Err(SpoolError::CapacityExceeded) => {
                        release_shared(shared, progress_bytes, 1);
                        poison_shared(shared, SpoolPoisonCause::ProgressFailed);
                        Err(SpoolError::ProgressFailed)
                    }
                    Err(other) => {
                        release_shared(shared, progress_bytes, 1);
                        poison_shared(shared, SpoolPoisonCause::ProgressFailed);
                        Err(other)
                    }
                }
            }
        },
    };

    release_completion_shared(shared);

    // Prefer not to reply Canceled when the admit path already failed for the caller.
    if !matches!(outcome, Err(SpoolError::Unavailable)) {
        let _ = reply.send(outcome);
    }
}

fn release_completion_shared<S>(shared: &CellShared<S>) {
    if let Ok(mut inflight) = shared.inflight_completions.lock() {
        *inflight = inflight.saturating_sub(1);
    }
}

fn release_shared<S>(shared: &CellShared<S>, bytes: usize, frames: usize) {
    if let Ok(mut reserved_bytes) = shared.reserved_bytes.lock() {
        *reserved_bytes = reserved_bytes.saturating_sub(bytes);
    }
    if let Ok(mut reserved_frames) = shared.reserved_frames.lock() {
        *reserved_frames = reserved_frames.saturating_sub(frames);
    }
}

fn poison_shared<S>(shared: &CellShared<S>, cause: SpoolPoisonCause) {
    if let Ok(mut state) = shared.state.lock()
        && matches!(*state, SpoolCellState::Serving)
    {
        *state = SpoolCellState::Poisoned { cause };
    }
}

/// Receipt future resolved by [`SpoolCell::run`] after commit and durable progress.
///
/// Dropping this future never cancels cell-owned completion work and never
/// reports success without a progress sync.
#[must_use = "receipts are learned by awaiting this future"]
pub struct SpoolReceiptFuture {
    receiver: oneshot::Receiver<Result<Receipt, SpoolError>>,
}

impl Future for SpoolReceiptFuture {
    type Output = Result<Receipt, SpoolError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(context)
            .map(|result| result.unwrap_or(Err(SpoolError::Unavailable)))
    }
}
