//! Multi-journal local owner service over Phase 1 [`ChunkDriverActor`].
//!
//! This layer owns task lifecycle and journal routing only. It never duplicates
//! admission, dedup, offset allocation, or timer policy — those stay in
//! [`scripture::ChunkDriverHandle`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use scripture::{
    ChunkDriverActor, ChunkDriverHandle, Clock, DriverError, JournalId, ReceiptFuture, Submission,
    Timer,
};
use tokio::task::JoinHandle;

/// Observable status of one registered local owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerStatus {
    /// The actor `run` future is still live and accepting work.
    Running,
    /// The actor observed an uncertain append and refuses new work.
    Poisoned,
    /// The actor task finished (channel closed / run returned). Not restarted.
    TaskFinished,
}

/// Snapshot returned by [`ChunkJournalService::health`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerHealth {
    /// Journal this owner serves.
    pub journal_id: JournalId,
    /// Local registration diagnostic only.
    ///
    /// This is the value supplied to [`ChunkJournalService::register_owner`].
    /// It is **not** a Holylog / ConditionalRegister fencing token and must not
    /// appear in fleet-facing protocol messages as an ownership generation.
    pub registered_owner_generation: u64,
    /// Live status observed by the service.
    pub status: OwnerStatus,
}

/// Errors at the multi-journal service boundary.
#[derive(Debug, thiserror::Error)]
pub enum ChunkServiceError {
    /// No owner is registered for this journal.
    #[error("unknown journal {journal_id:?}")]
    UnknownJournal {
        /// Requested journal.
        journal_id: JournalId,
    },
    /// The owner is poisoned or its task has finished.
    #[error("owner for journal {journal_id:?} is unavailable ({status:?})")]
    OwnerUnavailable {
        /// Journal whose owner cannot accept work.
        journal_id: JournalId,
        /// Observed status.
        status: OwnerStatus,
    },
    /// A registered owner rejected or failed a request.
    #[error(transparent)]
    Driver(#[from] DriverError),
    /// An owner is already registered for this journal.
    #[error("journal {journal_id:?} already has a registered owner")]
    AlreadyRegistered {
        /// Conflicting journal.
        journal_id: JournalId,
    },
}

struct OwnerSlot {
    journal_id: JournalId,
    registered_owner_generation: u64,
    handle: Option<ChunkDriverHandle>,
    status: Arc<Mutex<OwnerStatus>>,
    /// Held so the actor task is not detached without observation.
    task: JoinHandle<()>,
}

impl OwnerSlot {
    fn health(&self) -> OwnerHealth {
        let status = if self.task.is_finished() {
            OwnerStatus::TaskFinished
        } else if self
            .handle
            .as_ref()
            .is_some_and(|handle| handle.metrics().poisoned)
        {
            OwnerStatus::Poisoned
        } else {
            self.status
                .lock()
                .map(|guard| *guard)
                .unwrap_or(OwnerStatus::TaskFinished)
        };
        OwnerHealth {
            journal_id: self.journal_id,
            registered_owner_generation: self.registered_owner_generation,
            status,
        }
    }

    fn mark_poisoned(&self) {
        if let Ok(mut status) = self.status.lock()
            && *status == OwnerStatus::Running
        {
            *status = OwnerStatus::Poisoned;
        }
    }

    fn ensure_accepting(&self) -> Result<&ChunkDriverHandle, ChunkServiceError> {
        let status = self.health().status;
        if status != OwnerStatus::Running {
            return Err(ChunkServiceError::OwnerUnavailable {
                journal_id: self.journal_id,
                status,
            });
        }
        self.handle
            .as_ref()
            .ok_or(ChunkServiceError::OwnerUnavailable {
                journal_id: self.journal_id,
                status: OwnerStatus::TaskFinished,
            })
    }

    fn map_driver_error(&self, error: DriverError) -> ChunkServiceError {
        match error {
            DriverError::Poisoned | DriverError::Uncertain { .. } | DriverError::Unavailable => {
                self.mark_poisoned();
                ChunkServiceError::OwnerUnavailable {
                    journal_id: self.journal_id,
                    status: OwnerStatus::Poisoned,
                }
            }
            other => ChunkServiceError::Driver(other),
        }
    }
}

/// Local multi-journal registry of Phase 1 chunk owners.
///
/// Phase 1: static configuration only. The service never creates an owner on
/// demand, never restarts a finished task, and never selects a successor.
#[derive(Default)]
pub struct ChunkJournalService {
    owners: BTreeMap<JournalId, OwnerSlot>,
}

impl ChunkJournalService {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a pre-built owner and spawns its `run` future on Tokio.
    ///
    /// This is a **local lab composition** primitive: it does not observe Canon
    /// authority, recover a VirtualLog suffix, or grant a distributed fence.
    /// Prefer [`crate::recover_canon_owner`] when constructing a Canon-authorized
    /// owner from durable evidence, then register the returned handle/actor here
    /// only if a process-local registry is desired.
    ///
    /// The service observes task completion and surfaces it in
    /// [`Self::health`]. It does not recover or replace the owner.
    pub fn register_owner<C, T>(
        &mut self,
        journal_id: JournalId,
        registered_owner_generation: u64,
        handle: ChunkDriverHandle,
        actor: ChunkDriverActor<C, T>,
    ) -> Result<(), ChunkServiceError>
    where
        C: Clock + Send + 'static,
        T: Timer + Send + 'static,
    {
        if self.owners.contains_key(&journal_id) {
            return Err(ChunkServiceError::AlreadyRegistered { journal_id });
        }
        let status = Arc::new(Mutex::new(OwnerStatus::Running));
        let task_status = Arc::clone(&status);
        let task = tokio::spawn(async move {
            let _ = actor.run().await;
            if let Ok(mut guard) = task_status.lock() {
                *guard = OwnerStatus::TaskFinished;
            }
        });
        self.owners.insert(
            journal_id,
            OwnerSlot {
                journal_id,
                registered_owner_generation,
                handle: Some(handle),
                status,
                task,
            },
        );
        Ok(())
    }

    /// Drops the owner's submission endpoint so its `run` future exits.
    ///
    /// The registry entry remains. Health reports [`OwnerStatus::TaskFinished`].
    /// The service never restarts or replaces the owner.
    pub async fn stop_owner(&mut self, journal_id: JournalId) -> Result<(), ChunkServiceError> {
        let owner = self
            .owners
            .get_mut(&journal_id)
            .ok_or(ChunkServiceError::UnknownJournal { journal_id })?;
        drop(owner.handle.take());
        // Replace the join handle with a finished noop so we can await the old one.
        let task = std::mem::replace(&mut owner.task, tokio::spawn(async {}));
        let status = Arc::clone(&owner.status);
        let _ = owner;
        let _ = task.await;
        if let Ok(mut guard) = status.lock() {
            *guard = OwnerStatus::TaskFinished;
        }
        Ok(())
    }

    /// Submits through the registered owner for `journal_id`.
    pub async fn submit(
        &self,
        journal_id: JournalId,
        submission: Submission,
    ) -> Result<ReceiptFuture, ChunkServiceError> {
        let owner = self.owner(journal_id)?;
        let handle = owner.ensure_accepting()?;
        match handle.submit(submission).await {
            Ok(receipt) => Ok(receipt),
            Err(error) => Err(owner.map_driver_error(error)),
        }
    }

    /// Flushes the registered owner's open chunk.
    pub async fn flush(&self, journal_id: JournalId) -> Result<(), ChunkServiceError> {
        let owner = self.owner(journal_id)?;
        let handle = owner.ensure_accepting()?;
        match handle.flush().await {
            Ok(()) => Ok(()),
            Err(error) => Err(owner.map_driver_error(error)),
        }
    }

    /// Returns the observed health of a registered owner.
    pub fn health(&self, journal_id: JournalId) -> Result<OwnerHealth, ChunkServiceError> {
        Ok(self.owner(journal_id)?.health())
    }

    fn owner(&self, journal_id: JournalId) -> Result<&OwnerSlot, ChunkServiceError> {
        self.owners
            .get(&journal_id)
            .ok_or(ChunkServiceError::UnknownJournal { journal_id })
    }
}
