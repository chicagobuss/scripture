//! Multi-journal local owner service over Phase 1 [`ChunkDriverActor`].
//!
//! This layer owns task lifecycle, journal routing, and the local draining gate
//! for an operator-directed Canon handoff. It never duplicates admission,
//! dedup, offset allocation, or timer policy — those stay in
//! [`scripture::ChunkDriverHandle`].

use std::collections::BTreeMap;
use std::sync::Arc;

use scripture::{
    CanonAuthorityError, CanonAuthoritySnapshot, CanonOwner, ChunkDriverActor, ChunkDriverHandle,
    Clock, DriverError, JournalId, OwnerId, ReceiptFuture, Submission, Timer,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Observable status of one registered local owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerStatus {
    /// The actor `run` future is still live and accepting work.
    Running,
    /// Local drain is in progress or complete; new submit/flush are refused.
    ///
    /// This is a process-local lifecycle bit, not distributed Canon ownership.
    Draining,
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
    /// The owner is poisoned, draining, or its task has finished.
    #[error("owner for journal {journal_id:?} is unavailable ({status:?})")]
    OwnerUnavailable {
        /// Journal whose owner cannot accept work.
        journal_id: JournalId,
        /// Observed status.
        status: OwnerStatus,
    },
    /// The local owner has entered drain and refuses new admissions.
    #[error("owner for journal {journal_id:?} is draining")]
    OwnerDraining {
        /// Journal whose owner is draining.
        journal_id: JournalId,
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

/// Failures entering the local draining gate.
#[derive(Debug, thiserror::Error)]
pub enum DrainError {
    /// No owner is registered for this journal.
    #[error(transparent)]
    Service(#[from] ChunkServiceError),
    /// Canon authority refused the drain.
    #[error(transparent)]
    Authority(#[from] CanonAuthorityError),
    /// The owner is not in a serving state that can drain.
    #[error("owner for journal {journal_id:?} cannot drain from status {status:?}")]
    NotServing {
        /// Journal that cannot drain.
        journal_id: JournalId,
        /// Observed local status.
        status: OwnerStatus,
    },
    /// Flush or an admitted outcome left the owner poisoned/uncertain.
    #[error("drain failed for journal {journal_id:?}: owner is poisoned or uncertain")]
    DrainFailed {
        /// Journal whose drain failed.
        journal_id: JournalId,
    },
}

/// Proof that a local owner drained and may be passed to Canon publish.
///
/// Constructed only by [`ChunkJournalService::drain_owner`]. It is not a
/// distributed lease and does not authorize serving.
#[derive(Debug)]
pub struct DrainedOwner {
    journal_id: JournalId,
    owner_id: OwnerId,
    revision: u64,
}

impl DrainedOwner {
    /// Journal that was drained.
    #[must_use]
    pub const fn journal_id(&self) -> JournalId {
        self.journal_id
    }

    /// Owner identity validated against Canon at drain time.
    #[must_use]
    pub const fn owner_id(&self) -> OwnerId {
        self.owner_id
    }

    /// Canon revision observed when drain was authorized.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Test-only constructor for fail-closed publish validation paths.
    #[cfg(test)]
    pub(crate) const fn for_test(journal_id: JournalId, owner_id: OwnerId, revision: u64) -> Self {
        Self {
            journal_id,
            owner_id,
            revision,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalLifecycle {
    Serving,
    Draining,
    Stopped,
}

struct OwnerSlot {
    journal_id: JournalId,
    registered_owner_generation: u64,
    handle: Option<ChunkDriverHandle>,
    lifecycle: Arc<Mutex<LocalLifecycle>>,
    /// Held so the actor task is not detached without observation.
    task: JoinHandle<()>,
}

impl OwnerSlot {
    fn health(&self) -> OwnerHealth {
        let lifecycle = self
            .lifecycle
            .try_lock()
            .map(|guard| *guard)
            .unwrap_or(LocalLifecycle::Stopped);
        let status = if self.task.is_finished() || lifecycle == LocalLifecycle::Stopped {
            OwnerStatus::TaskFinished
        } else if self
            .handle
            .as_ref()
            .is_some_and(|handle| handle.metrics().poisoned)
        {
            OwnerStatus::Poisoned
        } else if lifecycle == LocalLifecycle::Draining {
            OwnerStatus::Draining
        } else {
            OwnerStatus::Running
        };
        OwnerHealth {
            journal_id: self.journal_id,
            registered_owner_generation: self.registered_owner_generation,
            status,
        }
    }

    fn map_driver_error(&self, error: DriverError) -> ChunkServiceError {
        match error {
            DriverError::Poisoned | DriverError::Uncertain { .. } | DriverError::Unavailable => {
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
        let lifecycle = Arc::new(Mutex::new(LocalLifecycle::Serving));
        let task_lifecycle = Arc::clone(&lifecycle);
        let task = tokio::spawn(async move {
            let _ = actor.run().await;
            *task_lifecycle.lock().await = LocalLifecycle::Stopped;
        });
        self.owners.insert(
            journal_id,
            OwnerSlot {
                journal_id,
                registered_owner_generation,
                handle: Some(handle),
                lifecycle,
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
        {
            let mut life = owner.lifecycle.lock().await;
            *life = LocalLifecycle::Stopped;
        }
        drop(owner.handle.take());
        // Replace the join handle with a finished noop so we can await the old one.
        let task = std::mem::replace(&mut owner.task, tokio::spawn(async {}));
        let lifecycle = Arc::clone(&owner.lifecycle);
        let _ = owner;
        let _ = task.await;
        *lifecycle.lock().await = LocalLifecycle::Stopped;
        Ok(())
    }

    /// Stops new admissions, flushes open work, and returns a drained token.
    ///
    /// Requires `authority` to name this journal and `expected_owner_id` as the
    /// current Canon owner. On success the owner stays locally
    /// [`OwnerStatus::Draining`] until publish stops it or an operator inspects.
    /// Poison or uncertain flush yields [`DrainError::DrainFailed`] and must not
    /// publish a successor.
    pub async fn drain_owner(
        &mut self,
        journal_id: JournalId,
        authority: &CanonAuthoritySnapshot,
        expected_owner_id: OwnerId,
    ) -> Result<DrainedOwner, DrainError> {
        if authority.fence.journal_id != journal_id {
            return Err(DrainError::Authority(
                CanonAuthorityError::JournalMismatch {
                    expected: journal_id,
                    actual: authority.fence.journal_id,
                },
            ));
        }
        match &authority.fence.owner {
            CanonOwner::Unowned => {
                return Err(DrainError::Authority(CanonAuthorityError::Unowned {
                    revision: authority.revision(),
                    line_id: authority.fence.line_id,
                }));
            }
            CanonOwner::Owned { owner_id, .. } if *owner_id != expected_owner_id => {
                return Err(DrainError::Authority(CanonAuthorityError::NotOwner {
                    revision: authority.revision(),
                    expected: expected_owner_id,
                    actual: *owner_id,
                }));
            }
            CanonOwner::Owned { .. } => {}
        }

        let owner = self
            .owners
            .get_mut(&journal_id)
            .ok_or(ChunkServiceError::UnknownJournal { journal_id })?;

        let handle = {
            let mut life = owner.lifecycle.lock().await;
            if *life != LocalLifecycle::Serving {
                let status = match *life {
                    LocalLifecycle::Draining => OwnerStatus::Draining,
                    LocalLifecycle::Stopped => OwnerStatus::TaskFinished,
                    LocalLifecycle::Serving => OwnerStatus::Running,
                };
                return Err(DrainError::NotServing { journal_id, status });
            }
            let handle = owner
                .handle
                .as_ref()
                .ok_or(ChunkServiceError::OwnerUnavailable {
                    journal_id,
                    status: OwnerStatus::TaskFinished,
                })?;
            if handle.metrics().poisoned {
                return Err(DrainError::NotServing {
                    journal_id,
                    status: OwnerStatus::Poisoned,
                });
            }
            *life = LocalLifecycle::Draining;
            handle.clone()
        };

        // Already-admitted driver work reaches a terminal durability outcome via
        // flush. New submit/flush at this service boundary sees Draining.
        match handle.flush().await {
            Ok(()) => {
                if handle.metrics().poisoned {
                    return Err(DrainError::DrainFailed { journal_id });
                }
                Ok(DrainedOwner {
                    journal_id,
                    owner_id: expected_owner_id,
                    revision: authority.revision(),
                })
            }
            Err(DriverError::Poisoned)
            | Err(DriverError::Uncertain { .. })
            | Err(DriverError::Unavailable) => Err(DrainError::DrainFailed { journal_id }),
            Err(other) => Err(DrainError::Service(ChunkServiceError::Driver(other))),
        }
    }

    /// Submits through the registered owner for `journal_id`.
    pub async fn submit(
        &self,
        journal_id: JournalId,
        submission: Submission,
    ) -> Result<ReceiptFuture, ChunkServiceError> {
        let owner = self.owner(journal_id)?;
        // Hold the lifecycle lock across admission so drain cannot interleave
        // between the Serving check and ChunkDriverHandle::submit.
        let _life = owner.lifecycle.lock().await;
        match *_life {
            LocalLifecycle::Draining => {
                return Err(ChunkServiceError::OwnerDraining { journal_id });
            }
            LocalLifecycle::Stopped => {
                return Err(ChunkServiceError::OwnerUnavailable {
                    journal_id,
                    status: OwnerStatus::TaskFinished,
                });
            }
            LocalLifecycle::Serving => {}
        }
        if owner
            .handle
            .as_ref()
            .is_some_and(|handle| handle.metrics().poisoned)
        {
            return Err(ChunkServiceError::OwnerUnavailable {
                journal_id,
                status: OwnerStatus::Poisoned,
            });
        }
        let handle = owner
            .handle
            .as_ref()
            .ok_or(ChunkServiceError::OwnerUnavailable {
                journal_id,
                status: OwnerStatus::TaskFinished,
            })?;
        match handle.submit(submission).await {
            Ok(receipt) => Ok(receipt),
            Err(error) => Err(owner.map_driver_error(error)),
        }
    }

    /// Flushes the registered owner's open chunk.
    pub async fn flush(&self, journal_id: JournalId) -> Result<(), ChunkServiceError> {
        let owner = self.owner(journal_id)?;
        let _life = owner.lifecycle.lock().await;
        match *_life {
            LocalLifecycle::Draining => {
                return Err(ChunkServiceError::OwnerDraining { journal_id });
            }
            LocalLifecycle::Stopped => {
                return Err(ChunkServiceError::OwnerUnavailable {
                    journal_id,
                    status: OwnerStatus::TaskFinished,
                });
            }
            LocalLifecycle::Serving => {}
        }
        if owner
            .handle
            .as_ref()
            .is_some_and(|handle| handle.metrics().poisoned)
        {
            return Err(ChunkServiceError::OwnerUnavailable {
                journal_id,
                status: OwnerStatus::Poisoned,
            });
        }
        let handle = owner
            .handle
            .as_ref()
            .ok_or(ChunkServiceError::OwnerUnavailable {
                journal_id,
                status: OwnerStatus::TaskFinished,
            })?;
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
