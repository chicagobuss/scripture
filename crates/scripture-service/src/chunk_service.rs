//! Multi-journal local owner service over Phase 1 [`ChunkDriverActor`].
//!
//! This layer owns task lifecycle, journal routing, and the local draining gate
//! for an operator-directed Canon handoff. It never duplicates admission,
//! dedup, offset allocation, or timer policy — those stay in
//! [`scripture::ChunkDriverHandle`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use scripture::{
    CanonAuthorityError, CanonFence, CanonOwner, ChunkDriverActor, ChunkDriverHandle, Clock,
    DriverError, JournalId, OwnerId, ReceiptFuture, Submission, Timer, VerseId,
    WitnessedCanonAuthority,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::canon_owner::RecoveredCanonOwner;

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
    /// This is the value supplied to [`ChunkJournalService::register_owner`] or
    /// the Canon revision from [`ChunkJournalService::register_canon_owner`].
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
    /// Recovered Canon authority cannot bind a local owner (unowned / mismatch).
    #[error(transparent)]
    Authority(#[from] CanonAuthorityError),
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
    /// The journal was registered without a Canon binding (lab path).
    #[error("journal {journal_id:?} has no Canon owner binding and cannot drain for publish")]
    NotCanonBound {
        /// Journal lacking a Canon binding.
        journal_id: JournalId,
    },
    /// Witnessed authority does not match the locally registered Canon owner.
    #[error(
        "Canon binding mismatch for journal {journal_id:?}: registered owner {registered_owner} revision {registered_revision}, authority owner {authority_owner:?} revision {authority_revision}"
    )]
    BindingMismatch {
        /// Journal being drained.
        journal_id: JournalId,
        /// Owner identity stored at Canon registration.
        registered_owner: OwnerId,
        /// Canon revision stored at registration.
        registered_revision: u64,
        /// Owner claimed by the witnessed authority, if owned.
        authority_owner: Option<OwnerId>,
        /// Revision claimed by the witnessed authority.
        authority_revision: u64,
    },
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

/// Proof that a local Canon-bound owner drained and may be passed to publish.
///
/// Constructed only by [`ChunkJournalService::drain_owner`] from the stored
/// Canon binding. It is not a distributed lease and does not authorize serving.
#[derive(Debug)]
pub struct DrainedOwner {
    journal_id: JournalId,
    verse_id: VerseId,
    owner_id: OwnerId,
    revision: u64,
}

impl DrainedOwner {
    /// Journal that was drained.
    #[must_use]
    pub const fn journal_id(&self) -> JournalId {
        self.journal_id
    }

    /// Physical Verse bound at Canon registration.
    #[must_use]
    pub const fn verse_id(&self) -> VerseId {
        self.verse_id
    }

    /// Owner identity from the local Canon binding.
    #[must_use]
    pub const fn owner_id(&self) -> OwnerId {
        self.owner_id
    }

    /// Canon revision from the local Canon binding.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Test-only constructor for fail-closed publish validation paths.
    #[cfg(test)]
    pub(crate) const fn for_test(
        journal_id: JournalId,
        verse_id: VerseId,
        owner_id: OwnerId,
        revision: u64,
    ) -> Self {
        Self {
            journal_id,
            verse_id,
            owner_id,
            revision,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum LocalLifecycle {
    Serving = 0,
    Draining = 1,
    Stopped = 2,
}

impl LocalLifecycle {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Serving,
            1 => Self::Draining,
            _ => Self::Stopped,
        }
    }
}

/// Identity of the Canon owner that was registered into a slot.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonOwnerBinding {
    journal_id: JournalId,
    verse_id: VerseId,
    owner_id: OwnerId,
    revision: u64,
    fence: CanonFence,
}

impl CanonOwnerBinding {
    fn from_fence(fence: &CanonFence) -> Result<Self, CanonAuthorityError> {
        match &fence.owner {
            CanonOwner::Unowned => Err(CanonAuthorityError::Unowned {
                revision: fence.revision,
                verse_id: fence.verse_id,
            }),
            CanonOwner::Owned { owner_id, .. } => Ok(Self {
                journal_id: fence.journal_id,
                verse_id: fence.verse_id,
                owner_id: *owner_id,
                revision: fence.revision,
                fence: fence.clone(),
            }),
        }
    }
}

struct OwnerSlot {
    journal_id: JournalId,
    registered_owner_generation: u64,
    canon: Option<CanonOwnerBinding>,
    handle: Option<ChunkDriverHandle>,
    /// Exclusion for Serving → Draining → Stopped transitions and admissions.
    lifecycle: Arc<Mutex<()>>,
    /// Lock-free snapshot for [`OwnerSlot::health`].
    lifecycle_view: Arc<AtomicU8>,
    /// Held so the actor task is not detached without observation.
    task: JoinHandle<()>,
}

impl OwnerSlot {
    fn lifecycle(&self) -> LocalLifecycle {
        LocalLifecycle::from_u8(self.lifecycle_view.load(Ordering::Acquire))
    }

    fn set_lifecycle(&self, next: LocalLifecycle) {
        self.lifecycle_view.store(next as u8, Ordering::Release);
    }

    fn health(&self) -> OwnerHealth {
        let lifecycle = self.lifecycle();
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
    /// Lab owners cannot be drained for Canon publish; use
    /// [`Self::register_canon_owner`] for handoff-capable registration.
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
        self.insert_owner(journal_id, registered_owner_generation, handle, actor, None)
    }

    /// Registers a recovered Canon owner, storing its fence identity for drain.
    ///
    /// The binding is taken from the factory-created [`RecoveredCanonOwner`]. Only this
    /// path can later produce a [`DrainedOwner`] suitable for
    /// [`crate::publish_canon_transition`].
    pub fn register_canon_owner<C, T>(
        &mut self,
        recovered: RecoveredCanonOwner<C, T>,
    ) -> Result<(), ChunkServiceError>
    where
        C: Clock + Send + 'static,
        T: Timer + Send + 'static,
    {
        let (authority, handle, actor) = recovered.into_canon_registration();
        let binding = CanonOwnerBinding::from_fence(&authority.fence)?;
        let journal_id = binding.journal_id;
        let revision = binding.revision;
        self.insert_owner(journal_id, revision, handle, actor, Some(binding))
    }

    fn insert_owner<C, T>(
        &mut self,
        journal_id: JournalId,
        registered_owner_generation: u64,
        handle: ChunkDriverHandle,
        actor: ChunkDriverActor<C, T>,
        canon: Option<CanonOwnerBinding>,
    ) -> Result<(), ChunkServiceError>
    where
        C: Clock + Send + 'static,
        T: Timer + Send + 'static,
    {
        if self.owners.contains_key(&journal_id) {
            return Err(ChunkServiceError::AlreadyRegistered { journal_id });
        }
        let lifecycle = Arc::new(Mutex::new(()));
        let lifecycle_view = Arc::new(AtomicU8::new(LocalLifecycle::Serving as u8));
        let task_view = Arc::clone(&lifecycle_view);
        let task = tokio::spawn(async move {
            let _ = actor.run().await;
            task_view.store(LocalLifecycle::Stopped as u8, Ordering::Release);
        });
        self.owners.insert(
            journal_id,
            OwnerSlot {
                journal_id,
                registered_owner_generation,
                canon,
                handle: Some(handle),
                lifecycle,
                lifecycle_view,
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
            let _guard = owner.lifecycle.lock().await;
            owner.set_lifecycle(LocalLifecycle::Stopped);
        }
        drop(owner.handle.take());
        // Replace the join handle with a finished noop so we can await the old one.
        let task = std::mem::replace(&mut owner.task, tokio::spawn(async {}));
        let lifecycle_view = Arc::clone(&owner.lifecycle_view);
        let _ = owner;
        let _ = task.await;
        lifecycle_view.store(LocalLifecycle::Stopped as u8, Ordering::Release);
        Ok(())
    }

    /// Stops new admissions, flushes open work, and returns a drained token.
    ///
    /// Requires a Canon-bound registration and a self-consistent
    /// [`WitnessedCanonAuthority`] that matches the stored binding. On success
    /// the owner stays locally [`OwnerStatus::Draining`] until publish stops it
    /// or an operator inspects. Binding failures leave the owner Serving.
    /// Poison or uncertain flush yields [`DrainError::DrainFailed`] and must not
    /// publish a successor.
    pub async fn drain_owner(
        &mut self,
        journal_id: JournalId,
        authority: &WitnessedCanonAuthority,
    ) -> Result<DrainedOwner, DrainError> {
        authority.validate()?;

        let owner = self
            .owners
            .get_mut(&journal_id)
            .ok_or(ChunkServiceError::UnknownJournal { journal_id })?;

        let binding = owner
            .canon
            .as_ref()
            .ok_or(DrainError::NotCanonBound { journal_id })?;

        if binding.journal_id != journal_id
            || authority.fence().journal_id != journal_id
            || authority.fence().verse_id != binding.verse_id
            || authority.revision() != binding.revision
            || authority.fence() != &binding.fence
        {
            return Err(DrainError::BindingMismatch {
                journal_id,
                registered_owner: binding.owner_id,
                registered_revision: binding.revision,
                authority_owner: match &authority.fence().owner {
                    CanonOwner::Owned { owner_id, .. } => Some(*owner_id),
                    CanonOwner::Unowned => None,
                },
                authority_revision: authority.revision(),
            });
        }
        match &authority.fence().owner {
            CanonOwner::Owned { owner_id, .. } if *owner_id == binding.owner_id => {}
            CanonOwner::Owned { owner_id, .. } => {
                return Err(DrainError::BindingMismatch {
                    journal_id,
                    registered_owner: binding.owner_id,
                    registered_revision: binding.revision,
                    authority_owner: Some(*owner_id),
                    authority_revision: authority.revision(),
                });
            }
            CanonOwner::Unowned => {
                return Err(DrainError::Authority(CanonAuthorityError::Unowned {
                    revision: authority.revision(),
                    verse_id: authority.fence().verse_id,
                }));
            }
        }

        let drained = DrainedOwner {
            journal_id: binding.journal_id,
            verse_id: binding.verse_id,
            owner_id: binding.owner_id,
            revision: binding.revision,
        };

        let handle = {
            let _guard = owner.lifecycle.lock().await;
            let life = owner.lifecycle();
            if life != LocalLifecycle::Serving {
                let status = match life {
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
            owner.set_lifecycle(LocalLifecycle::Draining);
            handle.clone()
        };

        // Already-admitted driver work reaches a terminal durability outcome via
        // flush. New submit/flush at this service boundary sees Draining.
        match handle.flush().await {
            Ok(()) => {
                if handle.metrics().poisoned {
                    return Err(DrainError::DrainFailed { journal_id });
                }
                Ok(drained)
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
        let _guard = owner.lifecycle.lock().await;
        match owner.lifecycle() {
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
        let _guard = owner.lifecycle.lock().await;
        match owner.lifecycle() {
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

    /// Returns a lock-free driver metrics snapshot when the owner handle is present.
    pub fn driver_metrics(
        &self,
        journal_id: JournalId,
    ) -> Result<Option<scripture::DriverMetrics>, ChunkServiceError> {
        Ok(self.owner(journal_id)?.handle.as_ref().map(|h| h.metrics()))
    }

    /// Read-only check used by Canon route resolution.
    ///
    /// Reports whether a Canon-bound local owner exactly matches the supplied
    /// journal / Verse / owner / revision identity and whether it is Running.
    /// Lab registrations and identity mismatches are [`LocalCanonOwnerMatch::Unavailable`].
    #[must_use]
    pub fn local_canon_owner_match(
        &self,
        journal_id: JournalId,
        verse_id: VerseId,
        owner_id: OwnerId,
        revision: u64,
    ) -> LocalCanonOwnerMatch {
        let Some(owner) = self.owners.get(&journal_id) else {
            return LocalCanonOwnerMatch::Unavailable;
        };
        let Some(binding) = owner.canon.as_ref() else {
            return LocalCanonOwnerMatch::Unavailable;
        };
        if binding.journal_id != journal_id
            || binding.verse_id != verse_id
            || binding.owner_id != owner_id
            || binding.revision != revision
        {
            return LocalCanonOwnerMatch::Unavailable;
        }
        let status = owner.health().status;
        if status == OwnerStatus::Running {
            LocalCanonOwnerMatch::ServeReady
        } else {
            LocalCanonOwnerMatch::BoundNotRunning { status }
        }
    }

    fn owner(&self, journal_id: JournalId) -> Result<&OwnerSlot, ChunkServiceError> {
        self.owners
            .get(&journal_id)
            .ok_or(ChunkServiceError::UnknownJournal { journal_id })
    }
}

/// Outcome of [`ChunkJournalService::local_canon_owner_match`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalCanonOwnerMatch {
    /// Exact Canon binding is present and [`OwnerStatus::Running`].
    ServeReady,
    /// Exact Canon binding is present but not serving.
    BoundNotRunning {
        /// Observed local status.
        status: OwnerStatus,
    },
    /// Missing journal, lab-only registration, or identity mismatch.
    Unavailable,
}
