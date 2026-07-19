//! Consumer binding fence and durable progress (never Serving Authority).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::types::{CanonRef, SourceOffset, VerseRef, WorkloadId};

/// Opaque compare-and-swap version for a progress or lease observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProgressVersion(u64);

impl ProgressVersion {
    /// Constructs a version token.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Integer form (test / debug only).
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Opaque owner token for a fenced consumer binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BindingToken(String);

impl BindingToken {
    /// Constructs a non-empty token.
    pub fn new(value: impl Into<String>) -> Result<Self, ProgressError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ProgressError::Io("empty binding token".into()));
        }
        Ok(Self(value))
    }

    /// Generates a fresh random token (32 hex chars).
    #[must_use]
    pub fn generate() -> Self {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).expect("os rng");
        Self(hex::encode(bytes))
    }

    /// String form for manifests / evidence.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Binding that authorizes a workload to consume a specific Canon/Verse lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerBinding {
    /// Workload identity.
    pub workload_id: WorkloadId,
    /// Source Canon.
    pub canon_id: CanonRef,
    /// Source Verse.
    pub verse_id: VerseRef,
    /// Binding epoch; must be nonzero; stale epochs fail closed.
    pub binding_epoch: u64,
}

impl ConsumerBinding {
    /// Validates epoch is nonzero.
    pub fn validate(&self) -> Result<(), ProgressError> {
        if self.binding_epoch == 0 {
            return Err(ProgressError::InvalidEpoch);
        }
        Ok(())
    }
}

/// Durable fenced ownership of one `(workload, Canon, Verse)` binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquiredBinding {
    /// Binding identity + epoch.
    pub binding: ConsumerBinding,
    /// Owner token required for checkpoint advancement and output commits.
    pub owner_token: BindingToken,
}

/// Consumer-owned next-offset progress for one binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerCheckpoint {
    /// Binding this checkpoint belongs to (includes epoch).
    pub binding: ConsumerBinding,
    /// Owner token that held the fence when this checkpoint was written.
    pub owner_token: BindingToken,
    /// Next source offset to consume (never last-processed).
    pub next_offset: SourceOffset,
}

/// Observe + acquire/renew fence + opaque-version CAS for checkpoints.
pub trait ConsumerProgressStore: Send + Sync {
    /// Acquires exclusive ownership of the binding, or renews if `owner_token` already holds it.
    ///
    /// Losers of a race receive [`ProgressError::FenceHeld`] and must not call
    /// `reconcile` / `apply`.
    fn acquire_or_renew(
        &self,
        binding: ConsumerBinding,
        owner_token: &BindingToken,
    ) -> Result<AcquiredBinding, ProgressError>;

    /// Reads the current checkpoint and version for a binding key.
    fn observe(
        &self,
        workload_id: &WorkloadId,
        canon_id: &CanonRef,
        verse_id: &VerseRef,
    ) -> Result<Option<(ConsumerCheckpoint, ProgressVersion)>, ProgressError>;

    /// Compare-and-swaps progress when `expected` matches and `owner_token` holds the fence.
    fn compare_and_swap(
        &self,
        checkpoint: ConsumerCheckpoint,
        expected: Option<ProgressVersion>,
        owner_token: &BindingToken,
    ) -> Result<ProgressVersion, ProgressError>;
}

/// Progress / fence store failures.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProgressError {
    /// CAS lost a race or used a stale version.
    #[error("progress CAS conflict")]
    CasConflict,
    /// Another owner holds the durable fence for this binding.
    #[error("consumer binding fence held by another owner")]
    FenceHeld,
    /// Checkpoint binding does not match the store key / epoch / token.
    #[error("stale or mismatched consumer binding")]
    StaleBinding,
    /// Binding epoch must be nonzero.
    #[error("binding_epoch must be nonzero")]
    InvalidEpoch,
    /// Underlying store I/O failed.
    #[error("progress store I/O: {0}")]
    Io(String),
}

#[derive(Debug, Clone)]
struct LeaseRecord {
    binding: ConsumerBinding,
    owner_token: BindingToken,
}

#[derive(Debug, Default)]
struct MemoryState {
    leases: HashMap<(String, String, String), LeaseRecord>,
    checkpoints: HashMap<(String, String, String), (ConsumerCheckpoint, ProgressVersion)>,
    next_version: u64,
}

/// In-process progress + fence store for deterministic contract tests.
#[derive(Debug, Clone, Default)]
pub struct InMemoryProgressStore {
    inner: Arc<Mutex<MemoryState>>,
}

impl InMemoryProgressStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl ConsumerProgressStore for InMemoryProgressStore {
    fn acquire_or_renew(
        &self,
        binding: ConsumerBinding,
        owner_token: &BindingToken,
    ) -> Result<AcquiredBinding, ProgressError> {
        binding.validate()?;
        let key = key_of(&binding.workload_id, &binding.canon_id, &binding.verse_id);
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ProgressError::Io("progress lock poisoned".into()))?;
        match guard.leases.get(&key) {
            None => {
                guard.leases.insert(
                    key,
                    LeaseRecord {
                        binding: binding.clone(),
                        owner_token: owner_token.clone(),
                    },
                );
                Ok(AcquiredBinding {
                    binding,
                    owner_token: owner_token.clone(),
                })
            }
            Some(existing) if existing.owner_token == *owner_token => {
                if existing.binding.binding_epoch != binding.binding_epoch {
                    return Err(ProgressError::StaleBinding);
                }
                if existing.binding.workload_id != binding.workload_id
                    || existing.binding.canon_id != binding.canon_id
                    || existing.binding.verse_id != binding.verse_id
                {
                    return Err(ProgressError::StaleBinding);
                }
                // Renew: refresh recorded binding (same token).
                guard.leases.insert(
                    key,
                    LeaseRecord {
                        binding: binding.clone(),
                        owner_token: owner_token.clone(),
                    },
                );
                Ok(AcquiredBinding {
                    binding,
                    owner_token: owner_token.clone(),
                })
            }
            Some(_) => Err(ProgressError::FenceHeld),
        }
    }

    fn observe(
        &self,
        workload_id: &WorkloadId,
        canon_id: &CanonRef,
        verse_id: &VerseRef,
    ) -> Result<Option<(ConsumerCheckpoint, ProgressVersion)>, ProgressError> {
        let key = key_of(workload_id, canon_id, verse_id);
        let guard = self
            .inner
            .lock()
            .map_err(|_| ProgressError::Io("progress lock poisoned".into()))?;
        Ok(guard.checkpoints.get(&key).cloned())
    }

    fn compare_and_swap(
        &self,
        checkpoint: ConsumerCheckpoint,
        expected: Option<ProgressVersion>,
        owner_token: &BindingToken,
    ) -> Result<ProgressVersion, ProgressError> {
        checkpoint.binding.validate()?;
        if checkpoint.owner_token != *owner_token {
            return Err(ProgressError::StaleBinding);
        }
        let key = key_of(
            &checkpoint.binding.workload_id,
            &checkpoint.binding.canon_id,
            &checkpoint.binding.verse_id,
        );
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ProgressError::Io("progress lock poisoned".into()))?;
        let lease = guard.leases.get(&key).ok_or(ProgressError::StaleBinding)?;
        if lease.owner_token != *owner_token {
            return Err(ProgressError::FenceHeld);
        }
        if lease.binding.binding_epoch != checkpoint.binding.binding_epoch {
            return Err(ProgressError::StaleBinding);
        }
        match (guard.checkpoints.get(&key), expected) {
            (None, None) => {}
            (Some((_, version)), Some(expected)) if *version == expected => {
                let (existing, _) = guard
                    .checkpoints
                    .get(&key)
                    .cloned()
                    .ok_or(ProgressError::CasConflict)?;
                if existing.binding.binding_epoch != checkpoint.binding.binding_epoch {
                    return Err(ProgressError::StaleBinding);
                }
                if existing.binding != checkpoint.binding {
                    return Err(ProgressError::StaleBinding);
                }
            }
            _ => return Err(ProgressError::CasConflict),
        }
        let version = ProgressVersion::new(guard.next_version);
        guard.next_version = guard.next_version.saturating_add(1);
        guard.checkpoints.insert(key, (checkpoint, version));
        Ok(version)
    }
}

fn key_of(
    workload_id: &WorkloadId,
    canon_id: &CanonRef,
    verse_id: &VerseRef,
) -> (String, String, String) {
    (
        workload_id.as_str().to_owned(),
        canon_id.as_str().to_owned(),
        verse_id.as_str().to_owned(),
    )
}

mod hex {
    pub(super) fn encode(bytes: [u8; 16]) -> String {
        let mut out = String::with_capacity(32);
        for byte in bytes {
            out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
            out.push(char::from_digit(u32::from(byte & 0xf), 16).unwrap_or('0'));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(epoch: u64) -> ConsumerBinding {
        ConsumerBinding {
            workload_id: WorkloadId::new("wl-1").expect("id"),
            canon_id: CanonRef::new("canon-a").expect("canon"),
            verse_id: VerseRef::new("verse-1").expect("verse"),
            binding_epoch: epoch,
        }
    }

    #[test]
    fn acquire_race_second_loses_before_any_output() {
        let store = InMemoryProgressStore::new();
        let t1 = BindingToken::new("token-a").expect("t");
        let t2 = BindingToken::new("token-b").expect("t");
        store.acquire_or_renew(binding(1), &t1).expect("first wins");
        assert_eq!(
            store.acquire_or_renew(binding(1), &t2),
            Err(ProgressError::FenceHeld)
        );
    }

    #[test]
    fn renew_same_token_ok_zero_epoch_rejected() {
        let store = InMemoryProgressStore::new();
        let token = BindingToken::new("token-a").expect("t");
        store.acquire_or_renew(binding(1), &token).expect("acquire");
        store.acquire_or_renew(binding(1), &token).expect("renew");
        assert_eq!(
            store.acquire_or_renew(binding(0), &token),
            Err(ProgressError::InvalidEpoch)
        );
    }

    #[test]
    fn cas_requires_fence_token() {
        let store = InMemoryProgressStore::new();
        let token = BindingToken::new("token-a").expect("t");
        let other = BindingToken::new("token-b").expect("t");
        store.acquire_or_renew(binding(1), &token).expect("acquire");
        let cp = ConsumerCheckpoint {
            binding: binding(1),
            owner_token: token.clone(),
            next_offset: SourceOffset::new(0),
        };
        store.compare_and_swap(cp, None, &token).expect("cas");
        let forged = ConsumerCheckpoint {
            binding: binding(1),
            owner_token: other.clone(),
            next_offset: SourceOffset::new(1),
        };
        assert_eq!(
            store.compare_and_swap(forged, Some(ProgressVersion::new(0)), &other),
            Err(ProgressError::FenceHeld)
        );
    }
}
