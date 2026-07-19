//! Consumer binding fence and progress register (never Serving Authority).
//!
//! One logical register record per binding:
//! `{ binding_epoch, binding_token, frontier, last_commit_ref }`.
//!
//! This in-memory store is a model/proof only — it does not claim durability.
//! An ObjectStore conditional-register adapter should map `acquire_or_renew` /
//! `advance` onto a single-object read + conditional put of [`ProgressRegister`].

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::types::{CanonRef, SourceOffset, VerseRef, WorkloadId};

/// Opaque durable compare-and-swap witness.
///
/// Equality-only: adapters mint this from the exact object-store conditional
/// witness (or a full digest of that witness). There is no numeric API and no
/// component accessors — callers must not invent or reinterpret versions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProgressVersion {
    opaque: std::sync::Arc<[u8]>,
}

impl ProgressVersion {
    /// Mints an opaque equality-only witness from durable bytes.
    ///
    /// Adapters and the in-memory proof store use this; product code must treat
    /// the contents as uninterpreted.
    #[must_use]
    pub fn from_opaque_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self {
            opaque: std::sync::Arc::from(bytes.as_ref()),
        }
    }
}

/// Shared async return type for progress-store operations.
pub(crate) type ProgressFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Opaque owner token for a fenced consumer binding.
///
/// Process-lifetime only: generated at process start, never persisted as a
/// renewal identity across process death. A restart always presents a fresh
/// token and therefore always bumps the epoch.
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

/// Binding identity without epoch (epoch is assigned by the progress register).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BindingKey {
    /// Workload identity.
    pub workload_id: WorkloadId,
    /// Source Canon.
    pub canon_id: CanonRef,
    /// Source Verse.
    pub verse_id: VerseRef,
}

impl BindingKey {
    /// Builds a key from validated identities.
    #[must_use]
    pub fn new(workload_id: WorkloadId, canon_id: CanonRef, verse_id: VerseRef) -> Self {
        Self {
            workload_id,
            canon_id,
            verse_id,
        }
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
    /// Binding epoch; must be nonzero; assigned by acquire, never caller-chosen.
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

    /// Identity key without epoch.
    #[must_use]
    pub fn key(&self) -> BindingKey {
        BindingKey {
            workload_id: self.workload_id.clone(),
            canon_id: self.canon_id.clone(),
            verse_id: self.verse_id.clone(),
        }
    }
}

/// Durable fenced ownership of one `(workload, Canon, Verse)` binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquiredBinding {
    /// Binding identity + store-assigned epoch.
    pub binding: ConsumerBinding,
    /// Owner token required for checkpoint advancement and output commits.
    pub owner_token: BindingToken,
}

/// One logical progress register record per binding.
///
/// Serializes as a single conditional-register value for a future ObjectStore
/// adapter. Tokens are carried in the live register while held; they are not a
/// durable renewal identity across process death.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressRegister {
    /// Binding identity + epoch.
    pub binding: ConsumerBinding,
    /// Process-lifetime holder token (not a cross-restart renewal identity).
    pub binding_token: BindingToken,
    /// Next source offset to consume (forward-only).
    pub frontier: SourceOffset,
    /// Opaque identity of the output commit that justified the latest advance.
    pub last_commit_ref: Option<String>,
}

/// Observe + acquire/renew + single-record CAS advance.
pub trait ConsumerProgressStore: Send + Sync {
    /// Acquires exclusive ownership of the binding, or renews if `owner_token`
    /// already holds it.
    ///
    /// - Absent → install epoch `1`, frontier `0`, no commit ref.
    /// - Same token → renew; epoch / frontier / last_commit_ref unchanged.
    /// - Different token → takeover: bump epoch, install token, carry frontier
    ///   and last_commit_ref forward unchanged.
    ///
    /// Tokens are never persisted as renewal identities; a restarted process
    /// must present a fresh token and therefore always bumps.
    fn acquire_or_renew<'a>(
        &'a self,
        key: BindingKey,
        owner_token: &'a BindingToken,
    ) -> ProgressFuture<'a, Result<AcquiredBinding, ProgressError>>;

    /// Reads the current register record and opaque CAS version.
    fn observe<'a>(
        &'a self,
        workload_id: &'a WorkloadId,
        canon_id: &'a CanonRef,
        verse_id: &'a VerseRef,
    ) -> ProgressFuture<'a, Result<Option<(ProgressRegister, ProgressVersion)>, ProgressError>>;

    /// Single-record CAS advance: require matching epoch+token, move frontier
    /// strictly forward, and write `last_commit_ref`.
    fn advance<'a>(
        &'a self,
        fence: &'a AcquiredBinding,
        new_frontier: SourceOffset,
        last_commit_ref: String,
    ) -> ProgressFuture<'a, Result<(ProgressRegister, ProgressVersion), ProgressError>>;
}

/// Progress / fence store failures.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProgressError {
    /// CAS lost a race or used a stale observation.
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
    /// Frontier must move strictly forward.
    #[error("frontier must advance strictly forward")]
    FrontierRegression,
    /// Stored register bytes are malformed or exceed documented bounds.
    #[error("malformed progress register record: {0}")]
    MalformedRecord(String),
    /// Conditional write outcome is unknown and must be resolved by explicit reread.
    #[error("indeterminate progress update: {0}")]
    Indeterminate(String),
    /// Underlying store I/O failed.
    #[error("progress store I/O: {0}")]
    Io(String),
}

#[derive(Debug, Default)]
struct MemoryState {
    /// One register record per binding key.
    records: HashMap<(String, String, String), (ProgressRegister, ProgressVersion)>,
    next_version: u64,
}

/// In-process progress register for deterministic contract tests (not durable).
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
    fn acquire_or_renew<'a>(
        &'a self,
        key: BindingKey,
        owner_token: &'a BindingToken,
    ) -> ProgressFuture<'a, Result<AcquiredBinding, ProgressError>> {
        let owner_token = owner_token.clone();
        Box::pin(async move {
            let map_key = key_of(&key.workload_id, &key.canon_id, &key.verse_id);
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| ProgressError::Io("progress lock poisoned".into()))?;
            match guard.records.get(&map_key).cloned() {
                None => {
                    let binding = ConsumerBinding {
                        workload_id: key.workload_id,
                        canon_id: key.canon_id,
                        verse_id: key.verse_id,
                        binding_epoch: 1,
                    };
                    binding.validate()?;
                    let register = ProgressRegister {
                        binding: binding.clone(),
                        binding_token: owner_token.clone(),
                        frontier: SourceOffset::new(0),
                        last_commit_ref: None,
                    };
                    let version = mint_memory_version(&mut guard);
                    guard.records.insert(map_key, (register, version));
                    Ok(AcquiredBinding {
                        binding,
                        owner_token: owner_token.clone(),
                    })
                }
                Some((existing, version)) if existing.binding_token == owner_token => {
                    // Same-token renewal during one process lifetime: retain epoch.
                    if existing.binding.workload_id != key.workload_id
                        || existing.binding.canon_id != key.canon_id
                        || existing.binding.verse_id != key.verse_id
                    {
                        return Err(ProgressError::StaleBinding);
                    }
                    // Touch version so adapters see a successful renew write.
                    let new_version = mint_memory_version(&mut guard);
                    let _ = version;
                    guard
                        .records
                        .insert(map_key, (existing.clone(), new_version));
                    Ok(AcquiredBinding {
                        binding: existing.binding,
                        owner_token: owner_token.clone(),
                    })
                }
                Some((existing, _)) => {
                    // Fresh process token / takeover: always bump epoch; carry frontier.
                    let next_epoch = existing
                        .binding
                        .binding_epoch
                        .checked_add(1)
                        .ok_or_else(|| ProgressError::Io("binding_epoch overflow".into()))?;
                    let binding = ConsumerBinding {
                        workload_id: key.workload_id,
                        canon_id: key.canon_id,
                        verse_id: key.verse_id,
                        binding_epoch: next_epoch,
                    };
                    binding.validate()?;
                    let register = ProgressRegister {
                        binding: binding.clone(),
                        binding_token: owner_token.clone(),
                        frontier: existing.frontier,
                        last_commit_ref: existing.last_commit_ref,
                    };
                    let version = mint_memory_version(&mut guard);
                    guard.records.insert(map_key, (register, version));
                    Ok(AcquiredBinding {
                        binding,
                        owner_token: owner_token.clone(),
                    })
                }
            }
        })
    }

    fn observe<'a>(
        &'a self,
        workload_id: &'a WorkloadId,
        canon_id: &'a CanonRef,
        verse_id: &'a VerseRef,
    ) -> ProgressFuture<'a, Result<Option<(ProgressRegister, ProgressVersion)>, ProgressError>>
    {
        Box::pin(async move {
            let key = key_of(workload_id, canon_id, verse_id);
            let guard = self
                .inner
                .lock()
                .map_err(|_| ProgressError::Io("progress lock poisoned".into()))?;
            Ok(guard.records.get(&key).cloned())
        })
    }

    fn advance<'a>(
        &'a self,
        fence: &'a AcquiredBinding,
        new_frontier: SourceOffset,
        last_commit_ref: String,
    ) -> ProgressFuture<'a, Result<(ProgressRegister, ProgressVersion), ProgressError>> {
        Box::pin(async move {
            fence.binding.validate()?;
            if last_commit_ref.trim().is_empty() {
                return Err(ProgressError::Io("empty last_commit_ref".into()));
            }
            let key = key_of(
                &fence.binding.workload_id,
                &fence.binding.canon_id,
                &fence.binding.verse_id,
            );
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| ProgressError::Io("progress lock poisoned".into()))?;
            let (existing, _) = guard
                .records
                .get(&key)
                .cloned()
                .ok_or(ProgressError::StaleBinding)?;
            if existing.binding_token != fence.owner_token {
                return Err(ProgressError::FenceHeld);
            }
            if existing.binding.binding_epoch != fence.binding.binding_epoch {
                return Err(ProgressError::StaleBinding);
            }
            if existing.binding.workload_id != fence.binding.workload_id
                || existing.binding.canon_id != fence.binding.canon_id
                || existing.binding.verse_id != fence.binding.verse_id
            {
                return Err(ProgressError::StaleBinding);
            }
            if new_frontier.get() <= existing.frontier.get() {
                return Err(ProgressError::FrontierRegression);
            }
            let register = ProgressRegister {
                binding: existing.binding,
                binding_token: fence.owner_token.clone(),
                frontier: new_frontier,
                last_commit_ref: Some(last_commit_ref),
            };
            let version = mint_memory_version(&mut guard);
            guard
                .records
                .insert(key, (register.clone(), version.clone()));
            Ok((register, version))
        })
    }
}

fn mint_memory_version(guard: &mut MemoryState) -> ProgressVersion {
    let n = guard.next_version;
    guard.next_version = guard.next_version.saturating_add(1);
    // Opaque proof witness only — not a durable object-store etag.
    ProgressVersion::from_opaque_bytes(format!("mem-v{n}").into_bytes())
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
    use futures::executor::block_on;

    fn key() -> BindingKey {
        BindingKey::new(
            WorkloadId::new("wl-1").expect("id"),
            CanonRef::new("canon-a").expect("canon"),
            VerseRef::new("verse-1").expect("verse"),
        )
    }

    #[test]
    fn acquire_race_second_token_takeovers_and_bumps_epoch() {
        let store = InMemoryProgressStore::new();
        let t1 = BindingToken::new("token-a").expect("t");
        let t2 = BindingToken::new("token-b").expect("t");
        let a = block_on(store.acquire_or_renew(key(), &t1)).expect("first");
        assert_eq!(a.binding.binding_epoch, 1);
        let b = block_on(store.acquire_or_renew(key(), &t2)).expect("takeover");
        assert_eq!(b.binding.binding_epoch, 2);
        let observed =
            block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
                .expect("observe")
                .expect("present");
        assert_eq!(observed.0.binding.binding_epoch, 2);
        assert_eq!(observed.0.binding_token, t2);
        assert_eq!(observed.0.frontier, SourceOffset::new(0));
    }

    #[test]
    fn renew_same_token_retains_epoch() {
        let store = InMemoryProgressStore::new();
        let token = BindingToken::new("token-a").expect("t");
        let first = block_on(store.acquire_or_renew(key(), &token)).expect("acquire");
        let renewed = block_on(store.acquire_or_renew(key(), &token)).expect("renew");
        assert_eq!(first.binding.binding_epoch, renewed.binding.binding_epoch);
        assert_eq!(renewed.binding.binding_epoch, 1);
    }

    #[test]
    fn advance_requires_fence_token_and_rejects_regression() {
        let store = InMemoryProgressStore::new();
        let token = BindingToken::new("token-a").expect("t");
        let other = BindingToken::new("token-b").expect("t");
        let fence = block_on(store.acquire_or_renew(key(), &token)).expect("acquire");
        block_on(store.advance(&fence, SourceOffset::new(3), "commit-a".into())).expect("advance");
        let forged = AcquiredBinding {
            binding: fence.binding.clone(),
            owner_token: other,
        };
        assert_eq!(
            block_on(store.advance(&forged, SourceOffset::new(4), "commit-b".into())),
            Err(ProgressError::FenceHeld)
        );
        assert_eq!(
            block_on(store.advance(&fence, SourceOffset::new(3), "commit-c".into())),
            Err(ProgressError::FrontierRegression)
        );
        assert_eq!(
            block_on(store.advance(&fence, SourceOffset::new(2), "commit-d".into())),
            Err(ProgressError::FrontierRegression)
        );
    }

    #[test]
    fn restart_token_always_bumps_epoch_and_carries_frontier() {
        let store = InMemoryProgressStore::new();
        let t1 = BindingToken::new("proc-1").expect("t");
        let fence = block_on(store.acquire_or_renew(key(), &t1)).expect("acquire");
        block_on(store.advance(&fence, SourceOffset::new(10), "c1".into())).expect("advance");
        let t2 = BindingToken::new("proc-2").expect("t");
        let restarted = block_on(store.acquire_or_renew(key(), &t2)).expect("restart");
        assert_eq!(restarted.binding.binding_epoch, 2);
        let observed =
            block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
                .expect("observe")
                .expect("present");
        assert_eq!(observed.0.frontier, SourceOffset::new(10));
        assert_eq!(observed.0.last_commit_ref.as_deref(), Some("c1"));
        assert_eq!(
            block_on(store.advance(&fence, SourceOffset::new(11), "stale".into())),
            Err(ProgressError::FenceHeld)
        );
    }
}
