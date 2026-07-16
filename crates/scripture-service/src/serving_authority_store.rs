//! Asynchronous `ServingAuthorityStore` contract and in-memory reference implementation.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use scripture::serving_authority::{AuthorityKey, ServingAuthorityRecord};

pub type AuthorityStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ServingAuthorityStoreError>> + Send + 'a>>;

/// Opaque, equality-only version/ETag token tracking one state row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreVersion(Vec<u8>);

impl StoreVersion {
    /// Wraps raw byte representation into a StoreVersion token.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Returns the raw byte reference.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// An observed snapshot of the Serving Authority state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritySnapshot {
    /// Opaque version token used for CAS writes.
    pub version: StoreVersion,
    /// Authoritative ServingAuthorityRecord.
    pub record: ServingAuthorityRecord,
}

/// Outcome of a Compare-and-Swap write on the ServingAuthorityStore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOutcome {
    /// The state was successfully updated.
    Applied,
    /// A concurrent coordinator modified the record; write was rejected.
    Conflict,
}

/// Bounded, backend-neutral error taxonomy for the ServingAuthorityStore.
#[derive(Debug, thiserror::Error)]
pub enum ServingAuthorityStoreError {
    /// The database or network is transiently or permanently unavailable.
    #[error("Serving Authority store is unavailable")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The write outcome is unknown because of a timeout or connection drop.
    ///
    /// # Safety Invariant
    ///
    /// This error must NEVER be converted to Applied or Conflict.
    #[error("Serving Authority store write outcome is indeterminate")]
    Indeterminate(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Stored bytes in the database failed to decode.
    #[error("Serving Authority store payload is malformed: {message}")]
    MalformedPayload {
        /// Parsing details.
        message: String,
    },
}

/// A transport-neutral durably-coordinated database register contract.
pub trait ServingAuthorityStore: Send + Sync {
    /// Observes the current, linearizable state snapshot.
    ///
    /// Returns `None` if no record exists yet (conditional creation namespace).
    fn observe(&self, key: AuthorityKey) -> AuthorityStoreFuture<'_, Option<AuthoritySnapshot>>;

    /// Atomically updates or bootstraps the authority record.
    ///
    /// Conditional creation uses `expected_version = None`.
    fn compare_and_swap(
        &self,
        key: AuthorityKey,
        expected_version: Option<StoreVersion>,
        next_record: ServingAuthorityRecord,
    ) -> AuthorityStoreFuture<'_, CasOutcome>;
}

#[derive(Debug)]
struct InMemoryRow {
    version: u64,
    encoded_bytes: Vec<u8>,
}

/// Clean in-process reference implementation of [`ServingAuthorityStore`].
#[derive(Debug, Default, Clone)]
pub struct InMemoryServingAuthorityStore {
    rows: Arc<Mutex<BTreeMap<AuthorityKey, InMemoryRow>>>,
}

impl InMemoryServingAuthorityStore {
    /// Constructs a fresh in-memory store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Seed a row directly with raw, potentially malformed bytes to test
    /// decode/corruption resilience.
    pub fn inject_raw_bytes(&self, key: AuthorityKey, version: u64, bytes: Vec<u8>) {
        self.rows.lock().expect("lock poisoned").insert(
            key,
            InMemoryRow {
                version,
                encoded_bytes: bytes,
            },
        );
    }
}

impl ServingAuthorityStore for InMemoryServingAuthorityStore {
    fn observe(&self, key: AuthorityKey) -> AuthorityStoreFuture<'_, Option<AuthoritySnapshot>> {
        let rows = Arc::clone(&self.rows);
        Box::pin(async move {
            let guard = rows.lock().map_err(|e| {
                ServingAuthorityStoreError::Unavailable(Box::new(std::io::Error::other(
                    e.to_string(),
                )))
            })?;

            match guard.get(&key) {
                None => Ok(None),
                Some(row) => {
                    let record =
                        ServingAuthorityRecord::decode(&row.encoded_bytes).map_err(|e| {
                            ServingAuthorityStoreError::MalformedPayload {
                                message: e.to_string(),
                            }
                        })?;
                    if record.key != key {
                        return Err(ServingAuthorityStoreError::MalformedPayload {
                            message: "record key mismatch".to_string(),
                        });
                    }
                    Ok(Some(AuthoritySnapshot {
                        version: StoreVersion::new(row.version.to_be_bytes().to_vec()),
                        record,
                    }))
                }
            }
        })
    }

    fn compare_and_swap(
        &self,
        key: AuthorityKey,
        expected_version: Option<StoreVersion>,
        next_record: ServingAuthorityRecord,
    ) -> AuthorityStoreFuture<'_, CasOutcome> {
        let rows = Arc::clone(&self.rows);
        Box::pin(async move {
            let mut guard = rows.lock().map_err(|e| {
                ServingAuthorityStoreError::Unavailable(Box::new(std::io::Error::other(
                    e.to_string(),
                )))
            })?;

            let current_row = guard.get(&key);

            if next_record.key != key {
                return Err(ServingAuthorityStoreError::Unavailable(Box::new(
                    std::io::Error::other("cannot CAS record with mismatched key".to_string()),
                )));
            }

            match (expected_version, current_row) {
                (None, None) => {
                    // Valid bootstrap
                    let encoded_bytes = next_record.encode().map_err(|e| {
                        ServingAuthorityStoreError::MalformedPayload {
                            message: e.to_string(),
                        }
                    })?;
                    guard.insert(
                        key,
                        InMemoryRow {
                            version: 1,
                            encoded_bytes,
                        },
                    );
                    Ok(CasOutcome::Applied)
                }
                (None, Some(_)) => {
                    // Bootstrap conflict (already exists)
                    Ok(CasOutcome::Conflict)
                }
                (Some(_), None) => {
                    // Stale expected version conflict (does not exist)
                    Ok(CasOutcome::Conflict)
                }
                (Some(expected), Some(current)) => {
                    let expected_bytes = expected.as_bytes();
                    let current_bytes = current.version.to_be_bytes().to_vec();
                    if expected_bytes == current_bytes {
                        let next_version = current.version.checked_add(1).ok_or_else(|| {
                            ServingAuthorityStoreError::Unavailable(Box::new(
                                std::io::Error::other("version exhaust".to_string()),
                            ))
                        })?;
                        let encoded_bytes = next_record.encode().map_err(|e| {
                            ServingAuthorityStoreError::MalformedPayload {
                                message: e.to_string(),
                            }
                        })?;
                        guard.insert(
                            key,
                            InMemoryRow {
                                version: next_version,
                                encoded_bytes,
                            },
                        );
                        Ok(CasOutcome::Applied)
                    } else {
                        Ok(CasOutcome::Conflict)
                    }
                }
            }
        })
    }
}

/// Generic, reusable store conformance runner to prove backend correctness.
pub async fn run_serving_authority_store_conformance(
    store: Arc<dyn ServingAuthorityStore>,
    key: AuthorityKey,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rec0 = ServingAuthorityRecord::new(
        key,
        scripture::serving_authority::AuthorityState::Unassigned,
    );

    // 1. Initial read is None
    let initial = store.observe(key).await?;
    assert!(initial.is_none());

    // 2. Applied updates (bootstrap None)
    let cas_boot = store.compare_and_swap(key, None, rec0.clone()).await?;
    assert_eq!(cas_boot, CasOutcome::Applied);

    // 3. Create race: duplicate bootstrap on None returns Conflict
    let cas_race = store.compare_and_swap(key, None, rec0.clone()).await?;
    assert_eq!(cas_race, CasOutcome::Conflict);

    // Read back and get snapshot
    let snapshot = store.observe(key).await?.expect("snapshot");
    assert_eq!(snapshot.record, rec0);

    // 4. Stale CAS conflict
    let stale_version = StoreVersion::new(b"not-the-right-version-token".to_vec());
    let cas_stale = store
        .compare_and_swap(key, Some(stale_version), rec0.clone())
        .await?;
    assert_eq!(cas_stale, CasOutcome::Conflict);

    // 5. Applied update on correct version
    let rec1 = ServingAuthorityRecord::new(
        key,
        scripture::serving_authority::AuthorityState::ReconciliationRequired {
            intent: scripture::serving_authority::TransitionIntent {
                transition_id: scripture::serving_authority::TransitionId::from_bytes([42; 16]),
                kind: scripture::serving_authority::TransitionKind::RecoveryPromotion,
                precondition: scripture::serving_authority::FoundationPrecondition::Empty,
                candidate_owner_id: scripture::canon::OwnerId::from_bytes([42; 16]),
                next_writer_term: scripture::serving_authority::WriterTerm::new(1).expect("valid"),
            },
            observed_generation: None,
        },
    );
    let cas_valid = store
        .compare_and_swap(key, Some(snapshot.version), rec1.clone())
        .await?;
    assert_eq!(cas_valid, CasOutcome::Applied);

    // Read back state1
    let final_snapshot = store.observe(key).await?.expect("final snapshot");
    assert_eq!(final_snapshot.record, rec1);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use scripture::canon::VerseId;
    use scripture::model::JournalId;

    #[tokio::test]
    async fn test_in_memory_store_conformance() {
        let store = Arc::new(InMemoryServingAuthorityStore::new());
        let key = AuthorityKey {
            journal_id: JournalId::from_bytes(*b"canon-journal-id"),
            verse_id: VerseId::from_bytes(*b"canon-line-id!!!"),
        };
        run_serving_authority_store_conformance(store, key)
            .await
            .expect("conformance passes");
    }

    #[tokio::test]
    async fn test_store_key_mismatch_rejection() {
        let store = InMemoryServingAuthorityStore::new();
        let key_a = AuthorityKey {
            journal_id: JournalId::from_bytes(*b"journal-aaaa-id!"),
            verse_id: VerseId::from_bytes(*b"verse-aaaa-id!!!"),
        };
        let key_b = AuthorityKey {
            journal_id: JournalId::from_bytes(*b"journal-bbbb-id!"),
            verse_id: VerseId::from_bytes(*b"verse-bbbb-id!!!"),
        };

        // 1. compare_and_swap rejects record with mismatched key
        let rec_b = ServingAuthorityRecord::new(
            key_b,
            scripture::serving_authority::AuthorityState::Unassigned,
        );
        let res = store.compare_and_swap(key_a, None, rec_b).await;
        assert!(res.is_err());

        // 2. observe rejects persisted record with mismatched key
        let raw_b_bytes = ServingAuthorityRecord::new(
            key_b,
            scripture::serving_authority::AuthorityState::Unassigned,
        )
        .encode()
        .expect("valid encode");
        store.inject_raw_bytes(key_a, 1, raw_b_bytes);
        let obs_res = store.observe(key_a).await;
        assert!(matches!(
            obs_res,
            Err(ServingAuthorityStoreError::MalformedPayload { .. })
        ));
    }
}
