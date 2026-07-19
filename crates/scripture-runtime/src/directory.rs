//! Fleet directory: a soft, self-published discovery plane in object storage.
//!
//! Answers "which Scribes exist and which endpoints are worth trying." It
//! never answers "who may commit" — that question keeps exactly one answer,
//! the conditional root register (decision 0014).
//!
//! Deliberately not called *membership*: Holylog already uses that word for
//! the generation chain (`observe_membership`), which is unrelated.
//!
//! Each node writes only its own key, so publication needs no coordination:
//! no CAS, no contention, and a heartbeat that cannot fail against a peer.
//! Losing the entire directory degrades discovery and cannot produce two
//! writers.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

/// Errors from directory publication or lookup.
#[derive(Debug)]
pub enum DirectoryError {
    /// Object-store read/write failure.
    Store(object_store::Error),
    /// A record could not be decoded; the key is named so it can be inspected.
    Decode { key: String, reason: String },
    /// A record could not be encoded.
    Encode(String),
}

impl std::fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(f, "directory store: {error}"),
            Self::Decode { key, reason } => write!(f, "directory decode {key}: {reason}"),
            Self::Encode(reason) => write!(f, "directory encode: {reason}"),
        }
    }
}

impl std::error::Error for DirectoryError {}

/// One assignment as advertised by its hosting node.
///
/// `disposition` is a ranking hint that may be stale in either direction. A
/// client that acts on it must still be prepared for the authority gate to
/// refuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryAssignment {
    /// Canon identity this assignment serves.
    pub canon: String,
    /// Verse identity this assignment serves.
    pub verse: String,
    /// Endpoint a producer would connect to for this assignment.
    pub advertise: String,
    /// Configured posture (`bootstrap-if-empty`, `standby`, …).
    pub posture: String,
    /// Disposition observed at publication time. A hint, never a grant.
    pub disposition: String,
    /// Whether this assignment was admitting committed ACKs at publication.
    pub admits_committed_acks: bool,
}

/// A node's self-published directory record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryRecord {
    /// Record format version.
    pub format_version: u32,
    /// Canonical owner id of the publishing node.
    pub owner_id: String,
    /// Process-level advertise endpoint.
    pub node_advertise: String,
    /// Publication wall-clock time, milliseconds since the Unix epoch.
    pub published_at_ms: u64,
    /// Advisory validity window. Expiry means "probably down", never "is down".
    pub valid_for_ms: u64,
    /// Assignments hosted by this node.
    pub assignments: Vec<DirectoryAssignment>,
}

impl DirectoryRecord {
    /// Returns whether this record is unexpired at `now_ms`.
    ///
    /// A live-but-partitioned node has an expired record and is still lawfully
    /// serving, so callers must treat `false` as a hint.
    #[must_use]
    pub fn is_fresh_at(&self, now_ms: u64) -> bool {
        now_ms < self.published_at_ms.saturating_add(self.valid_for_ms)
    }

    /// Milliseconds since publication at `now_ms`, saturating at zero.
    #[must_use]
    pub fn age_ms_at(&self, now_ms: u64) -> u64 {
        now_ms.saturating_sub(self.published_at_ms)
    }
}

/// Current wall-clock time in milliseconds since the Unix epoch.
#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Directory prefix for a store root. Kept separate from the authority root.
fn directory_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_end_matches('/');
    format!("{trimmed}/directory/nodes")
}

/// Object key for one node. Owner bytes are hex-encoded so that any owner id
/// maps to a path-safe key.
fn node_key(prefix: &str, owner_id: &str) -> Path {
    let hex: String = owner_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Path::from(format!("{}/{hex}.json", directory_prefix(prefix)))
}

/// Publishes (or refreshes) this node's directory record.
///
/// Last-write-wins is correct here because exactly one node writes this key.
pub async fn publish(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    record: &DirectoryRecord,
) -> Result<(), DirectoryError> {
    let body =
        serde_json::to_vec(record).map_err(|error| DirectoryError::Encode(error.to_string()))?;
    let key = node_key(prefix, &record.owner_id);
    store
        .put(&key, PutPayload::from(body))
        .await
        .map_err(DirectoryError::Store)?;
    Ok(())
}

/// Removes this node's record on graceful shutdown.
///
/// Expiry covers the ungraceful case, so a failure here is not fatal.
pub async fn withdraw(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    owner_id: &str,
) -> Result<(), DirectoryError> {
    let key = node_key(prefix, owner_id);
    match store.delete(&key).await {
        Ok(()) => Ok(()),
        // Already gone is the desired end state.
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(DirectoryError::Store(error)),
    }
}

/// Lists every directory record, fresh or stale.
///
/// Staleness is reported rather than filtered so that callers can distinguish
/// "no node has ever published" from "every node's record has expired" — those
/// are different operational situations.
pub async fn list_all(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Vec<DirectoryRecord>, DirectoryError> {
    use futures::StreamExt as _;

    let scan = Path::from(directory_prefix(prefix));
    let mut listing = store.list(Some(&scan));
    let mut keys = Vec::new();
    while let Some(entry) = listing.next().await {
        let meta = entry.map_err(DirectoryError::Store)?;
        keys.push(meta.location);
    }

    let mut records = Vec::with_capacity(keys.len());
    for key in keys {
        let bytes = store
            .get(&key)
            .await
            .map_err(DirectoryError::Store)?
            .bytes()
            .await
            .map_err(DirectoryError::Store)?;
        let record: DirectoryRecord =
            serde_json::from_slice(&bytes).map_err(|error| DirectoryError::Decode {
                key: key.to_string(),
                reason: error.to_string(),
            })?;
        records.push(record);
    }
    records.sort_by(|a, b| a.owner_id.cmp(&b.owner_id));
    Ok(records)
}

/// Endpoints worth trying for one `(canon, verse)`, best candidate first.
///
/// Ranking is a hint: entries claiming `Serving` and admitting committed ACKs
/// sort first, then other fresh entries, then stale ones. A caller must still
/// handle refusal by the authority gate, and must be willing to try a stale
/// entry — a partitioned-but-healthy node looks stale from here.
#[must_use]
pub fn rank_candidates(
    records: &[DirectoryRecord],
    canon: &str,
    verse: &str,
    now_ms: u64,
) -> Vec<RankedCandidate> {
    let mut candidates: Vec<RankedCandidate> = records
        .iter()
        .flat_map(|record| {
            record
                .assignments
                .iter()
                .filter(|assignment| assignment.canon == canon && assignment.verse == verse)
                .map(|assignment| RankedCandidate {
                    owner_id: record.owner_id.clone(),
                    endpoint: assignment.advertise.clone(),
                    disposition: assignment.disposition.clone(),
                    claims_serving: assignment.admits_committed_acks,
                    fresh: record.is_fresh_at(now_ms),
                    age_ms: record.age_ms_at(now_ms),
                })
        })
        .collect();

    candidates.sort_by(|a, b| {
        b.fresh
            .cmp(&a.fresh)
            .then(b.claims_serving.cmp(&a.claims_serving))
            .then(a.age_ms.cmp(&b.age_ms))
            .then(a.owner_id.cmp(&b.owner_id))
    });
    candidates
}

/// One ranked endpoint for a `(canon, verse)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedCandidate {
    /// Owner id of the hosting node.
    pub owner_id: String,
    /// Endpoint to try.
    pub endpoint: String,
    /// Disposition claimed at publication.
    pub disposition: String,
    /// Whether the entry claimed it was admitting committed ACKs.
    pub claims_serving: bool,
    /// Whether the record was unexpired at ranking time.
    pub fresh: bool,
    /// Record age in milliseconds.
    pub age_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(owner: &str, published_at_ms: u64, serving: bool) -> DirectoryRecord {
        DirectoryRecord {
            format_version: 1,
            owner_id: owner.to_owned(),
            node_advertise: format!("tcp://{owner}:9000"),
            published_at_ms,
            valid_for_ms: 15_000,
            assignments: vec![DirectoryAssignment {
                canon: "canon-a".to_owned(),
                verse: "verse-a".to_owned(),
                advertise: format!("tcp://{owner}:9001"),
                posture: "standby".to_owned(),
                disposition: if serving { "Serving" } else { "Standby" }.to_owned(),
                admits_committed_acks: serving,
            }],
        }
    }

    #[test]
    fn freshness_follows_the_validity_window() {
        let r = record("a", 1_000, true);
        assert!(r.is_fresh_at(1_000));
        assert!(r.is_fresh_at(15_999));
        assert!(!r.is_fresh_at(16_000));
    }

    #[test]
    fn serving_entries_rank_above_standby() {
        let records = vec![record("b", 1_000, false), record("a", 1_000, true)];
        let ranked = rank_candidates(&records, "canon-a", "verse-a", 2_000);
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].owner_id, "a");
        assert!(ranked[0].claims_serving);
    }

    #[test]
    fn fresh_standby_ranks_above_stale_serving() {
        // A stale record claiming Serving is weaker evidence than a fresh one
        // claiming Standby: the stale claim may predate a promotion.
        let records = vec![record("a", 1_000, true), record("b", 50_000, false)];
        let ranked = rank_candidates(&records, "canon-a", "verse-a", 51_000);
        assert_eq!(ranked[0].owner_id, "b");
        assert!(ranked[0].fresh);
        assert!(!ranked[1].fresh);
    }

    #[test]
    fn stale_entries_are_ranked_not_dropped() {
        // A partitioned-but-healthy node looks stale from here, so it must
        // remain reachable as a fallback.
        let records = vec![record("a", 1_000, true)];
        let ranked = rank_candidates(&records, "canon-a", "verse-a", 999_999);
        assert_eq!(ranked.len(), 1);
        assert!(!ranked[0].fresh);
    }

    #[test]
    fn other_verses_are_excluded() {
        let records = vec![record("a", 1_000, true)];
        assert!(rank_candidates(&records, "canon-a", "other", 2_000).is_empty());
    }

    #[test]
    fn node_key_is_path_safe_for_punctuated_owner_ids() {
        let key = node_key("scripture/live", "scripture-own-a!");
        assert!(
            key.to_string()
                .starts_with("scripture/live/directory/nodes/")
        );
        assert!(key.to_string().ends_with(".json"));
        assert!(!key.to_string().contains('!'));
    }

    #[test]
    fn record_round_trips_through_json() {
        let original = record("a", 1_000, true);
        let bytes = serde_json::to_vec(&original).expect("encode");
        let decoded: DirectoryRecord = serde_json::from_slice(&bytes).expect("decode");
        assert_eq!(original, decoded);
    }
}
