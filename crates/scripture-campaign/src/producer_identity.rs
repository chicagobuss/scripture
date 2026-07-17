//! Stable campaign producer identities derived from run, actor, and ordinal.

use scripture::ProducerId;

/// Derives a deterministic producer id from `run_id`, `actor`, and `ordinal`.
///
/// Closes the restart collision where process-local counters reused offsets
/// across campaign actors.
#[must_use]
pub(crate) fn campaign_producer(run_id: &str, actor: &str, ordinal: u64) -> ProducerId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"scripture-campaign-producer-id-v1\0");
    hash_field(&mut hasher, run_id.as_bytes());
    hash_field(&mut hasher, actor.as_bytes());
    hasher.update(&ordinal.to_le_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    ProducerId::from_bytes(bytes)
}

fn hash_field(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::campaign_producer;

    #[test]
    fn stable_across_calls() {
        let left = campaign_producer("run-1", "actor-a", 2);
        let right = campaign_producer("run-1", "actor-a", 2);
        assert_eq!(left, right);
    }

    #[test]
    fn distinct_for_different_actors() {
        let a = campaign_producer("run-1", "actor-a", 0);
        let b = campaign_producer("run-1", "actor-b", 0);
        assert_ne!(a, b);
    }

    #[test]
    fn preserves_the_full_ordinal() {
        assert_ne!(
            campaign_producer("run-1", "actor-a", 0),
            campaign_producer("run-1", "actor-a", 256)
        );
    }

    #[test]
    fn length_prefixes_identity_fields() {
        assert_ne!(
            campaign_producer("ab", "c", 1),
            campaign_producer("a", "bc", 1)
        );
    }
}
