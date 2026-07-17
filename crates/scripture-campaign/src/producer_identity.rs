//! Stable campaign producer identities derived from run, actor, and ordinal.

use scripture::ProducerId;

/// Derives a deterministic producer id from `run_id`, `actor`, and `ordinal`.
///
/// Closes the restart collision where process-local counters reused offsets
/// across campaign actors.
#[must_use]
pub(crate) fn campaign_producer(run_id: &str, actor: &str, ordinal: u64) -> ProducerId {
    let mut bytes = [0_u8; 16];
    bytes[0..4].copy_from_slice(b"cmpn");
    mix_into(&mut bytes[4..12], run_id.as_bytes());
    mix_into(&mut bytes[12..15], actor.as_bytes());
    bytes[15] = (ordinal & 0xFF) as u8;
    ProducerId::from_bytes(bytes)
}

fn mix_into(target: &mut [u8], source: &[u8]) {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in source {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    target.copy_from_slice(&hash.to_le_bytes()[..target.len()]);
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
}
