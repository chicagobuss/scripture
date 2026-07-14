//! Stable [`SequencerRequestKey`] derivation for Scripture identities.
//!
//! Holylog decision 0015 requires callers to supply producer-qualified keys for
//! remote sequencers. Scripture derives them from durable submission and chunk
//! identities — never from payload bytes or digests.

use holylog::remote_sequencer::{SequencerClientId, SequencerRequestId, SequencerRequestKey};

use crate::chunk::{ChunkId, ProducerId, WriterId};

/// Namespace bytes for submission-scoped request ids within one Verse owner.
///
/// Keeps submission keys disjoint from chunk-level keys, which use raw
/// [`ChunkId`] bytes as the request id.
const SUBMISSION_REQUEST_NAMESPACE: [u8; 4] = [0, 0, 0, 0];

/// Derives a stable sequencer request key from one producer submission identity.
///
/// Packing matches Holylog decision 0015 / 0014:
///
/// - `SequencerClientId` ← [`ProducerId::as_bytes`] (16 opaque bytes)
/// - `SequencerRequestId` ← 16 bytes: `producer_epoch` (`u32` BE) +
///   `sequence` (`u64` BE) + [`SUBMISSION_REQUEST_NAMESPACE`] (4 zero pad)
///
/// Equal `(producer_id, producer_epoch, sequence)` always yields equal keys.
/// Distinct identities yield distinct keys even when payload bytes would match.
/// Retries of one submission must retain the exact same key across transport
/// uncertainty.
#[must_use]
pub fn sequencer_request_key_for_submission(
    producer_id: ProducerId,
    producer_epoch: u32,
    sequence: u64,
) -> SequencerRequestKey {
    let epoch_bytes = producer_epoch.to_be_bytes();
    let sequence_bytes = sequence.to_be_bytes();
    let request_bytes = [
        epoch_bytes[0],
        epoch_bytes[1],
        epoch_bytes[2],
        epoch_bytes[3],
        sequence_bytes[0],
        sequence_bytes[1],
        sequence_bytes[2],
        sequence_bytes[3],
        sequence_bytes[4],
        sequence_bytes[5],
        sequence_bytes[6],
        sequence_bytes[7],
        SUBMISSION_REQUEST_NAMESPACE[0],
        SUBMISSION_REQUEST_NAMESPACE[1],
        SUBMISSION_REQUEST_NAMESPACE[2],
        SUBMISSION_REQUEST_NAMESPACE[3],
    ];
    SequencerRequestKey::new(
        SequencerClientId::from_bytes(producer_id.as_bytes()),
        SequencerRequestId::from_bytes(request_bytes),
    )
}

/// Derives a stable chunk-level sequencer request key for one sealed chunk append.
///
/// Packing:
///
/// - `SequencerClientId` ← [`WriterId::as_bytes`] (16 opaque bytes)
/// - `SequencerRequestId` ← [`ChunkId::as_bytes`] (16 opaque bytes)
///
/// A retry of the same sealed chunk reuses the same key because [`ChunkId`] is
/// assigned at seal time and is stable across transport retries.
#[must_use]
pub const fn sequencer_request_key_for_chunk(
    writer_id: WriterId,
    chunk_id: ChunkId,
) -> SequencerRequestKey {
    SequencerRequestKey::new(
        SequencerClientId::from_bytes(writer_id.as_bytes()),
        SequencerRequestId::from_bytes(chunk_id.as_bytes()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer(tag: u8) -> ProducerId {
        ProducerId::from_bytes([tag; 16])
    }

    #[test]
    fn equal_submission_identity_yields_equal_keys() {
        let key_a = sequencer_request_key_for_submission(producer(1), 3, 42);
        let key_b = sequencer_request_key_for_submission(producer(1), 3, 42);
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn distinct_sequence_yields_distinct_keys() {
        let base = sequencer_request_key_for_submission(producer(1), 3, 42);
        let other = sequencer_request_key_for_submission(producer(1), 3, 43);
        assert_ne!(base, other);
    }

    #[test]
    fn distinct_producer_yields_distinct_keys() {
        let base = sequencer_request_key_for_submission(producer(1), 3, 42);
        let other = sequencer_request_key_for_submission(producer(2), 3, 42);
        assert_ne!(base, other);
    }

    #[test]
    fn distinct_epoch_yields_distinct_keys() {
        let base = sequencer_request_key_for_submission(producer(1), 3, 42);
        let other = sequencer_request_key_for_submission(producer(1), 4, 42);
        assert_ne!(base, other);
    }

    #[test]
    fn submission_key_ignores_payload_relevance() {
        let key_a = sequencer_request_key_for_submission(producer(9), 1, 7);
        let key_b = sequencer_request_key_for_submission(producer(9), 1, 7);
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn chunk_key_is_stable_for_writer_and_chunk() {
        let writer = WriterId::from_bytes(*b"writer-id-012345");
        let chunk = ChunkId::from_bytes(*b"chunk-id-0123456");
        assert_eq!(
            sequencer_request_key_for_chunk(writer, chunk),
            sequencer_request_key_for_chunk(writer, chunk)
        );
    }

    #[test]
    fn chunk_and_submission_keys_do_not_collide_for_equal_bytes() {
        let id_bytes = *b"same-bytes!!!!!!";
        let submission =
            sequencer_request_key_for_submission(ProducerId::from_bytes(id_bytes), 0, 0);
        let chunk = sequencer_request_key_for_chunk(
            WriterId::from_bytes(id_bytes),
            ChunkId::from_bytes(id_bytes),
        );
        assert_ne!(submission, chunk);
    }
}
