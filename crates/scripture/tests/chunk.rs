//! Chunk codec tests (Phase 1, tests 1–7 of `docs/phase-1-chunk-driver.md`).

use std::collections::BTreeMap;

use bytes::Bytes;
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use scripture::{
    AttributeValue, ChunkError, ChunkHeader, ChunkId, CohortId, Frame, JournalId, ProducerId,
    ProducerRange, Record, RecordOffset, WriterId, decode_chunk, decode_frame, decode_index,
    encoded_chunk_len, seal_chunk,
};

fn header() -> ChunkHeader {
    ChunkHeader {
        chunk_id: ChunkId::from_bytes(*b"chunk-id-0123456"),
        cohort_id: CohortId::from_bytes(*b"cohort-id-012345"),
        generation: 7,
        writer_id: WriterId::from_bytes(*b"writer-id-012345"),
        created_at_micros: 1_783_900_000_000_000,
    }
}

fn journal(tag: u8) -> JournalId {
    let mut bytes = *b"journal-id-01234";
    bytes[15] = tag;
    JournalId::from_bytes(bytes)
}

fn record(value: i64) -> Record {
    Record::new(
        [
            ("kind".into(), AttributeValue::String("order".into())),
            ("value".into(), AttributeValue::I64(value)),
            ("valid".into(), AttributeValue::Bool(true)),
        ],
        Bytes::from(format!("payload-{value}")),
    )
}

fn producers(count: u64) -> Vec<ProducerRange> {
    vec![ProducerRange {
        producer_id: ProducerId::from_bytes(*b"producer-id-0123"),
        producer_epoch: 3,
        first_sequence: 0,
        last_sequence: count.saturating_sub(1),
    }]
}

fn frame(tag: u8, base: u64, count: i64) -> Frame {
    Frame {
        journal_id: journal(tag),
        base_offset: RecordOffset::new(base),
        records: (0..count).map(record).collect(),
        producers: producers(count.max(0) as u64),
    }
}

/// Test 1 + 2: a chunk round-trips, and re-encoding the same logical chunk
/// produces byte-identical output. The second property is what makes a kernel
/// retry a retry rather than corruption.
#[test]
fn round_trips_and_re_encodes_identically() {
    let frames = vec![frame(1, 0, 3)];
    let sealed = seal_chunk(header(), frames.clone()).expect("seal");
    let decoded = decode_chunk(&sealed.bytes).expect("decode");

    assert_eq!(decoded.header, header());
    assert_eq!(decoded.frames, frames);
    assert_eq!(decoded.frames[0].records.len(), 3);

    let resealed = seal_chunk(header(), frames).expect("re-seal");
    assert_eq!(
        resealed.bytes, sealed.bytes,
        "a retry must propose byte-identical bytes or the kernel sees corruption"
    );
    assert_eq!(resealed.chunk_id, sealed.chunk_id);
}

/// Test 6: the range-read path. `decode_index` must work on a *prefix* — the
/// header and index only — without the frame bytes ever being present.
#[test]
fn decode_index_reads_a_prefix_without_the_frames() {
    let sealed = seal_chunk(header(), vec![frame(1, 10, 4)]).expect("seal");
    let full = decode_index(&sealed.bytes).expect("index from the whole object");

    // Truncate to exactly the header + index. This is what a range read of the
    // object's first bytes would return.
    let frames_start = full.frames[0].frame_offset as usize;
    let prefix = &sealed.bytes[..frames_start];
    let from_prefix = decode_index(prefix).expect("index from a prefix alone");

    assert_eq!(from_prefix, full);
    assert_eq!(from_prefix.frames.len(), 1);
    assert_eq!(from_prefix.frames[0].journal_id, journal(1));
    assert_eq!(from_prefix.frames[0].record_count, 4);
    assert_eq!(from_prefix.frames[0].base_offset, RecordOffset::new(10));
    assert!(
        from_prefix.frame_for(journal(1)).is_some(),
        "a reader must be able to ask whether a journal is present without the frames"
    );
    assert!(from_prefix.frame_for(journal(9)).is_none());

    // And a single frame, fetched alone, verifies alone.
    let entry = &from_prefix.frames[0];
    let start = entry.frame_offset as usize;
    let end = start + entry.frame_len as usize;
    let records = decode_frame(entry, &sealed.bytes[start..end]).expect("frame alone");
    assert_eq!(records.len(), 4);
}

/// Test 4: corruption is corruption. A flipped byte in a frame or in the index
/// is never silently tolerated.
#[test]
fn crc_mismatches_are_corruption() {
    let sealed = seal_chunk(header(), vec![frame(1, 0, 2)]).expect("seal");
    let index = decode_index(&sealed.bytes).expect("index");

    let mut corrupt_frame = sealed.bytes.to_vec();
    let frame_byte = index.frames[0].frame_offset as usize;
    corrupt_frame[frame_byte] ^= 0xff;
    assert!(matches!(
        decode_chunk(&Bytes::from(corrupt_frame)),
        Err(ChunkError::CorruptFrame { .. })
    ));

    // Flip a byte inside the index region (the first index entry's journal id).
    let mut corrupt_index = sealed.bytes.to_vec();
    corrupt_index[index.frames[0].frame_offset as usize - 1] ^= 0xff;
    assert!(matches!(
        decode_chunk(&Bytes::from(corrupt_index)),
        Err(ChunkError::CorruptIndex | ChunkError::InvalidFrameRegions)
    ));
}

/// Test 5: truncation, trailing bytes, and a bad version are all rejected.
#[test]
fn truncation_trailing_bytes_and_bad_versions_are_rejected() {
    let sealed = seal_chunk(header(), vec![frame(1, 0, 2)]).expect("seal");

    let truncated = sealed.bytes.slice(0..sealed.bytes.len() - 1);
    assert!(decode_chunk(&truncated).is_err());

    let mut padded = sealed.bytes.to_vec();
    padded.push(0);
    assert!(decode_chunk(&Bytes::from(padded)).is_err());

    let mut wrong_major = sealed.bytes.to_vec();
    wrong_major[4] = 99;
    assert_eq!(
        decode_chunk(&Bytes::from(wrong_major)),
        Err(ChunkError::UnsupportedMajor { major: 99 })
    );

    let mut wrong_magic = sealed.bytes.to_vec();
    wrong_magic[0] = b'X';
    assert_eq!(
        decode_chunk(&Bytes::from(wrong_magic)),
        Err(ChunkError::InvalidMagic)
    );
}

/// An empty chunk carries no information and is not a valid durable value.
#[test]
fn an_empty_chunk_is_not_a_value() {
    assert_eq!(
        seal_chunk(header(), Vec::new()),
        Err(ChunkError::EmptyChunk)
    );
}

/// A journal may appear at most once in a chunk's index.
#[test]
fn a_journal_may_not_appear_twice() {
    let duplicate = vec![frame(1, 0, 1), frame(1, 5, 1)];
    assert_eq!(
        seal_chunk(header(), duplicate),
        Err(ChunkError::NonCanonicalIndex)
    );
}

/// `encoded_chunk_len` must predict the sealed size exactly — the accumulator
/// decides whether one more record breaches `max_chunk_bytes` *before* sealing,
/// so an estimate that is merely close would breach the byte bound.
#[test]
fn encoded_length_is_exact_before_sealing() {
    for count in 1..8_i64 {
        let frames = vec![frame(1, 0, count)];
        let predicted = encoded_chunk_len(&frames).expect("predict");
        let sealed = seal_chunk(header(), frames).expect("seal");
        assert_eq!(
            predicted,
            sealed.bytes.len(),
            "the predicted size must equal the sealed size, not approximate it"
        );
    }
}

/// The multi-frame layout works — it is the format co-packing will use once the
/// range-read gate opens (decision 0009). Phase 1's *driver* never emits more
/// than one frame; the codec is proved general anyway so that enabling
/// co-packing later is a policy change and not a format break.
#[test]
fn the_multi_frame_layout_is_already_correct() {
    let frames = vec![frame(3, 0, 2), frame(1, 100, 1), frame(2, 50, 3)];
    let sealed = seal_chunk(header(), frames).expect("seal");
    let decoded = decode_chunk(&sealed.bytes).expect("decode");

    // Canonically ordered by journal id, regardless of the order supplied.
    let ids: Vec<_> = decoded.frames.iter().map(|f| f.journal_id).collect();
    assert_eq!(ids, vec![journal(1), journal(2), journal(3)]);

    let index = decode_index(&sealed.bytes).expect("index");
    let entry = index.frame_for(journal(2)).expect("journal 2 is present");
    let start = entry.frame_offset as usize;
    let end = start + entry.frame_len as usize;
    let records = decode_frame(entry, &sealed.bytes[start..end]).expect("one frame, alone");
    assert_eq!(records.len(), 3);
    assert_eq!(entry.base_offset, RecordOffset::new(50));
}

proptest! {
    /// Test 1/2 as a property, over generated records.
    #[test]
    fn generated_chunks_round_trip_and_re_encode_identically(
        raw in vec(
            (
                btree_map("[a-z]{1,6}", "[a-z0-9]{0,10}", 0..4),
                vec(any::<u8>(), 0..48),
                any::<i64>(),
            ),
            1..12,
        ),
        base in any::<u32>(),
    ) {
        let records: Vec<Record> = raw
            .into_iter()
            .map(|(attrs, payload, number)| {
                let mut attributes: BTreeMap<String, AttributeValue> = attrs
                    .into_iter()
                    .map(|(k, v)| (k, AttributeValue::String(v)))
                    .collect();
                attributes.insert("n".into(), AttributeValue::I64(number));
                Record { attributes, payload: Bytes::from(payload) }
            })
            .collect();

        let count = records.len() as u64;
        let frames = vec![Frame {
            journal_id: journal(1),
            base_offset: RecordOffset::new(u64::from(base)),
            records,
            producers: producers(count),
        }];

        let predicted = encoded_chunk_len(&frames).expect("predict");
        let sealed = seal_chunk(header(), frames.clone()).expect("seal");
        prop_assert_eq!(predicted, sealed.bytes.len());

        let decoded = decode_chunk(&sealed.bytes).expect("decode");
        prop_assert_eq!(&decoded.frames, &frames);

        let resealed = seal_chunk(header(), frames).expect("re-seal");
        prop_assert_eq!(resealed.bytes, sealed.bytes);
    }

    /// Test 3: arbitrary bytes never panic. A decoder that panics on a corrupt
    /// object is a decoder that can be crashed by a corrupt object.
    #[test]
    fn arbitrary_bytes_never_panic(bytes in vec(any::<u8>(), 0..512)) {
        let _ = decode_chunk(&Bytes::from(bytes.clone()));
        let _ = decode_index(&bytes);
    }

    /// Corrupting any single byte of a sealed chunk must be detected, never
    /// silently decoded into different records.
    #[test]
    fn any_single_byte_corruption_is_detected(index in 0_usize..200) {
        let sealed = seal_chunk(header(), vec![frame(1, 0, 3)]).expect("seal");
        prop_assume!(index < sealed.bytes.len());

        let mut corrupt = sealed.bytes.to_vec();
        corrupt[index] ^= 0xff;
        let corrupt = Bytes::from(corrupt);

        match decode_chunk(&corrupt) {
            Err(_) => {}
            Ok(decoded) => {
                // A byte we do not checksum (a reserved/padding position) may
                // decode — but it must not change the records.
                let original = decode_chunk(&sealed.bytes).expect("original decodes");
                prop_assert_eq!(
                    decoded.frames, original.frames,
                    "corruption changed the records without being detected"
                );
            }
        }
    }
}
