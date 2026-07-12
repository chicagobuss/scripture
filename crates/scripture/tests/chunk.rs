//! Chunk codec tests (Phase 1, tests 1–7 of `docs/phase-1-chunk-driver.md`).

use std::collections::BTreeMap;

use bytes::Bytes;
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use scripture::{
    AttributeValue, ChunkDigest, ChunkError, ChunkHeader, ChunkId, CohortId, Frame, JournalId,
    ProducerId, Record, RecordOffset, SubmissionRef, WriterId, decode_chunk, decode_frame,
    decode_index, encoded_chunk_len, seal_chunk, seal_single_frame_chunk,
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

fn producer(tag: u8) -> ProducerId {
    let mut bytes = *b"producer-id-0123";
    bytes[15] = tag;
    ProducerId::from_bytes(bytes)
}

/// One single-record submission per record, which is the simple case.
fn submissions(count: u64) -> Vec<SubmissionRef> {
    (0..count)
        .map(|index| SubmissionRef {
            producer_id: producer(1),
            producer_epoch: 3,
            sequence: index,
            first_record: u32::try_from(index).expect("small"),
            record_count: 1,
        })
        .collect()
}

fn frame(tag: u8, base: u64, count: i64) -> Frame {
    Frame {
        journal_id: journal(tag),
        base_offset: RecordOffset::new(base),
        records: (0..count).map(record).collect(),
        submissions: submissions(count.max(0) as u64),
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

/// Test 7: the co-packing gate. The Phase-1 sealing path physically cannot
/// produce a chunk with more than one frame (decision 0009).
///
/// Without a range-read capability, a reader of a sparse journal inside a
/// co-packed chunk downloads the whole chunk to reach its frame. The gate lives
/// in code, not only in prose, so it cannot be crossed by accident.
#[test]
fn the_phase_one_sealing_path_forbids_co_packing() {
    let one = seal_single_frame_chunk(header(), vec![frame(1, 0, 2)]).expect("one frame is fine");
    assert_eq!(decode_chunk(&one.bytes).expect("decode").frames.len(), 1);

    assert_eq!(
        seal_single_frame_chunk(header(), vec![frame(1, 0, 1), frame(2, 0, 1)]),
        Err(ChunkError::CoPackingForbidden { frames: 2 }),
        "co-packing must be unreachable until the range-read gate opens"
    );
    assert_eq!(
        seal_single_frame_chunk(header(), Vec::new()),
        Err(ChunkError::CoPackingForbidden { frames: 0 })
    );
}

/// The digest identifies the sealed value. Equal bytes give equal digests, which
/// is what lets a retry be *proved* to be a retry rather than assumed to be one.
#[test]
fn the_digest_identifies_the_sealed_bytes() {
    let frames = vec![frame(1, 0, 3)];
    let sealed = seal_chunk(header(), frames.clone()).expect("seal");
    let resealed = seal_chunk(header(), frames).expect("re-seal");

    assert_eq!(sealed.digest, resealed.digest);
    assert_eq!(sealed.digest, ChunkDigest::of(&sealed.bytes));

    let different = seal_chunk(header(), vec![frame(1, 0, 4)]).expect("seal a different chunk");
    assert_ne!(
        sealed.digest, different.digest,
        "different bytes must not share a digest"
    );
}

/// The on-wire checksum is CRC-32C (Castagnoli), **not** CRC-32/IEEE.
///
/// This is a known-answer test against the standard "123456789" vector. It exists
/// because the two algorithms are trivially confused — an earlier draft of this
/// codec named the field `crc32c` while computing IEEE — and a reader that
/// computes the wrong one rejects every valid chunk. The format's name and its
/// algorithm must never drift apart again.
#[test]
fn the_wire_checksum_is_crc32c_castagnoli() {
    const CHECK_VECTOR: &[u8] = b"123456789";
    const CRC32C_CHECK: u32 = 0xE306_9283; // Castagnoli
    const CRC32_IEEE_CHECK: u32 = 0xCBF4_3926; // what we must NOT be computing

    assert_eq!(crc32c::crc32c(CHECK_VECTOR), CRC32C_CHECK);
    assert_ne!(CRC32C_CHECK, CRC32_IEEE_CHECK);

    // And the chunk actually uses it: recomputing a frame's CRC with Castagnoli
    // must match what the encoder wrote into the index.
    let sealed = seal_chunk(header(), vec![frame(1, 0, 2)]).expect("seal");
    let index = decode_index(&sealed.bytes).expect("index");
    let entry = &index.frames[0];
    let start = entry.frame_offset as usize;
    let end = start + entry.frame_len as usize;
    assert_eq!(
        entry.frame_crc,
        crc32c::crc32c(&sealed.bytes[start..end]),
        "the index CRC must be Castagnoli over exactly the frame's bytes"
    );
}

/// The reason `SubmissionRef` stores a record *span* rather than a sequence
/// range: a submission may carry many records, and a deduplicated retry must be
/// handed back the **same offsets the first attempt received** (decision 0010).
///
/// Knowing only that a sequence was committed is not enough. Recovery has to
/// answer "and where did it land?" — so the span is stored, never inferred.
#[test]
fn a_multi_record_submissions_offsets_are_recoverable_from_the_chunk() {
    // Three submissions of 2, 3, and 1 records, landing at base offset 100.
    let records: Vec<Record> = (0..6).map(record).collect();
    let spans = vec![
        SubmissionRef {
            producer_id: producer(1),
            producer_epoch: 2,
            sequence: 40,
            first_record: 0,
            record_count: 2,
        },
        SubmissionRef {
            producer_id: producer(1),
            producer_epoch: 2,
            sequence: 41,
            first_record: 2,
            record_count: 3,
        },
        SubmissionRef {
            producer_id: producer(2),
            producer_epoch: 9,
            sequence: 0,
            first_record: 5,
            record_count: 1,
        },
    ];
    let sealed = seal_single_frame_chunk(
        header(),
        vec![Frame {
            journal_id: journal(1),
            base_offset: RecordOffset::new(100),
            records,
            submissions: spans,
        }],
    )
    .expect("seal");

    // Recovery reads only the index — no frame bytes — and reconstructs the
    // exact receipt each submission was given.
    let index = decode_index(&sealed.bytes).expect("index");
    let entry = &index.frames[0];

    assert_eq!(
        entry.offsets_for(producer(1), 2, 40),
        Some((RecordOffset::new(100), 2)),
        "the first submission's two records began at offset 100"
    );
    assert_eq!(
        entry.offsets_for(producer(1), 2, 41),
        Some((RecordOffset::new(102), 3)),
        "the second submission began after the first, not at a guessed offset"
    );
    assert_eq!(
        entry.offsets_for(producer(2), 9, 0),
        Some((RecordOffset::new(105), 1))
    );

    // An epoch or producer that did not write here has no offsets to return.
    assert_eq!(entry.offsets_for(producer(1), 3, 40), None);
    assert_eq!(entry.offsets_for(producer(9), 2, 40), None);
}

/// Submissions must tile the frame's records exactly. A gap would mean a record
/// no producer claims; an overlap would mean two producers claiming the same
/// offsets. Both are corruption, and both are rejected at seal and at decode.
#[test]
fn submissions_must_tile_the_records_exactly() {
    let malformed = |spans: Vec<SubmissionRef>| {
        seal_single_frame_chunk(
            header(),
            vec![Frame {
                journal_id: journal(1),
                base_offset: RecordOffset::new(0),
                records: (0..3).map(record).collect(),
                submissions: spans,
            }],
        )
    };

    // A gap: records 0..1 claimed, record 2 orphaned.
    assert_eq!(
        malformed(vec![SubmissionRef {
            producer_id: producer(1),
            producer_epoch: 1,
            sequence: 0,
            first_record: 0,
            record_count: 2,
        }]),
        Err(ChunkError::InvalidSubmissionSpans)
    );

    // An overlap: two submissions both claim record 1.
    assert_eq!(
        malformed(vec![
            SubmissionRef {
                producer_id: producer(1),
                producer_epoch: 1,
                sequence: 0,
                first_record: 0,
                record_count: 2,
            },
            SubmissionRef {
                producer_id: producer(1),
                producer_epoch: 1,
                sequence: 1,
                first_record: 1,
                record_count: 2,
            },
        ]),
        Err(ChunkError::InvalidSubmissionSpans)
    );

    // An empty submission produced no records and cannot be identified.
    assert_eq!(
        malformed(vec![
            SubmissionRef {
                producer_id: producer(1),
                producer_epoch: 1,
                sequence: 0,
                first_record: 0,
                record_count: 0,
            },
            SubmissionRef {
                producer_id: producer(1),
                producer_epoch: 1,
                sequence: 1,
                first_record: 0,
                record_count: 3,
            },
        ]),
        Err(ChunkError::InvalidSubmissionSpans)
    );
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
            submissions: submissions(count),
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
