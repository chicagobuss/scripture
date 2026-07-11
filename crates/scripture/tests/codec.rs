use std::collections::BTreeMap;

use bytes::Bytes;
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use scripture::{
    AttributeValue, CodecError, JournalId, Record, RecordOffset, decode_batch, encode_batch,
    encoded_batch_len,
};

fn journal_id() -> JournalId {
    JournalId::from_bytes(*b"scripture-test!!")
}

#[test]
fn rejects_unknown_major_truncation_and_corrupt_footer() {
    let record = Record::new(
        [("kind".into(), AttributeValue::String("order".into()))],
        Bytes::from_static(b"payload"),
    );
    let encoded = encode_batch(journal_id(), RecordOffset::new(7), &[record]).expect("encode");

    let mut wrong_major = encoded.to_vec();
    wrong_major[4] = 99;
    assert_eq!(
        decode_batch(&Bytes::from(wrong_major)),
        Err(CodecError::UnsupportedMajor { major: 99 })
    );

    assert_eq!(
        decode_batch(&encoded.slice(..encoded.len() - 1)),
        Err(CodecError::InvalidMagic)
    );

    let mut corrupt_footer = encoded.to_vec();
    let first_footer_offset = corrupt_footer.len() - 12 - (4 + 4 + 8) + 8;
    corrupt_footer[first_footer_offset + 7] ^= 1;
    assert_eq!(
        decode_batch(&Bytes::from(corrupt_footer)),
        Err(CodecError::CorruptFooter)
    );
}

proptest! {
    #[test]
    fn canonical_batches_round_trip(
        raw_records in vec(
            (
                btree_map("[a-z]{1,8}", prop_oneof![
                    "[a-z0-9]{0,16}".prop_map(AttributeValue::String),
                    any::<i64>().prop_map(AttributeValue::I64),
                    any::<bool>().prop_map(AttributeValue::Bool),
                ], 0..5),
                vec(any::<u8>(), 0..128),
            ),
            0..16,
        ),
        base in 0_u64..1_000_000,
    ) {
        let records = raw_records
            .into_iter()
            .map(|(attributes, payload)| Record {
                attributes: attributes.into_iter().collect::<BTreeMap<_, _>>(),
                payload: Bytes::from(payload),
            })
            .collect::<Vec<_>>();
        let encoded = encode_batch(journal_id(), RecordOffset::new(base), &records)
            .expect("generated values fit");
        prop_assert_eq!(encoded.len(), encoded_batch_len(&records).expect("measure"));
        let decoded = decode_batch(&encoded).expect("decode canonical batch");
        prop_assert_eq!(decoded.journal_id, journal_id());
        prop_assert_eq!(decoded.base_offset, RecordOffset::new(base));
        prop_assert_eq!(&decoded.records, &records);
        prop_assert_eq!(
            encode_batch(decoded.journal_id, decoded.base_offset, &decoded.records)
                .expect("re-encode"),
            encoded,
            "canonical re-encoding must be byte-identical"
        );
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in vec(any::<u8>(), 0..512)) {
        let _ = decode_batch(&Bytes::from(bytes));
    }
}
