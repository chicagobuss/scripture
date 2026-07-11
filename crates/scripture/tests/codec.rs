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
                    any::<f64>().prop_filter("finite", |value| value.is_finite()).prop_map(AttributeValue::F64),
                    any::<i64>().prop_map(AttributeValue::TimestampMicros),
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

#[test]
fn rejects_non_finite_floats() {
    let record = Record::new(
        [("value".into(), AttributeValue::F64(f64::NAN))],
        Bytes::new(),
    );
    assert_eq!(
        encode_batch(journal_id(), RecordOffset::new(0), &[record]),
        Err(CodecError::NonFiniteFloat)
    );
}

#[test]
fn float_zero_is_canonical_and_timestamp_round_trips() {
    let negative_zero = Record::new(
        [
            ("value".into(), AttributeValue::F64(-0.0)),
            (
                "event_time".into(),
                AttributeValue::TimestampMicros(1_725_000_000_123_456),
            ),
        ],
        Bytes::new(),
    );
    let positive_zero = Record::new(
        [
            ("value".into(), AttributeValue::F64(0.0)),
            (
                "event_time".into(),
                AttributeValue::TimestampMicros(1_725_000_000_123_456),
            ),
        ],
        Bytes::new(),
    );
    let negative = encode_batch(journal_id(), RecordOffset::new(0), &[negative_zero])
        .expect("encode negative zero");
    let positive = encode_batch(journal_id(), RecordOffset::new(0), &[positive_zero])
        .expect("encode positive zero");
    assert_eq!(negative, positive);
    let decoded = decode_batch(&negative).expect("decode");
    assert_eq!(
        decoded.records[0].attributes.get("event_time"),
        Some(&AttributeValue::TimestampMicros(1_725_000_000_123_456))
    );
}
