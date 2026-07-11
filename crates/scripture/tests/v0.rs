use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::executor::block_on;
use holylog::atomic::AtomicLog;
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use proptest::prelude::*;
use scripture::{
    AttributeValue, BatchAccumulator, BatchPolicy, Checkpoint, JournalId, JournalReader,
    JournalWriter, ManualClock, PushResult, ReadError, ReadEvent, ReaderCheckpointError, Record,
    RecordOffset, ResumeHint, WriteError,
};

fn journal_id() -> JournalId {
    JournalId::from_bytes(*b"scripture-test!!")
}

fn log() -> AtomicLog {
    let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
    AtomicLog::builder(drive, 4).build().expect("build log")
}

fn record(number: i64) -> Record {
    Record::new(
        [
            ("number".into(), AttributeValue::I64(number)),
            ("valid".into(), AttributeValue::Bool(true)),
        ],
        Bytes::from(format!("record-{number}")),
    )
}

#[test]
fn writer_reader_checkpoint_and_trim_gap_form_one_ordered_history() {
    block_on(async {
        let log = log();
        let mut writer = JournalWriter::new(journal_id(), log.clone(), RecordOffset::new(0));
        let first = writer
            .append_batch(vec![record(0), record(1)])
            .await
            .expect("first batch");
        assert_eq!(first.slot, 0);
        assert_eq!(first.first_offset, RecordOffset::new(0));
        assert_eq!(first.next_offset, RecordOffset::new(2));
        let second = writer
            .append_batch(vec![record(2), record(3), record(4)])
            .await
            .expect("second batch");
        assert_eq!(second.slot, 1);
        assert_eq!(second.first_offset, RecordOffset::new(2));
        assert_eq!(writer.next_offset(), RecordOffset::new(5));

        let mut reader = JournalReader::from_start(journal_id(), log.clone());
        let ReadEvent::Record(zero) = reader.read_next().await.expect("record zero") else {
            panic!("expected record");
        };
        assert_eq!(zero.offset, RecordOffset::new(0));
        let checkpoint = reader.checkpoint();
        assert_eq!(checkpoint.next_offset, RecordOffset::new(1));
        assert_eq!(checkpoint.resume_hint, Some(ResumeHint::new(0, 1)));

        let mut resumed = JournalReader::from_checkpoint(journal_id(), log.clone(), checkpoint)
            .expect("resume checkpoint");
        let ReadEvent::Record(one) = resumed.read_next().await.expect("record one") else {
            panic!("expected record");
        };
        assert_eq!(one.offset, RecordOffset::new(1));
        let ReadEvent::Record(two) = resumed.read_next().await.expect("record two") else {
            panic!("expected record");
        };
        assert_eq!(two.offset, RecordOffset::new(2));

        writer.trim_to_slot(1).await.expect("logical trim");
        let mut lagging = JournalReader::from_start(journal_id(), log);
        let ReadEvent::Gap(gap) = lagging.read_next().await.expect("trim gap") else {
            panic!("expected explicit gap");
        };
        assert_eq!(gap.requested_slot, 0);
        assert_eq!(gap.new_start_slot, 1);
        assert_eq!(gap.expected_offset, RecordOffset::new(0));
        let ReadEvent::Record(after_gap) = lagging.read_next().await.expect("survivor") else {
            panic!("expected surviving record");
        };
        assert_eq!(after_gap.offset, RecordOffset::new(2));
    });
}

#[test]
fn writer_does_not_advance_offsets_for_empty_overflowing_or_sealed_batches() {
    block_on(async {
        let atomic_log = log();
        let mut writer = JournalWriter::new(journal_id(), atomic_log.clone(), RecordOffset::new(9));
        assert!(matches!(
            writer.append_batch(Vec::new()).await,
            Err(WriteError::EmptyBatch)
        ));
        assert_eq!(writer.next_offset(), RecordOffset::new(9));

        atomic_log.seal().await.expect("seal");
        assert!(matches!(
            writer.append_batch(vec![record(9)]).await,
            Err(WriteError::Log(_))
        ));
        assert_eq!(writer.next_offset(), RecordOffset::new(9));

        let fresh = log();
        let mut overflow = JournalWriter::new(journal_id(), fresh, RecordOffset::new(u64::MAX));
        assert!(matches!(
            overflow.append_batch(vec![record(1)]).await,
            Err(WriteError::Codec(_))
        ));
        assert_eq!(overflow.next_offset(), RecordOffset::new(u64::MAX));
    });
}

#[test]
fn checkpoints_are_bound_to_journal_identity_and_need_nonzero_hints() {
    let log = log();
    let other = JournalId::from_bytes(*b"another-journal!");
    let mismatch = JournalReader::from_checkpoint(
        journal_id(),
        log.clone(),
        Checkpoint {
            journal_id: other,
            next_offset: RecordOffset::new(0),
            resume_hint: None,
        },
    );
    assert!(matches!(
        mismatch,
        Err(ReaderCheckpointError::JournalMismatch { .. })
    ));

    let missing = JournalReader::from_checkpoint(
        journal_id(),
        log,
        Checkpoint {
            journal_id: journal_id(),
            next_offset: RecordOffset::new(7),
            resume_hint: None,
        },
    );
    assert!(matches!(
        missing,
        Err(ReaderCheckpointError::MissingResumeHint)
    ));
}

#[test]
fn reader_rejects_wrong_journal_discontinuity_and_invalid_resume_hint() {
    block_on(async {
        let other = JournalId::from_bytes(*b"another-journal!");

        let wrong_log = log();
        JournalWriter::new(other, wrong_log.clone(), RecordOffset::new(0))
            .append_batch(vec![record(0)])
            .await
            .expect("write other journal");
        let mut wrong_reader = JournalReader::from_start(journal_id(), wrong_log);
        assert!(matches!(
            wrong_reader.read_next().await,
            Err(ReadError::JournalMismatch { .. })
        ));

        let discontinuous_log = log();
        JournalWriter::new(
            journal_id(),
            discontinuous_log.clone(),
            RecordOffset::new(5),
        )
        .append_batch(vec![record(5)])
        .await
        .expect("write discontinuous batch");
        let mut discontinuous = JournalReader::from_start(journal_id(), discontinuous_log);
        assert!(matches!(
            discontinuous.read_next().await,
            Err(ReadError::OffsetDiscontinuity {
                expected: 0,
                actual: 5
            })
        ));

        let hinted_log = log();
        JournalWriter::new(journal_id(), hinted_log.clone(), RecordOffset::new(0))
            .append_batch(vec![record(0)])
            .await
            .expect("write hinted batch");
        let checkpoint = Checkpoint {
            journal_id: journal_id(),
            next_offset: RecordOffset::new(0),
            resume_hint: Some(ResumeHint::new(0, 9)),
        };
        let mut hinted = JournalReader::from_checkpoint(journal_id(), hinted_log, checkpoint)
            .expect("construct reader before durable hint validation");
        assert!(matches!(
            hinted.read_next().await,
            Err(ReadError::InvalidResumeHint {
                slot: 0,
                record_index: 9
            })
        ));
    });
}

#[test]
fn batching_policy_uses_exact_bytes_count_and_monotonic_age() {
    let clock = Arc::new(ManualClock::new());
    let policy = BatchPolicy {
        max_records: 2,
        max_bytes: usize::MAX,
        max_age: Duration::from_millis(10),
    };
    let mut batch = BatchAccumulator::new(policy, Arc::clone(&clock));
    assert_eq!(
        batch.push(record(0)).expect("stage"),
        PushResult::Accepted {
            should_flush: false
        }
    );
    assert!(!batch.is_due());
    clock.advance(Duration::from_millis(10));
    assert!(batch.is_due());
    assert_eq!(
        batch.push(record(1)).expect("stage to count bound"),
        PushResult::Accepted { should_flush: true }
    );
    assert_eq!(batch.take().len(), 2);
    assert!(batch.is_empty());

    let small = BatchPolicy {
        max_records: 10,
        max_bytes: 1,
        max_age: Duration::from_secs(1),
    };
    let mut batch = BatchAccumulator::new(small, ManualClock::new());
    assert!(matches!(
        batch.push(record(3)).expect("single oversized accepted"),
        PushResult::Accepted { should_flush: true }
    ));
    assert!(matches!(
        batch.push(record(4)).expect("must flush existing"),
        PushResult::FlushFirst(_)
    ));
}

proptest! {
    #[test]
    fn generated_batch_histories_read_back_dense_offsets(
        batch_sizes in proptest::collection::vec(1_usize..6, 1..8),
    ) {
        block_on(async {
            let log = log();
            let mut writer =
                JournalWriter::new(journal_id(), log.clone(), RecordOffset::new(0));
            let mut next_value = 0_i64;
            for size in &batch_sizes {
                let records = (0..*size)
                    .map(|_| {
                        let value = next_value;
                        next_value += 1;
                        record(value)
                    })
                    .collect();
                writer.append_batch(records).await.expect("append generated batch");
            }
            let expected_count = u64::try_from(next_value).expect("nonnegative small count");
            prop_assert_eq!(writer.next_offset(), RecordOffset::new(expected_count));

            let mut reader = JournalReader::from_start(journal_id(), log);
            for expected in 0..expected_count {
                let event = reader.read_next().await.expect("read generated record");
                let ReadEvent::Record(record) = event else {
                    prop_assert!(false, "expected record at offset {expected}");
                    unreachable!();
                };
                prop_assert_eq!(record.offset, RecordOffset::new(expected));
                prop_assert_eq!(record.payload, Bytes::from(format!("record-{expected}")));
            }
            prop_assert_eq!(
                reader.read_next().await.expect("caught up"),
                ReadEvent::CaughtUp {
                    next_offset: RecordOffset::new(expected_count)
                }
            );
            Ok(())
        })?;
    }
}
