//! Mutation safety gates for the deterministic shard reducer (tdv2 data plane).

use bytes::Bytes;
use scripture::shard::{
    AppendDataRef, DataRef, EventId, ShardCommand, ShardError, ShardReducer,
};
use scripture::{JournalId, ProducerId, RecordOffset};

fn journal() -> JournalId {
    JournalId::from_bytes(*b"shard-journal!!!")
}

fn producer() -> ProducerId {
    ProducerId::from_bytes(*b"shard-producer!!")
}

fn append(epoch: u32, sequence: u64) -> AppendDataRef {
    AppendDataRef {
        event_id: EventId {
            producer_id: producer(),
            producer_epoch: epoch,
            sequence,
        },
        journal_id: journal(),
        data_ref: DataRef {
            blob_id: format!("blob-{epoch}-{sequence}"),
            checksum: [0; 32],
            start: 0,
            end: 4,
        },
        payload: Bytes::from_static(b"test"),
    }
}

fn replay(commands: &[ShardCommand]) -> (ShardReducer, Vec<RecordOffset>) {
    let mut reducer = ShardReducer::new();
    let offsets = commands
        .iter()
        .map(|command| reducer.apply(command.clone()).expect("apply"))
        .collect();
    (reducer, offsets)
}

#[test]
fn rejects_stale_producer_epoch_after_advance() {
    let mut reducer = ShardReducer::new();
    reducer
        .apply(ShardCommand::Append(append(2, 0)))
        .expect("epoch 2");
    assert!(matches!(
        reducer.apply(ShardCommand::Append(append(1, 1))),
        Err(ShardError::FencedProducer { .. })
    ));
}

#[test]
fn retry_after_lost_response_returns_original_offset_without_double_insert() {
    let mut reducer = ShardReducer::new();
    let first = reducer
        .apply(ShardCommand::Append(append(1, 0)))
        .expect("first");
    let retry = reducer
        .apply(ShardCommand::Append(append(1, 0)))
        .expect("retry");
    let second = reducer
        .apply(ShardCommand::Append(append(1, 1)))
        .expect("second");
    assert_eq!(first, RecordOffset::new(0));
    assert_eq!(retry, RecordOffset::new(0));
    assert_eq!(second, RecordOffset::new(1));
    assert_eq!(reducer.state().refs.get(&journal()).map(Vec::len), Some(2));
}

#[test]
fn deterministic_replay_from_command_log() {
    let commands = vec![
        ShardCommand::Append(append(1, 0)),
        ShardCommand::Append(append(1, 0)), // simulated retry
        ShardCommand::Append(append(1, 1)),
    ];
    let (left, left_offsets) = replay(&commands);
    let (right, right_offsets) = replay(&commands);
    assert_eq!(left_offsets, right_offsets);
    assert_eq!(left.state(), right.state());
}

#[test]
fn new_epoch_resets_sequence_expectation() {
    let mut reducer = ShardReducer::new();
    reducer
        .apply(ShardCommand::Append(append(1, 0)))
        .expect("epoch 1");
    let first_in_epoch_2 = reducer
        .apply(ShardCommand::Append(append(2, 0)))
        .expect("epoch 2 starts at sequence 0");
    assert_eq!(first_in_epoch_2, RecordOffset::new(1));
}
