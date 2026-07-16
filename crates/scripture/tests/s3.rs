#![cfg(feature = "s3-smoke")]

//! Scripture's v0 data plane against Amazon S3 — the third measured provider.
//!
//! Two providers made the cost structure a hypothesis. A third is what decides
//! whether it is a model: the invariants below are asserted identically here,
//! on GCS, and on R2, so if S3 disagreed we would learn precisely which term is
//! provider-specific rather than structural.
//!
//! Requires `HOLYLOG_S3_BUCKET` and credentials in `AWS_ACCESS_KEY_ID` /
//! `AWS_SECRET_ACCESS_KEY`; `object_store` does not read `~/.aws/credentials`.

use std::error::Error;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::TryStreamExt;
use holylog::atomic::{AtomicLog, LogDriveSeal, LogDriveTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::logdrive::Address;
use holylog_object_store::{
    BackendCapabilities, ConditionalCreate, ListingOrder, ListingVisibility, ObjectStoreLogDrive,
    PointSemantics, WritePolicy,
};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use scripture::{
    AttributeValue, JournalId, JournalReader, JournalWriter, ReadEvent, Record, RecordOffset,
    RetentionAuthority,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn s3_store() -> TestResult<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("HOLYLOG_S3_BUCKET")?)
        .with_region(std::env::var("HOLYLOG_S3_REGION").unwrap_or_else(|_| "us-west-2".into()))
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()?;
    Ok(Arc::new(store))
}

/// S3 documents strongly consistent reads, lexicographically ordered listing,
/// and — only since 2024 — `If-None-Match` create-if-absent. The last of those
/// is why this declaration is a claim and the run is the evidence.
fn capabilities() -> BackendCapabilities {
    BackendCapabilities::new(
        PointSemantics::LinearizableSingleValue,
        ListingOrder::Lexicographic,
        ListingVisibility::Strong,
        ConditionalCreate::Atomic,
    )
}

fn drive(store: &Arc<dyn ObjectStore>, prefix: Path) -> TestResult<Arc<ObjectStoreLogDrive>> {
    Ok(Arc::new(ObjectStoreLogDrive::new(
        Arc::clone(store),
        prefix,
        capabilities(),
        WritePolicy::AtomicCreate,
    )?))
}

async fn clear_prefix(store: &Arc<dyn ObjectStore>, prefix: &Path) -> TestResult {
    let mut objects = store.list(Some(prefix));
    let mut paths = Vec::new();
    while let Some(meta) = objects.try_next().await? {
        paths.push(meta.location);
    }
    for path in paths {
        store.delete(&path).await?;
    }
    Ok(())
}

fn record(value: i64) -> Record {
    Record::new(
        [("value".into(), AttributeValue::I64(value))],
        Bytes::from(format!("s3-record-{value}")),
    )
}

async fn run_scripture_history(store: Arc<dyn ObjectStore>, root: &Path) -> TestResult {
    let data = drive(&store, root.clone().join("data"))?;
    let seal_drive = drive(&store, root.clone().join("seal"))?;
    let trim_drive = drive(&store, root.clone().join("trim"))?;
    let seal = Arc::new(LogDriveSeal::new(
        Arc::clone(&seal_drive) as Arc<dyn LogDrive>,
        Address::new(0)?,
        Bytes::from_static(b"sealed"),
    )) as Arc<dyn Seal>;
    let trim = Arc::new(LogDriveTrimPoint::new(
        Arc::clone(&trim_drive) as Arc<dyn LogDrive>
    )) as Arc<dyn TrimPoint>;

    let log = AtomicLog::builder(Arc::clone(&data) as Arc<dyn LogDrive>, 4)
        .seal(seal)
        .trim(trim)
        .build()?;
    let journal_id = JournalId::from_bytes(*b"scripture-s3-v0!");

    let mut writer = JournalWriter::new(journal_id, log.clone(), RecordOffset::new(0));
    writer
        .append_batch(vec![record(0), record(1), record(2)])
        .await?;
    writer.append_batch(vec![record(3), record(4)]).await?;
    assert_eq!(writer.next_offset(), RecordOffset::new(5));

    let recovered = JournalWriter::recover(journal_id, log.clone()).await?;
    assert_eq!(recovered.next_offset(), RecordOffset::new(5));

    let mut reader = JournalReader::from_start(journal_id, log.clone());
    assert_eq!(reader.refresh_tail().await?, 2);
    for expected in 0_u64..5 {
        let ReadEvent::Record(entry) = reader.read_next().await? else {
            return Err(std::io::Error::other("expected an S3-backed record").into());
        };
        assert_eq!(entry.offset, RecordOffset::new(expected));
        assert_eq!(entry.payload, Bytes::from(format!("s3-record-{expected}")));
    }

    RetentionAuthority::new(log.clone()).trim_to_slot(1).await?;
    let mut lagging = JournalReader::from_start(journal_id, log.clone());
    lagging.refresh_tail().await?;
    let ReadEvent::Gap(gap) = lagging.read_next().await? else {
        return Err(std::io::Error::other("expected an S3-backed trim gap").into());
    };
    assert_eq!(gap.new_start_slot, 1);

    log.seal().await?;
    assert_eq!(log.check_tail().await?.tail, 2);

    let data_cost = data.metrics().snapshot();
    let seal_cost = seal_drive.metrics().snapshot();
    let trim_cost = trim_drive.metrics().snapshot();
    println!(
        "S3 cost — data: {} PUT, {} GET, {} LIST, {} B up, {} B down",
        data_cost.puts,
        data_cost.gets,
        data_cost.lists,
        data_cost.uploaded_bytes,
        data_cost.downloaded_bytes
    );
    println!(
        "S3 cost — seal: {} PUT, {} GET | trim: {} PUT, {} GET, {} LIST",
        seal_cost.puts, seal_cost.gets, trim_cost.puts, trim_cost.gets, trim_cost.lists
    );

    // The same three invariants asserted on GCS and R2. Structure, not
    // coincidence.
    assert_eq!(data_cost.puts, 2, "one PUT per batch, not per record");
    assert_eq!(seal_cost.puts, 1, "the seal is written exactly once");
    assert_eq!(trim_cost.puts, 1, "one trim register write for one advance");
    assert_eq!(
        trim_cost.lists, 1,
        "the trim register lists exactly once, on cold discovery, and never per read"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires HOLYLOG_S3_BUCKET and AWS credentials; incurs real requests and charges"]
async fn scripture_v0_runs_against_s3() -> TestResult {
    let store = s3_store()?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = Path::from(format!("scripture-smoke/{unique}"));
    let result = run_scripture_history(Arc::clone(&store), &root).await;
    let cleanup = clear_prefix(&store, &root).await;
    result?;
    cleanup?;
    Ok(())
}
