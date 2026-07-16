#![cfg(feature = "r2-smoke")]

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
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use scripture::{
    AttributeValue, JournalId, JournalReader, JournalWriter, ReadEvent, Record, RecordOffset,
    RetentionAuthority,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn required_env(name: &str) -> TestResult<String> {
    std::env::var(name).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("required environment variable {name} is not set"),
        )
        .into()
    })
}

fn r2_store() -> TestResult<Arc<dyn ObjectStore>> {
    let endpoint = required_env("R2_ENDPOINT")?;
    let endpoint = if endpoint.contains("://") {
        endpoint
    } else {
        format!("https://{endpoint}")
    };
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_region(std::env::var("R2_REGION").unwrap_or_else(|_| "auto".into()))
        .with_bucket_name(required_env("R2_BUCKET")?)
        .with_access_key_id(required_env("R2_ACCESS_KEY_ID")?)
        .with_secret_access_key(required_env("R2_SECRET_ACCESS_KEY")?)
        .with_virtual_hosted_style_request(false)
        .build()?;
    Ok(Arc::new(store))
}

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
        Bytes::from(format!("r2-record-{value}")),
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
    let log = AtomicLog::builder(data.clone() as Arc<dyn LogDrive>, 4)
        .seal(seal)
        .trim(trim)
        .build()?;
    let journal_id = JournalId::from_bytes(*b"scripture-r2-v0!");

    let mut writer = JournalWriter::new(journal_id, log.clone(), RecordOffset::new(0));
    writer.append_batch(vec![record(0), record(1)]).await?;
    writer.append_batch(vec![record(2)]).await?;
    let recovered = JournalWriter::recover(journal_id, log.clone()).await?;
    assert_eq!(recovered.next_offset(), RecordOffset::new(3));

    let mut reader = JournalReader::from_start(journal_id, log.clone());
    assert_eq!(reader.refresh_tail().await?, 2);
    for expected in 0_u64..3 {
        let ReadEvent::Record(record) = reader.read_next().await? else {
            return Err(std::io::Error::other("expected R2-backed record").into());
        };
        assert_eq!(record.offset, RecordOffset::new(expected));
    }

    RetentionAuthority::new(log.clone()).trim_to_slot(1).await?;
    let mut lagging = JournalReader::from_start(journal_id, log.clone());
    lagging.refresh_tail().await?;
    let ReadEvent::Gap(gap) = lagging.read_next().await? else {
        return Err(std::io::Error::other("expected R2-backed trim gap").into());
    };
    assert_eq!(gap.new_start_slot, 1);

    log.seal().await?;
    assert_eq!(log.check_tail().await?.tail, 2);

    // Measured provider requests, per namespace. Reported in the same shape as
    // the GCS run so the two are comparable: a cost *model* needs at least two
    // providers, or it is only a trace of one.
    let data_cost = data.metrics().snapshot();
    let seal_cost = seal_drive.metrics().snapshot();
    let trim_cost = trim_drive.metrics().snapshot();
    println!(
        "R2 cost — data: {} PUT, {} GET, {} LIST, {} B up, {} B down",
        data_cost.puts,
        data_cost.gets,
        data_cost.lists,
        data_cost.uploaded_bytes,
        data_cost.downloaded_bytes
    );
    println!(
        "R2 cost — seal: {} PUT, {} GET | trim: {} PUT, {} GET, {} LIST",
        seal_cost.puts, seal_cost.gets, trim_cost.puts, trim_cost.gets, trim_cost.lists
    );

    assert_eq!(data_cost.puts, 2, "one PUT per batch, not per record");
    assert_eq!(seal_cost.puts, 1, "the seal is written exactly once");
    assert_eq!(trim_cost.puts, 1, "one trim register write for one advance");

    // The invariant that must hold on every provider: the trim register lists
    // once per instance, on cold discovery, and never once per log read.
    assert_eq!(
        trim_cost.lists, 1,
        "the trim register lists exactly once, on cold discovery, and never per read"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires R2_* credentials and incurs requests against the configured bucket"]
async fn scripture_v0_runs_against_r2() -> TestResult {
    let store = r2_store()?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = Path::from(format!("scripture-smoke/{unique}"));
    let result = run_scripture_history(Arc::clone(&store), &root).await;
    let cleanup = clear_prefix(&store, &root).await;
    result?;
    cleanup?;
    Ok(())
}
