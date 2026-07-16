#![cfg(feature = "gcs-smoke")]

//! Scripture's v0 data plane against real Google Cloud Storage.
//!
//! This is the first end-to-end run of the journal contract on a hosted object
//! store with attested conditional-create semantics, and the first place the
//! family's cost model (obligation 9) gets *measured* numbers rather than
//! in-memory counters. The assertions below therefore pin request counts, not
//! just behaviour: a change that silently doubles the PUTs per batch is a cost
//! regression, and cost regressions are correctness regressions here.

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
use object_store::gcp::GoogleCloudStorageBuilder;
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

fn gcs_store() -> TestResult<Arc<dyn ObjectStore>> {
    let store = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(required_env("HOLYLOG_GCS_BUCKET")?)
        .build()?;
    Ok(Arc::new(store))
}

/// Google Cloud Storage documents strongly consistent reads, lexicographically
/// ordered listing, and `ifGenerationMatch` preconditions, which `object_store`
/// maps to create-if-absent. This declaration is a claim; the run is evidence.
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
        Bytes::from(format!("gcs-record-{value}")),
    )
}

async fn run_scripture_history(store: Arc<dyn ObjectStore>, root: &Path) -> TestResult {
    // Data, seal, and trim each get an exclusive namespace, as the kernel's
    // caller contracts require: a seal marker inside the data prefix would
    // appear as a log entry in tail scans.
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
    let journal_id = JournalId::from_bytes(*b"scripture-gcs-v0");

    // Write: two batches, five records. Batching is the whole cost argument —
    // five records cost two PUTs, not five.
    let mut writer = JournalWriter::new(journal_id, log.clone(), RecordOffset::new(0));
    let first = writer
        .append_batch(vec![record(0), record(1), record(2)])
        .await?;
    let second = writer.append_batch(vec![record(3), record(4)]).await?;
    assert_eq!(first.slot, 0);
    assert_eq!(second.slot, 1);
    assert_eq!(writer.next_offset(), RecordOffset::new(5));

    // Recover the writer from durable bytes alone, as a restarted process must.
    let recovered = JournalWriter::recover(journal_id, log.clone()).await?;
    assert_eq!(recovered.next_offset(), RecordOffset::new(5));

    // Read back the dense record offsets across both batches.
    let mut reader = JournalReader::from_start(journal_id, log.clone());
    assert_eq!(reader.refresh_tail().await?, 2);
    for expected in 0_u64..5 {
        let ReadEvent::Record(entry) = reader.read_next().await? else {
            return Err(std::io::Error::other("expected a GCS-backed record").into());
        };
        assert_eq!(entry.offset, RecordOffset::new(expected));
        assert_eq!(entry.payload, Bytes::from(format!("gcs-record-{expected}")));
    }
    assert!(matches!(
        reader.read_next().await?,
        ReadEvent::CaughtUp { .. }
    ));

    // Retention: a logical trim, and a lagging consumer that is told about the
    // gap rather than silently skipped past it.
    RetentionAuthority::new(log.clone()).trim_to_slot(1).await?;
    let mut lagging = JournalReader::from_start(journal_id, log.clone());
    lagging.refresh_tail().await?;
    let ReadEvent::Gap(gap) = lagging.read_next().await? else {
        return Err(std::io::Error::other("expected a GCS-backed trim gap").into());
    };
    assert_eq!(gap.new_start_slot, 1);
    let ReadEvent::Record(after_gap) = lagging.read_next().await? else {
        return Err(std::io::Error::other("expected a record after the gap").into());
    };
    assert_eq!(after_gap.offset, RecordOffset::new(3));

    // Sealing keeps the read path serving, which VirtualLog recovery depends on.
    log.seal().await?;
    assert_eq!(log.check_tail().await?.tail, 2);

    // Measured cost, per prefix. These are provider requests, not logical
    // counters, and they are what obligation 9's formulas must be fitted to.
    let data_cost = data.metrics().snapshot();
    let seal_cost = seal_drive.metrics().snapshot();
    let trim_cost = trim_drive.metrics().snapshot();
    println!(
        "GCS cost — data: {} PUT, {} GET, {} LIST, {} B up, {} B down",
        data_cost.puts,
        data_cost.gets,
        data_cost.lists,
        data_cost.uploaded_bytes,
        data_cost.downloaded_bytes
    );
    println!(
        "GCS cost — seal: {} PUT, {} GET | trim: {} PUT, {} GET, {} LIST",
        seal_cost.puts, seal_cost.gets, trim_cost.puts, trim_cost.gets, trim_cost.lists
    );

    // Five records cost two data PUTs: batching divides request cost by the
    // batch size, which is the entire economic thesis of the journal layer.
    assert_eq!(data_cost.puts, 2, "one PUT per batch, not per record");
    assert_eq!(
        seal_cost.puts, 1,
        "the seal is written exactly once, on seal()"
    );
    assert_eq!(trim_cost.puts, 1, "one trim register write for one advance");

    // The trim point is consulted on every log read, so its listing count is
    // the number that matters: a listing is the most expensive request class an
    // object store bills, and a listing *per read* would be the single worst
    // cost defect in the system.
    //
    // Exactly one is correct, and one is not zero. A fresh instance must find
    // the register head, and one tail scan does that in a single request no
    // matter how long the trim history is; walking forward from zero instead
    // would cost a read per advance and grow without bound as the log ages.
    // Every read after that is warm and probes forward with point reads. So the
    // invariant is: listings are paid once per instance, never once per read.
    assert_eq!(
        trim_cost.lists, 1,
        "the trim register lists exactly once, on cold discovery, and never per read"
    );

    // The metadata registers cost more requests than the data does. That is a
    // real property of this design at small batch sizes, not a defect — but it
    // is the number the cost model must be fitted to, so pin it: if seal or
    // trim traffic grows relative to data, a caching policy is overdue.
    assert!(
        seal_cost.gets + trim_cost.gets > data_cost.puts + data_cost.gets,
        "expected metadata reads to dominate; if this flips, revisit the cost model"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires HOLYLOG_GCS_BUCKET and Google Cloud credentials; incurs real requests"]
async fn scripture_v0_runs_against_gcs() -> TestResult {
    let store = gcs_store()?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = Path::from(format!("scripture-smoke/{unique}"));
    let result = run_scripture_history(Arc::clone(&store), &root).await;
    let cleanup = clear_prefix(&store, &root).await;
    result?;
    cleanup?;
    Ok(())
}
