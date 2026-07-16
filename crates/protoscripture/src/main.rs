//! End-to-end spike: a filtered consumer and prefix trim over a journal backed
//! by a three-replica holylog quorum, assembled purely from holylog's public
//! API. Run with `cargo run -p protoscripture`.

use std::sync::Arc;

use bytes::Bytes;
use futures::executor::block_on;
use holylog::atomic::{AtomicLog, AtomicLogError};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::quorum::{QuorumError, QuorumLogDrive};
use protoscripture::{Journal, JournalError, Record};

const K: u64 = 4;

/// Construction failures keep their categories: quorum configuration errors
/// are not runtime drive errors.
#[derive(Debug, thiserror::Error)]
enum SetupError {
    #[error(transparent)]
    Quorum(#[from] QuorumError),

    #[error(transparent)]
    Log(#[from] AtomicLogError),
}

fn orders(region: &str, ids: &[u32]) -> Vec<Record> {
    ids.iter()
        .map(|id| {
            Record::new(
                [("kind", "order"), ("region", region)],
                Bytes::from(format!("order-{id}")),
            )
        })
        .collect()
}

fn build_journal() -> Result<(Journal, Arc<QuorumLogDrive>), SetupError> {
    let replicas: Vec<Arc<dyn LogDrive>> = (0..3)
        .map(|_| Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>)
        .collect();
    let quorum = Arc::new(QuorumLogDrive::new(replicas, 2)?);
    let log = AtomicLog::builder(Arc::clone(&quorum) as Arc<dyn LogDrive>, K).build()?;
    Ok((Journal::new("orders", log), quorum))
}

async fn run(journal: &Journal, quorum: &QuorumLogDrive) -> Result<(), JournalError> {
    // Producer: three batches, mixed regions.
    let batches = [
        orders("eu", &[1, 2]),
        orders("us", &[3, 4, 5]),
        orders("eu", &[6]),
    ];
    for batch in &batches {
        let position = journal.append_batch(batch).await?;
        println!(
            "appended batch at position {position} ({} records)",
            batch.len()
        );
    }

    // Consumer: read everything below a checked tail, filtering client-side.
    let tail = journal.checked_tail().await?;
    println!("checked tail: {tail}");
    let mut eu_orders = 0_usize;
    for position in 0..tail {
        let batch = journal.read_batch(position, tail).await?;
        let matches = batch
            .records
            .iter()
            .filter(|record| record.matches("region", "eu"))
            .count();
        eu_orders += matches;
        println!(
            "position {}: {} records, {} matching region=eu",
            batch.position,
            batch.records.len(),
            matches
        );
    }
    println!("filtered consumer saw {eu_orders} eu orders");

    // Retention: trim the first batch, then show the deterministic Trimmed
    // error a lagging consumer observes and how it skips forward.
    let effective = journal.trim_to(1).await?;
    println!("trimmed below position {effective}");
    match journal.read_batch(0, tail).await {
        Err(JournalError::Log(AtomicLogError::Trimmed {
            requested,
            trim_point,
        })) => {
            println!("lagging consumer: position {requested} trimmed; skipping to {trim_point}");
            let recovered = journal.read_batch(trim_point, tail).await?;
            println!(
                "resumed at position {} with {} records",
                recovered.position,
                recovered.records.len()
            );
        }
        other => println!("unexpected read outcome after trim: {other:?}"),
    }

    // Seal, then prove the read path still serves recovery.
    journal.seal().await?;
    let sealed_tail = journal.checked_tail().await?;
    let last = journal.read_batch(sealed_tail - 1, sealed_tail).await?;
    println!(
        "post-seal read at position {} returned {} records",
        last.position,
        last.records.len()
    );

    // First cost datapoints: logical kernel work for the run above.
    let metrics = quorum.metrics().snapshot();
    println!(
        "quorum work: {} replica writes ({} for repair), {} replica reads, \
         {} tail queries, {} bytes up, {} bytes down",
        metrics.replica_writes,
        metrics.repair_writes,
        metrics.replica_reads,
        metrics.replica_tail_queries,
        metrics.uploaded_bytes,
        metrics.downloaded_bytes
    );
    Ok(())
}

fn main() {
    let (journal, quorum) = match build_journal() {
        Ok(built) => built,
        Err(error) => {
            eprintln!("protoscripture setup failed: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = block_on(run(&journal, &quorum)) {
        eprintln!("protoscripture spike failed: {error}");
        std::process::exit(1);
    }
}
