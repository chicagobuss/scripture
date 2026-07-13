//! Runnable lab daemon for the raw-lines Scripture transport.
//!
//! This program intentionally uses Holylog's in-memory drive. It demonstrates
//! live ingestion and durable acknowledgements within one process, but neither
//! survives restart nor claims production recovery semantics.

use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use holylog::atomic::AtomicLog;
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use scripture::{
    ChunkDriverActor, ChunkLogWriter, ChunkPolicy, CohortId, JournalId, RecordOffset,
    RecoveryBound, SystemClock, WriterId,
};
use scripture_service::ChunkJournalService;
use scriptured::{RawLinesConfig, serve_chunk_raw_lines_connection};
use tokio::net::TcpListener;

const DEFAULT_BIND: &str = "0.0.0.0:9000";
const LAB_JOURNAL_ID: JournalId = JournalId::from_bytes(*b"scriptured-lab!!");
const LAB_COHORT_ID: CohortId = CohortId::from_bytes(*b"scriptured-cohrt");
const LAB_WRITER_ID: WriterId = WriterId::from_bytes(*b"scriptured-writr");

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = parse_bind(env::args().skip(1))?;
    let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
    let log = AtomicLog::builder(drive, 0).build()?;
    let writer = ChunkLogWriter::new(LAB_JOURNAL_ID, LAB_COHORT_ID, 1, log, RecordOffset::new(0));
    let (clock, timer) = SystemClock::pair();
    let policy = ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 8,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 64 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(16).ok_or("invalid recovery bound")?,
    };
    let (handle, actor) = ChunkDriverActor::new(
        LAB_JOURNAL_ID,
        LAB_COHORT_ID,
        LAB_WRITER_ID,
        1,
        writer,
        &[],
        policy,
        clock,
        timer,
        1_024,
    )?;
    let mut service = ChunkJournalService::new();
    service.register_owner(LAB_JOURNAL_ID, 1, handle, actor)?;
    let service = Arc::new(service);

    let listener = TcpListener::bind(&bind).await?;
    eprintln!(
        "scriptured raw-lines lab listening on {}; journal={LAB_JOURNAL_ID}; \
         chunk-driver owner; in-memory only",
        listener.local_addr()?
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            if let Err(error) = serve_chunk_raw_lines_connection(
                stream,
                service,
                LAB_JOURNAL_ID,
                RawLinesConfig::default(),
            )
            .await
            {
                eprintln!("raw-lines connection {peer} failed: {error}");
            }
        });
    }
}

fn parse_bind(arguments: impl Iterator<Item = String>) -> Result<String, Box<dyn Error>> {
    let mut bind = DEFAULT_BIND.to_owned();
    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--bind" => {
                bind = arguments.next().ok_or("--bind requires an address")?;
            }
            "--help" | "-h" => {
                println!("usage: scriptured [--bind HOST:PORT]");
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {argument}").into()),
        }
    }
    Ok(bind)
}
