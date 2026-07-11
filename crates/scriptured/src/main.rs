//! Runnable lab daemon for the raw-lines Scripture transport.
//!
//! This program intentionally uses Holylog's in-memory drive. It demonstrates
//! live ingestion and durable acknowledgements within one process, but neither
//! survives restart nor claims production recovery semantics.

use std::env;
use std::error::Error;
use std::sync::Arc;

use holylog::atomic::AtomicLog;
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use scripture::{JournalId, JournalWriter, RecordOffset};
use scripture_service::JournalActor;
use scriptured::{RawLinesConfig, serve_raw_lines_connection};
use tokio::net::TcpListener;

const DEFAULT_BIND: &str = "0.0.0.0:9000";
const LAB_JOURNAL_ID: JournalId = JournalId::from_bytes(*b"scriptured-lab!!");

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = parse_bind(env::args().skip(1))?;
    let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
    let log = AtomicLog::builder(drive, 4).build()?;
    let writer = JournalWriter::new(LAB_JOURNAL_ID, log, RecordOffset::new(0));
    let (journal, actor) = JournalActor::new(writer, 1_024);
    tokio::spawn(actor.run());

    let listener = TcpListener::bind(&bind).await?;
    eprintln!(
        "scriptured raw-lines lab listening on {}; journal={LAB_JOURNAL_ID}; \
         in-memory only",
        listener.local_addr()?
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let journal = journal.clone();
        tokio::spawn(async move {
            if let Err(error) =
                serve_raw_lines_connection(stream, journal, RawLinesConfig::default()).await
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
