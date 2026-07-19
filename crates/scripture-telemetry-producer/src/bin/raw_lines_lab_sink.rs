//! In-cluster / local lab raw-lines sink (not a product Scribe).
//!
//! Accepts newline-delimited JSON, assigns monotonic offsets, replies
//! `OK first next`, and appends committed rows to a JSONL ledger for Phase 2
//! evidence. Optional `--deny-once` returns `ERR not-owner` on the first line
//! then accepts subsequent lines (Denied reconnect drill).

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use scripture_telemetry_producer::{MetricEnvelope, SinkCommitRow};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

fn usage() -> ! {
    eprintln!("usage: raw-lines-lab-sink --listen <host:port> --ledger <path.jsonl> [--deny-once]");
    std::process::exit(2);
}

fn main() -> ExitCode {
    let mut listen = String::from("0.0.0.0:9101");
    let mut ledger_path = PathBuf::from("/var/lib/scripture-telemetry/sink-ledger.jsonl");
    let mut deny_once = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().unwrap_or_else(|| usage()),
            "--ledger" => {
                ledger_path = PathBuf::from(args.next().unwrap_or_else(|| usage()));
            }
            "--deny-once" => deny_once = true,
            "--help" | "-h" => usage(),
            other => {
                eprintln!("raw-lines-lab-sink: unknown arg {other}");
                usage();
            }
        }
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("raw-lines-lab-sink: runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(async move {
        if let Err(error) = run(listen, ledger_path, deny_once).await {
            eprintln!("raw-lines-lab-sink: {error}");
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        }
    })
}

async fn run(listen: String, ledger_path: PathBuf, deny_once: bool) -> Result<(), std::io::Error> {
    if let Some(parent) = ledger_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let ledger = Arc::new(Mutex::new(
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&ledger_path)
            .await?,
    ));
    let next_offset = Arc::new(AtomicU64::new(0));
    let deny_pending = Arc::new(AtomicBool::new(deny_once));
    let listener = TcpListener::bind(&listen).await?;
    eprintln!(
        "raw-lines-lab-sink: listening on {listen} ledger={}",
        ledger_path.display()
    );

    loop {
        let (stream, peer) = listener.accept().await?;
        let ledger = Arc::clone(&ledger);
        let next_offset = Arc::clone(&next_offset);
        let deny_pending = Arc::clone(&deny_pending);
        tokio::spawn(async move {
            if let Err(error) =
                handle_conn(stream, peer.to_string(), ledger, next_offset, deny_pending).await
            {
                eprintln!("raw-lines-lab-sink: peer={peer} err={error}");
            }
        });
    }
}

async fn handle_conn(
    stream: tokio::net::TcpStream,
    peer: String,
    ledger: Arc<Mutex<tokio::fs::File>>,
    next_offset: Arc<AtomicU64>,
    deny_pending: Arc<AtomicBool>,
) -> Result<(), std::io::Error> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let payload = line.trim_end_matches(['\r', '\n']).to_owned();
        if payload.is_empty() {
            continue;
        }
        if deny_pending.swap(false, Ordering::SeqCst) {
            writer.write_all(b"ERR not-owner\n").await?;
            writer.flush().await?;
            eprintln!("raw-lines-lab-sink: peer={peer} denied once");
            continue;
        }
        let digest = MetricEnvelope::payload_digest(&payload);
        let first = next_offset.fetch_add(1, Ordering::SeqCst);
        let next = first + 1;
        let row = SinkCommitRow {
            offset: first,
            payload_digest: digest,
            line: payload,
        };
        {
            let mut file = ledger.lock().await;
            let mut encoded = serde_json::to_string(&row).map_err(std::io::Error::other)?;
            encoded.push('\n');
            file.write_all(encoded.as_bytes()).await?;
            file.flush().await?;
        }
        let ack = format!("OK {first} {next}\n");
        writer.write_all(ack.as_bytes()).await?;
        writer.flush().await?;
    }
    Ok(())
}
