//! CLI entrypoint for the Scripture fleet-lab raw-lines load producer.

use std::env;
use std::error::Error;
use std::process;
use std::time::Duration;

use scripture_load::{LoadConfig, run_load};

#[tokio::main]
async fn main() {
    if let Err(error) = try_main().await {
        eprintln!("scripture-load: {error}");
        process::exit(1);
    }
}

async fn try_main() -> Result<(), Box<dyn Error>> {
    let config = parse_args(env::args().skip(1))?;
    let report = run_load(config).await?;
    println!("{}", report.summary_line());
    if report.errors > 0 || report.transport_failures > 0 {
        process::exit(2);
    }
    if report.accepted_records == 0 {
        return Err("no records accepted".into());
    }
    Ok(())
}

fn parse_args(arguments: impl Iterator<Item = String>) -> Result<LoadConfig, Box<dyn Error>> {
    let mut config = LoadConfig::default();
    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--endpoint" => {
                config.endpoint = required(&mut arguments, "--endpoint")?;
            }
            "--connections" => {
                config.connections = required(&mut arguments, "--connections")?.parse()?;
            }
            "--record-bytes" => {
                config.record_bytes = required(&mut arguments, "--record-bytes")?.parse()?;
            }
            "--duration-secs" => {
                let secs: u64 = required(&mut arguments, "--duration-secs")?.parse()?;
                config.duration = Duration::from_secs(secs);
            }
            "--max-bytes" => {
                config.max_bytes = required(&mut arguments, "--max-bytes")?.parse()?;
            }
            "--rate" => {
                let rate: u64 = required(&mut arguments, "--rate")?.parse()?;
                config.target_records_per_sec = Some(rate);
            }
            "--run-id" => {
                config.run_id = required(&mut arguments, "--run-id")?;
            }
            "--ack-timeout-ms" => {
                let ms: u64 = required(&mut arguments, "--ack-timeout-ms")?.parse()?;
                config.ack_timeout = Duration::from_millis(ms);
            }
            "--backend" => {
                config.backend = required(&mut arguments, "--backend")?;
            }
            "--chunk-policy-name" => {
                config.chunk_policy.name = required(&mut arguments, "--chunk-policy-name")?;
            }
            "--inflight-per-connection" => {
                config.inflight_per_connection =
                    required(&mut arguments, "--inflight-per-connection")?.parse()?;
            }
            "--help" | "-h" => {
                print_help();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok(config)
}

fn required(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, Box<dyn Error>> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a value").into())
}

fn print_help() {
    println!(
        "usage: scripture-load [options]\n\
         \n\
         --endpoint HOST:PORT          (default 127.0.0.1:9000)\n\
         --connections N               (default 4)\n\
         --record-bytes N              (default 256)\n\
         --duration-secs N             (default 5)\n\
         --max-bytes N                 (default 8388608)\n\
         --rate N                      optional records/sec across connections\n\
         --run-id ID                   deterministic run id (required for drills)\n\
         --ack-timeout-ms N            (default 5000)\n\
         --backend LABEL               reported backend name\n\
         --chunk-policy-name NAME      reported server policy label\n\
         --inflight-per-connection N   pipelined writes before ACK (default 1)\n"
    );
}
