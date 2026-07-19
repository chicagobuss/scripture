//! `scripture-telemetry-producer` — scrape → normalize → raw-lines (Phase 2).

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use scripture_telemetry_producer::{ProducerConfig, run_producer};

fn usage() -> ! {
    eprintln!(
        "usage: scripture-telemetry-producer --config <path.yaml> [--ledger <path.jsonl>] [--ack-timeout <Ns|Nms>] [--max-iterations <n>]"
    );
    std::process::exit(2);
}

fn main() -> ExitCode {
    let mut config_path: Option<PathBuf> = None;
    let mut ledger_path = PathBuf::from("/var/lib/scripture-telemetry/send-ledger.jsonl");
    let mut ack_timeout = Duration::from_secs(5);
    let mut max_iterations: Option<u64> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(args.next().unwrap_or_else(|| usage())));
            }
            "--ledger" => {
                ledger_path = PathBuf::from(args.next().unwrap_or_else(|| usage()));
            }
            "--ack-timeout" => {
                let raw = args.next().unwrap_or_else(|| usage());
                ack_timeout = parse_duration(&raw).unwrap_or_else(|error| {
                    eprintln!("scripture-telemetry-producer: {error}");
                    usage();
                });
            }
            "--max-iterations" => {
                let raw = args.next().unwrap_or_else(|| usage());
                max_iterations = Some(raw.parse().unwrap_or_else(|_| usage()));
            }
            "--help" | "-h" => usage(),
            other => {
                eprintln!("scripture-telemetry-producer: unknown arg {other}");
                usage();
            }
        }
    }

    let Some(config_path) = config_path else {
        eprintln!("scripture-telemetry-producer: --config is required");
        usage();
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("scripture-telemetry-producer: runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(async move {
        let config = match ProducerConfig::load_yaml_path(&config_path) {
            Ok(config) => config,
            Err(error) => {
                eprintln!("scripture-telemetry-producer: config: {error}");
                return ExitCode::FAILURE;
            }
        };
        match run_producer(config, &ledger_path, max_iterations, ack_timeout).await {
            Ok(counters) => {
                eprintln!(
                    "scripture-telemetry-producer: done committed={} unacked_attempts={} scrapes_ok={:?} scrape_errors={:?} dropped_records={:?} dropped_series={:?}",
                    counters.committed,
                    counters.unacked_attempts,
                    counters.scrapes_ok,
                    counters.scrape_errors,
                    counters.dropped_records,
                    counters.dropped_series
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("scripture-telemetry-producer: {error}");
                ExitCode::FAILURE
            }
        }
    })
}

fn parse_duration(raw: &str) -> Result<Duration, String> {
    if let Some(millis) = raw.strip_suffix("ms") {
        let n: u64 = millis
            .parse()
            .map_err(|_| format!("invalid duration {raw}"))?;
        return Ok(Duration::from_millis(n));
    }
    if let Some(seconds) = raw.strip_suffix('s') {
        let n: u64 = seconds
            .parse()
            .map_err(|_| format!("invalid duration {raw}"))?;
        return Ok(Duration::from_secs(n));
    }
    Err(format!("expected Ns or Nms, got {raw}"))
}
