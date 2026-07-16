//! Experimental Scripture correctness-campaign entrypoint.
//!
//! Runs a named scenario through the product recovery path, writes a redacted
//! evidence bundle (NDJSON trace + final observations + checker verdict), and
//! exits with a status that distinguishes Pass / Fail / Inconclusive from
//! execution failure. Never accepts or prints secret values.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;

use scripture_campaign::{CampaignBackend, CampaignReport, Scenario, run_campaign};
use scripture_runtime::{BackendProfile, connect_s3_compat, resolve_credentials};

struct CampaignArgs {
    run_id: String,
    scenario: Scenario,
    backend: BackendKind,
    artifact_dir: PathBuf,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: String,
    prefix: Option<String>,
}

enum BackendKind {
    Memory,
    RustFs,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut arguments = std::env::args().skip(1).peekable();
    if let Err(error) = run(&mut arguments).await {
        eprintln!("scripture-campaign: {error}");
        let mut source = error.source();
        while let Some(cause) = source {
            eprintln!("scripture-campaign: caused by: {cause}");
            source = cause.source();
        }
        process::exit(1);
    }
}

pub async fn run(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<(), Box<dyn Error>> {
    let args = parse_args(arguments)?;
    let backend = build_backend(&args)?;
    let report = run_campaign(&args.run_id, args.scenario, backend).await?;
    write_artifacts(&args.artifact_dir, &report)?;
    eprintln!(
        "scripture-campaign: complete run_id={} scenario={} backend={} verdict={}",
        report.run_id,
        report.scenario,
        report.backend,
        report.verdict_label(),
    );
    process::exit(report.exit_code());
}

fn build_backend(args: &CampaignArgs) -> Result<CampaignBackend, Box<dyn Error>> {
    match args.backend {
        BackendKind::Memory => Ok(CampaignBackend::InMemory),
        BackendKind::RustFs => {
            let endpoint = args
                .endpoint
                .as_deref()
                .ok_or("--endpoint is required for --backend rustfs")?;
            let bucket = args
                .bucket
                .as_deref()
                .ok_or("--bucket is required for --backend rustfs")?;
            let prefix = args
                .prefix
                .as_deref()
                .ok_or("--prefix is required for --backend rustfs")?;
            if prefix.trim().is_empty() || prefix.contains("..") {
                return Err("--prefix must be a non-empty path without '..'".into());
            }
            if !prefix.contains(&args.run_id) {
                return Err(
                    "--prefix must include the run_id so each campaign owns an exclusive root"
                        .into(),
                );
            }
            let profile = BackendProfile::RustFs;
            let credentials = resolve_credentials(profile)?;
            let store = connect_s3_compat(
                endpoint,
                bucket,
                &args.region,
                &credentials.access_key,
                &credentials.secret_key,
            )?;
            drop(credentials);
            Ok(CampaignBackend::rustfs(store, prefix))
        }
    }
}

fn write_artifacts(dir: &Path, report: &CampaignReport) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(dir.join("traces"))?;
    fs::create_dir_all(dir.join("observations"))?;

    fs::write(dir.join("traces/campaign.ndjson"), report.trace_ndjson()?)?;
    fs::write(
        dir.join("observations/final-root.json"),
        serde_json::to_vec_pretty(&report.final_root)?,
    )?;
    fs::write(
        dir.join("observations/final-authority.json"),
        serde_json::to_vec_pretty(&report.final_authority)?,
    )?;
    fs::write(
        dir.join("environment.json"),
        serde_json::to_vec_pretty(&report.environment)?,
    )?;
    fs::write(
        dir.join("checker-verdict.json"),
        serde_json::to_vec_pretty(&report.verdict_json()?)?,
    )?;
    fs::write(dir.join("report.md"), render_report(report))?;
    Ok(())
}

fn render_report(report: &CampaignReport) -> String {
    format!(
        "# Correctness campaign report\n\n\
         - run_id: `{}`\n\
         - scenario: `{}`\n\
         - backend: `{}`\n\
         - events: {}\n\
         - verdict: **{}**\n\n\
         See `traces/campaign.ndjson`, `observations/`, `environment.json`, and \
         `checker-verdict.json` for the redacted evidence bundle.\n\n\
         Non-claims: single-process A/B roles inside this Job are not a multi-node \
         process-separation proof; this run makes no object-store replica, \
         durability, or multi-site availability claim.\n",
        report.run_id,
        report.scenario,
        report.backend,
        report.events.len(),
        report.verdict_label(),
    )
}

fn parse_args(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<CampaignArgs, Box<dyn Error>> {
    let mut run_id = None;
    let mut scenario = None;
    let mut backend = None;
    let mut artifact_dir = None;
    let mut endpoint = None;
    let mut bucket = None;
    let mut region = "auto".to_owned();
    let mut prefix = None;

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--run-id" => run_id = Some(required(arguments, "--run-id")?),
            "--scenario" => {
                let raw = required(arguments, "--scenario")?;
                scenario = Some(Scenario::parse(&raw)?);
            }
            "--backend" => {
                let raw = required(arguments, "--backend")?;
                backend = Some(match raw.as_str() {
                    "memory" => BackendKind::Memory,
                    "rustfs" => BackendKind::RustFs,
                    other => {
                        return Err(
                            format!("unknown backend {other:?} (expected memory|rustfs)").into(),
                        );
                    }
                });
            }
            "--artifact-dir" => {
                artifact_dir = Some(PathBuf::from(required(arguments, "--artifact-dir")?));
            }
            "--endpoint" => endpoint = Some(required(arguments, "--endpoint")?),
            "--bucket" => bucket = Some(required(arguments, "--bucket")?),
            "--region" => region = required(arguments, "--region")?,
            "--prefix" => prefix = Some(required(arguments, "--prefix")?),
            "--access-key" | "--secret-key" | "--config" => {
                return Err(
                    "campaign does not accept secrets or a full serve config on argv; \
                     use --endpoint/--bucket/--prefix and process environment credentials"
                        .into(),
                );
            }
            "--help" | "-h" => {
                print_campaign_help();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    let run_id = run_id.ok_or("campaign requires --run-id ID")?;
    if run_id.trim().is_empty()
        || run_id.contains('/')
        || run_id.contains("..")
        || run_id.chars().any(char::is_whitespace)
    {
        return Err("--run-id must be a non-empty token without whitespace, '/', or '..'".into());
    }

    Ok(CampaignArgs {
        run_id,
        scenario: scenario.ok_or("campaign requires --scenario NAME")?,
        backend: backend.ok_or("campaign requires --backend memory|rustfs")?,
        artifact_dir: artifact_dir.ok_or("campaign requires --artifact-dir PATH")?,
        endpoint,
        bucket,
        region,
        prefix,
    })
}

fn required(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, Box<dyn Error>> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a value").into())
}

pub fn print_campaign_help() {
    eprintln!(
        "\
scripture-campaign — experimental correctness scenario driver

Usage:
  scripture-campaign \\
    --run-id <ID> \\
    --scenario <NAME> \\
    --backend memory|rustfs \\
    --artifact-dir <PATH> \\
    [--endpoint URL --bucket NAME --region REGION --prefix PATH]

Scenarios:
  baseline-committed-ack
  root-cas-reply-lost
  writer-dies-after-payload

Credentials (rustfs only) come from the process environment:
  RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
  (or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)

Exit codes:
  0  checker Pass
  1  execution failure
  2  checker Fail
  3  checker Inconclusive

A Job completing is not itself a correctness verdict — read checker-verdict.json."
    );
}
