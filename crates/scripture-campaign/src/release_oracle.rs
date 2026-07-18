//! Holylog durable-oracle helper for the WP09 release-drill runner.
//!
//! Credentials must arrive via environment variables (never argv).

use std::path::PathBuf;
use std::process;
use std::time::Duration;

use scripture::OwnerId;

use crate::cutover_oracle::{self, ExpectedAuthority};

/// CLI entry for `scripture-campaign release-oracle`.
pub async fn main(arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>) {
    match run(arguments).await {
        Ok(()) => {}
        Err(error) => {
            eprintln!("release-oracle: {error}");
            process::exit(1);
        }
    }
}

async fn run(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<(), String> {
    let mut endpoint = None;
    let mut bucket = None;
    let mut prefix = None;
    let mut payloads_file = None;
    let mut owner = None;
    let mut term = None;
    let mut require_cutover = true;
    let mut out = None;
    let mut timeout_secs = 120u64;

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--endpoint" => endpoint = Some(required(arguments, "--endpoint")?),
            "--bucket" => bucket = Some(required(arguments, "--bucket")?),
            "--prefix" => prefix = Some(required(arguments, "--prefix")?),
            "--payloads-file" => {
                payloads_file = Some(PathBuf::from(required(arguments, "--payloads-file")?));
            }
            "--owner" => owner = Some(required(arguments, "--owner")?),
            "--term" => {
                term = Some(
                    required(arguments, "--term")?
                        .parse::<u64>()
                        .map_err(|error| format!("--term: {error}"))?,
                );
            }
            "--baseline" => {
                require_cutover = false;
            }
            "--out" => out = Some(PathBuf::from(required(arguments, "--out")?)),
            "--timeout-secs" => {
                timeout_secs = required(arguments, "--timeout-secs")?
                    .parse()
                    .map_err(|error| format!("--timeout-secs: {error}"))?;
            }
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let endpoint = endpoint.ok_or("missing --endpoint")?;
    let bucket = bucket.ok_or("missing --bucket")?;
    let prefix = prefix.ok_or("missing --prefix")?;
    let payloads_file = payloads_file.ok_or("missing --payloads-file")?;
    let owner_raw = owner.ok_or("missing --owner (16-byte owner id string)")?;
    let term = term.ok_or("missing --term")?;
    let owner_bytes = owner_id_bytes(&owner_raw)?;

    let access_key = std::env::var("RUSTFS_ACCESS_KEY")
        .map_err(|_| "RUSTFS_ACCESS_KEY must be set in the environment".to_owned())?;
    let secret_key = std::env::var("RUSTFS_SECRET_KEY")
        .map_err(|_| "RUSTFS_SECRET_KEY must be set in the environment".to_owned())?;
    if access_key.is_empty() || secret_key.is_empty() {
        return Err("RustFS credentials must be non-empty".into());
    }

    let payloads_blob = std::fs::read_to_string(&payloads_file)
        .map_err(|error| format!("read payloads: {error}"))?;
    let payloads: Vec<&str> = payloads_blob
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    if payloads.is_empty() {
        return Err("payloads file is empty".into());
    }

    let report = cutover_oracle::wait_for_durable_payloads(
        &endpoint,
        &bucket,
        &access_key,
        &secret_key,
        &prefix,
        &payloads,
        ExpectedAuthority {
            owner: OwnerId::from_bytes(owner_bytes),
            term,
        },
        require_cutover,
        Duration::from_secs(timeout_secs),
    )
    .await
    .map_err(|error| error.to_string())?;

    let encoded = serde_json::to_vec_pretty(&report.observation)
        .map_err(|error| format!("encode observation: {error}"))?;
    if let Some(path) = out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| format!("create out dir: {error}"))?;
        }
        std::fs::write(&path, &encoded).map_err(|error| format!("write out: {error}"))?;
        eprintln!("release-oracle: wrote {}", path.display());
    } else {
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        use std::io::Write;
        handle
            .write_all(&encoded)
            .map_err(|error| format!("write stdout: {error}"))?;
        handle
            .write_all(b"\n")
            .map_err(|error| format!("write stdout: {error}"))?;
    }
    Ok(())
}

fn owner_id_bytes(raw: &str) -> Result<[u8; 16], String> {
    let bytes = raw.as_bytes();
    if bytes.len() != 16 {
        return Err(format!(
            "owner must be exactly 16 bytes (got {} for {raw:?})",
            bytes.len()
        ));
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(bytes);
    Ok(out)
}

fn required(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn print_help() {
    eprintln!(
        "\
scripture-campaign release-oracle

Holylog durable readback for the WP09 release drill. Credentials via env only:
  RUSTFS_ACCESS_KEY  RUSTFS_SECRET_KEY

Required:
  --endpoint URL
  --bucket NAME
  --prefix PATH
  --payloads-file PATH   (one payload per line)
  --owner 16-byte-id
  --term N

Optional:
  --baseline          (single-generation; default is cutover)
  --out PATH
  --timeout-secs N    (default 120)"
    );
}
