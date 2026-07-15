//! Scripture process command.
//!
//! Product surface:
//! - `scripture validate --config PATH`
//! - `scripture bootstrap --config PATH --loglet-id ID`
//! - `scripture serve --config PATH`

#![allow(unreachable_pub)]

mod assemble;
mod bootstrap;
mod config;
mod serve;

use std::error::Error;
use std::path::PathBuf;
use std::process;

use config::ScriptureConfig;

fn main() {
    if let Err(error) = try_main() {
        eprintln!("scripture: {error}");
        let mut source = error.source();
        while let Some(cause) = source {
            eprintln!("scripture: caused by: {cause}");
            source = cause.source();
        }
        process::exit(1);
    }
}

#[tokio::main(flavor = "current_thread")]
async fn try_main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1).peekable();
    let Some(command) = arguments.next() else {
        print_help();
        process::exit(2);
    };
    match command.as_str() {
        "validate" => {
            let config_path = parse_config_only_args(&mut arguments, "validate")?;
            let config = ScriptureConfig::load(&config_path)?;
            // Touch typed getters so identity/store tokens fail like serve/bootstrap.
            let _ = config.owner_id()?;
            let _ = config.advertise()?;
            let _ = config.backend()?;
            let _ = config.verse_runtime_config()?;
            eprintln!(
                "scripture: validate ok version={} owner={} advertise={} backend={} prefix={}",
                config.version,
                config.node.owner_id,
                config.node.advertise,
                config.store.backend,
                config.store.prefix.trim_end_matches('/'),
            );
            Ok(())
        }
        "serve" => {
            let config_path = parse_config_only_args(&mut arguments, "serve")?;
            let config = ScriptureConfig::load(&config_path)?;
            serve::serve(config).await
        }
        "bootstrap" => {
            let (config_path, loglet_id) = parse_bootstrap_args(&mut arguments)?;
            let config = ScriptureConfig::load(&config_path)?;
            bootstrap::bootstrap(config, loglet_id).await
        }
        "--help" | "-h" | "help" => {
            print_help();
            Ok(())
        }
        other => {
            Err(format!("unknown command {other:?} (expected validate|bootstrap|serve)").into())
        }
    }
}

fn parse_config_only_args(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    command: &str,
) -> Result<PathBuf, Box<dyn Error>> {
    let mut config = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                config = Some(PathBuf::from(required(arguments, "--config")?));
            }
            "--access-key" | "--secret-key" | "--loglet-id" | "--takeover-successor" => {
                return Err(format!(
                    "{command} does not accept secrets, bootstrap ids, or recovery flags on argv"
                )
                .into());
            }
            "--help" | "-h" => {
                print_help();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    config.ok_or_else(|| format!("{command} requires --config PATH").into())
}

fn parse_bootstrap_args(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<(PathBuf, String), Box<dyn Error>> {
    let mut config = None;
    let mut loglet_id = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                config = Some(PathBuf::from(required(arguments, "--config")?));
            }
            "--loglet-id" => {
                loglet_id = Some(required(arguments, "--loglet-id")?);
            }
            "--access-key" | "--secret-key" | "--takeover-successor" => {
                return Err(
                    "secrets and recovery flags must not be passed on argv; use process environment / accepted recovery surface"
                        .into(),
                );
            }
            "--help" | "-h" => {
                print_help();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok((
        config.ok_or("bootstrap requires --config PATH")?,
        loglet_id.ok_or("bootstrap requires --loglet-id ID")?,
    ))
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
    eprintln!(
        "\
Scripture — durable journal process (ha_claim=false)

Usage:
  scripture validate --config /path/to/scripture.yaml
  scripture bootstrap --config /path/to/scripture.yaml --loglet-id <ID>
  scripture serve --config /path/to/scripture.yaml

validate:  load + validate non-secret YAML; no network; no ownership.
bootstrap: one-shot greenfield Canon publication; exits; never opens ingress.
serve:     long-running process from existing Canon evidence only.

After bootstrap exits, an open generation may yield RecoveryRequired on the
named owner until an accepted seal-and-replace decision exists. serve does
not invent that path.

Non-secret settings come from the YAML file. Credentials come from the
process environment only:
  rustfs: RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
          (or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
  r2:     R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY
Never from ConfigMap, argv, or logs.

Probes (when metrics.status_bind is set):
  /livez   process alive
  /readyz  HTTP 200 only when disposition is Serving
  /status  Canon disposition report

This command does not claim automatic failover, restart fencing, a public
producer protocol, or Decision 0012 recovery."
    );
}
