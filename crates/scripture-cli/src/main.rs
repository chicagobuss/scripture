//! Scripture process command.
//!
//! Product surface:
//! - `scripture validate --config PATH`
//! - `scripture bootstrap --config PATH --loglet-id ID`
//! - `scripture replace --config PATH --successor-loglet-id ID`
//! - `scripture serve --config PATH`

#![allow(unreachable_pub)]

mod assemble;
mod bootstrap;
#[cfg(feature = "campaign-faults")]
mod campaign_faults;
mod config;
mod ha_activate;
mod promote;
mod replace;
mod scribe;
mod serve;

use std::error::Error;
use std::path::PathBuf;
use std::process;

use config::ScriptureConfig;

fn main() {
    // The object-store and Kubernetes client can select different Rustls crypto
    // backends transitively. Select one before either client is constructed.
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("scripture: Rustls crypto provider was already configured unexpectedly");
        process::exit(1);
    }
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
            if config.is_multi_assignment() {
                let scribe = config.scribe.as_ref().expect("multi");
                for assignment in &scribe.assignments {
                    let _ = config.assignment_runtime_config(assignment)?;
                    let _ = config.assignment_advertise(assignment)?;
                    let _ = config.assignment_store_root(assignment)?;
                }
                eprintln!(
                    "scripture: validate ok version={} owner={} advertise={} backend={} prefix={} assignments={}",
                    config.version,
                    config.node.owner_id,
                    config.node.advertise,
                    config.store.backend,
                    config.store.prefix.trim_end_matches('/'),
                    scribe.assignments.len(),
                );
            } else {
                let _ = config.verse_runtime_config()?;
                eprintln!(
                    "scripture: validate ok version={} owner={} advertise={} backend={} prefix={}",
                    config.version,
                    config.node.owner_id,
                    config.node.advertise,
                    config.store.backend,
                    config.store.prefix.trim_end_matches('/'),
                );
            }
            Ok(())
        }
        "serve" => {
            let config_path = parse_config_only_args(&mut arguments, "serve")?;
            let config = ScriptureConfig::load(&config_path)?;
            serve::serve(config).await
        }
        "bootstrap" => {
            let (config_path, loglet_id, initial_term) = parse_bootstrap_args(&mut arguments)?;
            let config = ScriptureConfig::load(&config_path)?;
            bootstrap::bootstrap(config, loglet_id, initial_term).await
        }
        "replace" => {
            let (config_path, successor) = parse_replace_args(&mut arguments)?;
            let config = ScriptureConfig::load(&config_path)?;
            replace::replace(config, successor).await
        }
        "promote" => {
            let (config_path, term, assignment_id) = parse_promote_args(&mut arguments)?;
            let config = ScriptureConfig::load(&config_path)?;
            promote::promote(config, term, assignment_id.as_deref()).await
        }
        "--help" | "-h" | "help" => {
            print_help();
            Ok(())
        }
        other => Err(format!(
            "unknown command {other:?} (expected validate|bootstrap|replace|promote|serve)"
        )
        .into()),
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
            "--access-key"
            | "--secret-key"
            | "--loglet-id"
            | "--successor-loglet-id"
            | "--takeover-successor" => {
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
) -> Result<(PathBuf, Option<String>, u64), Box<dyn Error>> {
    let mut config = None;
    let mut loglet_id = None;
    let mut initial_term = 1_u64;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                config = Some(PathBuf::from(required(arguments, "--config")?));
            }
            "--loglet-id" => {
                loglet_id = Some(required(arguments, "--loglet-id")?);
            }
            "--initial-term" => {
                let raw = required(arguments, "--initial-term")?;
                initial_term = raw.parse::<u64>().map_err(|error| {
                    format!("--initial-term must be a positive integer: {error}")
                })?;
                if initial_term == 0 {
                    return Err("--initial-term must be non-zero".into());
                }
            }
            "--access-key" | "--secret-key" | "--successor-loglet-id" | "--takeover-successor" => {
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
        loglet_id,
        initial_term,
    ))
}

fn parse_replace_args(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<(PathBuf, String), Box<dyn Error>> {
    let mut config = None;
    let mut successor = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                config = Some(PathBuf::from(required(arguments, "--config")?));
            }
            "--successor-loglet-id" => {
                successor = Some(required(arguments, "--successor-loglet-id")?);
            }
            "--access-key" | "--secret-key" | "--loglet-id" | "--takeover-successor" => {
                return Err(
                    "secrets, bootstrap ids, and removed recovery flags must not be passed on replace argv"
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
        config.ok_or("replace requires --config PATH")?,
        successor.ok_or("replace requires --successor-loglet-id ID")?,
    ))
}

fn parse_promote_args(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<(PathBuf, u64, Option<String>), Box<dyn Error>> {
    let mut config = None;
    let mut term = None;
    let mut assignment_id = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                config = Some(PathBuf::from(required(arguments, "--config")?));
            }
            "--candidate-term" => {
                let raw = required(arguments, "--candidate-term")?;
                term = Some(raw.parse::<u64>().map_err(|error| {
                    format!("--candidate-term must be a positive integer: {error}")
                })?);
            }
            "--assignment" => {
                assignment_id = Some(required(arguments, "--assignment")?);
            }
            "--access-key" | "--secret-key" | "--loglet-id" | "--successor-loglet-id" => {
                return Err("secrets and loglet ids must not be passed on promote argv".into());
            }
            "--help" | "-h" => {
                print_help();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok((
        config.ok_or("promote requires --config PATH")?,
        term.ok_or("promote requires --candidate-term N")?,
        assignment_id,
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
  scripture bootstrap --config /path/to/scripture.yaml [--initial-term N]   # HA mode
  scripture replace --config /path/to/scripture.yaml --successor-loglet-id <ID>
  scripture promote --config /path/to/scripture.yaml --candidate-term <N>
  scripture promote --config multi.yaml --assignment <ID> --candidate-term <N>
  scripture serve --config /path/to/scripture.yaml
validate:  load + validate non-secret YAML; no network; no ownership.
bootstrap: legacy one-shot Canon publication, or (ha.mode: serving-authority)
           long-lived bootstrap-and-serve (one-record VirtualLog root fence).
           With scribe.assignments: multi-assignment Scribe supervisor (one
           independent authority root / listener per assignment).
replace:   legacy empty open-generation activation; exits; never opens ingress.
promote:   long-lived promote-and-serve under ha.mode: serving-authority.
           Single-assignment: --candidate-term only (unchanged).
           Multi-assignment: --assignment ID --candidate-term N (targeted Verse
           promote only; siblings keep posture — standby is a dormant candidate).
serve:     long-running legacy Canon path; refused under Serving-Authority mode
           (writables cannot cross process exit — use bootstrap/promote).
HA YAML (portable; no secrets):
  ha:
    mode: serving-authority
Multi-assignment YAML (requires Serving Authority; omits top-level listener/verse):
  scribe:
    assignments:
      - id: example
        canon: \"................\"
        verse: \"................\"
        cohort_id: \"................\"
        writer_id: \"................\"
        posture: bootstrap-if-empty   # or standby (dormant candidate)
        advertise: \"tcp://10.0.0.1:9000\"
        ingress:
          bind: \"10.0.0.1:9000\"
Authority is membership + Scripture fence on the Holylog VirtualLog root only.
There is no separate ServingAuthorityStore / CRD backend.
No live SSH/ZeroTier drill until deterministic multi-assignment HA tests are green.

Non-secret settings come from the YAML file. Object-store credentials come from
the process environment only:
  rustfs: RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
          (or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
  r2:     R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY
Never from ConfigMap, argv, or logs.

Probes (when metrics.status_bind is set):
  /livez   process alive
  /readyz  HTTP 200 only when Serving
  /status  disposition report

This command does not claim automatic failover, restart fencing, a public
producer protocol, or Decision 0012 recovery.",
    );
}
