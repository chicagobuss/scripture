//! Scripture process command.
//!
//! Product surface:
//! - `scripture validate --config PATH`
//! - `scripture bootstrap --config PATH --loglet-id ID`
//! - `scripture replace --config PATH --successor-loglet-id ID`
//! - `scripture serve --config PATH`
//! - `scripture consume --config PATH --canon ID --verse ID`

#![allow(unreachable_pub)]

mod assemble;
mod bootstrap;
#[cfg(feature = "campaign-faults")]
mod campaign_faults;
mod config;
mod consume;
mod consume_lab;
mod directory_cmd;
mod doctor;
mod ha_activate;
mod produce_lab;
mod promote;
mod replace;
mod scribe;
mod scribe_run;
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
            scribe_run::bootstrap_compat(config, loglet_id, initial_term).await
        }
        "replace" => {
            let (config_path, successor) = parse_replace_args(&mut arguments)?;
            let config = ScriptureConfig::load(&config_path)?;
            replace::replace(config, successor).await
        }
        "promote" => {
            let (config_path, term, assignment_id) = parse_promote_args(&mut arguments)?;
            let config = ScriptureConfig::load(&config_path)?;
            scribe_run::promote_compat(config, term, assignment_id.as_deref()).await
        }
        "scribe" => {
            let Some(subcommand) = arguments.next() else {
                return Err("scribe requires a subcommand (run)".into());
            };
            match subcommand.as_str() {
                "run" => {
                    let (config_path, peer_grace_ms, initial_term) =
                        parse_scribe_run_args(&mut arguments)?;
                    let config = ScriptureConfig::load(std::path::Path::new(&config_path))?;
                    scribe_run::scribe_run(config, peer_grace_ms, initial_term).await
                }
                "--help" | "-h" | "help" => {
                    print_scribe_run_help();
                    Ok(())
                }
                other => Err(format!("unknown scribe subcommand {other:?} (expected run)").into()),
            }
        }
        "directory" => {
            let (config_path, canon, verse) = parse_directory_args(&mut arguments)?;
            let config = ScriptureConfig::load(std::path::Path::new(&config_path))?;
            directory_cmd::directory(config, canon.as_deref(), verse.as_deref()).await
        }
        "doctor" => {
            let (config_path, format) = parse_doctor_args(&mut arguments)?;
            let config = ScriptureConfig::load(std::path::Path::new(&config_path))?;
            doctor::doctor(config, format).await
        }
        "produce-lab" => {
            let (config_path, options) = parse_produce_lab_args(&mut arguments)?;
            let config = ScriptureConfig::load(std::path::Path::new(&config_path))?;
            produce_lab::produce_lab(config, options).await
        }
        "consume" => {
            let (config_path, options) = parse_consume_args(&mut arguments)?;
            let config = ScriptureConfig::load(std::path::Path::new(&config_path))?;
            consume::consume(config, options).await
        }
        "consume-lab" => {
            let (config_path, options) = parse_consume_lab_args(&mut arguments)?;
            let config = ScriptureConfig::load(std::path::Path::new(&config_path))?;
            consume_lab::consume_lab(config, options).await
        }
        "--help" | "-h" | "help" => {
            print_help();
            Ok(())
        }
        other => Err(format!(
            "unknown command {other:?} (expected validate|bootstrap|replace|promote|scribe|serve|directory|doctor|consume|produce-lab|consume-lab)"
        )
        .into()),
    }
}

/// Parses `scribe run --config PATH [--peer-grace-ms N] [--initial-term N]`.
fn parse_scribe_run_args(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<(String, u64, u64), Box<dyn Error>> {
    let mut config_path = None;
    let mut peer_grace_ms = 2_000_u64;
    let mut initial_term = 1_u64;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => config_path = Some(arguments.next().ok_or("--config requires a path")?),
            "--peer-grace-ms" => {
                peer_grace_ms = arguments
                    .next()
                    .ok_or("--peer-grace-ms requires N")?
                    .parse()
                    .map_err(|error| format!("--peer-grace-ms must be an integer: {error}"))?;
                if peer_grace_ms == 0 {
                    return Err("--peer-grace-ms must be non-zero".into());
                }
            }
            "--initial-term" => {
                initial_term = arguments
                    .next()
                    .ok_or("--initial-term requires N")?
                    .parse()
                    .map_err(|error| format!("--initial-term must be an integer: {error}"))?;
                if initial_term == 0 {
                    return Err("--initial-term must be non-zero".into());
                }
            }
            "--help" | "-h" => {
                print_scribe_run_help();
                std::process::exit(0);
            }
            other => return Err(format!("scribe run: unexpected argument {other}").into()),
        }
    }
    Ok((
        config_path.ok_or("scribe run requires --config")?,
        peer_grace_ms,
        initial_term,
    ))
}

fn print_scribe_run_help() {
    eprintln!(
        "\
scripture scribe run — normal same-Verse fleet lifecycle (no promote/standby)

Usage:
  scripture scribe run --config PATH [--peer-grace-ms N] [--initial-term N]

Every fleet member starts with the same command. The process observes the durable
VirtualLog root, bootstraps when empty, joins as a healthy non-writer when another
owner lawfully Serves, and attempts a lawful successor CAS only after the peer
advertise endpoint looks unreachable for --peer-grace-ms.

Liveness arms recovery; the conditional root remains the authority boundary.
Prefer this over `bootstrap` / `promote` for Serving-Authority fleets."
    );
}

/// Parses `produce-lab --config <p> --canon <c> --verse <v> [--workers N]
/// [--per-worker N] [--payload-bytes N] [--records-per-submission N]`.
fn parse_produce_lab_args(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<(String, produce_lab::LabOptions), Box<dyn Error>> {
    let mut config_path = None;
    let (mut canon, mut verse) = (None, None);
    let (mut workers, mut per_worker, mut payload_bytes, mut records_per_submission) =
        (3usize, 200u64, 64usize, 1usize);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => config_path = Some(arguments.next().ok_or("--config requires a path")?),
            "--canon" => canon = Some(arguments.next().ok_or("--canon requires an id")?),
            "--verse" => verse = Some(arguments.next().ok_or("--verse requires an id")?),
            "--workers" => workers = arguments.next().ok_or("--workers requires N")?.parse()?,
            "--per-worker" => {
                per_worker = arguments.next().ok_or("--per-worker requires N")?.parse()?;
            }
            "--payload-bytes" => {
                payload_bytes = arguments
                    .next()
                    .ok_or("--payload-bytes requires N")?
                    .parse()?;
            }
            "--records-per-submission" => {
                records_per_submission = arguments
                    .next()
                    .ok_or("--records-per-submission requires N")?
                    .parse()?;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("produce-lab: unexpected argument {other}").into()),
        }
    }
    Ok((
        config_path.ok_or("produce-lab requires --config")?,
        produce_lab::LabOptions {
            canon: canon.ok_or("produce-lab requires --canon")?,
            verse: verse.ok_or("produce-lab requires --verse")?,
            workers,
            per_worker,
            payload_bytes,
            records_per_submission,
        },
    ))
}

/// Parses `consume --config <p> --canon <c> --verse <v> [options]`.
fn parse_consume_args(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<(String, consume::ConsumeOptions), Box<dyn Error>> {
    let mut config_path = None;
    let (mut canon, mut verse) = (None, None);
    let mut from = 0_u64;
    let mut until_records = None;
    let mut seconds = consume::ConsumeOptions::default_seconds();
    let mut format = consume::OutputFormat::Text;
    let mut no_follow = false;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => config_path = Some(arguments.next().ok_or("--config requires a path")?),
            "--canon" => canon = Some(arguments.next().ok_or("--canon requires an id")?),
            "--verse" => verse = Some(arguments.next().ok_or("--verse requires an id")?),
            "--from" => {
                from = arguments
                    .next()
                    .ok_or("--from requires OFFSET")?
                    .parse()
                    .map_err(|error| format!("--from must be an integer: {error}"))?;
            }
            "--until-records" => {
                let value: u64 = arguments
                    .next()
                    .ok_or("--until-records requires N")?
                    .parse()
                    .map_err(|error| format!("--until-records must be an integer: {error}"))?;
                until_records = Some(value);
            }
            "--seconds" => {
                seconds = arguments
                    .next()
                    .ok_or("--seconds requires N")?
                    .parse()
                    .map_err(|error| format!("--seconds must be an integer: {error}"))?;
            }
            "--format" => {
                format = consume::OutputFormat::parse(
                    &arguments.next().ok_or("--format requires text|jsonl")?,
                )?;
            }
            "--no-follow" => no_follow = true,
            "--help" | "-h" => {
                consume::print_help();
                std::process::exit(0);
            }
            other => return Err(format!("consume: unexpected argument {other}").into()),
        }
    }
    if !no_follow && seconds == 0 {
        return Err(
            "consume: --seconds 0 is rejected (use --no-follow for a bounded one-shot)".into(),
        );
    }
    Ok((
        config_path.ok_or("consume requires --config")?,
        consume::ConsumeOptions {
            canon: canon.ok_or("consume requires --canon")?,
            verse: verse.ok_or("consume requires --verse")?,
            from,
            until_records,
            seconds,
            format,
            no_follow,
        },
    ))
}

/// Parses `consume-lab --config <p> --canon <c> --verse <v> [--seconds N]
/// [--until-records N]`.
fn parse_consume_lab_args(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<(String, consume_lab::ConsumeOptions), Box<dyn Error>> {
    let mut config_path = None;
    let (mut canon, mut verse) = (None, None);
    let (mut seconds, mut until_records) = (60u64, 0u64);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => config_path = Some(arguments.next().ok_or("--config requires a path")?),
            "--canon" => canon = Some(arguments.next().ok_or("--canon requires an id")?),
            "--verse" => verse = Some(arguments.next().ok_or("--verse requires an id")?),
            "--seconds" => seconds = arguments.next().ok_or("--seconds requires N")?.parse()?,
            "--until-records" => {
                until_records = arguments
                    .next()
                    .ok_or("--until-records requires N")?
                    .parse()?;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("consume-lab: unexpected argument {other}").into()),
        }
    }
    Ok((
        config_path.ok_or("consume-lab requires --config")?,
        consume_lab::ConsumeOptions {
            canon: canon.ok_or("consume-lab requires --canon")?,
            verse: verse.ok_or("consume-lab requires --verse")?,
            seconds,
            until_records,
        },
    ))
}

/// Config path plus the optional `(canon, verse)` filter for `directory`.
type DirectoryArgs = (String, Option<String>, Option<String>);

/// Parses `directory --config <path> [--canon <id> --verse <id>]`.
fn parse_directory_args(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<DirectoryArgs, Box<dyn Error>> {
    let mut config_path = None;
    let mut canon = None;
    let mut verse = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                config_path = Some(arguments.next().ok_or("--config requires a path")?);
            }
            "--canon" => {
                canon = Some(arguments.next().ok_or("--canon requires an id")?);
            }
            "--verse" => {
                verse = Some(arguments.next().ok_or("--verse requires an id")?);
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("directory: unexpected argument {other}").into()),
        }
    }
    let config_path = config_path.ok_or("directory requires --config")?;
    if canon.is_some() != verse.is_some() {
        return Err("directory: --canon and --verse must be given together".into());
    }
    Ok((config_path, canon, verse))
}

/// Parses `doctor --config <path> [--format human|json]`.
fn parse_doctor_args(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<(String, doctor::DoctorFormat), Box<dyn Error>> {
    let mut config_path = None;
    let mut format = doctor::DoctorFormat::Human;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                config_path = Some(arguments.next().ok_or("--config requires a path")?);
            }
            "--format" => {
                let value = arguments.next().ok_or("--format requires human|json")?;
                format = match value.as_str() {
                    "human" => doctor::DoctorFormat::Human,
                    "json" => doctor::DoctorFormat::Json,
                    other => {
                        return Err(format!(
                            "doctor: unknown --format {other} (expected human|json)"
                        )
                        .into());
                    }
                };
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("doctor: unexpected argument {other}").into()),
        }
    }
    let config_path = config_path.ok_or("doctor requires --config")?;
    Ok((config_path, format))
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
  scripture scribe run --config /path/to/scripture.yaml [--peer-grace-ms N]
  scripture bootstrap --config /path/to/scripture.yaml --loglet-id <ID>
  scripture bootstrap --config /path/to/scripture.yaml [--initial-term N]   # compat
  scripture replace --config /path/to/scripture.yaml --successor-loglet-id <ID>
  scripture promote --config /path/to/scripture.yaml --candidate-term <N>   # compat
  scripture promote --config multi.yaml --assignment <ID> --candidate-term <N>
  scripture serve --config /path/to/scripture.yaml
  scripture directory --config /path/to/scripture.yaml [--canon ID --verse ID]
  scripture doctor --config /path/to/scripture.yaml [--format human|json]
  scripture consume --config /path/to/scripture.yaml --canon ID --verse ID \\
      [--from N] [--until-records N] [--seconds N] [--format text|jsonl] [--no-follow]
  scripture produce-lab --config PATH --canon ID --verse ID [lab options]
  scripture consume-lab --config PATH --canon ID --verse ID [lab options]
validate:  load + validate non-secret YAML; no network; no ownership.
scribe run: normal fleet lifecycle for one Canon/Verse. Same command on every
           member. Observes the durable root; bootstraps if empty; joins as a
           healthy non-writer when another owner Serves; recovers via root CAS
           only after peer unreachability (liveness arms, never grants).
bootstrap: compatibility Empty→Serving / legacy one-shot. Prefer `scribe run`.
replace:   legacy empty open-generation activation; exits; never opens ingress.
promote:   compatibility Expected→Serving. Prefer `scribe run` for fleets.
serve:     long-running legacy Canon path; refused under Serving-Authority mode
           (writables cannot cross process exit — use scribe run).
directory: list fleet-directory records (decision 0014); optional (canon, verse)
           ranking. Discovery only — never authority.
doctor:    durability/availability capability report for the four failure
           boundaries (Canon history, producer continuity, Scribe availability,
           failure-domain durability). Availability is observed heartbeats from
           the fleet directory, not a replica count.
consume:   read-only debug/demo consumer. Prints logical Scripture records from
           a configured Canon/Verse to stdout. Owns no checkpoint; not a durable
           consumer product. Membership is re-observed while following.
           `consume-lab` remains a throughput/stall lab instrument.
produce-lab / consume-lab:
           lab load and continuity instruments (not product subscription APIs).
HA YAML (portable; no secrets):
  ha:
    mode: serving-authority
Fleet members share the Verse store root and differ by node.owner_id / advertise /
listener.bind. Start each with: scripture scribe run --config member.yaml
Multi-assignment YAML (requires Serving Authority; omits top-level listener/verse):
  scribe:
    assignments:
      - id: example
        canon: \"................\"
        verse: \"................\"
        cohort_id: \"................\"
        writer_id: \"................\"
        posture: bootstrap-if-empty   # compatibility; prefer scribe run per Verse
        advertise: \"tcp://10.0.0.1:9000\"
        ingress:
          bind: \"10.0.0.1:9000\"
Authority is membership + Scripture fence on the Holylog VirtualLog root only.
There is no separate ServingAuthorityStore / CRD backend.

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

This command does not claim unbounded distributed liveness, a public producer
protocol, or Decision 0012 recovery. Peer-grace recovery is a bounded arm.",
    );
}
