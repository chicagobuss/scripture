//! Autonomous Scripture correctness campaign runner (WP04 Slice 1).

use std::error::Error;
use std::path::PathBuf;
use std::process;

use scripture_campaign::{
    Profile, RunOptions, Suite, default_topology_path, detect_repo_root,
};

struct RunArgs {
    profile: String,
    suite: Suite,
    run_id: Option<String>,
    artifact_dir: PathBuf,
    execute: bool,
    topology: Option<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut arguments = std::env::args().skip(1).peekable();
    if arguments.peek().is_some_and(|arg| arg == "run") {
        arguments.next();
        if let Err(error) = run_campaign(&mut arguments).await {
            exit_with_error(error);
        }
        return;
    }
    if let Err(error) = run_legacy(&mut arguments).await {
        exit_with_error(error);
    }
}

async fn run_campaign(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<(), Box<dyn Error>> {
    let args = parse_run_args(arguments)?;
    let repo_root = detect_repo_root();
    let topology = args
        .topology
        .or_else(|| Some(default_topology_path(&repo_root)));
    let profile = Profile::parse(&args.profile, topology.as_deref())?;
    let outcome = RunOptions {
        profile,
        suite: args.suite,
        run_id: args.run_id,
        artifact_dir: args.artifact_dir,
        execute: args.execute,
    }
    .run()
    .await?;
    eprintln!(
        "scripture-campaign: {} run_id={} artifacts={}",
        if outcome.dry_run {
            "preflight complete"
        } else {
            "suite complete"
        },
        outcome.run_id,
        outcome.artifact_dir.display(),
    );
    process::exit(outcome.exit_code);
}

async fn run_legacy(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<(), Box<dyn Error>> {
    scripture_campaign::legacy_main(arguments).await
}

fn parse_run_args(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<RunArgs, Box<dyn Error>> {
    let mut profile = None;
    let mut suite = None;
    let mut run_id = None;
    let mut artifact_dir = None;
    let mut execute = false;
    let mut topology = None;

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--profile" => profile = Some(required(arguments, "--profile")?),
            "--suite" => {
                suite = Some(Suite::parse(&required(arguments, "--suite")?)?);
            }
            "--run-id" => run_id = Some(required(arguments, "--run-id")?),
            "--artifact-dir" => {
                artifact_dir = Some(PathBuf::from(required(arguments, "--artifact-dir")?));
            }
            "--topology" => topology = Some(PathBuf::from(required(arguments, "--topology")?)),
            "--execute" => execute = true,
            "--keep-failed" => {}
            "--help" | "-h" => {
                print_run_help();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    let repo_root = detect_repo_root();
    Ok(RunArgs {
        profile: profile.ok_or("run requires --profile NAME")?,
        suite: suite.ok_or("run requires --suite core|composition|resilience|all")?,
        run_id,
        artifact_dir: artifact_dir.unwrap_or_else(|| {
            repo_root.join("config/local/correctness-testing/runs")
        }),
        execute,
        topology,
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

fn print_run_help() {
    eprintln!(
        "\
scripture-campaign run — autonomous correctness gauntlet (WP04)

Usage:
  scripture-campaign run \\
    --profile memory|rustfs-home-fleet \\
    --suite core|composition|resilience|all \\
    [--run-id ID] \\
    [--artifact-dir PATH] \\
    [--topology PATH] \\
    [--execute] \\
    [--keep-failed]

Default is dry-run preflight only (no cluster writes, no scenarios executed).
`--execute` runs the selected suite after preflight passes.

Exit codes:
  0  pass / preflight ok
  2  checker fail
  3  inconclusive
  4  invalid orchestration / preflight failure
"
    );
}

fn exit_with_error(error: Box<dyn Error>) {
    eprintln!("scripture-campaign: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        eprintln!("scripture-campaign: caused by: {cause}");
        source = cause.source();
    }
    process::exit(1);
}
