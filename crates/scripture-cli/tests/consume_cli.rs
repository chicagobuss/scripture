//! Binary-level coverage for `scripture consume` help and argument validation.

use std::process::Command;

fn scripture() -> Command {
    Command::new(env!("CARGO_BIN_EXE_scripture"))
}

#[test]
fn help_lists_consume_with_read_only_disclaimer() {
    let output = scripture()
        .arg("help")
        .output()
        .expect("run scripture help");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("scripture consume"));
    assert!(stderr.contains("read-only debug/demo consumer"));
    assert!(stderr.contains("produce-lab"));
    assert!(stderr.contains("consume-lab"));
}

#[test]
fn consume_help_exits_zero_with_usage() {
    let output = scripture()
        .args(["consume", "--help"])
        .output()
        .expect("run consume --help");
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--until-records"));
    assert!(stderr.contains("--no-follow"));
    assert!(stderr.contains("no checkpoint"));
}

#[test]
fn consume_rejects_seconds_zero() {
    let output = scripture()
        .args([
            "consume",
            "--config",
            "missing.yaml",
            "--canon",
            "demo-canon!!!!!!",
            "--verse",
            "demo-verse!!!!!!",
            "--seconds",
            "0",
        ])
        .output()
        .expect("run consume");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--seconds 0 is rejected"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn consume_requires_config_canon_verse() {
    let output = scripture()
        .args(["consume", "--canon", "demo"])
        .output()
        .expect("run consume");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("requires --config") || stderr.contains("requires --verse"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn unknown_command_mentions_consume() {
    let output = scripture()
        .arg("not-a-command")
        .output()
        .expect("run unknown");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("consume"));
}

#[test]
fn help_lists_scribe_run() {
    let output = scripture().arg("help").output().expect("help");
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("scribe run"));
    assert!(
        stderr.contains("Prefer `scribe run`") || stderr.contains("prefer `scripture scribe run`")
    );
}

#[test]
fn scribe_run_help_exits_zero() {
    let output = scripture()
        .args(["scribe", "run", "--help"])
        .output()
        .expect("scribe run help");
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("peer-grace"));
    assert!(stderr.contains("no promote/standby") || stderr.contains("lawful successor"));
}
