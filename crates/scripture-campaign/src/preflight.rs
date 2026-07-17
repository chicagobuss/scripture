//! Default-dry-run preflight for autonomous campaigns.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use crate::profile::{Profile, RustFsHomeFleetProfile};

/// Outcome of read-only preflight checks.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PreflightReport {
    /// Whether all required checks passed.
    pub ok: bool,
    /// Individual check results.
    pub checks: BTreeMap<String, CheckResult>,
    /// Advisory notes (missing optional resources, degraded placement, …).
    pub notes: Vec<String>,
}

/// One named preflight check.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckResult {
    /// Pass/fail.
    pub ok: bool,
    /// Human-readable detail.
    pub detail: String,
}

impl PreflightReport {
    /// Runs profile-specific preflight without mutating the cluster.
    pub fn run(profile: &Profile, for_execute: bool) -> Self {
        match profile {
            Profile::Memory => Self::memory(),
            Profile::RustFsHomeFleet(config) => Self::rustfs_home_fleet(config, for_execute),
        }
    }

    fn memory() -> Self {
        let mut checks = BTreeMap::new();
        checks.insert(
            "backend".into(),
            CheckResult {
                ok: true,
                detail: "in-memory backend requires no cluster".into(),
            },
        );
        Self {
            ok: true,
            checks,
            notes: Vec::new(),
        }
    }

    fn rustfs_home_fleet(config: &RustFsHomeFleetProfile, for_execute: bool) -> Self {
        let mut checks = BTreeMap::new();
        let mut notes = Vec::new();

        let context_ok = kubectl(&["config", "current-context"])
            .map(|output| output.trim() == config.kube_context)
            .unwrap_or(false);
        checks.insert(
            "kube_context".into(),
            CheckResult {
                ok: context_ok,
                detail: format!(
                    "expected context {}; got {}",
                    config.kube_context,
                    kubectl(&["config", "current-context"])
                        .unwrap_or_default()
                        .trim()
                ),
            },
        );

        let nodes_output = kubectl(&[
            "--context",
            &config.kube_context,
            "get",
            "nodes",
            "-o",
            "json",
            "--request-timeout=10s",
        ])
        .unwrap_or_default();
        let ready_nodes = ready_node_names(&nodes_output);
        let missing: Vec<&str> = config
            .required_nodes
            .iter()
            .map(String::as_str)
            .filter(|node| !ready_nodes.iter().any(|ready| ready == node))
            .collect();
        checks.insert(
            "required_nodes".into(),
            CheckResult {
                ok: missing.is_empty(),
                detail: if missing.is_empty() {
                    format!(
                        "all required nodes ready: {}",
                        config.required_nodes.join(", ")
                    )
                } else {
                    format!("missing nodes: {}", missing.join(", "))
                },
            },
        );

        let rustfs_svc = kubectl(&[
            "--context",
            &config.kube_context,
            "-n",
            &config.rustfs_namespace,
            "get",
            "svc",
            &config.rustfs_service,
            "--request-timeout=10s",
        ]);
        checks.insert(
            "rustfs_service".into(),
            CheckResult {
                ok: rustfs_svc.is_ok(),
                detail: format!(
                    "svc/{}/{} in {}",
                    config.rustfs_service, config.rustfs_namespace, config.rustfs_namespace
                ),
            },
        );

        let secret_exists = kubectl(&[
            "--context",
            &config.kube_context,
            "-n",
            &config.store_secret_namespace,
            "get",
            "secret",
            &config.store_secret,
            "--request-timeout=10s",
        ])
        .is_ok();
        if !secret_exists && !for_execute {
            notes.push(format!(
                "secret {}/{} not found yet (expected before first --execute)",
                config.store_secret_namespace, config.store_secret
            ));
        }
        checks.insert(
            "store_secret".into(),
            CheckResult {
                ok: secret_exists || !for_execute,
                detail: if secret_exists {
                    format!(
                        "secret {}/{} present (values never read)",
                        config.store_secret_namespace, config.store_secret
                    )
                } else if for_execute {
                    format!(
                        "secret {}/{} missing; required for --execute",
                        config.store_secret_namespace, config.store_secret
                    )
                } else {
                    format!(
                        "secret {}/{} absent (advisory until first --execute)",
                        config.store_secret_namespace, config.store_secret
                    )
                },
            },
        );

        let placement = [
            &config.writer_a_node,
            &config.writer_b_node,
            &config.checker_node,
            &config.rustfs_node,
        ];
        let placement_ok = placement
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == placement.len();
        checks.insert(
            "placement".into(),
            CheckResult {
                ok: placement_ok,
                detail: format!(
                    "A={} B={} checker={} rustfs={}",
                    config.writer_a_node,
                    config.writer_b_node,
                    config.checker_node,
                    config.rustfs_node
                ),
            },
        );

        let ok = checks.values().all(|check| check.ok);
        Self { ok, checks, notes }
    }

    /// Writes `preflight.json` under `artifact_dir`.
    pub fn write(&self, artifact_dir: &Path) -> Result<(), PreflightError> {
        std::fs::create_dir_all(artifact_dir)?;
        let path = artifact_dir.join("preflight.json");
        std::fs::write(path, serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }
}

fn ready_node_names(nodes_output: &str) -> Vec<String> {
    let Ok(nodes) = serde_json::from_str::<serde_json::Value>(nodes_output) else {
        return Vec::new();
    };
    nodes["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|node| {
            node["status"]["conditions"]
                .as_array()
                .is_some_and(|conditions| {
                    conditions.iter().any(|condition| {
                        condition["type"].as_str() == Some("Ready")
                            && condition["status"].as_str() == Some("True")
                    })
                })
        })
        .filter_map(|node| node["metadata"]["name"].as_str().map(str::to_owned))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::ready_node_names;

    #[test]
    fn accepts_only_nodes_with_a_true_ready_condition() {
        let nodes = r#"{
          "items": [
            {"metadata":{"name":"ready"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
            {"metadata":{"name":"not-ready"},"status":{"conditions":[{"type":"Ready","status":"False"}]}},
            {"metadata":{"name":"unknown"},"status":{"conditions":[{"type":"MemoryPressure","status":"False"}]}}
          ]
        }"#;
        assert_eq!(ready_node_names(nodes), vec!["ready"]);
    }

    #[test]
    fn malformed_node_response_fails_closed() {
        assert!(ready_node_names("not json").is_empty());
    }
}

fn kubectl(args: &[&str]) -> Result<String, PreflightError> {
    let output = Command::new("kubectl")
        .args(args)
        .output()
        .map_err(|error| PreflightError::Command(format!("kubectl: {error}")))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(PreflightError::Command(format!(
            "kubectl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

/// Preflight failures.
#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    /// kubectl or filesystem failure.
    #[error("{0}")]
    Command(String),
    /// JSON serialization failure.
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    /// IO failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Default local topology config path relative to the Scripture repo root.
#[must_use]
pub fn default_topology_path(repo_root: &Path) -> std::path::PathBuf {
    repo_root.join("config/local/correctness-testing/topology.json")
}
