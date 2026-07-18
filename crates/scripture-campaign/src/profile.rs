//! Backend profiles for autonomous campaign runs (WP05 v3).

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Named campaign profile (operator-local topology projection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Profile {
    /// In-memory backend for fast local scenario validation.
    Memory,
    /// Home-fleet profile: placement only; RustFS is ephemeral per run.
    RustFsHomeFleet(Box<RustFsHomeFleetProfile>),
}

/// Placement-only home-fleet profile. No shared scripture-lab / Tracker store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustFsHomeFleetProfile {
    /// kubectl context name.
    pub kube_context: String,
    /// Nodes that must be Ready before execute.
    pub required_nodes: Vec<String>,
    /// Node for the ephemeral in-namespace RustFS pod.
    pub rustfs_node: String,
    /// Preferred writer A node.
    pub writer_a_node: String,
    /// Preferred writer B / recovery node.
    pub writer_b_node: String,
    /// Checker / coordinator node.
    pub checker_node: String,
    /// Campaign tool container image reference.
    pub image: String,
    /// Scripture product image for process-separated actors.
    #[serde(default = "default_scripture_image")]
    pub scripture_image: String,
    /// Namespace prefix; run namespace is `{prefix}-{sanitized_run_id}`.
    #[serde(default = "default_namespace_prefix")]
    pub correctness_namespace_prefix: String,
    /// Optional operator notes (non-secret).
    #[serde(default)]
    pub notes: Vec<String>,
}

fn default_scripture_image() -> String {
    "scripture:0.1.0".to_owned()
}

fn default_namespace_prefix() -> String {
    "scripture-correctness".to_owned()
}

impl RustFsHomeFleetProfile {
    /// Validates WP05 v3 isolation constraints on the operator topology.
    pub fn validate_isolation(&self) -> Result<(), ProfileError> {
        // Scan identity/placement fields only — operator notes may mention forbidden
        // names in "do not use …" guidance without being a store target.
        let identity_blob = [
            self.kube_context.as_str(),
            self.rustfs_node.as_str(),
            self.writer_a_node.as_str(),
            self.writer_b_node.as_str(),
            self.checker_node.as_str(),
            self.image.as_str(),
            self.scripture_image.as_str(),
            self.correctness_namespace_prefix.as_str(),
        ]
        .join("\n")
        .to_lowercase();
        let forbidden = [
            ("scripture-lab", "fixed scripture-lab store is forbidden"),
            (
                "10.0.0.240",
                "LAN Tracker/object-store endpoints are forbidden",
            ),
            ("10.10.10.10", "ZeroTier Tracker endpoints are forbidden"),
        ];
        for (needle, reason) in forbidden {
            if identity_blob.contains(needle) {
                return Err(ProfileError::InvalidConfig(reason.to_owned()));
            }
        }
        // Reject "tracker" only as a token that looks like a store/host identity,
        // not as a substring of an unrelated word.
        for token in identity_blob.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-') {
            if token == "tracker" || token.starts_with("tracker-") || token.ends_with("-tracker") {
                return Err(ProfileError::InvalidConfig(
                    "Tracker object-store identity must not appear in topology".into(),
                ));
            }
        }
        if self.image.trim().is_empty() || self.image.ends_with(":latest") {
            return Err(ProfileError::InvalidConfig(
                "campaign image must be a non-empty, non-latest reference".into(),
            ));
        }
        if self.scripture_image.trim().is_empty() || self.scripture_image.ends_with(":latest") {
            return Err(ProfileError::InvalidConfig(
                "scripture image must be a non-empty, non-latest reference".into(),
            ));
        }
        let placement = [
            self.writer_a_node.as_str(),
            self.writer_b_node.as_str(),
            self.checker_node.as_str(),
            self.rustfs_node.as_str(),
        ];
        if placement
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != placement.len()
        {
            return Err(ProfileError::InvalidConfig(
                "A, B, checker, and RustFS nodes must be four distinct hostnames".into(),
            ));
        }
        Ok(())
    }

    /// Builds the ephemeral run namespace name.
    #[must_use]
    pub fn run_namespace(&self, run_id: &str) -> String {
        let sanitized = sanitize_k8s_label(run_id);
        let prefix = self.correctness_namespace_prefix.trim_end_matches('-');
        let mut name = format!("{prefix}-{sanitized}");
        if name.len() > 63 {
            name.truncate(63);
            while name.ends_with('-') {
                name.pop();
            }
        }
        name
    }
}

/// Kubernetes DNS-1123 label sanitizer (lowercase alnum / hyphen).
#[must_use]
pub(crate) fn sanitize_k8s_label(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if (ch == '-' || ch == '_' || ch == '.') && !out.ends_with('-') {
            out.push('-');
        }
    }
    while out.starts_with('-') {
        out.remove(0);
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() { "run".into() } else { out }
}

impl Profile {
    /// Parses a profile token and optional local config path.
    pub fn parse(name: &str, config_path: Option<&Path>) -> Result<Self, ProfileError> {
        match name {
            "memory" => Ok(Self::Memory),
            "rustfs-home-fleet" => {
                let path = config_path.ok_or(ProfileError::MissingConfig(
                    "rustfs-home-fleet requires config/local/correctness-testing/topology.json"
                        .to_owned(),
                ))?;
                let raw = std::fs::read_to_string(path).map_err(|error| {
                    ProfileError::MissingConfig(format!("read {}: {error}", path.display()))
                })?;
                let profile: RustFsHomeFleetProfile = serde_json::from_str(&raw)
                    .map_err(|error| ProfileError::InvalidConfig(error.to_string()))?;
                profile.validate_isolation()?;
                Ok(Self::RustFsHomeFleet(Box::new(profile)))
            }
            other => Err(ProfileError::Unknown(other.to_owned())),
        }
    }

    /// Stable profile label for artifacts.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::RustFsHomeFleet(_) => "rustfs-home-fleet",
        }
    }
}

/// Profile resolution failures.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// Unknown profile token.
    #[error("unknown profile {0:?}")]
    Unknown(String),
    /// Required local config missing or unreadable.
    #[error("{0}")]
    MissingConfig(String),
    /// Local config failed validation.
    #[error("invalid profile config: {0}")]
    InvalidConfig(String),
}

#[cfg(test)]
mod tests {
    use super::{RustFsHomeFleetProfile, sanitize_k8s_label};

    fn sample() -> RustFsHomeFleetProfile {
        RustFsHomeFleetProfile {
            kube_context: "Default".into(),
            required_nodes: vec![
                "bignlittles".into(),
                "node-a".into(),
                "node-b".into(),
                "node-c".into(),
            ],
            rustfs_node: "bignlittles".into(),
            writer_a_node: "node-a".into(),
            writer_b_node: "node-b".into(),
            checker_node: "node-c".into(),
            image: "scripture-campaign:0.1.0".into(),
            scripture_image: "scripture:0.1.0".into(),
            correctness_namespace_prefix: "scripture-correctness".into(),
            notes: Vec::new(),
        }
    }

    #[test]
    fn rejects_scripture_lab_topology() {
        let mut profile = sample();
        profile.correctness_namespace_prefix = "scripture-lab".into();
        assert!(profile.validate_isolation().is_err());
    }

    #[test]
    fn notes_may_mention_forbidden_names() {
        let mut profile = sample();
        profile
            .notes
            .push("Do not point at Tracker RustFS, scripture-lab, or shared PVC.".into());
        assert!(profile.validate_isolation().is_ok());
    }

    #[test]
    fn run_namespace_is_dns1123() {
        let ns = sample().run_namespace("Memory-WP05.tranche_1");
        assert!(ns.starts_with("scripture-correctness-"));
        assert!(!ns.contains('_'));
        assert!(!ns.contains('.'));
        assert!(ns.len() <= 63);
    }

    #[test]
    fn sanitize_collapses_separators() {
        assert_eq!(sanitize_k8s_label("a__b..c"), "a-b-c");
    }
}
