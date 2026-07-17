//! Backend profiles for autonomous campaign runs.

use std::path::Path;

use serde::Deserialize;

/// Named campaign profile (operator-local topology projection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Profile {
    /// In-memory backend for fast local scenario validation.
    Memory,
    /// Home-fleet RustFS profile (k0s + existing RustFS service).
    RustFsHomeFleet(Box<RustFsHomeFleetProfile>),
}

/// Redacted home-fleet RustFS profile loaded from local operator config.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RustFsHomeFleetProfile {
    /// kubectl context name.
    pub kube_context: String,
    /// Nodes that must be Ready before execute.
    pub required_nodes: Vec<String>,
    /// Node hosting RustFS.
    pub rustfs_node: String,
    /// Namespace of the RustFS Service.
    pub rustfs_namespace: String,
    /// RustFS Service name.
    pub rustfs_service: String,
    /// S3-compatible endpoint URL (cluster DNS or port-forward).
    pub rustfs_service_dns: String,
    /// Object-store bucket.
    pub rustfs_bucket: String,
    /// S3 region label.
    #[serde(default = "default_region")]
    pub rustfs_region: String,
    /// Preferred writer A node.
    pub writer_a_node: String,
    /// Preferred writer B / recovery node.
    pub writer_b_node: String,
    /// Checker / coordinator node.
    pub checker_node: String,
    /// Campaign container image reference.
    pub image: String,
    /// Secret name holding store credentials (names only).
    pub store_secret: String,
    /// Namespace for the store secret.
    pub store_secret_namespace: String,
    /// Ephemeral campaign namespace prefix stem.
    pub correctness_namespace: String,
}

fn default_region() -> String {
    "us-east-1".to_owned()
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
