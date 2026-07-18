//! Versioned YAML configuration for `scripture serve`.
//!
//! Credentials are never present in this file. They are loaded from the process
//! environment per the product secret-location contract in
//! [`scripture_runtime::resolve_credentials`].

use std::path::{Path, PathBuf};
use std::time::Duration;

use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, VerseId, WriterId,
};
use scripture_runtime::BackendProfile;
use scripture_service::VerseRuntimeConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Supported configuration schema version.
pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptureConfig {
    /// Schema version. Must equal [`CONFIG_VERSION`].
    pub version: u32,
    pub node: NodeConfig,
    pub listener: ListenerConfig,
    pub verse: VerseConfig,
    pub store: StoreConfig,
    #[serde(default)]
    pub paths: PathsConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Optional HA / Serving Authority selection. Default is legacy (Canon-only).
    #[serde(default)]
    pub ha: HaConfig,
    /// Optional authenticated admin bind (promotion only). Never secrets here.
    #[serde(default)]
    pub admin: AdminConfig,
}

/// Portable HA control-plane selection (no personal/backend secrets).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HaConfig {
    /// `legacy` (default) or `serving-authority`.
    #[serde(default)]
    pub mode: HaMode,
    /// Required under `serving-authority` for long-lived `scripture serve`.
    #[serde(default)]
    pub startup_role: Option<StartupRole>,
    /// Initial writer term for `bootstrap-if-empty` (default 1).
    #[serde(default = "default_initial_term")]
    pub initial_term: u64,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            mode: HaMode::Legacy,
            startup_role: None,
            initial_term: default_initial_term(),
        }
    }
}

fn default_initial_term() -> u64 {
    1
}

/// How `scripture serve` enters Serving Authority mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum StartupRole {
    /// Empty → Serving bootstrap in this process, then serve.
    BootstrapIfEmpty,
    /// Live standby: probes up, producer refuses committed ACK until admin promote.
    Standby,
}

/// Authenticated admin surface (promotion). Token comes from `SCRIPTURE_ADMIN_TOKEN`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    /// Bind address for the admin listener (e.g. `127.0.0.1:9200`).
    #[serde(default)]
    pub bind: Option<String>,
}

/// Whether Serving Authority gates the serve path.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum HaMode {
    /// Canon disposition only (existing non-HA product path).
    #[default]
    Legacy,
    /// Require one-record VirtualLog root Serving fence before committed ACKs.
    ServingAuthority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Exactly 16 ASCII bytes.
    pub owner_id: String,
    /// Advertised owner endpoint published into Canon fences.
    pub advertise: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerConfig {
    pub bind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerseConfig {
    pub journal_id: String,
    pub verse_id: String,
    pub cohort_id: String,
    pub writer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreConfig {
    /// Durable-store profile: `rustfs` or `r2`.
    pub backend: String,
    pub endpoint: String,
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// Exclusive object-store root prefix for this deployment (never bucket-wide).
    pub prefix: String,
}

fn default_region() -> String {
    "auto".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathsConfig {
    #[serde(default)]
    pub spool_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Optional bind for `/livez`, `/readyz`, and `/status` (read-only).
    #[serde(default)]
    pub status_bind: Option<String>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("unsupported config version {found} (expected {CONFIG_VERSION})")]
    UnsupportedVersion { found: u32 },
    #[error("{0}")]
    Invalid(String),
}

impl ScriptureConfig {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = serde_yaml::from_str(&text)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version != CONFIG_VERSION {
            return Err(ConfigError::UnsupportedVersion {
                found: self.version,
            });
        }
        BackendProfile::parse(&self.store.backend)
            .map_err(|error| ConfigError::Invalid(error.to_string()))?;
        if self.store.prefix.trim().is_empty() || self.store.prefix.contains("..") {
            return Err(ConfigError::Invalid(
                "store.prefix must be a non-empty path without '..'".into(),
            ));
        }
        if self.listener.bind.trim().is_empty() {
            return Err(ConfigError::Invalid("listener.bind is required".into()));
        }
        OwnerEndpoint::new(&self.node.advertise)
            .map_err(|error| ConfigError::Invalid(format!("node.advertise: {error}")))?;
        parse_fixed_id("node.owner_id", &self.node.owner_id)?;
        parse_fixed_id("verse.journal_id", &self.verse.journal_id)?;
        parse_fixed_id("verse.verse_id", &self.verse.verse_id)?;
        parse_fixed_id("verse.cohort_id", &self.verse.cohort_id)?;
        parse_fixed_id("verse.writer_id", &self.verse.writer_id)?;
        if self.ha.initial_term == 0 {
            return Err(ConfigError::Invalid("ha.initial_term must be >= 1".into()));
        }
        if let Some(bind) = &self.admin.bind
            && bind.trim().is_empty()
        {
            return Err(ConfigError::Invalid("admin.bind must be non-empty".into()));
        }
        Ok(())
    }

    /// Extra checks for long-lived `scripture serve` under Serving Authority.
    pub fn validate_serve_ha(&self) -> Result<(), ConfigError> {
        if self.ha.mode != HaMode::ServingAuthority {
            return Ok(());
        }
        match self.ha.startup_role {
            None => {
                return Err(ConfigError::Invalid(
                    "ha.startup_role is required for scripture serve under ha.mode: serving-authority"
                        .into(),
                ));
            }
            Some(StartupRole::Standby) => {
                if self
                    .admin
                    .bind
                    .as_ref()
                    .is_none_or(|bind| bind.trim().is_empty())
                {
                    return Err(ConfigError::Invalid(
                        "admin.bind is required for ha.startup_role: standby".into(),
                    ));
                }
            }
            Some(StartupRole::BootstrapIfEmpty) => {}
        }
        Ok(())
    }

    pub fn owner_id(&self) -> Result<OwnerId, ConfigError> {
        Ok(OwnerId::from_bytes(parse_fixed_id(
            "node.owner_id",
            &self.node.owner_id,
        )?))
    }

    pub fn advertise(&self) -> Result<OwnerEndpoint, ConfigError> {
        OwnerEndpoint::new(&self.node.advertise)
            .map_err(|error| ConfigError::Invalid(format!("node.advertise: {error}")))
    }

    pub fn backend(&self) -> Result<BackendProfile, ConfigError> {
        BackendProfile::parse(&self.store.backend)
            .map_err(|error| ConfigError::Invalid(error.to_string()))
    }

    pub fn verse_runtime_config(&self) -> Result<VerseRuntimeConfig, ConfigError> {
        Ok(VerseRuntimeConfig {
            journal_id: JournalId::from_bytes(parse_fixed_id(
                "verse.journal_id",
                &self.verse.journal_id,
            )?),
            verse_id: VerseId::from_bytes(parse_fixed_id("verse.verse_id", &self.verse.verse_id)?),
            owner_id: self.owner_id()?,
            cohort_id: CohortId::from_bytes(parse_fixed_id(
                "verse.cohort_id",
                &self.verse.cohort_id,
            )?),
            writer_id: WriterId::from_bytes(parse_fixed_id(
                "verse.writer_id",
                &self.verse.writer_id,
            )?),
            policy: default_chunk_policy(),
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 256,
        })
    }
}

fn default_chunk_policy() -> ChunkPolicy {
    ChunkPolicy {
        max_chunk_bytes: 64 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 256,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 256 * 1024,
        max_inflight_chunks: 1,
        max_uncommitted_age: Duration::from_secs(60),
        recovery_scan: RecoveryBound::new(8).expect("bound"),
    }
}

fn parse_fixed_id(field: &str, raw: &str) -> Result<[u8; 16], ConfigError> {
    let bytes = raw.as_bytes();
    if bytes.len() != 16 {
        return Err(ConfigError::Invalid(format!(
            "{field} must be exactly 16 ASCII bytes (got {})",
            bytes.len()
        )));
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(bytes);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_yaml() -> &'static str {
        r#"
version: 1
node:
  owner_id: "scripture-own-a!"
  advertise: "tcp://scripture-owner:9000"
listener:
  bind: "0.0.0.0:9000"
verse:
  journal_id: "scripture-jrnl!!"
  verse_id: "scripture-verse!"
  cohort_id: "scripture-cohrt!"
  writer_id: "scripture-wrtr!!"
store:
  backend: r2
  endpoint: "https://example.r2.cloudflarestorage.com"
  bucket: "example"
  region: auto
  prefix: "scripture/deployments/example"
metrics:
  status_bind: "127.0.0.1:9100"
"#
    }

    #[test]
    fn accepts_valid_config() {
        let config: ScriptureConfig = serde_yaml::from_str(sample_yaml()).expect("parse");
        config.validate().expect("valid");
        assert_eq!(config.node.owner_id.len(), 16);
    }

    #[test]
    fn rejects_unknown_fields() {
        let bad = sample_yaml().to_owned() + "\nextra_top_level: true\n";
        let err = serde_yaml::from_str::<ScriptureConfig>(&bad).expect_err("unknown");
        assert!(err.to_string().contains("unknown field") || err.to_string().contains("extra"));
    }

    #[test]
    fn rejects_bad_version_and_id_length() {
        let mut bad = sample_yaml().replace("version: 1", "version: 99");
        let config: ScriptureConfig = serde_yaml::from_str(&bad).expect("parse");
        assert!(matches!(
            config.validate(),
            Err(ConfigError::UnsupportedVersion { found: 99 })
        ));
        bad = sample_yaml().replace("scripture-own-a!", "too-short");
        let config: ScriptureConfig = serde_yaml::from_str(&bad).expect("parse");
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_prefix_escape() {
        let bad = sample_yaml().replace("scripture/deployments/example", "scripture/../escape");
        let config: ScriptureConfig = serde_yaml::from_str(&bad).expect("parse");
        assert!(config.validate().is_err());
    }

    #[test]
    fn accepts_serving_authority_ha_mode() {
        let yaml = sample_yaml().to_owned()
            + r#"
ha:
  mode: serving-authority
  startup_role: bootstrap-if-empty
  initial_term: 1
"#;
        let config: ScriptureConfig = serde_yaml::from_str(&yaml).expect("parse");
        config.validate().expect("valid");
        config.validate_serve_ha().expect("serve ha");
        assert_eq!(config.ha.mode, HaMode::ServingAuthority);
        assert_eq!(config.ha.startup_role, Some(StartupRole::BootstrapIfEmpty));
    }

    #[test]
    fn serving_authority_serve_requires_startup_role() {
        let yaml = sample_yaml().to_owned()
            + r#"
ha:
  mode: serving-authority
"#;
        let config: ScriptureConfig = serde_yaml::from_str(&yaml).expect("parse");
        config.validate().expect("base valid");
        assert!(config.validate_serve_ha().is_err());
    }

    #[test]
    fn standby_requires_admin_bind_for_serve() {
        let yaml = sample_yaml().to_owned()
            + r#"
ha:
  mode: serving-authority
  startup_role: standby
"#;
        let config: ScriptureConfig = serde_yaml::from_str(&yaml).expect("parse");
        assert!(config.validate_serve_ha().is_err());
        let yaml = sample_yaml().to_owned()
            + r#"
ha:
  mode: serving-authority
  startup_role: standby
admin:
  bind: "127.0.0.1:9200"
"#;
        let config: ScriptureConfig = serde_yaml::from_str(&yaml).expect("parse");
        config.validate_serve_ha().expect("standby with admin.bind");
    }
}
