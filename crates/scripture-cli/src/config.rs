//! Versioned YAML configuration for `scripture serve`.
//!
//! Credentials are never present in this file. They are loaded from the process
//! environment per the product secret-location contract in
//! [`scripture_runtime::resolve_credentials`].
//!
//! Two shapes are accepted:
//! - **Single-assignment** (compat): top-level `listener` + `verse` (`journal_id`/`verse_id`).
//! - **Multi-assignment**: `scribe.assignments[]` with one listener and advertise per
//!   assignment (`canon`/`verse`). Top-level `listener`/`verse` must be omitted.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use scripture::{
    ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, VerseId, WriterId,
};
use scripture_runtime::{BackendProfile, assignment_durable_root};
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
    /// Required for single-assignment configs; omitted when `scribe.assignments` is set.
    #[serde(default)]
    pub listener: Option<ListenerConfig>,
    /// Required for single-assignment configs; omitted when `scribe.assignments` is set.
    #[serde(default)]
    pub verse: Option<VerseConfig>,
    pub store: StoreConfig,
    #[serde(default)]
    pub paths: PathsConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Optional HA / Serving Authority selection. Default is legacy (Canon-only).
    #[serde(default)]
    pub ha: HaConfig,
    /// Multi-assignment Scribe supervisor configuration.
    #[serde(default)]
    pub scribe: Option<ScribeConfig>,
}

/// Static multi-assignment Scribe layout for this process.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScribeConfig {
    /// Node-wide resource bounds shared across assignments.
    #[serde(default)]
    pub limits: ScribeLimits,
    /// Independent Canon/Verse assignments hosted by this Scribe.
    pub assignments: Vec<AssignmentConfig>,
}

/// Bounded node-wide controls for a multi-assignment Scribe.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScribeLimits {
    /// Hard cap on configured assignments in this process.
    #[serde(default = "default_max_assignments")]
    pub max_assignments: usize,
    /// Aggregate admitted-but-unacked payload bytes across all assignments.
    #[serde(default = "default_max_pending_bytes")]
    pub max_pending_bytes: usize,
    /// Aggregate admitted-but-unacked records across all assignments.
    #[serde(default = "default_max_pending_records")]
    pub max_pending_records: usize,
    /// Cap on concurrent ingress connection tasks across all assignments.
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,
}

impl Default for ScribeLimits {
    fn default() -> Self {
        Self {
            max_assignments: default_max_assignments(),
            max_pending_bytes: default_max_pending_bytes(),
            max_pending_records: default_max_pending_records(),
            max_concurrent_tasks: default_max_concurrent_tasks(),
        }
    }
}

fn default_max_assignments() -> usize {
    16
}
fn default_max_pending_bytes() -> usize {
    4 * 1024 * 1024
}
fn default_max_pending_records() -> usize {
    1024
}
fn default_max_concurrent_tasks() -> usize {
    256
}

/// One independent Canon/Verse assignment hosted by a Scribe.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentConfig {
    /// Operator display/handle id (not a durability boundary).
    pub id: String,
    /// Canon identity (logical stream; 16 ASCII bytes). Maps to [`JournalId`].
    pub canon: String,
    /// Verse identity (independent ordering lane; 16 ASCII bytes). Maps to [`VerseId`].
    pub verse: String,
    pub cohort_id: String,
    pub writer_id: String,
    /// Startup posture for this assignment.
    pub posture: AssignmentPosture,
    pub ingress: ListenerConfig,
    /// Advertised owner endpoint published as the writer route for this assignment.
    /// Must be reachable for this Verse; not the process-wide `node.advertise`.
    pub advertise: String,
}

/// Per-assignment startup posture under Serving Authority.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AssignmentPosture {
    /// Bootstrap Empty → Serving in this process when the root is empty.
    BootstrapIfEmpty,
    /// Hold without claiming Serving authority (no committed ACKs).
    Standby,
}

/// Portable HA control-plane selection (no personal/backend secrets).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HaConfig {
    /// `legacy` (default) or `serving-authority`.
    #[serde(default)]
    pub mode: HaMode,
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

    /// True when this config uses the multi-assignment Scribe shape.
    #[must_use]
    pub fn is_multi_assignment(&self) -> bool {
        self.scribe
            .as_ref()
            .is_some_and(|scribe| !scribe.assignments.is_empty())
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
        OwnerEndpoint::new(&self.node.advertise)
            .map_err(|error| ConfigError::Invalid(format!("node.advertise: {error}")))?;
        parse_fixed_id("node.owner_id", &self.node.owner_id)?;

        if self.is_multi_assignment() {
            self.validate_multi_assignment()?;
        } else {
            self.validate_single_assignment()?;
        }
        Ok(())
    }

    fn validate_single_assignment(&self) -> Result<(), ConfigError> {
        if self.scribe.is_some() {
            return Err(ConfigError::Invalid(
                "scribe.assignments must be non-empty when scribe is set".into(),
            ));
        }
        let listener = self.listener.as_ref().ok_or_else(|| {
            ConfigError::Invalid("listener is required for single-assignment config".into())
        })?;
        let verse = self.verse.as_ref().ok_or_else(|| {
            ConfigError::Invalid("verse is required for single-assignment config".into())
        })?;
        if listener.bind.trim().is_empty() {
            return Err(ConfigError::Invalid("listener.bind is required".into()));
        }
        parse_fixed_id("verse.journal_id", &verse.journal_id)?;
        parse_fixed_id("verse.verse_id", &verse.verse_id)?;
        parse_fixed_id("verse.cohort_id", &verse.cohort_id)?;
        parse_fixed_id("verse.writer_id", &verse.writer_id)?;
        Ok(())
    }

    fn validate_multi_assignment(&self) -> Result<(), ConfigError> {
        if self.listener.is_some() || self.verse.is_some() {
            return Err(ConfigError::Invalid(
                "multi-assignment config must omit top-level listener and verse (use scribe.assignments[].ingress / canon+verse)"
                    .into(),
            ));
        }
        if self.paths.spool_dir.is_some() {
            return Err(ConfigError::Invalid(
                "paths.spool_dir is not supported with scribe.assignments (per-assignment spool is out of scope)"
                    .into(),
            ));
        }
        let scribe = self
            .scribe
            .as_ref()
            .expect("checked by is_multi_assignment");
        if scribe.assignments.is_empty() {
            return Err(ConfigError::Invalid(
                "scribe.assignments must be non-empty".into(),
            ));
        }
        if scribe.assignments.len() > scribe.limits.max_assignments {
            return Err(ConfigError::Invalid(format!(
                "scribe.assignments length {} exceeds limits.max_assignments {}",
                scribe.assignments.len(),
                scribe.limits.max_assignments
            )));
        }
        if scribe.limits.max_assignments == 0
            || scribe.limits.max_pending_bytes == 0
            || scribe.limits.max_pending_records == 0
            || scribe.limits.max_concurrent_tasks == 0
        {
            return Err(ConfigError::Invalid(
                "scribe.limits values must be >= 1".into(),
            ));
        }
        if self.ha.mode != HaMode::ServingAuthority {
            return Err(ConfigError::Invalid(
                "scribe.assignments requires ha.mode: serving-authority".into(),
            ));
        }

        let deployment_prefix = self.store.prefix.trim().trim_end_matches('/');
        let mut ids = HashSet::new();
        let mut binds = HashSet::new();
        let mut advertises = HashSet::new();
        let mut keys = HashSet::new();
        let mut roots = HashSet::new();
        for assignment in &scribe.assignments {
            if assignment.id.trim().is_empty()
                || assignment.id.contains('/')
                || assignment.id.contains("..")
            {
                return Err(ConfigError::Invalid(format!(
                    "assignment id {:?} must be a non-empty path segment without '/' or '..'",
                    assignment.id
                )));
            }
            if !ids.insert(assignment.id.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate assignment id {:?}",
                    assignment.id
                )));
            }
            if assignment.ingress.bind.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "assignment {:?} ingress.bind is required",
                    assignment.id
                )));
            }
            if !binds.insert(assignment.ingress.bind.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate ingress.bind {:?} (one listener per assignment)",
                    assignment.ingress.bind
                )));
            }
            OwnerEndpoint::new(&assignment.advertise).map_err(|error| {
                ConfigError::Invalid(format!("assignment[{}].advertise: {error}", assignment.id))
            })?;
            if !advertises.insert(assignment.advertise.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate assignment advertise {:?} (one writer route per assignment)",
                    assignment.advertise
                )));
            }
            let canon = parse_fixed_id(
                &format!("assignment[{}].canon", assignment.id),
                &assignment.canon,
            )?;
            let verse = parse_fixed_id(
                &format!("assignment[{}].verse", assignment.id),
                &assignment.verse,
            )?;
            parse_fixed_id(
                &format!("assignment[{}].cohort_id", assignment.id),
                &assignment.cohort_id,
            )?;
            parse_fixed_id(
                &format!("assignment[{}].writer_id", assignment.id),
                &assignment.writer_id,
            )?;
            if !keys.insert((canon, verse)) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate canon/verse for assignment {:?} (durable roots are Canon/Verse-derived; renaming id alone does not change the root)",
                    assignment.id
                )));
            }
            let root = assignment_durable_root(
                &self.store.prefix,
                JournalId::from_bytes(canon),
                VerseId::from_bytes(verse),
            );
            if root.contains("..") {
                return Err(ConfigError::Invalid(format!(
                    "assignment[{}] derived store root must not contain '..'",
                    assignment.id
                )));
            }
            if !root.starts_with(deployment_prefix)
                || (root.len() > deployment_prefix.len()
                    && !root[deployment_prefix.len()..].starts_with('/'))
            {
                return Err(ConfigError::Invalid(format!(
                    "assignment[{}] derived store root escapes deployment prefix",
                    assignment.id
                )));
            }
            if !roots.insert(root) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate derived durable root for assignment {:?}",
                    assignment.id
                )));
            }
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

    /// Single-assignment verse runtime config (compat path).
    pub fn verse_runtime_config(&self) -> Result<VerseRuntimeConfig, ConfigError> {
        let verse = self.verse.as_ref().ok_or_else(|| {
            ConfigError::Invalid(
                "verse_runtime_config requires single-assignment verse section".into(),
            )
        })?;
        Ok(VerseRuntimeConfig {
            journal_id: JournalId::from_bytes(parse_fixed_id(
                "verse.journal_id",
                &verse.journal_id,
            )?),
            verse_id: VerseId::from_bytes(parse_fixed_id("verse.verse_id", &verse.verse_id)?),
            owner_id: self.owner_id()?,
            cohort_id: CohortId::from_bytes(parse_fixed_id("verse.cohort_id", &verse.cohort_id)?),
            writer_id: WriterId::from_bytes(parse_fixed_id("verse.writer_id", &verse.writer_id)?),
            policy: default_chunk_policy(),
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 256,
            dataref_blobs: None,
        })
    }

    /// Per-assignment verse runtime config (`canon`/`verse` → JournalId/VerseId).
    pub fn assignment_runtime_config(
        &self,
        assignment: &AssignmentConfig,
    ) -> Result<VerseRuntimeConfig, ConfigError> {
        Ok(VerseRuntimeConfig {
            journal_id: JournalId::from_bytes(parse_fixed_id("canon", &assignment.canon)?),
            verse_id: VerseId::from_bytes(parse_fixed_id("verse", &assignment.verse)?),
            owner_id: self.owner_id()?,
            cohort_id: CohortId::from_bytes(parse_fixed_id("cohort_id", &assignment.cohort_id)?),
            writer_id: WriterId::from_bytes(parse_fixed_id("writer_id", &assignment.writer_id)?),
            policy: default_chunk_policy(),
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 256,
            dataref_blobs: None,
        })
    }

    /// Advertised writer route for one assignment (not process-wide `node.advertise`).
    pub fn assignment_advertise(
        &self,
        assignment: &AssignmentConfig,
    ) -> Result<OwnerEndpoint, ConfigError> {
        OwnerEndpoint::new(&assignment.advertise).map_err(|error| {
            ConfigError::Invalid(format!("assignment[{}].advertise: {error}", assignment.id))
        })
    }

    /// Exclusive object-store root for one assignment (Canon/Verse-derived).
    pub fn assignment_store_root(
        &self,
        assignment: &AssignmentConfig,
    ) -> Result<String, ConfigError> {
        let journal_id = JournalId::from_bytes(parse_fixed_id("canon", &assignment.canon)?);
        let verse_id = VerseId::from_bytes(parse_fixed_id("verse", &assignment.verse)?);
        let root = assignment_durable_root(&self.store.prefix, journal_id, verse_id);
        if root.contains("..") {
            return Err(ConfigError::Invalid(format!(
                "assignment[{}] derived store root must not contain '..'",
                assignment.id
            )));
        }
        let deployment_prefix = self.store.prefix.trim().trim_end_matches('/');
        if !root.starts_with(deployment_prefix)
            || (root.len() > deployment_prefix.len()
                && !root[deployment_prefix.len()..].starts_with('/'))
        {
            return Err(ConfigError::Invalid(format!(
                "assignment[{}] derived store root escapes deployment prefix",
                assignment.id
            )));
        }
        Ok(root)
    }

    /// Single-assignment listener bind (compat path).
    pub fn listener_bind(&self) -> Result<&str, ConfigError> {
        self.listener
            .as_ref()
            .map(|listener| listener.bind.as_str())
            .ok_or_else(|| {
                ConfigError::Invalid(
                    "listener.bind requires single-assignment listener section".into(),
                )
            })
    }
}

fn default_chunk_policy() -> ChunkPolicy {
    ChunkPolicy {
        // Sized so produce-lab can measure --records-per-submission up to 1000
        // with modest payloads without rejecting the unit as SubmissionTooLarge.
        max_chunk_bytes: 256 * 1024,
        max_record_bytes: 16 * 1024,
        max_chunk_records: 1024,
        max_chunk_age: Duration::from_secs(60),
        max_buffered_bytes: 512 * 1024,
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
    let mut out = [0_u8; 16];
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

    fn multi_yaml() -> &'static str {
        r#"
version: 1
node:
  owner_id: "scripture-own-a!"
  advertise: "tcp://scripture-owner:9000"
store:
  backend: rustfs
  endpoint: "http://127.0.0.1:9000"
  bucket: "scripture"
  region: us-east-1
  prefix: "scripture/drills/example"
ha:
  mode: serving-authority
metrics:
  status_bind: "127.0.0.1:9100"
scribe:
  limits:
    max_assignments: 4
  assignments:
    - id: telemetry-host-a
      canon: "telemetry-jrnl!!"
      verse: "telemetry-host-a"
      cohort_id: "telemetry-cohrt!"
      writer_id: "telemetry-wrtr!!"
      posture: bootstrap-if-empty
      advertise: "tcp://127.0.0.1:9001"
      ingress:
        bind: "127.0.0.1:9001"
    - id: audit-ingress
      canon: "audit-journal!!!"
      verse: "audit-ingress-0!"
      cohort_id: "audit-cohort!!!!"
      writer_id: "audit-writer!!!!"
      posture: standby
      advertise: "tcp://127.0.0.1:9002"
      ingress:
        bind: "127.0.0.1:9002"
"#
    }

    #[test]
    fn accepts_valid_config() {
        let config: ScriptureConfig = serde_yaml::from_str(sample_yaml()).expect("parse");
        config.validate().expect("valid");
        assert_eq!(config.node.owner_id.len(), 16);
        assert!(!config.is_multi_assignment());
    }

    #[test]
    fn accepts_multi_assignment_config() {
        let config: ScriptureConfig = serde_yaml::from_str(multi_yaml()).expect("parse");
        config.validate().expect("valid");
        assert!(config.is_multi_assignment());
        let scribe = config.scribe.as_ref().expect("scribe");
        assert_eq!(scribe.assignments.len(), 2);
        let root = config
            .assignment_store_root(&scribe.assignments[0])
            .expect("root");
        let expected = assignment_durable_root(
            "scripture/drills/example",
            JournalId::from_bytes(*b"telemetry-jrnl!!"),
            VerseId::from_bytes(*b"telemetry-host-a"),
        );
        assert_eq!(root, expected);
        assert!(root.contains("/cv/"));
        assert!(!root.contains("/assignments/"));
    }

    #[test]
    fn rejects_mixed_single_and_multi_shape() {
        let bad = sample_yaml().to_owned()
            + r#"
ha:
  mode: serving-authority
scribe:
  assignments:
    - id: only
      canon: "telemetry-jrnl!!"
      verse: "telemetry-host-a"
      cohort_id: "telemetry-cohrt!"
      writer_id: "telemetry-wrtr!!"
      posture: standby
      advertise: "tcp://127.0.0.1:9001"
      ingress:
        bind: "127.0.0.1:9001"
"#;
        let config: ScriptureConfig = serde_yaml::from_str(&bad).expect("parse");
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        let bad = sample_yaml().to_owned() + "\nextra_top_level: true\n";
        let err = serde_yaml::from_str::<ScriptureConfig>(&bad).expect_err("unknown");
        assert!(err.to_string().contains("unknown field") || err.to_string().contains("extra"));
    }

    #[test]
    fn rejects_assignment_prefix_override() {
        let bad = multi_yaml().to_owned().replace(
            "advertise: \"tcp://127.0.0.1:9001\"",
            "advertise: \"tcp://127.0.0.1:9001\"\n      prefix: \"scripture/drills/example/custom\"",
        );
        let err = serde_yaml::from_str::<ScriptureConfig>(&bad).expect_err("unknown prefix");
        assert!(err.to_string().contains("unknown field") || err.to_string().contains("prefix"));
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
"#;
        let config: ScriptureConfig = serde_yaml::from_str(&yaml).expect("parse");
        config.validate().expect("valid");
        assert_eq!(config.ha.mode, HaMode::ServingAuthority);
    }

    #[test]
    fn rejects_duplicate_assignment_ids_and_binds() {
        let bad = multi_yaml().replace("bind: \"127.0.0.1:9002\"", "bind: \"127.0.0.1:9001\"");
        let config: ScriptureConfig = serde_yaml::from_str(&bad).expect("parse");
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_advertise() {
        let bad = multi_yaml().replace(
            "advertise: \"tcp://127.0.0.1:9002\"",
            "advertise: \"tcp://127.0.0.1:9001\"",
        );
        let config: ScriptureConfig = serde_yaml::from_str(&bad).expect("parse");
        let err = config.validate().expect_err("dup advertise");
        assert!(err.to_string().contains("duplicate assignment advertise"));
    }

    #[test]
    fn rejects_duplicate_canon_verse_even_with_different_ids() {
        // Renaming assignment id alone must not create a second durable root for
        // the same Canon/Verse — roots are Canon/Verse-derived.
        let bad = multi_yaml()
            .replace("id: audit-ingress", "id: telemetry-host-a-renamed")
            .replace("canon: \"audit-journal!!!\"", "canon: \"telemetry-jrnl!!\"")
            .replace("verse: \"audit-ingress-0!\"", "verse: \"telemetry-host-a\"");
        let config: ScriptureConfig = serde_yaml::from_str(&bad).expect("parse");
        let err = config.validate().expect_err("dup canon/verse");
        let message = err.to_string();
        assert!(
            message.contains("duplicate canon/verse"),
            "unexpected error: {message}"
        );
        assert!(message.contains("Canon/Verse-derived") || message.contains("renaming id"));
    }

    #[test]
    fn derived_root_stable_across_assignment_id_rename() {
        let config: ScriptureConfig = serde_yaml::from_str(multi_yaml()).expect("parse");
        let a = &config.scribe.as_ref().expect("scribe").assignments[0];
        let root_before = config.assignment_store_root(a).expect("root");
        let mut renamed = a.clone();
        renamed.id = "renamed-display-id".into();
        let root_after = config.assignment_store_root(&renamed).expect("root");
        assert_eq!(root_before, root_after);
    }

    #[test]
    fn rejects_invalid_assignment_advertise() {
        let bad = multi_yaml().replace("advertise: \"tcp://127.0.0.1:9001\"", "advertise: \"\"");
        let config: ScriptureConfig = serde_yaml::from_str(&bad).expect("parse");
        let err = config.validate().expect_err("bad advertise");
        assert!(err.to_string().contains("advertise"));
    }
}
