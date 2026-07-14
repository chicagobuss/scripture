//! Credential and non-secret config loading for `fleet-lab-node`.
//!
//! Secrets never appear in argv examples, logs, or summary JSON. Values may be
//! loaded from the process environment or a local env file whose path is given
//! by `--env-file`. The env file is read into an overlay map; it does not mutate
//! the process environment (workspace forbids `unsafe` / `set_var`).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::object_store_lab::{BackendProfile, ObjectStoreLabError, fleet_exercise_root};

/// Non-secret object-store endpoint settings for one backend profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreEndpointConfig {
    pub profile: BackendProfile,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub root: String,
}

/// Credential pair loaded from the environment (never logged).
#[derive(Debug, Clone)]
pub struct StoreCredentials {
    pub access_key: String,
    pub secret_key: String,
}

/// Failures while resolving env files or required credential variables.
#[derive(Debug, thiserror::Error)]
pub enum FleetConfigError {
    #[error(transparent)]
    Lab(#[from] ObjectStoreLabError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Message(String),
}

/// Loads `KEY=VALUE` lines from a local env file into an overlay map.
///
/// Values are returned only to credential/endpoint resolution; callers must not
/// log or serialize them into summaries.
pub fn load_env_file(path: &Path) -> Result<BTreeMap<String, String>, FleetConfigError> {
    let text = fs::read_to_string(path).map_err(|error| {
        FleetConfigError::Message(format!(
            "failed to read env file {}: {error}",
            path.display()
        ))
    })?;
    let mut overlay = BTreeMap::new();
    for (line_no, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(FleetConfigError::Message(format!(
                "{}:{}: expected KEY=VALUE",
                path.display(),
                line_no + 1
            )));
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(FleetConfigError::Message(format!(
                "{}:{}: empty key",
                path.display(),
                line_no + 1
            )));
        }
        overlay.insert(key.to_owned(), strip_quotes(value.trim()).to_owned());
    }
    Ok(overlay)
}

fn strip_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'')
        {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn lookup(overlay: &BTreeMap<String, String>, name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| overlay.get(name).cloned().filter(|v| !v.is_empty()))
}

/// Resolves non-secret endpoint settings for `profile` and `run_id`.
pub fn resolve_endpoint_config(
    profile: BackendProfile,
    run_id: &str,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    overlay: &BTreeMap<String, String>,
) -> Result<StoreEndpointConfig, FleetConfigError> {
    let root = fleet_exercise_root(run_id)?;
    match profile {
        BackendProfile::RustFs => Ok(StoreEndpointConfig {
            profile,
            endpoint: endpoint
                .or_else(|| lookup(overlay, "RUSTFS_ENDPOINT"))
                .unwrap_or_else(|| "http://127.0.0.1:9100".into()),
            bucket: bucket
                .or_else(|| lookup(overlay, "RUSTFS_BUCKET"))
                .unwrap_or_else(|| "holylog-rustfs".into()),
            region: region
                .or_else(|| lookup(overlay, "RUSTFS_REGION"))
                .unwrap_or_else(|| "us-east-1".into()),
            root,
        }),
        BackendProfile::CloudflareR2 => {
            let endpoint = endpoint
                .or_else(|| lookup(overlay, "R2_ENDPOINT"))
                .ok_or_else(|| {
                    FleetConfigError::Message(
                        "r2 requires --endpoint or R2_ENDPOINT (never log the value)".into(),
                    )
                })?;
            let endpoint = if endpoint.contains("://") {
                endpoint
            } else {
                format!("https://{endpoint}")
            };
            let bucket = bucket
                .or_else(|| lookup(overlay, "R2_BUCKET"))
                .ok_or_else(|| {
                    FleetConfigError::Message("r2 requires --bucket or R2_BUCKET".into())
                })?;
            let region = region
                .or_else(|| lookup(overlay, "R2_REGION"))
                .unwrap_or_else(|| "auto".into());
            Ok(StoreEndpointConfig {
                profile,
                endpoint,
                bucket,
                region,
                root,
            })
        }
    }
}

/// Loads credentials for `profile` from process env and optional env-file overlay.
pub fn resolve_credentials(
    profile: BackendProfile,
    overlay: &BTreeMap<String, String>,
) -> Result<StoreCredentials, FleetConfigError> {
    match profile {
        BackendProfile::RustFs => Ok(StoreCredentials {
            access_key: lookup(overlay, "RUSTFS_ACCESS_KEY")
                .or_else(|| lookup(overlay, "AWS_ACCESS_KEY_ID"))
                .unwrap_or_else(|| "holylog-rustfs".into()),
            secret_key: lookup(overlay, "RUSTFS_SECRET_KEY")
                .or_else(|| lookup(overlay, "AWS_SECRET_ACCESS_KEY"))
                .unwrap_or_else(|| "holylog-rustfs-local-secret".into()),
        }),
        BackendProfile::CloudflareR2 => Ok(StoreCredentials {
            access_key: require_lookup(overlay, "R2_ACCESS_KEY_ID")?,
            secret_key: require_lookup(overlay, "R2_SECRET_ACCESS_KEY")?,
        }),
    }
}

fn require_lookup(
    overlay: &BTreeMap<String, String>,
    name: &str,
) -> Result<String, FleetConfigError> {
    lookup(overlay, name).ok_or_else(|| {
        FleetConfigError::Message(format!(
            "required environment variable {name} is not set (load via --env-file or the process env; value is never logged)"
        ))
    })
}

/// Redacted preflight map for harness/status output (names only, never values).
#[must_use]
pub fn credential_presence(
    profile: BackendProfile,
    overlay: &BTreeMap<String, String>,
) -> BTreeMap<&'static str, bool> {
    let mut out = BTreeMap::new();
    match profile {
        BackendProfile::RustFs => {
            out.insert(
                "RUSTFS_ACCESS_KEY|AWS_ACCESS_KEY_ID",
                lookup(overlay, "RUSTFS_ACCESS_KEY")
                    .or_else(|| lookup(overlay, "AWS_ACCESS_KEY_ID"))
                    .is_some(),
            );
            out.insert(
                "RUSTFS_SECRET_KEY|AWS_SECRET_ACCESS_KEY",
                lookup(overlay, "RUSTFS_SECRET_KEY")
                    .or_else(|| lookup(overlay, "AWS_SECRET_ACCESS_KEY"))
                    .is_some(),
            );
        }
        BackendProfile::CloudflareR2 => {
            for name in [
                "R2_ACCESS_KEY_ID",
                "R2_SECRET_ACCESS_KEY",
                "R2_ENDPOINT",
                "R2_BUCKET",
            ] {
                out.insert(name, lookup(overlay, name).is_some());
            }
        }
    }
    out
}

/// Helper used by harness scripts: path existence without reading contents.
#[must_use]
pub fn env_file_exists(path: &Path) -> bool {
    path.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_env_file_parses_overlay_without_mutating_process_env() {
        let dir = tempfile_dir();
        let path = dir.join("lab.env");
        fs::write(
            &path,
            "FLEET_TEST_KEY=from-file\n# comment\nR2_BUCKET=bucket-a\n",
        )
        .expect("write");
        let overlay = load_env_file(&path).expect("load");
        assert_eq!(
            overlay.get("FLEET_TEST_KEY").map(String::as_str),
            Some("from-file")
        );
        assert_eq!(
            overlay.get("R2_BUCKET").map(String::as_str),
            Some("bucket-a")
        );
        assert!(std::env::var_os("FLEET_TEST_KEY").is_none());
    }

    #[test]
    fn resolve_rustfs_defaults_without_secrets_in_config() {
        let overlay = BTreeMap::new();
        let cfg =
            resolve_endpoint_config(BackendProfile::RustFs, "run-a", None, None, None, &overlay)
                .expect("cfg");
        assert_eq!(cfg.root, "scripture-fleet-exercise/run-a");
        assert_eq!(cfg.bucket, "holylog-rustfs");
        assert_eq!(cfg.profile.label(), "rustfs");
    }

    fn tempfile_dir() -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "scriptured-fleet-cfg-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("mkdir");
        path
    }
}
