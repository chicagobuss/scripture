//! Product secret-location contract for durable-store credentials.
//!
//! Credentials are never accepted from YAML, argv, ConfigMaps, Tracker, or logs.
//! Supported sources:
//! - process environment variables (below);
//! - the same variable names provided by a Secret-mounted environment (e.g.
//!   Kubernetes `envFrom.secretRef`).
//!
//! Optional future: Secret-mounted files may be supported by populating the
//! same environment variables before process start. Values are never logged.

use crate::object_store::BackendProfile;

/// Credential pair loaded from the environment (never logged).
#[derive(Debug, Clone)]
pub struct StoreCredentials {
    pub access_key: String,
    pub secret_key: String,
}

/// Failures while resolving required credential variables.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CredentialError(String);

impl CredentialError {
    fn missing(name: &str) -> Self {
        Self(format!(
            "required environment variable {name} is not set (value is never logged)"
        ))
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Loads credentials for `profile` from the process environment.
///
/// Contract:
/// - `file`: no credentials (local directory).
/// - `rustfs`: `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY`, or AWS-compatible
///   `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (no built-in defaults).
/// - `r2`: `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` (required).
/// - `s3`: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (required; the SDK
///   profile file is deliberately not consulted).
pub fn resolve_credentials(profile: BackendProfile) -> Result<StoreCredentials, CredentialError> {
    match profile {
        BackendProfile::LocalFile => Ok(StoreCredentials {
            access_key: String::new(),
            secret_key: String::new(),
        }),
        BackendProfile::RustFs => Ok(StoreCredentials {
            access_key: env_nonempty("RUSTFS_ACCESS_KEY")
                .or_else(|| env_nonempty("AWS_ACCESS_KEY_ID"))
                .ok_or_else(|| CredentialError::missing("RUSTFS_ACCESS_KEY|AWS_ACCESS_KEY_ID"))?,
            secret_key: env_nonempty("RUSTFS_SECRET_KEY")
                .or_else(|| env_nonempty("AWS_SECRET_ACCESS_KEY"))
                .ok_or_else(|| {
                    CredentialError::missing("RUSTFS_SECRET_KEY|AWS_SECRET_ACCESS_KEY")
                })?,
        }),
        // object_store does not read ~/.aws/credentials. Without AWS_* in the
        // environment it falls through to the EC2 metadata endpoint and fails
        // after a long timeout with a retry error, which looks nothing like a
        // credential problem. Require them explicitly.
        BackendProfile::AmazonS3 => Ok(StoreCredentials {
            access_key: env_nonempty("AWS_ACCESS_KEY_ID")
                .ok_or_else(|| CredentialError::missing("AWS_ACCESS_KEY_ID"))?,
            secret_key: env_nonempty("AWS_SECRET_ACCESS_KEY")
                .ok_or_else(|| CredentialError::missing("AWS_SECRET_ACCESS_KEY"))?,
        }),
        BackendProfile::CloudflareR2 => Ok(StoreCredentials {
            access_key: env_nonempty("R2_ACCESS_KEY_ID")
                .ok_or_else(|| CredentialError::missing("R2_ACCESS_KEY_ID"))?,
            secret_key: env_nonempty("R2_SECRET_ACCESS_KEY")
                .ok_or_else(|| CredentialError::missing("R2_SECRET_ACCESS_KEY"))?,
        }),
    }
}
