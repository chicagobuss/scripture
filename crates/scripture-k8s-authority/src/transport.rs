//! Injectable Kubernetes API seam for the Serving Authority register.
//!
//! This is not a second authority abstraction: the only public store contract
//! remains [`scripture_service::ServingAuthorityStore`]. The seam exists so
//! status/error mapping can be proven without a live cluster.

use std::future::Future;
use std::pin::Pin;

use crate::crd::ServingAuthority;

pub type TransportFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, TransportError>> + Send + 'a>>;

/// Classified Kubernetes register errors before they enter store mapping.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Object does not exist.
    #[error("ServingAuthority object not found")]
    NotFound,
    /// HTTP 409 create race or stale resourceVersion replace.
    #[error("ServingAuthority conditional write conflict")]
    Conflict,
    /// Write dispatch outcome is unknown (timeout/connection loss after send).
    #[error("ServingAuthority write outcome indeterminate: {0}")]
    Indeterminate(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// API refusal, 5xx on read, auth, or other clear failure.
    #[error("ServingAuthority API unavailable: {0}")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Narrow CRUD operations against one namespaced ServingAuthority API.
pub trait ServingAuthorityKubeTransport: Send + Sync {
    /// Current GET by exact name. Must not use watch-cache `resourceVersion=0`.
    fn get<'a>(&'a self, name: &'a str) -> TransportFuture<'a, ServingAuthority>;

    /// CREATE complete typed object.
    fn create<'a>(&'a self, object: ServingAuthority) -> TransportFuture<'a, ServingAuthority>;

    /// Full replace with exact `metadata.resourceVersion` already on `object`.
    fn replace<'a>(
        &'a self,
        name: &'a str,
        object: ServingAuthority,
    ) -> TransportFuture<'a, ServingAuthority>;
}
