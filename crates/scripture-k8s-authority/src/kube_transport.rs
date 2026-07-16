//! Production transport over [`kube::Api<ServingAuthority>`].

use kube::api::{Api, PostParams};
use kube::{Client, Error as KubeError};

use crate::crd::ServingAuthority;
use crate::transport::{ServingAuthorityKubeTransport, TransportError, TransportFuture};

/// Live Kubernetes API transport.
#[derive(Clone)]
pub struct KubeServingAuthorityTransport {
    api: Api<ServingAuthority>,
}

impl std::fmt::Debug for KubeServingAuthorityTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KubeServingAuthorityTransport")
            .finish_non_exhaustive()
    }
}

impl KubeServingAuthorityTransport {
    /// Namespaced API using an existing client.
    #[must_use]
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
        }
    }

    fn map_read_error(error: KubeError) -> TransportError {
        match error {
            KubeError::Api(status) if status.code == 404 => TransportError::NotFound,
            other => TransportError::Unavailable(Box::new(other)),
        }
    }

    fn map_write_error(error: KubeError) -> TransportError {
        match error {
            KubeError::Api(status) if status.code == 409 => TransportError::Conflict,
            KubeError::Api(status) if (400..500).contains(&status.code) => {
                TransportError::Unavailable(Box::new(KubeError::Api(status)))
            }
            // Timeouts, connection resets, and 5xx after dispatch cannot prove
            // whether the write landed.
            other => TransportError::Indeterminate(Box::new(other)),
        }
    }
}

impl ServingAuthorityKubeTransport for KubeServingAuthorityTransport {
    fn get<'a>(&'a self, name: &'a str) -> TransportFuture<'a, ServingAuthority> {
        let api = self.api.clone();
        Box::pin(async move { api.get(name).await.map_err(Self::map_read_error) })
    }

    fn create<'a>(&'a self, object: ServingAuthority) -> TransportFuture<'a, ServingAuthority> {
        let api = self.api.clone();
        Box::pin(async move {
            api.create(&PostParams::default(), &object)
                .await
                .map_err(Self::map_write_error)
        })
    }

    fn replace<'a>(
        &'a self,
        name: &'a str,
        object: ServingAuthority,
    ) -> TransportFuture<'a, ServingAuthority> {
        let api = self.api.clone();
        Box::pin(async move {
            api.replace(name, &PostParams::default(), &object)
                .await
                .map_err(Self::map_write_error)
        })
    }
}
