//! [`ServingAuthorityStore`] adapter over a Kubernetes `ServingAuthority` CRD.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{Client, ResourceExt};
use scripture::serving_authority::{AuthorityKey, ServingAuthorityRecord};
use scripture_service::{
    AuthoritySnapshot, AuthorityStoreFuture, CasOutcome, ServingAuthorityStore,
    ServingAuthorityStoreError, StoreVersion,
};

use crate::crd::{
    RECORD_FORMAT_V1, ServingAuthority, ServingAuthorityDisplay, ServingAuthoritySpec,
    display_from_record,
};
use crate::kube_transport::KubeServingAuthorityTransport;
use crate::name::authority_object_name;
use crate::transport::{ServingAuthorityKubeTransport, TransportError};

/// Durable Kubernetes CRD register for one Scripture authority domain namespace.
pub struct KubernetesServingAuthorityStore<T> {
    transport: T,
    namespace: String,
    /// When set, every key must derive exactly this object name.
    object_name_override: Option<String>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for KubernetesServingAuthorityStore<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KubernetesServingAuthorityStore")
            .field("namespace", &self.namespace)
            .field("object_name_override", &self.object_name_override)
            .field("transport", &self.transport)
            .finish()
    }
}

impl KubernetesServingAuthorityStore<KubeServingAuthorityTransport> {
    /// Builds a store for `namespace` using an existing Kubernetes client.
    pub fn from_client(
        client: Client,
        namespace: impl Into<String>,
        object_name_override: Option<String>,
    ) -> Result<Self, StoreConfigError> {
        let namespace = namespace.into();
        if namespace.is_empty() {
            return Err(StoreConfigError::EmptyNamespace);
        }
        if let Some(ref name) = object_name_override
            && !crate::name::is_dns1123_subdomain(name)
        {
            return Err(StoreConfigError::InvalidObjectName { name: name.clone() });
        }
        Ok(Self {
            transport: KubeServingAuthorityTransport::new(client, &namespace),
            namespace,
            object_name_override,
        })
    }

    /// In-cluster / default kubeconfig client construction.
    pub async fn try_default(
        namespace: impl Into<String>,
        object_name_override: Option<String>,
    ) -> Result<Self, StoreConfigError> {
        let client = Client::try_default()
            .await
            .map_err(|error| StoreConfigError::Client(error.to_string()))?;
        Self::from_client(client, namespace, object_name_override)
    }
}

impl<T> KubernetesServingAuthorityStore<T> {
    /// Test / injection constructor over a custom transport.
    pub fn from_transport(
        transport: T,
        namespace: impl Into<String>,
        object_name_override: Option<String>,
    ) -> Result<Self, StoreConfigError> {
        let namespace = namespace.into();
        if namespace.is_empty() {
            return Err(StoreConfigError::EmptyNamespace);
        }
        if let Some(ref name) = object_name_override
            && !crate::name::is_dns1123_subdomain(name)
        {
            return Err(StoreConfigError::InvalidObjectName { name: name.clone() });
        }
        Ok(Self {
            transport,
            namespace,
            object_name_override,
        })
    }

    fn object_name(&self, key: &AuthorityKey) -> Result<String, ServingAuthorityStoreError> {
        let derived = authority_object_name(key);
        match &self.object_name_override {
            None => Ok(derived),
            Some(configured) if configured == &derived => Ok(derived),
            Some(configured) => Err(ServingAuthorityStoreError::MalformedPayload {
                message: format!(
                    "authority_store.object_name {configured:?} does not match deterministic name {derived:?} for AuthorityKey"
                ),
            }),
        }
    }

    fn build_spec(
        record: &ServingAuthorityRecord,
    ) -> Result<ServingAuthoritySpec, ServingAuthorityStoreError> {
        let bytes =
            record
                .encode()
                .map_err(|error| ServingAuthorityStoreError::MalformedPayload {
                    message: error.to_string(),
                })?;
        Ok(ServingAuthoritySpec {
            record_format: RECORD_FORMAT_V1.to_owned(),
            record: BASE64.encode(bytes),
            display: Some(display_from_record(record)),
        })
    }

    fn decode_object(
        &self,
        expected_key: &AuthorityKey,
        expected_name: &str,
        object: ServingAuthority,
    ) -> Result<AuthoritySnapshot, ServingAuthorityStoreError> {
        if object.metadata.name.as_deref() != Some(expected_name) {
            return Err(ServingAuthorityStoreError::MalformedPayload {
                message: "ServingAuthority metadata.name does not match the requested AuthorityKey"
                    .to_owned(),
            });
        }
        if object.metadata.namespace.as_deref() != Some(self.namespace.as_str()) {
            return Err(ServingAuthorityStoreError::MalformedPayload {
                message:
                    "ServingAuthority metadata.namespace does not match the configured namespace"
                        .to_owned(),
            });
        }
        let resource_version = object.resource_version().ok_or_else(|| {
            ServingAuthorityStoreError::MalformedPayload {
                message: "ServingAuthority missing metadata.resourceVersion".to_owned(),
            }
        })?;
        if object.spec.record_format != RECORD_FORMAT_V1 {
            return Err(ServingAuthorityStoreError::MalformedPayload {
                message: format!(
                    "unsupported recordFormat {:?}; expected {RECORD_FORMAT_V1}",
                    object.spec.record_format
                ),
            });
        }
        let bytes = BASE64
            .decode(object.spec.record.as_bytes())
            .map_err(|error| ServingAuthorityStoreError::MalformedPayload {
                message: format!("invalid base64 record: {error}"),
            })?;
        let record = ServingAuthorityRecord::decode(&bytes).map_err(|error| {
            ServingAuthorityStoreError::MalformedPayload {
                message: error.to_string(),
            }
        })?;
        if record.key != *expected_key {
            return Err(ServingAuthorityStoreError::MalformedPayload {
                message: "record AuthorityKey does not match observe/CAS key".to_owned(),
            });
        }
        let expected_display = display_from_record(&record);
        let display = object.spec.display.as_ref().ok_or_else(|| {
            ServingAuthorityStoreError::MalformedPayload {
                message: "ServingAuthority is missing required derived spec.display".to_owned(),
            }
        })?;
        if !displays_agree(display, &expected_display) {
            return Err(ServingAuthorityStoreError::MalformedPayload {
                message: "spec.display disagrees with decoded canonical record".to_owned(),
            });
        }
        Ok(AuthoritySnapshot {
            version: StoreVersion::new(resource_version.into_bytes()),
            record,
        })
    }

    fn map_read_transport(error: TransportError) -> ServingAuthorityStoreError {
        match error {
            TransportError::NotFound => unreachable!("caller maps NotFound"),
            TransportError::Conflict => ServingAuthorityStoreError::Unavailable(Box::new(
                std::io::Error::other("unexpected conflict on read"),
            )),
            TransportError::Indeterminate(error) | TransportError::Unavailable(error) => {
                ServingAuthorityStoreError::Unavailable(error)
            }
        }
    }

    fn map_write_transport(
        error: TransportError,
    ) -> Result<CasOutcome, ServingAuthorityStoreError> {
        match error {
            TransportError::Conflict => Ok(CasOutcome::Conflict),
            TransportError::Indeterminate(error) => {
                Err(ServingAuthorityStoreError::Indeterminate(error))
            }
            TransportError::NotFound => Ok(CasOutcome::Conflict),
            TransportError::Unavailable(error) => {
                Err(ServingAuthorityStoreError::Unavailable(error))
            }
        }
    }
}

fn displays_agree(a: &ServingAuthorityDisplay, b: &ServingAuthorityDisplay) -> bool {
    a.journal == b.journal
        && a.verse == b.verse
        && a.state == b.state
        && a.writer_term == b.writer_term
}

/// Configuration errors when constructing the Kubernetes store.
#[derive(Debug, thiserror::Error)]
pub enum StoreConfigError {
    /// Namespace must be non-empty.
    #[error("ha.authority_store.namespace must be non-empty")]
    EmptyNamespace,
    /// Optional object name must be DNS-1123.
    #[error("ha.authority_store.object_name {name:?} is not a DNS-1123 subdomain")]
    InvalidObjectName {
        /// Rejected name.
        name: String,
    },
    /// Kubernetes client construction failed.
    #[error("kubernetes client unavailable: {0}")]
    Client(String),
}

impl<T> ServingAuthorityStore for KubernetesServingAuthorityStore<T>
where
    T: ServingAuthorityKubeTransport + 'static,
{
    fn observe(&self, key: AuthorityKey) -> AuthorityStoreFuture<'_, Option<AuthoritySnapshot>> {
        Box::pin(async move {
            let name = self.object_name(&key)?;
            match self.transport.get(&name).await {
                Ok(object) => self.decode_object(&key, &name, object).map(Some),
                Err(TransportError::NotFound) => Ok(None),
                Err(error) => Err(Self::map_read_transport(error)),
            }
        })
    }

    fn compare_and_swap(
        &self,
        key: AuthorityKey,
        expected_version: Option<StoreVersion>,
        next_record: ServingAuthorityRecord,
    ) -> AuthorityStoreFuture<'_, CasOutcome> {
        Box::pin(async move {
            if next_record.key != key {
                return Err(ServingAuthorityStoreError::MalformedPayload {
                    message: "cannot CAS record with mismatched AuthorityKey".to_owned(),
                });
            }
            let name = self.object_name(&key)?;
            let spec = Self::build_spec(&next_record)?;
            match expected_version {
                None => {
                    let object = ServingAuthority {
                        metadata: ObjectMeta {
                            name: Some(name.clone()),
                            namespace: Some(self.namespace.clone()),
                            ..Default::default()
                        },
                        spec,
                    };
                    match self.transport.create(object).await {
                        Ok(_) => Ok(CasOutcome::Applied),
                        Err(error) => Self::map_write_transport(error),
                    }
                }
                Some(version) => {
                    let resource_version =
                        std::str::from_utf8(version.as_bytes()).map_err(|_| {
                            ServingAuthorityStoreError::MalformedPayload {
                                message: "StoreVersion is not valid UTF-8 resourceVersion"
                                    .to_owned(),
                            }
                        })?;
                    let object = ServingAuthority {
                        metadata: ObjectMeta {
                            name: Some(name.clone()),
                            namespace: Some(self.namespace.clone()),
                            resource_version: Some(resource_version.to_owned()),
                            ..Default::default()
                        },
                        spec,
                    };
                    match self.transport.replace(&name, object).await {
                        Ok(_) => Ok(CasOutcome::Applied),
                        Err(error) => Self::map_write_transport(error),
                    }
                }
            }
        })
    }
}

/// Type-erased helper for CLI composition roots.
pub type DynKubernetesServingAuthorityStore =
    KubernetesServingAuthorityStore<KubeServingAuthorityTransport>;

/// Wraps a store as the shared trait object used by the coordinator.
#[must_use]
pub fn into_shared_store(
    store: DynKubernetesServingAuthorityStore,
) -> Arc<dyn ServingAuthorityStore> {
    Arc::new(store)
}
