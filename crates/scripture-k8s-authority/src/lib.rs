//! Kubernetes CRD adapter for [`scripture_service::ServingAuthorityStore`].
//!
//! Kubernetes is a durable conditional register for Serving Authority only. It
//! is not Holylog Journal Foundation, health, readiness, a Lease, or an
//! election mechanism. kube SDK types stay in this crate; composition selects
//! it from the CLI root.
//!
//! # Lease / ConfigMap / status prohibition
//!
//! Authority is the base64 `spec.record` on a dedicated `ServingAuthority` CR.
//! Lease, ConfigMap, Pod/Service status, Deployment annotations, and direct
//! etcd access are rejected as authority storage.

#![allow(clippy::module_name_repetitions)]

mod crd;
mod kube_transport;
mod name;
mod store;
mod transport;

pub use crd::{
    RECORD_FORMAT_V1, ServingAuthority, ServingAuthorityDisplay, ServingAuthoritySpec,
    display_from_record,
};
pub use kube_transport::KubeServingAuthorityTransport;
pub use name::{
    NAME_DOMAIN, authority_key_fixed_bytes, authority_object_name, is_dns1123_subdomain,
};
pub use store::{
    DynKubernetesServingAuthorityStore, KubernetesServingAuthorityStore, StoreConfigError,
    into_shared_store,
};
pub use transport::{ServingAuthorityKubeTransport, TransportError, TransportFuture};

/// Validates that the deployment CRD YAML agrees with the typed adapter contract.
pub fn assert_checked_in_crd_contract(yaml: &str) -> Result<(), String> {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|error| format!("CRD YAML parse: {error}"))?;
    let group = doc
        .get("spec")
        .and_then(|spec| spec.get("group"))
        .and_then(serde_yaml::Value::as_str)
        .ok_or_else(|| "missing spec.group".to_owned())?;
    if group != "scripture.dev" {
        return Err(format!("expected group scripture.dev, got {group:?}"));
    }
    let kind = doc
        .get("spec")
        .and_then(|spec| spec.get("names"))
        .and_then(|names| names.get("kind"))
        .and_then(serde_yaml::Value::as_str)
        .ok_or_else(|| "missing names.kind".to_owned())?;
    if kind != "ServingAuthority" {
        return Err(format!("expected kind ServingAuthority, got {kind:?}"));
    }
    let versions = doc
        .get("spec")
        .and_then(|spec| spec.get("versions"))
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or_else(|| "missing spec.versions".to_owned())?;
    let v1 = versions
        .iter()
        .find(|version| version.get("name").and_then(serde_yaml::Value::as_str) == Some("v1alpha1"))
        .ok_or_else(|| "missing v1alpha1 version".to_owned())?;
    let required = yaml_get(
        v1,
        &[
            "schema",
            "openAPIV3Schema",
            "properties",
            "spec",
            "required",
        ],
    )
    .and_then(serde_yaml::Value::as_sequence)
    .ok_or_else(|| "missing spec.required".to_owned())?;
    let required_fields: Vec<&str> = required
        .iter()
        .filter_map(serde_yaml::Value::as_str)
        .collect();
    if !required_fields.contains(&"recordFormat")
        || !required_fields.contains(&"record")
        || !required_fields.contains(&"display")
    {
        return Err(format!(
            "spec.required must include recordFormat, record, and display, got {required_fields:?}"
        ));
    }
    let format_enum = yaml_get(
        v1,
        &[
            "schema",
            "openAPIV3Schema",
            "properties",
            "spec",
            "properties",
            "recordFormat",
            "enum",
        ],
    )
    .and_then(serde_yaml::Value::as_sequence)
    .ok_or_else(|| "missing recordFormat enum".to_owned())?;
    if !format_enum
        .iter()
        .any(|value| value.as_str() == Some(RECORD_FORMAT_V1))
    {
        return Err(format!(
            "recordFormat enum must include {RECORD_FORMAT_V1:?}"
        ));
    }
    let columns = v1
        .get("additionalPrinterColumns")
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or_else(|| "missing additionalPrinterColumns".to_owned())?;
    let paths: Vec<&str> = columns
        .iter()
        .filter_map(|column| column.get("jsonPath").and_then(serde_yaml::Value::as_str))
        .collect();
    if !paths.contains(&".spec.display.state") || !paths.contains(&".spec.display.writerTerm") {
        return Err(format!(
            "printer columns must include state and writerTerm display paths, got {paths:?}"
        ));
    }
    Ok(())
}

fn yaml_get<'a>(value: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a serde_yaml::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    #[test]
    fn checked_in_crd_matches_adapter_contract() {
        let yaml =
            include_str!("../../../deploy/kubernetes/serving-authority/serving-authority-crd.yaml");
        assert_checked_in_crd_contract(yaml).expect("CRD contract");
    }

    #[test]
    fn rbac_grants_register_verbs_only() {
        let yaml = include_str!("../../../deploy/kubernetes/serving-authority/rbac.yaml");
        assert!(yaml.contains("servingauthorities"));
        assert!(yaml.contains("- get"));
        assert!(yaml.contains("- create"));
        assert!(yaml.contains("- update"));
        assert!(!yaml.contains("leases"));
        assert!(!yaml.contains("configmaps"));
        assert!(!yaml.contains("pods"));
    }
}
