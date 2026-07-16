//! Typed `ServingAuthority` custom resource.
//!
//! The authoritative payload is [`ServingAuthoritySpec::record`] (base64 of the
//! canonical `ServingAuthorityRecord`). Display fields are kubectl-only and
//! must be re-derived and checked on every read/write.

use kube::CustomResource;
use schemars::JsonSchema;
use scripture::serving_authority::{AuthorityState, ServingAuthorityRecord};
use serde::{Deserialize, Serialize};

/// Canonical `spec.recordFormat` value for v1 payloads.
pub const RECORD_FORMAT_V1: &str = "scripture-serving-authority-v1";

/// Spec for one Serving Authority register object.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "scripture.dev",
    version = "v1alpha1",
    kind = "ServingAuthority",
    plural = "servingauthorities",
    shortname = "sauth",
    namespaced,
    printcolumn = r#"{"name":"State","type":"string","jsonPath":".spec.display.state"}"#,
    printcolumn = r#"{"name":"Term","type":"string","jsonPath":".spec.display.writerTerm"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ServingAuthoritySpec {
    /// Version of the base64-encoded canonical authority record.
    pub record_format: String,
    /// Base64-encoded canonical ServingAuthorityRecord bytes.
    pub record: String,
    /// Advisory copies for kubectl; never authoritative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<ServingAuthorityDisplay>,
}

/// Narrow display copies derived from the canonical record.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServingAuthorityDisplay {
    /// Journal identity (hex of fixed bytes).
    pub journal: String,
    /// Verse identity (hex of fixed bytes).
    pub verse: String,
    /// Authority state label.
    pub state: String,
    /// Active or next writer term, or empty when unassigned.
    #[serde(rename = "writerTerm")]
    pub writer_term: String,
}

/// Builds display fields from a decoded canonical record.
#[must_use]
pub fn display_from_record(record: &ServingAuthorityRecord) -> ServingAuthorityDisplay {
    let (state, writer_term) = match &record.state {
        AuthorityState::Unassigned => ("Unassigned".to_owned(), String::new()),
        AuthorityState::Transitioning { intent } => (
            "Transitioning".to_owned(),
            intent.next_writer_term.to_string(),
        ),
        AuthorityState::Serving { authority, .. } => {
            ("Serving".to_owned(), authority.writer_term.to_string())
        }
        AuthorityState::ReconciliationRequired { intent, .. } => (
            "ReconciliationRequired".to_owned(),
            intent.next_writer_term.to_string(),
        ),
    };
    ServingAuthorityDisplay {
        journal: record.key.journal_id.to_string(),
        verse: record.key.verse_id.to_string(),
        state,
        writer_term,
    }
}
