//! Scripture product runtime composition.
//!
//! Owns generic Verse-node supervision, durable object-store parts, credential
//! resolution, and the temporary Canon-gated ingress used for HA testing.
//! Lab-only fleet orchestration does not live here.

mod credentials;
mod ingress;
mod node;
mod object_store;
mod raw_lines;
mod status;

pub use credentials::{CredentialError, StoreCredentials, resolve_credentials};
pub use ingress::{
    serve_canon_raw_lines_connection, serve_canon_raw_lines_connection_with_metrics,
    serve_canon_raw_lines_connection_with_spool,
};
pub use node::{
    DurableLogletParts, InMemoryPartsFactory, NodeIdentity, PartsFactory, PartsFactoryError,
    ProcessLogletResolver, SharedMemoryPartsFactory, SupervisorError, VerseControlOutcome,
    VerseNodeSupervisor,
};
pub use object_store::{
    BackendProfile, ObjectStoreError, ObjectStorePartsFactory, connect_s3_compat,
};
pub use raw_lines::{
    BatchingSnapshot, RawLinesConfig, RawLinesConnectionMetrics, RawLinesConnectionSnapshot,
};
pub use status::{disposition_label, is_ready_to_serve, status_body};
