//! Scripture product runtime composition.
//!
//! Owns generic Verse-node supervision, durable object-store parts, credential
//! resolution, and the temporary Canon-gated ingress used for HA testing.
//! Lab-only fleet orchestration does not live here.

mod assignment_root;
mod authority_bootstrap;
mod authority_gate;
pub mod counting_store;
mod credentials;
pub mod directory;
mod ha_session;
mod holylog_foundation;
mod ingress;
mod node;
mod object_store;
mod producer_routing;
mod raw_lines;
mod scribe;
mod status;

pub use assignment_root::assignment_durable_root;
pub use authority_bootstrap::bootstrap_authority_domain;
pub use authority_gate::{AuthorityGateDecision, AuthorityGateDenial, evaluate_authority_gate};
pub use credentials::{CredentialError, StoreCredentials, resolve_credentials};
pub use ha_session::{
    HaActivationError, HaAdmissionError, HaServingSession, bootstrap_and_serve, promote_and_serve,
    system_clocks,
};
pub use holylog_foundation::{
    DefaultFreshLogletIdPolicy, FoundationTransitionCheckpoint, FoundationTransitionObserver,
    FreshLogletIdPolicy, HolylogJournalFoundation, NoopFoundationTransitionObserver,
    owned_with_writer_term,
};
pub use ingress::{
    serve_canon_raw_lines_connection, serve_canon_raw_lines_connection_with_metrics,
    serve_canon_raw_lines_connection_with_spool, serve_ha_raw_lines_connection,
    serve_ha_raw_lines_connection_with_budgets,
};
pub use node::{
    DurableLogletParts, InMemoryPartsFactory, NodeIdentity, PartsFactory, PartsFactoryError,
    ProcessLogletResolver, SharedMemoryPartsFactory, SupervisorError, VerseControlOutcome,
    VerseNodeSupervisor,
};
pub use object_store::{
    BackendProfile, ObjectStoreError, ObjectStorePartsFactory, connect_s3_compat,
};
pub use producer_routing::{
    CommittedAck, DirectoryRouteSource, OutboundRecord, ProducerRoute, ProducerRoutingError,
    RecordId, RetryPolicy, RouteSource, RoutingProducer, resolve_route,
};
pub use raw_lines::{
    BatchingSnapshot, RawLinesConfig, RawLinesConnectionMetrics, RawLinesConnectionSnapshot,
};
pub use scribe::{
    AssignmentDisposition, AssignmentResourceBudget, AssignmentResourceLimits, AssignmentRuntime,
    IngressBudgets, NodeResourceBudget, NodeResourceSnapshot, ScribeError, ScribeResourceLimits,
    ScribeSupervisor,
};
pub use status::{disposition_label, is_ready_to_serve, status_body};
