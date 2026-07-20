//! Shared imports for VerseNodeSupervisor control-plane submodules.

pub use std::sync::Arc;

pub use holylog::provision::{OpenReattachError, refuse_open_writable_reattach, resolve_read_seal};
pub use holylog::virtual_log::{LogletId, ReceiptReconfiguration, VirtualLogError};
pub use scripture::{
    CanonFence, CanonOwner, Clock, Timer, observe_canon_authority_witnessed,
};
pub use scripture_service::{
    AbandonedProvisionCandidate, ProvisionedSuccessor, ScriptureNode, ScriptureNodeStart,
    VerseHandoffRequest,
};
