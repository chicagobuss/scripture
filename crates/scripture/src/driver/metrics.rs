//! Owner counter snapshot visible through [`super::ChunkDriverHandle`].

/// Snapshot of owner counters. Numbers only — no actor internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriverMetrics {
    /// Declared policy bytes-at-risk bound ([`super::ChunkPolicy::bytes_at_risk`]).
    pub bytes_at_risk: usize,
    /// Bytes currently reserved (open chunk plus sealed-but-uncommitted work).
    pub reserved_bytes: usize,
    /// Chunks sealed and awaiting or undergoing append.
    pub inflight_chunks: usize,
    /// Successful dedup hits.
    pub dedup_hits: u64,
    /// Submissions admitted since construction/recovery.
    pub admitted: u64,
    /// Submissions rejected before admission.
    pub rejected: u64,
    /// True after the actor emits [`crate::trace::Event::OwnerPoisoned`].
    ///
    /// Survives while `run` continues in the poisoned drain loop, so a service
    /// can observe poison from [`super::ChunkDriverHandle::metrics`] without waiting
    /// for a later client request.
    pub poisoned: bool,
}
