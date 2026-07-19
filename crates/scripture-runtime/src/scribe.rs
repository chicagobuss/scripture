//! Multi-assignment Scribe supervisor primitives.
//!
//! A Scribe may host zero or more independent Canon/Verse assignments in one
//! process. Authority, writers, sessions, and source offsets are never shared
//! across assignments. Shared async executor, object-store client pools, and
//! credentials are permitted. There is no process-global authority grant.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use scripture::serving_authority::AuthorityKey;
use thiserror::Error;

use crate::ha_session::{HaActivationError, HaServingSession};

/// Node-wide resource bounds for a multi-assignment Scribe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScribeResourceLimits {
    /// Hard cap on configured assignments.
    pub max_assignments: usize,
    /// Aggregate admitted-but-unacked payload bytes.
    pub max_pending_bytes: usize,
    /// Aggregate admitted-but-unacked records.
    pub max_pending_records: usize,
    /// Cap on concurrent ingress connection tasks.
    pub max_concurrent_tasks: usize,
}

impl Default for ScribeResourceLimits {
    fn default() -> Self {
        Self {
            max_assignments: 16,
            max_pending_bytes: 4 * 1024 * 1024,
            max_pending_records: 1024,
            max_concurrent_tasks: 256,
        }
    }
}

/// Shared node-wide pending / concurrency budget.
///
/// Per-assignment pipelines still enforce their own caps; this budget prevents
/// one busy assignment from exhausting the whole process.
#[derive(Debug)]
pub struct NodeResourceBudget {
    limits: ScribeResourceLimits,
    pending_bytes: AtomicUsize,
    pending_records: AtomicUsize,
    concurrent_tasks: AtomicUsize,
}

impl NodeResourceBudget {
    /// Builds a budget from configured limits.
    #[must_use]
    pub fn new(limits: ScribeResourceLimits) -> Self {
        Self {
            limits,
            pending_bytes: AtomicUsize::new(0),
            pending_records: AtomicUsize::new(0),
            concurrent_tasks: AtomicUsize::new(0),
        }
    }

    /// Configured limits.
    #[must_use]
    pub fn limits(&self) -> ScribeResourceLimits {
        self.limits
    }

    /// Attempts to reserve one ingress connection task slot.
    pub fn try_acquire_task(&self) -> bool {
        let mut current = self.concurrent_tasks.load(Ordering::Relaxed);
        loop {
            if current >= self.limits.max_concurrent_tasks {
                return false;
            }
            match self.concurrent_tasks.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    /// Releases one ingress connection task slot.
    pub fn release_task(&self) {
        self.concurrent_tasks.fetch_sub(1, Ordering::AcqRel);
    }

    /// Attempts to reserve pending record/byte capacity.
    pub fn try_reserve_pending(&self, records: usize, bytes: usize) -> bool {
        // Conservative two-phase reservation; concurrent overshoot is bounded by
        // CAS retries and is acceptable for backpressure (never under-counts release).
        loop {
            let current_records = self.pending_records.load(Ordering::Relaxed);
            let current_bytes = self.pending_bytes.load(Ordering::Relaxed);
            if current_records.saturating_add(records) > self.limits.max_pending_records
                || current_bytes.saturating_add(bytes) > self.limits.max_pending_bytes
            {
                return false;
            }
            if self
                .pending_records
                .compare_exchange_weak(
                    current_records,
                    current_records + records,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }
            if self
                .pending_bytes
                .compare_exchange_weak(
                    current_bytes,
                    current_bytes + bytes,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                self.pending_records.fetch_sub(records, Ordering::AcqRel);
                continue;
            }
            return true;
        }
    }

    /// Releases previously reserved pending capacity.
    pub fn release_pending(&self, records: usize, bytes: usize) {
        self.pending_records.fetch_sub(records, Ordering::AcqRel);
        self.pending_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// True when the node budget could admit one more record of `bytes` size.
    #[must_use]
    pub fn can_admit(&self, records: usize, bytes: usize) -> bool {
        let current_records = self.pending_records.load(Ordering::Relaxed);
        let current_bytes = self.pending_bytes.load(Ordering::Relaxed);
        current_records.saturating_add(records) <= self.limits.max_pending_records
            && current_bytes.saturating_add(bytes) <= self.limits.max_pending_bytes
    }

    /// Snapshot for status / tests.
    #[must_use]
    pub fn snapshot(&self) -> NodeResourceSnapshot {
        NodeResourceSnapshot {
            pending_bytes: self.pending_bytes.load(Ordering::Relaxed),
            pending_records: self.pending_records.load(Ordering::Relaxed),
            concurrent_tasks: self.concurrent_tasks.load(Ordering::Relaxed),
            limits: self.limits,
        }
    }
}

/// Per-assignment pending ceilings (and optional floor reserved for fairness).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentResourceLimits {
    /// Max admitted-but-unacked payload bytes for this assignment.
    pub max_pending_bytes: usize,
    /// Max admitted-but-unacked records for this assignment.
    pub max_pending_records: usize,
    /// Soft floor: status reports whether usage is below this (not enforced as a hold).
    pub min_pending_bytes_floor: usize,
}

impl Default for AssignmentResourceLimits {
    fn default() -> Self {
        Self {
            max_pending_bytes: 256 * 1024,
            max_pending_records: 32,
            min_pending_bytes_floor: 0,
        }
    }
}

/// Per-assignment pending budget enforced at raw-lines admission.
#[derive(Debug)]
pub struct AssignmentResourceBudget {
    limits: AssignmentResourceLimits,
    pending_bytes: AtomicUsize,
    pending_records: AtomicUsize,
}

impl AssignmentResourceBudget {
    /// Builds an assignment budget.
    #[must_use]
    pub fn new(limits: AssignmentResourceLimits) -> Self {
        Self {
            limits,
            pending_bytes: AtomicUsize::new(0),
            pending_records: AtomicUsize::new(0),
        }
    }

    /// Configured limits.
    #[must_use]
    pub fn limits(&self) -> AssignmentResourceLimits {
        self.limits
    }

    /// Attempts to reserve pending capacity for this assignment.
    pub fn try_reserve_pending(&self, records: usize, bytes: usize) -> bool {
        loop {
            let current_records = self.pending_records.load(Ordering::Relaxed);
            let current_bytes = self.pending_bytes.load(Ordering::Relaxed);
            if current_records.saturating_add(records) > self.limits.max_pending_records
                || current_bytes.saturating_add(bytes) > self.limits.max_pending_bytes
            {
                return false;
            }
            if self
                .pending_records
                .compare_exchange_weak(
                    current_records,
                    current_records + records,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }
            if self
                .pending_bytes
                .compare_exchange_weak(
                    current_bytes,
                    current_bytes + bytes,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                self.pending_records.fetch_sub(records, Ordering::AcqRel);
                continue;
            }
            return true;
        }
    }

    /// Releases previously reserved pending capacity.
    pub fn release_pending(&self, records: usize, bytes: usize) {
        self.pending_records.fetch_sub(records, Ordering::AcqRel);
        self.pending_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// True when this assignment could admit one more record of `bytes` size.
    #[must_use]
    pub fn can_admit(&self, records: usize, bytes: usize) -> bool {
        let current_records = self.pending_records.load(Ordering::Relaxed);
        let current_bytes = self.pending_bytes.load(Ordering::Relaxed);
        current_records.saturating_add(records) <= self.limits.max_pending_records
            && current_bytes.saturating_add(bytes) <= self.limits.max_pending_bytes
    }

    /// Snapshot for status / tests.
    #[must_use]
    pub fn snapshot(&self) -> (usize, usize) {
        (
            self.pending_records.load(Ordering::Relaxed),
            self.pending_bytes.load(Ordering::Relaxed),
        )
    }
}

/// Combined node + assignment budgets wired into raw-lines admission.
#[derive(Debug, Clone)]
pub struct IngressBudgets {
    /// Process-wide budget.
    pub node: Arc<NodeResourceBudget>,
    /// Per-assignment ceiling budget.
    pub assignment: Arc<AssignmentResourceBudget>,
}

impl IngressBudgets {
    /// Builds combined budgets.
    #[must_use]
    pub fn new(node: Arc<NodeResourceBudget>, assignment: Arc<AssignmentResourceBudget>) -> Self {
        Self { node, assignment }
    }

    /// True when both budgets could admit.
    #[must_use]
    pub fn can_admit(&self, records: usize, bytes: usize) -> bool {
        self.assignment.can_admit(records, bytes) && self.node.can_admit(records, bytes)
    }

    /// Reserves on assignment then node; rolls back assignment on node failure.
    pub fn try_reserve_pending(&self, records: usize, bytes: usize) -> bool {
        if !self.assignment.try_reserve_pending(records, bytes) {
            return false;
        }
        if !self.node.try_reserve_pending(records, bytes) {
            self.assignment.release_pending(records, bytes);
            return false;
        }
        true
    }

    /// Releases both budgets.
    pub fn release_pending(&self, records: usize, bytes: usize) {
        self.assignment.release_pending(records, bytes);
        self.node.release_pending(records, bytes);
    }
}

/// Point-in-time node budget observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeResourceSnapshot {
    /// Bytes currently reserved under the node budget.
    pub pending_bytes: usize,
    /// Records currently reserved under the node budget.
    pub pending_records: usize,
    /// Ingress tasks currently held.
    pub concurrent_tasks: usize,
    /// Configured limits.
    pub limits: ScribeResourceLimits,
}

/// Startup / live disposition for one assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignmentDisposition {
    /// This process holds Serving authority for the assignment's Verse.
    Serving,
    /// Dormant candidate: no Serving authority, no warm recovery, no committed ACKs.
    Standby,
    /// Activation refused; assignment remains non-serving in this process.
    FailClosed {
        /// Operator-visible reason (no secrets).
        reason: String,
    },
}

impl AssignmentDisposition {
    /// Stable token for logs and status bodies.
    #[must_use]
    pub fn label(&self) -> &str {
        match self {
            Self::Serving => "Serving",
            Self::Standby => "Standby",
            Self::FailClosed { .. } => "FailClosed",
        }
    }
}

/// One independently activated assignment inside a Scribe process.
pub struct AssignmentRuntime {
    /// Operator display/handle id (not a durability boundary).
    pub id: String,
    /// Authority scope for this assignment (never process-global).
    pub key: AuthorityKey,
    /// Current disposition.
    pub disposition: AssignmentDisposition,
    /// Live Serving session when disposition is Serving.
    pub session: Option<Arc<HaServingSession>>,
    /// Exclusive Canon/Verse-derived store root.
    pub store_root: String,
    /// Writer route advertised for this assignment.
    pub advertise: String,
    /// Per-assignment pending budget (always present for ingress wiring).
    pub budget: Arc<AssignmentResourceBudget>,
}

impl AssignmentRuntime {
    /// Builds a Serving assignment handle.
    #[must_use]
    pub fn serving(
        id: impl Into<String>,
        key: AuthorityKey,
        session: HaServingSession,
        store_root: impl Into<String>,
        advertise: impl Into<String>,
        budget: Arc<AssignmentResourceBudget>,
    ) -> Self {
        Self {
            id: id.into(),
            key,
            disposition: AssignmentDisposition::Serving,
            session: Some(Arc::new(session)),
            store_root: store_root.into(),
            advertise: advertise.into(),
            budget,
        }
    }

    /// Builds a dormant Standby assignment handle (no Serving session).
    #[must_use]
    pub fn standby(
        id: impl Into<String>,
        key: AuthorityKey,
        store_root: impl Into<String>,
        advertise: impl Into<String>,
        budget: Arc<AssignmentResourceBudget>,
    ) -> Self {
        Self {
            id: id.into(),
            key,
            disposition: AssignmentDisposition::Standby,
            session: None,
            store_root: store_root.into(),
            advertise: advertise.into(),
            budget,
        }
    }

    /// Builds a fail-closed assignment handle after activation refusal.
    #[must_use]
    pub fn fail_closed(
        id: impl Into<String>,
        key: AuthorityKey,
        store_root: impl Into<String>,
        advertise: impl Into<String>,
        budget: Arc<AssignmentResourceBudget>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            key,
            disposition: AssignmentDisposition::FailClosed {
                reason: reason.into(),
            },
            session: None,
            store_root: store_root.into(),
            advertise: advertise.into(),
            budget,
        }
    }

    /// True when this assignment may open committed-ACK ingress.
    #[must_use]
    pub fn admits_committed_acks(&self) -> bool {
        matches!(self.disposition, AssignmentDisposition::Serving) && self.session.is_some()
    }

    /// Combined ingress budgets for this assignment under a node budget.
    #[must_use]
    pub fn ingress_budgets(&self, node: Arc<NodeResourceBudget>) -> IngressBudgets {
        IngressBudgets::new(node, Arc::clone(&self.budget))
    }
}

/// Failures while constructing a multi-assignment Scribe.
#[derive(Debug, Error)]
pub enum ScribeError {
    /// Configured assignment count exceeds the node limit.
    #[error("assignment count {count} exceeds max_assignments {max}")]
    TooManyAssignments {
        /// Configured count.
        count: usize,
        /// Configured max.
        max: usize,
    },
    /// Assignment activation failed and was not converted to fail-closed.
    #[error("assignment {id} activation failed: {source}")]
    Activation {
        /// Assignment id.
        id: String,
        /// Underlying activation error.
        #[source]
        source: HaActivationError,
    },
}

/// In-process multi-assignment Scribe state after activation.
pub struct ScribeSupervisor {
    budget: Arc<NodeResourceBudget>,
    assignments: Vec<AssignmentRuntime>,
}

impl ScribeSupervisor {
    /// Activates the provided assignment runtimes under node-wide limits.
    ///
    /// Callers perform per-assignment bootstrap/standby decisions and pass the
    /// resulting [`AssignmentRuntime`] values. This type owns isolation bookkeeping
    /// and status aggregation; it never grants process-global authority.
    pub fn from_assignments(
        limits: ScribeResourceLimits,
        assignments: Vec<AssignmentRuntime>,
    ) -> Result<Self, ScribeError> {
        if assignments.len() > limits.max_assignments {
            return Err(ScribeError::TooManyAssignments {
                count: assignments.len(),
                max: limits.max_assignments,
            });
        }
        Ok(Self {
            budget: Arc::new(NodeResourceBudget::new(limits)),
            assignments,
        })
    }

    /// Shared node budget.
    #[must_use]
    pub fn budget(&self) -> Arc<NodeResourceBudget> {
        Arc::clone(&self.budget)
    }

    /// All assignment runtimes in configuration order.
    #[must_use]
    pub fn assignments(&self) -> &[AssignmentRuntime] {
        &self.assignments
    }

    /// Lookup by assignment id.
    #[must_use]
    pub fn assignment(&self, id: &str) -> Option<&AssignmentRuntime> {
        self.assignments
            .iter()
            .find(|assignment| assignment.id == id)
    }

    /// True when at least one assignment is Serving.
    #[must_use]
    pub fn any_serving(&self) -> bool {
        self.assignments
            .iter()
            .any(|assignment| matches!(assignment.disposition, AssignmentDisposition::Serving))
    }

    /// Compact multi-assignment status body (no secrets).
    #[must_use]
    pub fn status_body(&self) -> String {
        let mut body = String::from("scribe=multi-assignment\nha_claim=false\n");
        let snap = self.budget.snapshot();
        body.push_str(&format!(
            "node_pending_bytes={}/{}\nnode_pending_records={}/{}\nnode_tasks={}/{}\n",
            snap.pending_bytes,
            snap.limits.max_pending_bytes,
            snap.pending_records,
            snap.limits.max_pending_records,
            snap.concurrent_tasks,
            snap.limits.max_concurrent_tasks
        ));
        for assignment in &self.assignments {
            let journal_bytes = assignment.key.journal_id.as_bytes();
            let verse_bytes = assignment.key.verse_id.as_bytes();
            let journal = String::from_utf8_lossy(&journal_bytes);
            let verse = String::from_utf8_lossy(&verse_bytes);
            body.push_str(&format!(
                "assignment id={} disposition={} standby_kind={} canon={} verse={} store_root={} advertise={} admits_committed_acks={}\n",
                assignment.id,
                assignment.disposition.label(),
                if matches!(assignment.disposition, AssignmentDisposition::Standby) {
                    "dormant"
                } else {
                    "n/a"
                },
                journal,
                verse,
                assignment.store_root,
                assignment.advertise,
                assignment.admits_committed_acks(),
            ));
            let (pending_records, pending_bytes) = assignment.budget.snapshot();
            body.push_str(&format!(
                "assignment_id={} pending_records={}/{} pending_bytes={}/{}\n",
                assignment.id,
                pending_records,
                assignment.budget.limits().max_pending_records,
                pending_bytes,
                assignment.budget.limits().max_pending_bytes,
            ));
            if let AssignmentDisposition::FailClosed { reason } = &assignment.disposition {
                body.push_str(&format!(
                    "assignment_id={} fail_closed_reason={reason}\n",
                    assignment.id
                ));
            }
        }
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assignment_durable_root;
    use scripture::{JournalId, VerseId};

    fn key(journal: &[u8; 16], verse: &[u8; 16]) -> AuthorityKey {
        AuthorityKey {
            journal_id: JournalId::from_bytes(*journal),
            verse_id: VerseId::from_bytes(*verse),
        }
    }

    #[test]
    fn budget_enforces_task_and_pending_caps() {
        let budget = NodeResourceBudget::new(ScribeResourceLimits {
            max_assignments: 2,
            max_pending_bytes: 100,
            max_pending_records: 2,
            max_concurrent_tasks: 1,
        });
        assert!(budget.try_acquire_task());
        assert!(!budget.try_acquire_task());
        budget.release_task();
        assert!(budget.try_acquire_task());

        assert!(budget.try_reserve_pending(1, 40));
        assert!(budget.try_reserve_pending(1, 40));
        assert!(!budget.try_reserve_pending(1, 1));
        budget.release_pending(1, 40);
        assert!(budget.try_reserve_pending(1, 40));
    }

    #[test]
    fn supervisor_status_names_authority_scope_per_assignment() {
        let budget = Arc::new(AssignmentResourceBudget::new(
            AssignmentResourceLimits::default(),
        ));
        let a = AssignmentRuntime::standby(
            "telemetry-host-a",
            key(b"telemetry-jrnl!!", b"telemetry-host-a"),
            assignment_durable_root(
                "root",
                JournalId::from_bytes(*b"telemetry-jrnl!!"),
                VerseId::from_bytes(*b"telemetry-host-a"),
            ),
            "tcp://10.0.0.1:9001",
            Arc::clone(&budget),
        );
        let b = AssignmentRuntime::fail_closed(
            "audit-ingress",
            key(b"audit-journal!!!", b"audit-ingress-0!"),
            assignment_durable_root(
                "root",
                JournalId::from_bytes(*b"audit-journal!!!"),
                VerseId::from_bytes(*b"audit-ingress-0!"),
            ),
            "tcp://10.0.0.1:9002",
            Arc::new(AssignmentResourceBudget::new(
                AssignmentResourceLimits::default(),
            )),
            "bootstrap refused: already initialized",
        );
        let scribe =
            ScribeSupervisor::from_assignments(ScribeResourceLimits::default(), vec![a, b])
                .expect("supervisor");
        let status = scribe.status_body();
        assert!(status.contains("assignment id=telemetry-host-a disposition=Standby"));
        assert!(status.contains("standby_kind=dormant"));
        assert!(status.contains("canon=telemetry-jrnl!!"));
        assert!(status.contains("verse=telemetry-host-a"));
        assert!(status.contains("advertise=tcp://10.0.0.1:9001"));
        assert!(status.contains("assignment id=audit-ingress disposition=FailClosed"));
        assert!(!status.contains("the scribe failed over"));
    }

    #[test]
    fn ingress_budgets_roll_back_assignment_when_node_full() {
        let node = Arc::new(NodeResourceBudget::new(ScribeResourceLimits {
            max_assignments: 2,
            max_pending_bytes: 50,
            max_pending_records: 1,
            max_concurrent_tasks: 1,
        }));
        let assignment = Arc::new(AssignmentResourceBudget::new(AssignmentResourceLimits {
            max_pending_bytes: 100,
            max_pending_records: 8,
            min_pending_bytes_floor: 0,
        }));
        let budgets = IngressBudgets::new(Arc::clone(&node), Arc::clone(&assignment));
        assert!(budgets.try_reserve_pending(1, 40));
        assert!(!budgets.try_reserve_pending(1, 20));
        let (records, bytes) = assignment.snapshot();
        assert_eq!(records, 1);
        assert_eq!(bytes, 40);
    }
}
