//! Producer receipt vocabulary: named durability evidence, not an ordinal scale.
//!
//! A receipt is evidence that a named durability predicate was met for one stable
//! producer event. Requests are **requirements**; Scripture may satisfy them with
//! that level or a stronger one, never with a weaker one. The satisfaction
//! relation is written down here — do not take a `min()` of enums.

use std::time::Duration;

use crate::chunk::{ChunkId, ProducerId};
use crate::model::{JournalId, RecordOffset};
use crate::spool::ProgressIdentity;

/// Public receipt requirement a producer may request (V1 surface).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptRequirement {
    /// Fsynced in one named Scribe-local spool; not a Canon commit.
    Spooled,
    /// Lawfully committed into the target Canon/Verse.
    Committed,
}

/// Achieved durability profile reported on a receipt.
///
/// `replicated_spooled` is a reserved name and must never be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AchievedProfile {
    /// One Scribe-local spool holds a durable copy.
    Spooled,
    /// Canon commit evidence exists.
    Committed,
}

/// Verse-owned floor and default for producer receipts.
///
/// Travels with the Verse so moving it between Scribes never silently weakens
/// an ACK's meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerseReceiptPolicy {
    /// Weakest requirement this Verse will ever acknowledge.
    pub minimum: ReceiptRequirement,
    /// Default when the producer does not override.
    pub default: ReceiptRequirement,
    /// Whether a producer may request [`ReceiptRequirement::Spooled`].
    pub allow_spooled: bool,
}

impl Default for VerseReceiptPolicy {
    fn default() -> Self {
        Self {
            minimum: ReceiptRequirement::Committed,
            default: ReceiptRequirement::Committed,
            allow_spooled: false,
        }
    }
}

impl VerseReceiptPolicy {
    /// Resolves the effective requirement (producer override or Verse default),
    /// then enforces the Verse floor and `allow_spooled`.
    pub fn effective_requirement(
        &self,
        requested: Option<ReceiptRequirement>,
    ) -> Result<ReceiptRequirement, ReceiptPolicyError> {
        let required = requested.unwrap_or(self.default);
        if matches!(required, ReceiptRequirement::Spooled) && !self.allow_spooled {
            return Err(ReceiptPolicyError::SpooledNotPermitted);
        }
        Ok(raise_to_floor(required, self.minimum))
    }
}

/// Raise a request so it never undercuts the Verse floor.
#[must_use]
pub fn raise_to_floor(
    requested: ReceiptRequirement,
    floor: ReceiptRequirement,
) -> ReceiptRequirement {
    match (requested, floor) {
        (_, ReceiptRequirement::Committed) => ReceiptRequirement::Committed,
        (ReceiptRequirement::Committed, _) => ReceiptRequirement::Committed,
        (ReceiptRequirement::Spooled, ReceiptRequirement::Spooled) => ReceiptRequirement::Spooled,
    }
}

/// True when `achieved` meets or exceeds `required` under the V1 relation:
///
/// ```text
/// committed  satisfies  spooled
/// spooled    does not satisfy  committed
/// ```
#[must_use]
pub fn profile_satisfies(achieved: AchievedProfile, required: ReceiptRequirement) -> bool {
    match (achieved, required) {
        (AchievedProfile::Committed, ReceiptRequirement::Committed) => true,
        (AchievedProfile::Committed, ReceiptRequirement::Spooled) => true,
        (AchievedProfile::Spooled, ReceiptRequirement::Spooled) => true,
        (AchievedProfile::Spooled, ReceiptRequirement::Committed) => false,
    }
}

/// Scribe-local spool capability. Cannot redefine a receipt's meaning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScribeSpoolCapability {
    /// Local path or identity of this spool.
    pub path: String,
    /// Maximum retained bytes.
    pub max_bytes: u64,
    /// Sync boundary before issuing `spooled`.
    pub fsync: SpoolFsyncPolicy,
    /// Behavior when capacity cannot be reserved.
    pub on_full: SpoolOnFull,
    /// Published loss budget when acknowledging below `committed`.
    ///
    /// A profile offering `spooled` without a loss budget is invalid.
    pub loss_budget: Duration,
    /// Stable Scribe identity named on `spooled` receipts.
    pub scribe_id: String,
}

/// When durable bytes become visible to a `spooled` receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpoolFsyncPolicy {
    /// `sync` after every admitted record before the ack.
    EveryRecord,
}

/// Capacity refusal policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpoolOnFull {
    /// Reject admission; never acknowledge then evict.
    Reject,
}

impl ScribeSpoolCapability {
    /// Fails closed when a below-committed profile lacks a published loss budget.
    pub fn validate(&self) -> Result<(), ReceiptPolicyError> {
        if self.loss_budget.is_zero() {
            return Err(ReceiptPolicyError::MissingLossBudget);
        }
        if self.scribe_id.trim().is_empty() {
            return Err(ReceiptPolicyError::InvalidScribeId);
        }
        if self.max_bytes == 0 {
            return Err(ReceiptPolicyError::InvalidCapacity);
        }
        Ok(())
    }
}

/// Policy / capability construction failures.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ReceiptPolicyError {
    /// Verse does not permit `spooled` requests.
    #[error("verse receipt policy does not allow spooled")]
    SpooledNotPermitted,
    /// A below-committed profile was configured without a loss budget.
    #[error("spool profile below committed requires a published loss budget")]
    MissingLossBudget,
    /// Scribe id missing for a scoped `spooled` receipt.
    #[error("spool capability requires a nonempty scribe_id")]
    InvalidScribeId,
    /// Capacity must be positive.
    #[error("spool max_bytes must be nonzero")]
    InvalidCapacity,
    /// Request cannot be met with available capability (no silent downgrade).
    #[error("receipt requirement {required:?} cannot be satisfied (achievable {achievable:?})")]
    Unsatisfiable {
        /// What the producer (after Verse policy) requires.
        required: ReceiptRequirement,
        /// Strongest profile this Scribe can offer right now.
        achievable: Option<AchievedProfile>,
    },
}

/// A `spooled` receipt: durable on one named Scribe disk, **no Canon offset**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpooledReceipt {
    /// Always [`AchievedProfile::Spooled`].
    pub profile: AchievedProfile,
    /// Scribe that holds the local copy.
    pub scribe_id: String,
    /// Stable producer event identity retained for idempotent forward.
    pub identity: ProgressIdentity,
}

impl SpooledReceipt {
    /// Constructs a scoped `spooled` receipt (no offsets by design).
    #[must_use]
    pub fn new(scribe_id: impl Into<String>, identity: ProgressIdentity) -> Self {
        Self {
            profile: AchievedProfile::Spooled,
            scribe_id: scribe_id.into(),
            identity,
        }
    }
}

/// Canon-commit evidence returned when `committed` is achieved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedReceipt {
    /// Always [`AchievedProfile::Committed`].
    pub profile: AchievedProfile,
    /// Journal that received the records.
    pub journal_id: JournalId,
    /// First dense offset.
    pub first_offset: RecordOffset,
    /// Offset after the last record.
    pub next_offset: RecordOffset,
    /// Immutable chunk identity.
    pub chunk_id: ChunkId,
    /// Holylog slot.
    pub slot: u64,
    /// Generation that accepted the append.
    pub canon_revision: u64,
    /// Stable producer identity.
    pub producer_id: ProducerId,
    /// Producer epoch.
    pub producer_epoch: u32,
    /// Producer sequence.
    pub sequence: u64,
}

/// Achieved receipt returned to a producer (reports the **achieved** profile).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerReceipt {
    /// Local spool durability only.
    Spooled(SpooledReceipt),
    /// Canon commit.
    Committed(CommittedReceipt),
}

impl ProducerReceipt {
    /// Achieved profile for satisfaction checks.
    #[must_use]
    pub fn profile(&self) -> AchievedProfile {
        match self {
            Self::Spooled(receipt) => receipt.profile,
            Self::Committed(receipt) => receipt.profile,
        }
    }

    /// True when this receipt meets `required`.
    #[must_use]
    pub fn satisfies(&self, required: ReceiptRequirement) -> bool {
        profile_satisfies(self.profile(), required)
    }

    /// Canon offsets exist only at `committed`.
    #[must_use]
    pub fn canon_offsets(&self) -> Option<(RecordOffset, RecordOffset)> {
        match self {
            Self::Spooled(_) => None,
            Self::Committed(receipt) => Some((receipt.first_offset, receipt.next_offset)),
        }
    }
}

/// Decide what ack path to take given policy, requirement, and live capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitPlan {
    /// Persist, fsync, return [`ProducerReceipt::Spooled`] immediately.
    IssueSpooled,
    /// Wait for Canon commit; never issue `spooled` for this request.
    WaitForCommitted,
}

/// Plans admission without silently weakening the requirement.
pub fn plan_admission(
    policy: &VerseReceiptPolicy,
    requested: Option<ReceiptRequirement>,
    spool: Option<&ScribeSpoolCapability>,
) -> Result<AdmitPlan, ReceiptPolicyError> {
    let required = policy.effective_requirement(requested)?;
    match required {
        ReceiptRequirement::Committed => Ok(AdmitPlan::WaitForCommitted),
        ReceiptRequirement::Spooled => {
            if let Some(capability) = spool {
                capability.validate()?;
                Ok(AdmitPlan::IssueSpooled)
            } else {
                // No spool: still serve by waiting for committed (stronger satisfies).
                Ok(AdmitPlan::WaitForCommitted)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_satisfies_spooled_but_not_converse() {
        assert!(profile_satisfies(
            AchievedProfile::Committed,
            ReceiptRequirement::Spooled
        ));
        assert!(!profile_satisfies(
            AchievedProfile::Spooled,
            ReceiptRequirement::Committed
        ));
    }

    #[test]
    fn verse_floor_raises_spooled_request_to_committed() {
        let policy = VerseReceiptPolicy {
            minimum: ReceiptRequirement::Committed,
            default: ReceiptRequirement::Committed,
            allow_spooled: true,
        };
        assert_eq!(
            policy
                .effective_requirement(Some(ReceiptRequirement::Spooled))
                .expect("allow"),
            ReceiptRequirement::Committed
        );
    }

    #[test]
    fn missing_loss_budget_fails_construction() {
        let capability = ScribeSpoolCapability {
            path: "/tmp/spool".into(),
            max_bytes: 1,
            fsync: SpoolFsyncPolicy::EveryRecord,
            on_full: SpoolOnFull::Reject,
            loss_budget: Duration::ZERO,
            scribe_id: "scribe-a".into(),
        };
        assert_eq!(
            capability.validate(),
            Err(ReceiptPolicyError::MissingLossBudget)
        );
    }
}
