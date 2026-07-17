//! Scenario registry and suite selection.

use crate::Scenario;

/// Named scenario suites for autonomous campaigns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Suite {
    /// Core AtomicLog / VirtualLog scenarios (Slice 1 subset).
    Core,
    /// Composition scenarios (Slice 2 — not yet wired).
    Composition,
    /// Real backend / process resilience (Slice 3 subset).
    Resilience,
    /// All implemented scenarios.
    All,
}

impl Suite {
    /// Parses a suite token.
    pub fn parse(raw: &str) -> Result<Self, SuiteError> {
        match raw {
            "core" => Ok(Self::Core),
            "composition" => Ok(Self::Composition),
            "resilience" => Ok(Self::Resilience),
            "all" => Ok(Self::All),
            other => Err(SuiteError::Unknown(other.to_owned())),
        }
    }

    /// Scenarios selected for this suite today.
    #[must_use]
    pub fn scenarios(self) -> Vec<Scenario> {
        let core = vec![
            Scenario::BaselineCommittedAck,
            Scenario::RootCasReplyLost,
            Scenario::WriterDiesAfterPayload,
        ];
        match self {
            Self::Core => core,
            Self::Composition => Vec::new(),
            Self::Resilience => core.clone(),
            Self::All => core,
        }
    }

    /// Human-readable schedule persisted before execution.
    #[must_use]
    pub fn schedule_label(self) -> &'static str {
        match self {
            Self::Core => "core-v1",
            Self::Composition => "composition-v1",
            Self::Resilience => "resilience-v1",
            Self::All => "all-v1",
        }
    }
}

/// Suite parse failures.
#[derive(Debug, thiserror::Error)]
pub enum SuiteError {
    /// Unknown suite token.
    #[error("unknown suite {0:?}; expected core|composition|resilience|all")]
    Unknown(String),
}
