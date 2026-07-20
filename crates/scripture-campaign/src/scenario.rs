//! Scenario registry and suite selection.

use crate::Scenario;

/// Named scenario suites for autonomous campaigns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Suite {
    /// Core AtomicLog / VirtualLog scenarios (Slice 1 subset).
    Core,
    /// Composition scenarios (Slice 2 — not yet wired).
    Composition,
    /// Real backend / process resilience (Slice 3; not yet wired).
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
            Self::Composition => vec![Scenario::MultiScribeRollingRestart],
            Self::Resilience => Vec::new(),
            Self::All => {
                let mut all = core;
                all.push(Scenario::MultiScribeRollingRestart);
                all
            }
        }
    }

    /// Human-readable schedule persisted before execution.
    #[must_use]
    pub fn schedule_label(self) -> &'static str {
        match self {
            Self::Core => "core-slice1",
            Self::Composition => "composition-multi-scribe-continuity",
            Self::Resilience => "resilience-not-implemented",
            Self::All => "all-implemented",
        }
    }

    /// Whether this suite has at least one executable scenario in this build.
    #[must_use]
    pub fn is_implemented(self) -> bool {
        !self.scenarios().is_empty()
    }
}

/// Suite parse failures.
#[derive(Debug, thiserror::Error)]
pub enum SuiteError {
    /// Unknown suite token.
    #[error("unknown suite {0:?}; expected core|composition|resilience|all")]
    Unknown(String),
}

#[cfg(test)]
mod tests {
    use super::Suite;
    use crate::Scenario;

    #[test]
    fn unavailable_suites_are_not_reported_as_implemented() {
        assert!(Suite::Composition.is_implemented());
        assert!(!Suite::Resilience.is_implemented());
        assert!(Suite::Core.is_implemented());
        assert!(Suite::All.is_implemented());
        assert_eq!(
            Suite::Composition.scenarios(),
            vec![Scenario::MultiScribeRollingRestart]
        );
    }
}
