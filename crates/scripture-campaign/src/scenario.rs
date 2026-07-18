//! Scenario registry and suite selection.

use crate::Scenario;

/// Named scenario suites for autonomous campaigns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Suite {
    /// Core AtomicLog / VirtualLog / Scripture scenarios.
    Core,
    /// Composition scenarios (striped / quorum / K-window AtomicLog).
    Composition,
    /// Real backend / process resilience (ephemeral in-namespace RustFS).
    Resilience,
    /// All implemented scenarios for the active profile.
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
            Scenario::KWindowDelayedCompletion,
            Scenario::KWindowPermanentWedgeSeal,
        ];
        let composition = vec![
            Scenario::KWindowDelayedCompletion,
            Scenario::KWindowPermanentWedgeSeal,
            Scenario::PermanentWedgeSealSuccessor,
            Scenario::SealTailRace,
            Scenario::StripedModuloMapping,
            Scenario::StripedLaggingScanReconstruction,
            Scenario::QuorumPartialWriteNotGlobal,
            Scenario::QuorumRepairUnavailability,
            Scenario::NestedStripeQuorumSchedules,
        ];
        // Sole real resilience claim until 12/14–17 are rebuilt on this path.
        let resilience = vec![Scenario::RawLinesAbCutover];
        match self {
            Self::Core => core,
            Self::Composition => composition,
            Self::Resilience => resilience,
            Self::All => {
                let mut all = core;
                for scenario in [
                    Scenario::PermanentWedgeSealSuccessor,
                    Scenario::SealTailRace,
                    Scenario::StripedModuloMapping,
                    Scenario::StripedLaggingScanReconstruction,
                    Scenario::QuorumPartialWriteNotGlobal,
                    Scenario::QuorumRepairUnavailability,
                    Scenario::NestedStripeQuorumSchedules,
                ] {
                    if !all.contains(&scenario) {
                        all.push(scenario);
                    }
                }
                all
            }
        }
    }

    /// Human-readable schedule persisted before execution.
    #[must_use]
    pub fn schedule_label(self) -> &'static str {
        match self {
            Self::Core => "core-wp05",
            Self::Composition => "composition-wp05",
            Self::Resilience => "resilience-wp05-raw-lines-ab-cutover",
            Self::All => "all-implemented-wp05",
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

    #[test]
    fn resilience_is_implemented_after_wp05_v3() {
        assert!(Suite::Composition.is_implemented());
        assert!(Suite::Resilience.is_implemented());
        assert!(Suite::Core.is_implemented());
        assert!(Suite::All.is_implemented());
    }
}
