//! Read-only process status surfaces.
//!
//! Kubernetes and operators observe these results; they never grant ownership.
//! - **liveness**: process/event-loop alive
//! - **readiness**: HTTP 200 only when disposition is Serving
//! - **status**: reports actual Canon disposition

use crate::node::VerseControlOutcome;

/// Stable disposition token for status bodies and logs.
#[must_use]
pub fn disposition_label(outcome: &VerseControlOutcome) -> &'static str {
    match outcome {
        VerseControlOutcome::Serving => "Serving",
        VerseControlOutcome::Standby => "Standby",
        VerseControlOutcome::RecoveryRequired { .. } => "RecoveryRequired",
        VerseControlOutcome::ConflictNeedsInspect { .. } => "ConflictNeedsInspect",
        VerseControlOutcome::StartFailed(_) => "StartFailed",
    }
}

/// Readiness succeeds only for an independently established Serving disposition.
#[must_use]
pub fn is_ready_to_serve(outcome: &VerseControlOutcome) -> bool {
    matches!(outcome, VerseControlOutcome::Serving)
}

/// Builds a compact status body (no secrets).
#[must_use]
pub fn status_body(disposition: &str, serving: bool, standby: bool, ready: bool) -> String {
    format!(
        "disposition={disposition}\nserving={serving}\nstandby={standby}\nready={ready}\nha_claim=false\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_only_when_serving() {
        assert!(is_ready_to_serve(&VerseControlOutcome::Serving));
        assert!(!is_ready_to_serve(&VerseControlOutcome::Standby));
    }
}
