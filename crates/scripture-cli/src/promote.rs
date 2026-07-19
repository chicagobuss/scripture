//! `scripture promote` — long-lived promote-and-serve under Serving Authority.
//!
//! Holylog soft-sequencer writables cannot cross process exit. Promote remains
//! the serving process. Authority is the VirtualLog root fence only.
//!
//! Multi-assignment configs use a targeted promote: only `--assignment ID`
//! receives promote-and-serve; siblings activate by posture (standby stays a
//! dormant candidate).

use std::error::Error;

use crate::config::{HaMode, ScriptureConfig};
use crate::ha_activate;
use crate::scribe;

pub async fn promote(
    config: ScriptureConfig,
    candidate_term: u64,
    assignment_id: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    match config.ha.mode {
        HaMode::Legacy => Err("promote requires ha.mode: serving-authority".into()),
        HaMode::ServingAuthority => {
            if config.is_multi_assignment() {
                let Some(assignment_id) = assignment_id else {
                    return Err(
                        "multi-assignment promote requires --assignment ID (targeted Verse promote; never process-global)"
                            .into(),
                    );
                };
                scribe::promote_multi_assignment(config, assignment_id, candidate_term).await
            } else {
                if assignment_id.is_some() {
                    return Err(
                        "--assignment is only valid with scribe.assignments (multi-assignment config)"
                            .into(),
                    );
                }
                ha_activate::promote_and_serve_cli(config, candidate_term).await
            }
        }
    }
}
