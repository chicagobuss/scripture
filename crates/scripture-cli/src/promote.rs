//! `scripture promote` — long-lived promote-and-serve under Serving Authority.
//!
//! Holylog soft-sequencer writables cannot cross process exit. Promote remains
//! the serving process. Authority is the VirtualLog root fence only.

use std::error::Error;

use crate::config::{HaMode, ScriptureConfig};
use crate::ha_activate;

pub async fn promote(config: ScriptureConfig, candidate_term: u64) -> Result<(), Box<dyn Error>> {
    match config.ha.mode {
        HaMode::Legacy => Err("promote requires ha.mode: serving-authority".into()),
        HaMode::ServingAuthority => {
            ha_activate::promote_and_serve_cli(config, candidate_term).await
        }
    }
}
