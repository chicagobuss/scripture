//! `scripture promote` — long-lived promote-and-serve under Serving Authority.
//!
//! Holylog soft-sequencer writables cannot cross process exit. Promote remains
//! the serving process when `ha.authority_store.kind: kubernetes` is configured.

use std::error::Error;

use crate::config::{AuthorityStoreConfig, HaMode, ScriptureConfig};
use crate::ha_activate;

pub async fn promote(config: ScriptureConfig, candidate_term: u64) -> Result<(), Box<dyn Error>> {
    match config.ha.mode {
        HaMode::Legacy => Err(
            "promote requires ha.mode: serving-authority and ha.authority_store.kind: kubernetes"
                .into(),
        ),
        HaMode::ServingAuthority => match &config.ha.authority_store {
            AuthorityStoreConfig::Kubernetes { .. } => {
                ha_activate::promote_and_serve_cli(config, candidate_term).await
            }
            AuthorityStoreConfig::Memory => Err(
                "refusing CLI promote with kind: memory — durable activation requires \
                 ha.authority_store.kind: kubernetes and stays in-process via promote-and-serve"
                    .into(),
            ),
        },
    }
}
