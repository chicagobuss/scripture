//! One-shot empty-generation activation.
//!
//! `replace` is deliberately separate from `serve`: it observes the explicit
//! RecoveryRequired boundary, invokes only the narrow runtime primitive, and
//! exits without opening an ingress listener.

use std::error::Error;

use holylog::virtual_log::LogletId;
use scripture::{SystemClock, SystemTimer};
use scripture_runtime::{SupervisorError, VerseControlOutcome, disposition_label};

use crate::assemble;
use crate::config::ScriptureConfig;

pub async fn replace(
    config: ScriptureConfig,
    successor_loglet_id: String,
) -> Result<(), Box<dyn Error>> {
    if successor_loglet_id.trim().is_empty() {
        return Err("replace requires a non-empty --successor-loglet-id".into());
    }
    let successor = LogletId::new(successor_loglet_id.as_str())?;
    let assembled = assemble::assemble_supervisor(&config)?;
    let observed = assembled
        .node
        .start_configured(SystemClock::new(), SystemTimer::new(), 2)
        .await?;
    if assembled.node.runtime().await.is_some() {
        return Err(
            "refusing replace: this process already holds a Verse runtime; use a fresh one-shot process"
                .into(),
        );
    }
    if !matches!(observed, VerseControlOutcome::RecoveryRequired { .. }) {
        return Err(format!(
            "replace requires RecoveryRequired(MustSealAndReplace); observed disposition={}",
            disposition_label(&observed)
        )
        .into());
    }

    match assembled
        .node
        .activate_empty_open_generation(successor, SystemClock::new(), SystemTimer::new(), 2)
        .await
    {
        Ok(VerseControlOutcome::Serving) => {
            eprintln!(
                "scripture: replace ok ha_claim=false disposition=Serving owner={} advertise={} backend={} prefix={} successor_loglet_id={successor_loglet_id}",
                config.node.owner_id,
                assembled.advertise.as_str(),
                assembled.backend.label(),
                assembled.store_root,
            );
            eprintln!("scripture: exiting (no ingress); start serve separately");
            Ok(())
        }
        Ok(other) => Err(format!(
            "replace did not reach Serving; disposition={} (candidate remains inspectable if conflicting)",
            disposition_label(&other)
        )
        .into()),
        Err(SupervisorError::NonEmptyTail { tail }) => Err(format!(
            "refusing replace: durable open generation is non-empty (tail={tail})"
        )
        .into()),
        Err(SupervisorError::InvalidActivationDisposition { disposition }) => Err(format!(
            "refusing replace: expected RecoveryRequired(MustSealAndReplace), got {disposition:?}"
        )
        .into()),
        Err(error) => Err(error.into()),
    }
}
