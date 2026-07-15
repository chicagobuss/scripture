//! `scripture bootstrap` — one-shot greenfield Canon publication.
//!
//! Validates config, provisions and publishes only when Canon is uninitialized,
//! then exits. Never opens an ingress listener.

use std::error::Error;

use holylog::virtual_log::LogletId;
use scripture_runtime::SupervisorError;

use crate::assemble;
use crate::config::ScriptureConfig;

pub async fn bootstrap(config: ScriptureConfig, loglet_id: String) -> Result<(), Box<dyn Error>> {
    if loglet_id.trim().is_empty() {
        return Err("bootstrap requires a non-empty --loglet-id".into());
    }
    let loglet = LogletId::new(loglet_id.as_str())?;
    let assembled = assemble::assemble_supervisor(&config)?;
    match assembled.node.bootstrap_canon(loglet, 2).await {
        Ok(()) => {
            eprintln!(
                "scripture: bootstrap ok ha_claim=false owner={} advertise={} backend={} prefix={} loglet_id={loglet_id}",
                config.node.owner_id,
                assembled.advertise.as_str(),
                assembled.backend.label(),
                assembled.store_root,
            );
            eprintln!("scripture: exiting (no ingress). Start with: scripture serve --config …");
            Ok(())
        }
        Err(SupervisorError::AlreadyInitialized) => Err(
            "Canon already initialized; refusing bootstrap (no provision or publish performed)"
                .into(),
        ),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use holylog::virtual_log::{ConditionalRegister, InMemoryConditionalRegister, LogletId};
    use scripture::{
        ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, RecoveryBound, SystemClock,
        VerseId, WriterId,
    };
    use scripture_runtime::{
        NodeIdentity, ProcessLogletResolver, SharedMemoryPartsFactory, SupervisorError,
        VerseControlOutcome, VerseNodeSupervisor,
    };
    use scripture_service::VerseRuntimeConfig;

    fn config(owner: OwnerId) -> VerseRuntimeConfig {
        VerseRuntimeConfig {
            journal_id: JournalId::from_bytes(*b"boot-cmd-jrnl!!!"),
            verse_id: VerseId::from_bytes(*b"boot-cmd-verse!!"),
            owner_id: owner,
            cohort_id: CohortId::from_bytes(*b"boot-cmd-cohort!"),
            writer_id: WriterId::from_bytes(*b"boot-cmd-writer!"),
            policy: ChunkPolicy {
                max_chunk_bytes: 64 * 1024,
                max_record_bytes: 16 * 1024,
                max_chunk_records: 8,
                max_chunk_age: std::time::Duration::from_secs(60),
                max_buffered_bytes: 64 * 1024,
                max_inflight_chunks: 1,
                max_uncommitted_age: std::time::Duration::from_secs(60),
                recovery_scan: RecoveryBound::new(8).expect("bound"),
            },
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 16,
        }
    }

    #[tokio::test]
    async fn bootstrap_once_then_serve_observes_canon() {
        let register = Arc::new(InMemoryConditionalRegister::new());
        let parts = Arc::new(SharedMemoryPartsFactory::default());
        let owner = OwnerId::from_bytes(*b"boot-cmd-own-a!!");
        let resolver = Arc::new(ProcessLogletResolver::default());
        let node = VerseNodeSupervisor::with_parts_factory(
            NodeIdentity {
                owner_id: owner,
                endpoint: OwnerEndpoint::new("tcp://owner.local:9000").expect("ep"),
            },
            Arc::clone(&register) as Arc<dyn ConditionalRegister>,
            Arc::clone(&resolver),
            Arc::clone(&parts) as Arc<dyn scripture_runtime::PartsFactory>,
            config(owner),
        );
        let loglet = LogletId::new("gen-a0").expect("id");
        node.bootstrap_canon(loglet.clone(), 2)
            .await
            .expect("first bootstrap");
        // No runtime / ingress was started by bootstrap_canon.
        assert!(node.runtime().await.is_none());

        let err = node
            .bootstrap_canon(LogletId::new("gen-a1").expect("id"), 2)
            .await
            .expect_err("second bootstrap must fail closed");
        assert!(matches!(err, SupervisorError::AlreadyInitialized));

        let outcome = node
            .start_configured(SystemClock::new(), scripture::SystemTimer::new(), 2)
            .await
            .expect("observe after bootstrap");
        assert!(
            matches!(outcome, VerseControlOutcome::RecoveryRequired { .. }),
            "open generation after bootstrap exit requires seal-and-replace; ordinary serve must not invent it: {outcome:?}"
        );
        // Product serve does not take over. Recovery/replace remains undecided.
        assert!(node.runtime().await.is_none());
    }
}
