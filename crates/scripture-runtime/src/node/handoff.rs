use super::prelude::*;
use super::{owned_with_sequencer, SupervisorError, VerseControlOutcome, VerseNodeSupervisor, VerseStore};

impl VerseNodeSupervisor {
    pub async fn replace_after_lost_sequencer<C, T>(
        &self,
        successor: LogletId,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let _control = self.control.lock().await;
        let (active, parts) = {
            let store = self.store.lock().await;
            let active = store
                .active
                .clone()
                .ok_or(SupervisorError::NoActiveLoglet { key: self.key })?;
            let parts = store
                .parts
                .get(&active)
                .cloned()
                .ok_or(SupervisorError::NoActiveLoglet { key: self.key })?;
            (active, parts)
        };

        // Observe before mutating. If another reconfigurer already moved the
        // active generation, do not seal/provision against a stale local active.
        let observed = self.virtual_log().observe_membership().await?;
        let observed_active = observed
            .state
            .active()
            .ok_or(VirtualLogError::EmptyMembership)?
            .loglet_id
            .clone();
        if observed_active != active {
            // No successor was provisioned yet; synthesize nothing to abandon.
            // Callers that pre-provisioned must retain their own receipt.
            return Err(SupervisorError::StaleActive {
                local: active,
                observed: observed_active,
            });
        }

        let historical = resolve_read_seal(parts.components(k)).await?;
        if !historical.observe_durable().await?.sealed() {
            historical.seal().await?;
        }
        let sealed_view = Arc::new(historical);
        self.resolver
            .insert_read_seal(active.clone(), Arc::clone(&sealed_view));

        let (next_parts, provisioned) = self.provision_and_install(successor.clone(), k).await?;

        let next_revision =
            observed
                .state
                .revision
                .checked_add(1)
                .ok_or(SupervisorError::RevisionOverflow {
                    revision: observed.state.revision,
                })?;
        let fence = CanonFence::new(
            next_revision,
            self.config.journal_id,
            self.config.verse_id,
            owned_with_sequencer(
                self.identity.owner_id,
                self.identity.endpoint.clone(),
                next_revision,
            ),
        );
        let ProvisionedSuccessor {
            receipt,
            writable,
            bind,
        } = provisioned;
        let outcome = self
            .virtual_log()
            .reconfigure_with_receipt(&observed, receipt, writable.as_ref(), &bind, fence.encode())
            .await?;

        match outcome {
            ReceiptReconfiguration::Applied { .. } => {
                let mut store = self.store.lock().await;
                store.parts.insert(successor.clone(), next_parts);
                store.active = Some(successor);
                self.start_verse_locked(clock, timer).await
            }
            ReceiptReconfiguration::Conflict { receipt } => {
                self.resolver.remove(&successor);
                Ok(VerseControlOutcome::ConflictNeedsInspect {
                    candidate: AbandonedProvisionCandidate {
                        receipt,
                        writable,
                        bind,
                    },
                })
            }
        }
    }

    /// Activates an empty open generation after an explicit process boundary.
    ///
    /// This is deliberately narrower than general lost-sequencer replacement:
    /// it only accepts a locally owned, open generation with durable tail zero,
    /// and only when this supervisor is not already running a Verse runtime.
    pub async fn crash_active_writer(&self) -> Result<(), SupervisorError> {
        let _control = self.control.lock().await;
        let active = self
            .store
            .lock()
            .await
            .active
            .clone()
            .ok_or(SupervisorError::NoActiveLoglet { key: self.key })?;
        self.resolver.remove(&active);
        *self.runtime.lock().await = None;
        *self.node.lock().await = None;
        Ok(())
    }

    /// Drops local VerseStore and runtime while retaining shared durable parts
    /// (process-boundary simulation when using [`SharedMemoryPartsFactory`]).
    pub async fn drop_process_local_state(&self) -> Result<(), SupervisorError> {
        let _control = self.control.lock().await;
        if let Some(active) = self.store.lock().await.active.clone() {
            self.resolver.remove(&active);
        }
        // Clear all resolver entries this process knew.
        // Historical gens may remain if shared resolver — clear known parts keys.
        let ids: Vec<LogletId> = self.store.lock().await.parts.keys().cloned().collect();
        for id in ids {
            self.resolver.remove(&id);
        }
        *self.store.lock().await = VerseStore::new();
        *self.runtime.lock().await = None;
        *self.node.lock().await = None;
        Ok(())
    }

    /// Borrow the started runtime for listener admission (cloneable Arc).
    pub async fn drain_seal_publish(
        &self,
        request: VerseHandoffRequest,
    ) -> Result<scripture_service::CanonTransitionOutcome, SupervisorError> {
        let _control = self.control.lock().await;
        let runtime = self
            .runtime
            .lock()
            .await
            .take()
            .ok_or(SupervisorError::UnknownVerse { key: self.key })?;
        let runtime = Arc::try_unwrap(runtime)
            .map_err(|_| SupervisorError::RuntimeInUse { key: self.key })?;
        match runtime.drain_seal_publish(request).await {
            Ok((runtime, outcome)) => {
                *self.runtime.lock().await = Some(Arc::new(runtime));
                Ok(outcome)
            }
            Err(failure) => {
                *self.runtime.lock().await = Some(Arc::new(failure.runtime));
                Err(SupervisorError::Handoff(failure.error))
            }
        }
    }

}
