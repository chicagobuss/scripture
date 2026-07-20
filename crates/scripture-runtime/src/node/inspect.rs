use super::prelude::*;
use super::{
    DurableLogletParts, SupervisorError, VerseControlOutcome, VerseNodeSupervisor, VerseStore,
};

impl VerseNodeSupervisor {
    pub(super) async fn start_or_require_recovery_locked<C, T>(
        &self,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        // Same-process crash path: VerseStore still holds durable parts.
        let local = {
            let store = self.store.lock().await;
            match (
                &store.active,
                store.active.as_ref().and_then(|id| store.parts.get(id)),
            ) {
                (Some(active), Some(parts)) => Some((active.clone(), parts.clone())),
                _ => None,
            }
        };
        if let Some((active, parts)) = local {
            return self.refuse_writable(active, &parts, k).await;
        }

        match self.virtual_log().observe_membership().await {
            Err(VirtualLogError::Uninitialized) => self.start_verse_locked(clock, timer).await,
            Err(error) => Err(SupervisorError::VirtualLog(error)),
            Ok(observed) => {
                let fence = CanonFence::from_virtual_log_state(&observed.state)?;
                if fence.journal_id != self.config.journal_id
                    || fence.verse_id != self.config.verse_id
                {
                    return Err(SupervisorError::CanonIdentityMismatch {
                        fence_journal: fence.journal_id,
                        fence_verse: fence.verse_id,
                        config_journal: self.config.journal_id,
                        config_verse: self.config.verse_id,
                    });
                }

                let generations = &observed.state.generations;
                if generations.is_empty() {
                    return Err(SupervisorError::VirtualLog(
                        VirtualLogError::EmptyMembership,
                    ));
                }

                let mut store = VerseStore::new();
                let last = generations.len() - 1;
                for (index, generation) in generations.iter().enumerate() {
                    let parts = self.parts.open(&generation.loglet_id)?;
                    let view = resolve_read_seal(parts.components(k)).await?;
                    self.resolver
                        .insert_read_seal(generation.loglet_id.clone(), Arc::new(view));
                    store.parts.insert(generation.loglet_id.clone(), parts);
                    if index == last {
                        store.active = Some(generation.loglet_id.clone());
                    }
                }
                *self.store.lock().await = store;

                let active = generations[last].loglet_id.clone();
                let parts = self
                    .store
                    .lock()
                    .await
                    .parts
                    .get(&active)
                    .cloned()
                    .ok_or(SupervisorError::NoActiveLoglet { key: self.key })?;

                let locally_owned = matches!(
                    &fence.owner,
                    CanonOwner::Owned { owner_id, .. } if *owner_id == self.identity.owner_id
                );
                if locally_owned {
                    // Read/seal views are installed; refuse writable recovery.
                    return self.refuse_writable(active, &parts, k).await;
                }

                self.start_verse_locked(clock, timer).await
            }
        }
    }

    async fn refuse_writable(
        &self,
        active: LogletId,
        parts: &DurableLogletParts,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError> {
        match refuse_open_writable_reattach(active, parts.components(k)).await {
            Ok(_) => unreachable!("refuse_open_writable_reattach never returns Ok"),
            Err(error) => Ok(VerseControlOutcome::RecoveryRequired { error }),
        }
    }
}
