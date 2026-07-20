use super::prelude::*;
use super::{owned_with_sequencer, SupervisorError, VerseControlOutcome, VerseNodeSupervisor};

impl VerseNodeSupervisor {
    pub async fn bootstrap_canon(
        &self,
        loglet_id: LogletId,
        k: u64,
    ) -> Result<(), SupervisorError> {
        let _control = self.control.lock().await;
        if self.config.owner_id != self.identity.owner_id {
            return Err(SupervisorError::OwnerMismatch {
                configured: self.config.owner_id,
                node: self.identity.owner_id,
            });
        }

        match self.virtual_log().observe_membership().await {
            Err(VirtualLogError::Uninitialized) => {}
            Ok(_) => return Err(SupervisorError::AlreadyInitialized),
            Err(error) => return Err(SupervisorError::VirtualLog(error)),
        }

        let (parts, successor) = self.provision_and_install(loglet_id.clone(), k).await?;

        let fence = CanonFence::new(
            0,
            self.config.journal_id,
            self.config.verse_id,
            owned_with_sequencer(self.identity.owner_id, self.identity.endpoint.clone(), 0),
        );
        match self
            .virtual_log()
            .bootstrap_with_receipt(
                successor.receipt,
                successor.writable.as_ref(),
                &successor.bind,
                fence.encode(),
            )
            .await
        {
            Ok(()) => {}
            Err(error) => {
                self.resolver.remove(&loglet_id);
                return Err(SupervisorError::VirtualLog(error));
            }
        }

        // One-shot product bootstrap publishes Canon then exits. Drop the
        // process-local writable install so a later `start_configured` in a
        // fresh process (or same-process test) observes durable evidence rather
        // than treating this process as a crashed local owner.
        self.resolver.remove(&loglet_id);
        let _ = parts;
        Ok(())
    }

    /// Explicit bootstrap of a brand-new Verse for in-process tests/demos:
    /// provision, publish Canon, install local active parts, then start Serving.
    ///
    /// Product CLI uses [`Self::bootstrap_canon`] (no runtime/ingress). Ordinary
    /// Serving afterward is a separate process running `start_configured`.
    pub async fn bootstrap_verse<C, T>(
        &self,
        loglet_id: LogletId,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let _control = self.control.lock().await;
        if self.config.owner_id != self.identity.owner_id {
            return Err(SupervisorError::OwnerMismatch {
                configured: self.config.owner_id,
                node: self.identity.owner_id,
            });
        }

        match self.virtual_log().observe_membership().await {
            Err(VirtualLogError::Uninitialized) => {}
            Ok(_) => return Err(SupervisorError::AlreadyInitialized),
            Err(error) => return Err(SupervisorError::VirtualLog(error)),
        }

        let (parts, successor) = self.provision_and_install(loglet_id.clone(), k).await?;

        let fence = CanonFence::new(
            0,
            self.config.journal_id,
            self.config.verse_id,
            owned_with_sequencer(self.identity.owner_id, self.identity.endpoint.clone(), 0),
        );
        match self
            .virtual_log()
            .bootstrap_with_receipt(
                successor.receipt,
                successor.writable.as_ref(),
                &successor.bind,
                fence.encode(),
            )
            .await
        {
            Ok(()) => {}
            Err(error) => {
                self.resolver.remove(&loglet_id);
                return Err(SupervisorError::VirtualLog(error));
            }
        }

        {
            let mut store = self.store.lock().await;
            store.parts.insert(loglet_id.clone(), parts);
            store.active = Some(loglet_id);
        }

        self.start_verse_locked(clock, timer).await
    }

    /// Starts from existing durable Canon evidence.
    ///
    /// Does not provision or replace. Materializes every Canon-referenced
    /// generation through [`PartsFactory::open`]. A locally owned open active
    /// generation returns [`VerseControlOutcome::RecoveryRequired`] before any
    /// owner-recovery attempt.
    pub async fn start_configured<C, T>(
        &self,
        clock: C,
        timer: T,
        k: u64,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let _control = self.control.lock().await;
        self.start_or_require_recovery_locked(clock, timer, k).await
    }

    pub async fn activate_empty_open_generation<C, T>(
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

        // Read-only disposition/tail gate. No seal, claim, or Canon mutation
        // occurs until every precondition below has passed.
        match refuse_open_writable_reattach(active.clone(), parts.components(k)).await {
            Err(OpenReattachError::MustSealAndReplace {
                observed_tail: 0, ..
            }) => {}
            Err(OpenReattachError::MustSealAndReplace { observed_tail, .. }) => {
                return Err(SupervisorError::NonEmptyTail {
                    tail: observed_tail,
                });
            }
            disposition => {
                return Err(SupervisorError::InvalidActivationDisposition { disposition });
            }
        }

        let observed = observe_canon_authority_witnessed(
            &self.virtual_log(),
            self.config.journal_id,
            self.config.verse_id,
            self.identity.owner_id,
        )
        .await?;

        // A live Serving/Standby runtime means this is not the explicit
        // post-bootstrap RecoveryRequired process boundary.
        if self.runtime.lock().await.is_some() {
            return Err(SupervisorError::RuntimeInUse { key: self.key });
        }
        let observed_active = observed
            .observed()
            .state
            .active()
            .ok_or(VirtualLogError::EmptyMembership)?
            .loglet_id
            .clone();
        if observed_active != active {
            return Err(SupervisorError::StaleActive {
                local: active,
                observed: observed_active,
            });
        }
        observed.validate()?;

        let historical = resolve_read_seal(parts.components(k)).await?;
        if !historical.observe_durable().await?.sealed() {
            historical.seal().await?;
        }
        self.resolver
            .insert_read_seal(active.clone(), Arc::new(historical));

        let next_parts = self.parts.fresh(&successor)?;
        let namespaces = self.parts.namespaces(&successor)?;
        let bind = Self::bind_for(&successor);
        let (receipt, writable) = self
            .authority
            .provision_fresh(
                successor.clone(),
                namespaces,
                bind.clone(),
                next_parts.components(k),
            )
            .await?;
        let writable = Arc::new(writable);
        let next_revision =
            observed
                .revision()
                .checked_add(1)
                .ok_or(SupervisorError::RevisionOverflow {
                    revision: observed.revision(),
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
        match self
            .virtual_log()
            .reconfigure_with_receipt(
                observed.observed(),
                receipt,
                writable.as_ref(),
                &bind,
                fence.encode(),
            )
            .await?
        {
            ReceiptReconfiguration::Applied { .. } => {
                self.resolver
                    .insert_writable(successor.clone(), Arc::clone(&writable));
                let mut store = self.store.lock().await;
                store.parts.insert(successor.clone(), next_parts);
                store.active = Some(successor);
                self.start_verse_locked(clock, timer).await
            }
            ReceiptReconfiguration::Conflict { receipt } => {
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

    /// Simulates losing in-process writer handles for the active Loglet.
    pub(super) async fn start_verse_locked<C, T>(
        &self,
        clock: C,
        timer: T,
    ) -> Result<VerseControlOutcome, SupervisorError>
    where
        C: Clock + Clone + Send + 'static,
        T: Timer + Clone + Send + 'static,
    {
        let started = ScriptureNode::start(
            vec![self.config.clone()],
            |_| self.virtual_log(),
            clock,
            timer,
        )
        .await?;
        self.install_start(started).await
    }

    async fn install_start(
        &self,
        started: ScriptureNodeStart,
    ) -> Result<VerseControlOutcome, SupervisorError> {
        if let Some(error) = started.failures.into_values().next() {
            return Ok(VerseControlOutcome::StartFailed(error));
        }
        let mut runtimes_map = started.runtimes;
        let Some(runtime) = runtimes_map.remove(&self.key) else {
            return Err(SupervisorError::UnknownVerse { key: self.key });
        };
        let serving = runtime.is_serving();
        *self.runtime.lock().await = Some(Arc::new(runtime));
        *self.node.lock().await = Some(ScriptureNode::from_started(runtimes_map));
        Ok(if serving {
            VerseControlOutcome::Serving
        } else {
            VerseControlOutcome::Standby
        })
    }
}
