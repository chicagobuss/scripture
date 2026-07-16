//! Long-lived HA candidate activation: promote/bootstrap then serve in-process.
//!
//! Holylog forbids handing a soft-sequencer writable capability across process
//! exit. Therefore promotion and initial Serving must complete inside the
//! process that will admit committed work.

use std::sync::Arc;

use holylog::atomic::SealStatus;
use holylog::virtual_log::{ConditionalRegister, LogletResolver, VirtualLog};
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, FoundationPrecondition, JournalGenerationRef,
    ServingAuthorityRecord, WriterTerm,
};
use scripture::{Clock, ReceiptFuture, Submission, SystemClock, SystemTimer, Timer};
use scripture_service::{
    AuthorityCoordinator, CoordinatorError, LocalServingEligibility, ServingAuthorityStore,
    VerseRuntime, VerseRuntimeConfig, VerseRuntimeStartError,
};
use thiserror::Error;

use crate::authority_gate::{AuthorityGateDecision, AuthorityGateDenial, evaluate_authority_gate};
use crate::holylog_foundation::HolylogJournalFoundation;
use crate::node::ProcessLogletResolver;

/// Failures while activating an HA serving runtime in-process.
#[derive(Debug, Error)]
pub enum HaActivationError {
    /// Serving Authority / Foundation coordinator failed.
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    /// Authority gate refused admission after a successful CAS.
    #[error("effective-writer gate denied after activation: {reason:?}")]
    GateDenied {
        /// Denial detail.
        reason: AuthorityGateDenial,
    },
    /// VerseRuntime failed to start from the freshly installed capability.
    #[error(transparent)]
    Runtime(#[from] VerseRuntimeStartError),
    /// Runtime started but is not serving.
    #[error("VerseRuntime started non-serving after HA activation")]
    NotServing,
    /// Local resolver does not hold a writable for the Serving generation.
    #[error("resolver lacks writable capability for active generation {loglet}")]
    MissingWritable {
        /// Active Loglet id.
        loglet: String,
    },
}

/// A refusal at the live Serving Authority admission boundary.
#[derive(Debug, Error)]
pub enum HaAdmissionError {
    /// A current authority observation denies this process permission to admit
    /// work or return a committed acknowledgement.
    #[error("effective-writer gate denied: {reason:?}")]
    GateDenied {
        /// Why the current authority observation denied admission.
        reason: AuthorityGateDenial,
    },
    /// The underlying Canon-bound runtime refused the operation.
    #[error(transparent)]
    Runtime(#[from] scripture_service::VerseAdmitError),
}

#[derive(Clone)]
struct AuthorityAdmission {
    store: Arc<dyn ServingAuthorityStore>,
    key: AuthorityKey,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
    owner_id: scripture::OwnerId,
    generation: JournalGenerationRef,
}

impl AuthorityAdmission {
    async fn ensure_effective(&self) -> Result<(), HaAdmissionError> {
        let virtual_log = VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        );
        // Any inability to establish that the local active generation remains
        // open is a sealed fact for admission purposes. The subsequent current
        // gate observation also rejects any Foundation mismatch.
        let is_sealed = !matches!(
            virtual_log.check_tail().await,
            Ok(tail) if tail.seal_status != SealStatus::Sealed
        );
        let decision = evaluate_authority_gate(
            self.store.as_ref(),
            self.key,
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
            self.owner_id,
            self.resolver.is_writable(&self.generation.active_loglet_id),
            is_sealed,
        )
        .await;
        match decision {
            AuthorityGateDecision::EffectiveWriter { .. } => Ok(()),
            AuthorityGateDecision::Denied { reason } => {
                Err(HaAdmissionError::GateDenied { reason })
            }
        }
    }
}

/// A long-lived candidate that holds both Authority Serving and a live VerseRuntime.
pub struct HaServingSession {
    /// Confirmed Serving Authority record.
    pub record: ServingAuthorityRecord,
    runtime: Arc<VerseRuntime>,
    admission: AuthorityAdmission,
}

impl HaServingSession {
    /// Active generation named by the Serving record.
    #[must_use]
    pub fn generation(&self) -> &JournalGenerationRef {
        match &self.record.state {
            AuthorityState::Serving { authority, .. } => &authority.generation_ref,
            _ => unreachable!("HaServingSession only constructed from Serving"),
        }
    }

    /// Whether the in-process Canon runtime is live. Admission still requires a
    /// fresh Serving Authority check for every submission and acknowledgement.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        self.runtime.is_serving()
    }

    /// Returns whether this live process is still the current effective writer.
    /// Readiness and every admission path must use this rather than treating a
    /// live runtime as authority.
    pub async fn is_effective_writer(&self) -> bool {
        self.runtime.is_serving() && self.admission.ensure_effective().await.is_ok()
    }

    /// Admits one submission only while this process is the current effective
    /// writer. The returned receipt rechecks authority before it can resolve
    /// successfully, so a Transitioning record cannot yield a committed ACK.
    pub async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, HaAdmissionError> {
        self.admission.ensure_effective().await?;
        let receipt = self.runtime.submit(submission).await?;
        let admission = self.admission.clone();
        let (sender, receiver) = futures::channel::oneshot::channel();
        tokio::spawn(async move {
            let result = match receipt.await {
                Ok(receipt) => match admission.ensure_effective().await {
                    Ok(()) => Ok(receipt),
                    Err(_) => Err(scripture::DriverError::Unavailable),
                },
                Err(error) => Err(error),
            };
            let _ = sender.send(result);
        });
        Ok(ReceiptFuture::from_receiver(receiver))
    }

    /// Flushes only while this process remains the current effective writer.
    pub async fn flush(&self) -> Result<(), HaAdmissionError> {
        self.admission.ensure_effective().await?;
        self.runtime.flush().await?;
        Ok(())
    }
}

/// Bootstraps Empty → Serving, then starts a VerseRuntime in this process.
#[allow(clippy::too_many_arguments)]
pub async fn bootstrap_and_serve<C, T>(
    coordinator: &AuthorityCoordinator,
    foundation: &HolylogJournalFoundation,
    store: Arc<dyn ServingAuthorityStore>,
    key: AuthorityKey,
    initial_term: WriterTerm,
    runtime_config: VerseRuntimeConfig,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
    clock: C,
    timer: T,
) -> Result<HaServingSession, HaActivationError>
where
    C: Clock + Clone + Send + 'static,
    T: Timer + Clone + Send + 'static,
{
    let record = coordinator
        .promote(
            key,
            initial_term,
            FoundationPrecondition::Empty,
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await?;
    activate_after_serving_cas(
        foundation,
        store,
        key,
        record,
        runtime_config,
        Arc::clone(&register),
        resolver,
        clock,
        timer,
    )
    .await
}

/// Promotes Expected → Serving for an explicit candidate term, then serves here.
#[allow(clippy::too_many_arguments)]
pub async fn promote_and_serve<C, T>(
    coordinator: &AuthorityCoordinator,
    foundation: &HolylogJournalFoundation,
    store: Arc<dyn ServingAuthorityStore>,
    key: AuthorityKey,
    candidate_term: WriterTerm,
    expected: JournalGenerationRef,
    runtime_config: VerseRuntimeConfig,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
    clock: C,
    timer: T,
) -> Result<HaServingSession, HaActivationError>
where
    C: Clock + Clone + Send + 'static,
    T: Timer + Clone + Send + 'static,
{
    let record = coordinator
        .promote(
            key,
            candidate_term,
            FoundationPrecondition::Expected(expected),
            LocalServingEligibility {
                is_writable: true,
                is_sealed: false,
            },
        )
        .await?;
    activate_after_serving_cas(
        foundation,
        store,
        key,
        record,
        runtime_config,
        Arc::clone(&register),
        resolver,
        clock,
        timer,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn activate_after_serving_cas<C, T>(
    _foundation: &HolylogJournalFoundation,
    store: Arc<dyn ServingAuthorityStore>,
    key: AuthorityKey,
    record: ServingAuthorityRecord,
    runtime_config: VerseRuntimeConfig,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
    clock: C,
    timer: T,
) -> Result<HaServingSession, HaActivationError>
where
    C: Clock + Clone + Send + 'static,
    T: Timer + Clone + Send + 'static,
{
    let generation = match &record.state {
        AuthorityState::Serving { authority, .. } => authority.generation_ref.clone(),
        _ => {
            return Err(HaActivationError::NotServing);
        }
    };
    if !resolver.is_writable(&generation.active_loglet_id) {
        return Err(HaActivationError::MissingWritable {
            loglet: generation.active_loglet_id.to_string(),
        });
    }

    let gate = evaluate_authority_gate(
        store.as_ref(),
        key,
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        runtime_config.owner_id,
        true,
        false,
    )
    .await;
    match gate {
        AuthorityGateDecision::EffectiveWriter { .. } => {}
        AuthorityGateDecision::Denied { reason } => {
            return Err(HaActivationError::GateDenied { reason });
        }
    }

    let virtual_log = VirtualLog::new(
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
    );
    let runtime = VerseRuntime::start(runtime_config, virtual_log, clock, timer).await?;
    if !runtime.is_serving() {
        return Err(HaActivationError::NotServing);
    }

    // Fresh gate after runtime install.
    let gate = evaluate_authority_gate(
        store.as_ref(),
        key,
        Arc::clone(&register),
        Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        runtime.owner_id(),
        true,
        false,
    )
    .await;
    if !matches!(gate, AuthorityGateDecision::EffectiveWriter { .. }) {
        let AuthorityGateDecision::Denied { reason } = gate else {
            unreachable!()
        };
        return Err(HaActivationError::GateDenied { reason });
    }

    Ok(HaServingSession {
        admission: AuthorityAdmission {
            store,
            key,
            register,
            resolver,
            owner_id: runtime.owner_id(),
            generation,
        },
        record,
        runtime: Arc::new(runtime),
    })
}

/// Convenience clocks for tests/CLI activation.
#[must_use]
pub fn system_clocks() -> (SystemClock, SystemTimer) {
    (SystemClock::new(), SystemTimer::new())
}
