//! Automatic same-Verse Scribe lifecycle: observe, join, recover, rejoin.
//!
//! Operator surface is `scripture scribe run`. Liveness/peer probes may arm a
//! recovery attempt; only the durable VirtualLog-root conditional CAS grants
//! write authority. Directory heartbeats remain soft discovery.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use holylog::provision::resolve_read_seal;
use holylog::virtual_log::{ConditionalRegister, LogletResolver, VirtualLog, VirtualLogError};
use scripture::serving_authority::{
    AuthorityKey, AuthorityState, JournalGenerationRef, RouteHint, ServingAuthorityRecord,
    WriterTerm,
};
use scripture::{Clock, OwnerId, Timer};
use scripture_service::{AuthorityCoordinator, ObservedRootAuthority};
use thiserror::Error;

use crate::ha_session::{
    HaActivationError, HaAdmissionError, HaServingSession, bootstrap_and_serve, promote_and_serve,
};
use crate::holylog_foundation::HolylogJournalFoundation;
use crate::node::{PartsFactory, ProcessLogletResolver};
use scripture_service::VerseRuntimeConfig;

const LOGLET_K: u64 = 2;

/// Failures from the automatic Scribe lifecycle.
#[derive(Debug, Error)]
pub enum ScribeLifecycleError {
    /// Root observation or coordinator failure.
    #[error(transparent)]
    Coordinator(#[from] scripture_service::CoordinatorError),
    /// Activation (bootstrap/promote) failed.
    #[error(transparent)]
    Activation(#[from] HaActivationError),
    /// Membership materialization failed.
    #[error("membership materialization: {0}")]
    Membership(String),
    /// Ambiguous or corrupt durable root — fail closed.
    #[error("fail-closed: {0}")]
    FailClosed(String),
    /// Writer term arithmetic overflow / invalid.
    #[error("invalid writer term transition: {0}")]
    WriterTerm(String),
}

/// Probe whether the root-named peer advertise endpoint is reachable.
///
/// Reachability may arm a recovery attempt; it never grants write authority.
#[async_trait]
pub trait PeerProbe: Send + Sync {
    /// Returns true when the peer at `route_hint` looks alive for Serving.
    async fn is_reachable(&self, route_hint: &str) -> bool;
}

/// TCP connect probe against `tcp://host:port` route hints.
#[derive(Debug, Clone)]
pub struct TcpAdvertiseProbe {
    /// Connect timeout.
    pub timeout: Duration,
}

impl Default for TcpAdvertiseProbe {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(500),
        }
    }
}

#[async_trait]
impl PeerProbe for TcpAdvertiseProbe {
    async fn is_reachable(&self, route_hint: &str) -> bool {
        let Some(addr) = advertise_socket_addr(route_hint) else {
            return false;
        };
        matches!(
            tokio::time::timeout(self.timeout, tokio::net::TcpStream::connect(addr)).await,
            Ok(Ok(_))
        )
    }
}

/// Test/injection probe with an explicit reachable flag.
#[derive(Debug, Default)]
pub struct InjectedPeerProbe {
    reachable: AtomicBool,
}

impl InjectedPeerProbe {
    /// Constructs a probe with the given initial reachability.
    #[must_use]
    pub fn new(reachable: bool) -> Self {
        Self {
            reachable: AtomicBool::new(reachable),
        }
    }

    /// Updates reachability (tests drive writer death / return).
    pub fn set_reachable(&self, reachable: bool) {
        self.reachable.store(reachable, Ordering::SeqCst);
    }
}

#[async_trait]
impl PeerProbe for InjectedPeerProbe {
    async fn is_reachable(&self, _route_hint: &str) -> bool {
        self.reachable.load(Ordering::SeqCst)
    }
}

/// Options for one reconcile pass / run loop.
#[derive(Debug, Clone)]
pub struct ScribeRunOptions {
    /// How long a peer must look unreachable before a recovery CAS is armed.
    pub peer_grace: Duration,
    /// Term used for Empty→Serving bootstrap.
    pub initial_term: u64,
}

impl Default for ScribeRunOptions {
    fn default() -> Self {
        Self {
            peer_grace: Duration::from_secs(2),
            initial_term: 1,
        }
    }
}

/// Healthy non-writer fleet member.
#[derive(Clone)]
pub struct HealthyMember {
    /// Observed Serving Authority record (other owner, or named-but-not-writable).
    pub record: ServingAuthorityRecord,
    /// Whether this process is a ready non-writer member (history readable).
    pub member_ready: bool,
    /// Why this process is not the lawful writer.
    pub reason: String,
    /// Endpoint the root currently names, when Serving.
    pub serving_endpoint: Option<String>,
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
    owner_id: OwnerId,
    key: AuthorityKey,
}

impl HealthyMember {
    /// Endpoint producers should prefer, when known.
    #[must_use]
    pub fn serving_endpoint(&self) -> Option<&str> {
        self.serving_endpoint.as_deref()
    }

    /// Always denies write admission; returns redirect when available.
    pub async fn refuse_write(&self) -> HaAdmissionError {
        HaAdmissionError::GateDenied {
            reason: crate::authority_gate::AuthorityGateDenial::NotEffectiveWriter {
                state: "HealthyMember",
            },
            serving_endpoint: self.serving_endpoint.clone(),
        }
    }

    /// Re-evaluates the authority gate (must remain denied for writes).
    pub async fn ensure_not_writer(&self) -> Result<(), HaAdmissionError> {
        let decision = crate::evaluate_authority_gate(
            self.key,
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
            self.owner_id,
            self.resolver.is_writable(match &self.record.state {
                AuthorityState::Serving { authority, .. } => {
                    &authority.generation_ref.active_loglet_id
                }
                _ => {
                    return Err(HaAdmissionError::GateDenied {
                        reason: crate::authority_gate::AuthorityGateDenial::NotEffectiveWriter {
                            state: "HealthyMember",
                        },
                        serving_endpoint: self.serving_endpoint.clone(),
                    });
                }
            }),
            false,
        )
        .await;
        match decision {
            crate::AuthorityGateDecision::EffectiveWriter { .. } => {
                Err(HaAdmissionError::GateDenied {
                    reason: crate::authority_gate::AuthorityGateDenial::NotEffectiveWriter {
                        state: "HealthyMemberUnexpectedWriter",
                    },
                    serving_endpoint: self.serving_endpoint.clone(),
                })
            }
            crate::AuthorityGateDecision::Denied { .. } => Ok(()),
        }
    }
}

/// Result of one reconcile observation.
pub enum ScribeRunOutcome {
    /// This process holds lawful Serving and a live writable.
    LawfulWriter(HaServingSession),
    /// Healthy (or recovering) non-writer member.
    HealthyMember(HealthyMember),
}

/// Inputs shared across reconcile attempts for one Scribe process.
pub struct ScribeLifecycle<'a, C, T> {
    pub coordinator: &'a AuthorityCoordinator,
    pub foundation: &'a HolylogJournalFoundation,
    pub key: AuthorityKey,
    pub owner_id: OwnerId,
    pub runtime_config: VerseRuntimeConfig,
    pub register: Arc<dyn ConditionalRegister>,
    pub resolver: Arc<ProcessLogletResolver>,
    pub parts: Arc<dyn PartsFactory>,
    pub clock: C,
    pub timer: T,
    pub options: ScribeRunOptions,
    pub peer: Arc<dyn PeerProbe>,
}

impl<'a, C, T> ScribeLifecycle<'a, C, T>
where
    C: Clock + Clone + Send + 'static,
    T: Timer + Clone + Send + 'static,
{
    /// One-shot observe + bootstrap/join/recover decision.
    ///
    /// When `attempt_recovery` is true and the peer looks unreachable, try the
    /// lawful successor CAS. Callers enforce peer_grace before setting that.
    pub async fn reconcile_once(
        &self,
        attempt_recovery: bool,
    ) -> Result<ScribeRunOutcome, ScribeLifecycleError> {
        match self.coordinator.observe_root_authority().await? {
            ObservedRootAuthority::Uninitialized => {
                let term = WriterTerm::new(self.options.initial_term)
                    .map_err(|error| ScribeLifecycleError::WriterTerm(error.to_string()))?;
                let session = bootstrap_and_serve(
                    self.coordinator,
                    self.foundation,
                    self.key,
                    term,
                    self.runtime_config.clone(),
                    Arc::clone(&self.register),
                    Arc::clone(&self.resolver),
                    self.clock.clone(),
                    self.timer.clone(),
                )
                .await?;
                Ok(ScribeRunOutcome::LawfulWriter(session))
            }
            ObservedRootAuthority::AbsentOrMalformed { message } => {
                Err(ScribeLifecycleError::FailClosed(format!(
                    "root authority absent or malformed: {}",
                    message.unwrap_or_else(|| "undecodable fence".to_owned())
                )))
            }
            ObservedRootAuthority::Record(record) => {
                self.reconcile_record(*record, attempt_recovery).await
            }
        }
    }

    async fn reconcile_record(
        &self,
        record: ServingAuthorityRecord,
        attempt_recovery: bool,
    ) -> Result<ScribeRunOutcome, ScribeLifecycleError> {
        if record.key != self.key {
            return Err(ScribeLifecycleError::FailClosed(format!(
                "authority key mismatch: root={:?} local={:?}",
                record.key, self.key
            )));
        }

        match &record.state {
            AuthorityState::Unassigned => Err(ScribeLifecycleError::FailClosed(
                "root reports Unassigned; refusing silent repair".to_owned(),
            )),
            AuthorityState::ReconciliationRequired { .. } => Err(ScribeLifecycleError::FailClosed(
                "root reports ReconciliationRequired; operator/tooling required".to_owned(),
            )),
            AuthorityState::Transitioning { intent } => {
                if intent.candidate_owner_id == self.owner_id && attempt_recovery {
                    self.try_recovery_from_serving_record(&record).await
                } else {
                    self.join_member(
                        record,
                        false,
                        "transitioning; waiting for durable resolution".to_owned(),
                    )
                    .await
                }
            }
            AuthorityState::Serving {
                authority,
                route_hint,
            } => {
                let serving_owner = authority.owner_id;
                let active_loglet = authority.generation_ref.active_loglet_id.clone();
                let route = route_hint.as_str().to_owned();
                if serving_owner == self.owner_id {
                    let writable = self.resolver.is_writable(&active_loglet);
                    if writable {
                        return Err(ScribeLifecycleError::FailClosed(
                            "local writable present for Serving owner without activated session; fail closed"
                                .to_owned(),
                        ));
                    }
                    return self
                        .join_member(
                            record,
                            false,
                            "named Serving owner without local writable; refusing reattach"
                                .to_owned(),
                        )
                        .await;
                }

                let reachable = self.peer.is_reachable(&route).await;
                if reachable && !attempt_recovery {
                    return self
                        .join_member(
                            record,
                            true,
                            format!(
                                "other owner {} is Serving and reachable",
                                hex_owner(serving_owner)
                            ),
                        )
                        .await;
                }
                if attempt_recovery || !reachable {
                    match self.try_recovery_from_serving_record(&record).await {
                        Ok(outcome) => Ok(outcome),
                        Err(ScribeLifecycleError::Activation(error)) => {
                            self.join_member(
                                record,
                                true,
                                format!("recovery attempt did not win CAS: {error}"),
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    self.join_member(
                        record,
                        true,
                        format!(
                            "other owner {} Serving; peer unreachable but grace not armed",
                            hex_owner(serving_owner)
                        ),
                    )
                    .await
                }
            }
        }
    }

    async fn try_recovery_from_serving_record(
        &self,
        record: &ServingAuthorityRecord,
    ) -> Result<ScribeRunOutcome, ScribeLifecycleError> {
        let (expected, next_term) = match &record.state {
            AuthorityState::Serving { authority, .. } => {
                let next = authority.writer_term.get().checked_add(1).ok_or_else(|| {
                    ScribeLifecycleError::WriterTerm("writer term overflow".to_owned())
                })?;
                let term = WriterTerm::new(next)
                    .map_err(|error| ScribeLifecycleError::WriterTerm(error.to_string()))?;
                (authority.generation_ref.clone(), term)
            }
            AuthorityState::Transitioning { intent } => {
                let expected = match &intent.precondition {
                    scripture::serving_authority::FoundationPrecondition::Expected(generation) => {
                        generation.clone()
                    }
                    other => {
                        return Err(ScribeLifecycleError::FailClosed(format!(
                            "cannot resume transitioning with precondition {other:?}"
                        )));
                    }
                };
                (expected, intent.next_writer_term)
            }
            _ => {
                return Err(ScribeLifecycleError::FailClosed(
                    "recovery requires Serving or our Transitioning intent".to_owned(),
                ));
            }
        };

        // Fresh resolver for the successor process path: materialize historical
        // generations as read-seal, then let promote install the writable.
        self.materialize_all_generations().await?;
        let session = promote_and_serve(
            self.coordinator,
            self.foundation,
            self.key,
            next_term,
            expected,
            self.runtime_config.clone(),
            Arc::clone(&self.register),
            Arc::clone(&self.resolver),
            self.clock.clone(),
            self.timer.clone(),
        )
        .await?;
        Ok(ScribeRunOutcome::LawfulWriter(session))
    }

    async fn join_member(
        &self,
        record: ServingAuthorityRecord,
        member_ready: bool,
        reason: String,
    ) -> Result<ScribeRunOutcome, ScribeLifecycleError> {
        self.materialize_all_generations().await?;
        let serving_endpoint = match &record.state {
            AuthorityState::Serving { route_hint, .. } => Some(route_hint.as_str().to_owned()),
            _ => None,
        };
        Ok(ScribeRunOutcome::HealthyMember(HealthyMember {
            record,
            member_ready,
            reason,
            serving_endpoint,
            register: Arc::clone(&self.register),
            resolver: Arc::clone(&self.resolver),
            owner_id: self.owner_id,
            key: self.key,
        }))
    }

    /// Installs read-seal views for every durable membership generation.
    pub async fn materialize_all_generations(&self) -> Result<(), ScribeLifecycleError> {
        let log = VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        );
        let observed = match log.observe_membership().await {
            Ok(observed) => observed,
            Err(VirtualLogError::Uninitialized) => return Ok(()),
            Err(error) => {
                return Err(ScribeLifecycleError::Membership(error.to_string()));
            }
        };
        for generation in &observed.state.generations {
            let durable = self.parts.open(&generation.loglet_id).map_err(|error| {
                ScribeLifecycleError::Membership(format!(
                    "open loglet {}: {error}",
                    generation.loglet_id
                ))
            })?;
            let view = resolve_read_seal(durable.components(LOGLET_K))
                .await
                .map_err(|error| ScribeLifecycleError::Membership(error.to_string()))?;
            self.resolver
                .insert_read_seal(generation.loglet_id.clone(), Arc::new(view));
        }
        Ok(())
    }

    /// Loops until this process becomes the lawful writer or fail-closes.
    ///
    /// Peer unreachability longer than `peer_grace` arms a recovery attempt.
    pub async fn await_lawful_writer(&self) -> Result<HaServingSession, ScribeLifecycleError> {
        let mut unreachable_since: Option<Instant> = None;
        loop {
            let attempt = matches!(
                unreachable_since,
                Some(since) if since.elapsed() >= self.options.peer_grace
            );
            match self.reconcile_once(attempt).await? {
                ScribeRunOutcome::LawfulWriter(session) => return Ok(session),
                ScribeRunOutcome::HealthyMember(member) => {
                    let route = member.serving_endpoint.clone().unwrap_or_default();
                    let reachable = if route.is_empty() {
                        false
                    } else {
                        self.peer.is_reachable(&route).await
                    };
                    if reachable {
                        unreachable_since = None;
                    } else if unreachable_since.is_none() {
                        unreachable_since = Some(Instant::now());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

/// Strips `tcp://` from an advertise / route hint for TcpStream::connect.
#[must_use]
pub fn advertise_socket_addr(route_hint: &str) -> Option<String> {
    let trimmed = route_hint.trim();
    if let Some(rest) = trimmed.strip_prefix("tcp://")
        && rest.contains(':')
    {
        return Some(rest.to_owned());
    }
    None
}

fn hex_owner(owner: OwnerId) -> String {
    owner
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Observes the current Serving generation ref from the register (promote Expected).
pub async fn observe_expected_generation(
    register: Arc<dyn ConditionalRegister>,
    resolver: Arc<ProcessLogletResolver>,
) -> Result<JournalGenerationRef, Box<dyn Error>> {
    let virtual_log = VirtualLog::new(register, resolver as Arc<dyn LogletResolver>);
    let versioned = virtual_log.observe_membership().await?;
    Ok(JournalGenerationRef::from_virtual_log_state(
        &versioned.state,
    )?)
}

/// Convenience: build a [`RouteHint`] from an advertise string.
pub fn route_hint_from_advertise(advertise: &str) -> Result<RouteHint, Box<dyn Error>> {
    Ok(RouteHint::new(advertise)?)
}
