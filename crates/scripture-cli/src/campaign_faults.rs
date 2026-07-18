//! Test-only campaign fault + trace bridge for temporary HA actors.
//!
//! Enabled only with the `campaign-faults` Cargo feature. Armed exclusively by
//! explicit environment variables; default product builds omit this module.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use holylog::atomic::Seal;
use holylog::drive::LogDrive;
use holylog::provision::LogletObjectNamespaces;
use holylog::virtual_log::{ConditionalRegister, LogletId};
use holylog_correctness::faults::{FaultableConditionalRegister, FaultableLogDrive, FaultableSeal};
use holylog_correctness::{
    ActorId, ActorTrace, ArmedFault, EventKind, FaultController, OperationId, RecordingSink, RunId,
    TraceEvent, TraceSink,
};
use scripture::Receipt;
use scripture_runtime::{DurableLogletParts, PartsFactory, PartsFactoryError};
use scripture_service::{OperationContext, RuntimeObservationSession, RuntimeObserver};

use crate::assemble::AssembledNode;

const ENV_TRACE_PATH: &str = "SCRIPTURE_CAMPAIGN_TRACE_PATH";
const ENV_RUN_ID: &str = "SCRIPTURE_CAMPAIGN_RUN_ID";
const ENV_ACTOR_ID: &str = "SCRIPTURE_CAMPAIGN_ACTOR_ID";
const ENV_DIE_AFTER_PAYLOAD: &str = "SCRIPTURE_FAULT_DIE_AFTER_PAYLOAD";
const ENV_DIE_AFTER_PAYLOAD_SKIP: &str = "SCRIPTURE_FAULT_DIE_AFTER_PAYLOAD_SKIP";
const ENV_ROOT_CAS_REPLY_LOSS: &str = "SCRIPTURE_FAULT_ROOT_CAS_REPLY_LOSS";

/// Shared campaign fault/trace handles installed into an assembled node.
pub struct CampaignFaultContext {
    /// Process-wide one-shot fault controller.
    pub faults: Arc<FaultController>,
    /// Actor-scoped trace (also mirrored to file when configured).
    pub trace: ActorTrace,
    /// In-memory recording mirror for nonempty checks.
    #[allow(dead_code)]
    pub sink: Arc<RecordingSink>,
    /// Active loglet id for ScriptureCommittedAck enrichment.
    active_loglet: Arc<Mutex<String>>,
    /// Committed ACK count (post-Serving) before DieAfterPayload arms.
    ack_count: Arc<AtomicU64>,
    /// Arm DieAfterPayload after this many committed ACKs (0 = arm on first write after Serving via observer arm-before-next).
    die_after_ack_count: u64,
    /// Whether DieAfterPayload is requested.
    die_after_payload: bool,
}

/// Returns true when any campaign-fault env is present.
#[must_use]
pub fn campaign_env_requested() -> bool {
    std::env::var_os(ENV_TRACE_PATH).is_some()
        || env_flag(ENV_DIE_AFTER_PAYLOAD)
        || env_flag(ENV_ROOT_CAS_REPLY_LOSS)
}

/// Wraps assembled durable seams with faultable/traced adapters when requested.
pub fn install_into_assembled(
    assembled: &mut AssembledNode,
) -> Result<Option<CampaignFaultContext>, Box<dyn std::error::Error>> {
    if !campaign_env_requested() {
        return Ok(None);
    }
    let run_id = std::env::var(ENV_RUN_ID).unwrap_or_else(|_| "campaign-run".into());
    let actor_id = std::env::var(ENV_ACTOR_ID).unwrap_or_else(|_| "scripture-actor".into());
    let sink = RecordingSink::new().shared();
    let mut sinks: Vec<Arc<dyn TraceSink>> = vec![Arc::clone(&sink) as Arc<dyn TraceSink>];
    if let Ok(path) = std::env::var(ENV_TRACE_PATH) {
        sinks.push(Arc::new(FileTraceSink::open(Path::new(&path))?) as Arc<dyn TraceSink>);
        eprintln!("scripture: campaign-faults trace file={path}");
    }
    let fanout = Arc::new(FanoutTraceSink { sinks });
    let trace = ActorTrace::new(
        RunId::new(run_id),
        ActorId::new(actor_id),
        Arc::clone(&fanout) as Arc<dyn TraceSink>,
    );
    let faults = Arc::new(FaultController::new());
    if env_flag(ENV_ROOT_CAS_REPLY_LOSS) {
        faults.arm(ArmedFault::RootCasReplyLost);
        eprintln!("scripture: campaign-faults armed RootCasReplyLost");
    }
    // DieAfterPayload is armed after N committed ACKs (see observer), so
    // bootstrap LogDrive writes and batched chunk writes cannot consume it early.
    let die_after_payload = env_flag(ENV_DIE_AFTER_PAYLOAD);
    let die_after_ack_count = env_u64(ENV_DIE_AFTER_PAYLOAD_SKIP).unwrap_or(0);
    if die_after_payload {
        eprintln!(
            "scripture: campaign-faults DieAfterPayload will arm after {die_after_ack_count} committed ACK(s)"
        );
    }

    assembled.register = Arc::new(FaultableConditionalRegister::new(
        Arc::clone(&assembled.register),
        Arc::clone(&faults),
        trace.clone(),
    )) as Arc<dyn ConditionalRegister>;
    // Wrap LogDrive only when DieAfterPayload is armed. Do not wrap Seal
    // unconditionally — FaultableSeal wrapping correlated with flaky promote
    // paths that emit committed ACKs without successor object-store payloads.
    if die_after_payload {
        assembled.parts = Arc::new(FaultingPartsFactory {
            inner: Arc::clone(&assembled.parts),
            faults: Arc::clone(&faults),
            trace: trace.clone(),
            wrap_drive: true,
        }) as Arc<dyn PartsFactory>;
    }

    Ok(Some(CampaignFaultContext {
        faults,
        trace,
        sink,
        active_loglet: Arc::new(Mutex::new(String::new())),
        ack_count: Arc::new(AtomicU64::new(0)),
        die_after_ack_count,
        die_after_payload,
    }))
}

/// Attaches a Holylog TraceEvent bridge to a serving HA session.
#[must_use]
pub fn observe_session(
    session: scripture_runtime::HaServingSession,
    ctx: &CampaignFaultContext,
    actor: &str,
) -> scripture_runtime::HaServingSession {
    if let Ok(mut guard) = ctx.active_loglet.lock() {
        *guard = session.generation().active_loglet_id.as_str().to_owned();
    }
    // skip==0: arm immediately so the first post-Serving producer write wedges.
    if ctx.die_after_payload && ctx.die_after_ack_count == 0 {
        ctx.faults.arm(ArmedFault::DieAfterPayload);
        eprintln!("scripture: campaign-faults armed DieAfterPayload (skip=0)");
    }
    let observer = Arc::new(CampaignRuntimeObserver {
        trace: ctx.trace.clone(),
        faults: Arc::clone(&ctx.faults),
        active_loglet: Arc::clone(&ctx.active_loglet),
        ack_count: Arc::clone(&ctx.ack_count),
        die_after_ack_count: ctx.die_after_ack_count,
        die_after_payload: ctx.die_after_payload,
    });
    session.with_observation(RuntimeObservationSession::new(actor, observer))
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

struct FanoutTraceSink {
    sinks: Vec<Arc<dyn TraceSink>>,
}

impl TraceSink for FanoutTraceSink {
    fn emit(&self, event: TraceEvent) {
        for sink in &self.sinks {
            sink.emit(event.clone());
        }
    }
}

struct FileTraceSink {
    file: Mutex<File>,
}

impl FileTraceSink {
    fn open(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

impl TraceSink for FileTraceSink {
    fn emit(&self, event: TraceEvent) {
        let Ok(line) = serde_json::to_string(&event) else {
            return;
        };
        if let Ok(mut file) = self.file.lock() {
            let _ = writeln!(file, "{line}");
            let _ = file.flush();
        }
    }
}

struct FaultingPartsFactory {
    inner: Arc<dyn PartsFactory>,
    faults: Arc<FaultController>,
    trace: ActorTrace,
    wrap_drive: bool,
}

impl FaultingPartsFactory {
    fn wrap(&self, loglet_id: &LogletId, raw: DurableLogletParts) -> DurableLogletParts {
        let drive = if self.wrap_drive {
            Arc::new(FaultableLogDrive::new(
                raw.drive(),
                Arc::clone(&self.faults),
                self.trace.clone(),
                loglet_id.to_string(),
            )) as Arc<dyn LogDrive>
        } else {
            raw.drive()
        };
        let seal = Arc::new(FaultableSeal::new(
            raw.seal(),
            Arc::clone(&self.faults),
            self.trace.clone(),
            loglet_id.to_string(),
        )) as Arc<dyn Seal>;
        DurableLogletParts::from_components(drive, seal, raw.trim())
    }
}

impl PartsFactory for FaultingPartsFactory {
    fn fresh(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        let raw = self.inner.fresh(loglet_id)?;
        Ok(self.wrap(loglet_id, raw))
    }

    fn open(&self, loglet_id: &LogletId) -> Result<DurableLogletParts, PartsFactoryError> {
        let raw = self.inner.open(loglet_id)?;
        Ok(self.wrap(loglet_id, raw))
    }

    fn namespaces(
        &self,
        loglet_id: &LogletId,
    ) -> Result<LogletObjectNamespaces, PartsFactoryError> {
        self.inner.namespaces(loglet_id)
    }
}

struct CampaignRuntimeObserver {
    trace: ActorTrace,
    faults: Arc<FaultController>,
    active_loglet: Arc<Mutex<String>>,
    ack_count: Arc<AtomicU64>,
    die_after_ack_count: u64,
    die_after_payload: bool,
}

impl RuntimeObserver for CampaignRuntimeObserver {
    fn runtime_started(&self, actor: &str, _sequence: u64) {
        self.trace.emit(
            None,
            EventKind::ScheduleStep {
                step: format!("runtime_started:{actor}"),
            },
        );
    }

    fn runtime_stopped(&self, actor: &str, _sequence: u64, reason: &str) {
        self.trace.emit(
            None,
            EventKind::ScheduleStep {
                step: format!("runtime_stopped:{actor}:{reason}"),
            },
        );
    }

    fn committed_ack(&self, ctx: &OperationContext, receipt: &Receipt, _sequence: u64) {
        let loglet_id = ctx
            .loglet_id
            .clone()
            .filter(|id| !id.is_empty())
            .or_else(|| self.active_loglet.lock().ok().map(|g| g.clone()))
            .unwrap_or_default();
        self.trace.emit(
            Some(OperationId::new(&ctx.operation_id)),
            EventKind::ScriptureCommittedAck {
                logical_offset: receipt.first_offset.get(),
                digest: ctx.payload_digest.clone().unwrap_or_default(),
                size: ctx.payload_size.unwrap_or_default(),
                loglet_id,
            },
        );
        self.trace.emit(
            Some(OperationId::new(&ctx.operation_id)),
            EventKind::ScheduleStep {
                step: format!("receipt_deduplicated:{}", receipt.deduplicated),
            },
        );
        if self.die_after_payload && self.die_after_ack_count > 0 {
            let n = self.ack_count.fetch_add(1, Ordering::SeqCst) + 1;
            if n == self.die_after_ack_count {
                self.faults.arm(ArmedFault::DieAfterPayload);
                eprintln!(
                    "scripture: campaign-faults armed DieAfterPayload after {n} committed ACK(s)"
                );
                // Match in-process family-2 semantics: after the durable write
                // hangs, open the death gate so the append fails closed (no OK)
                // instead of leaving a forever-wedged future across process kill.
                let faults = Arc::clone(&self.faults);
                tokio::spawn(async move {
                    for _ in 0..200 {
                        if faults.fired_count() > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            faults.death_gate().open();
                            eprintln!(
                                "scripture: campaign-faults opened DieAfterPayload death gate"
                            );
                            return;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                });
            }
        }
    }

    fn receipt_denied(&self, ctx: &OperationContext, reason: &str, _sequence: u64) {
        self.trace.emit(
            Some(OperationId::new(&ctx.operation_id)),
            EventKind::ScheduleStep {
                step: format!("receipt_denied:{reason}"),
            },
        );
    }
}
