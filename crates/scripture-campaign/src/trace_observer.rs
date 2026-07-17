//! Bridges [`RuntimeObserver`] to the shared Holylog correctness trace.

use std::sync::Arc;

use holylog_correctness::{
    ActorTrace, EventKind, OperationId, TraceSink, payload_digest,
};
use scripture::Receipt;
use scripture_service::{OperationContext, RuntimeObserver};

/// Emits runtime-originated observations into a Holylog [`ActorTrace`].
pub(crate) struct TraceRuntimeObserver {
    trace: ActorTrace,
    default_loglet_id: Option<String>,
}

impl TraceRuntimeObserver {
    /// Creates an observer writing to `trace`.
    #[must_use]
    pub(crate) fn new(trace: ActorTrace, default_loglet_id: Option<String>) -> Self {
        Self {
            trace,
            default_loglet_id,
        }
    }

    /// Wraps `observer` in a shared session for one actor.
    pub(crate) fn session(
        run_id: &str,
        actor: &str,
        sink: Arc<dyn TraceSink>,
        default_loglet_id: Option<String>,
    ) -> scripture_service::RuntimeObservationSession {
        let trace = ActorTrace::new(
            holylog_correctness::RunId::new(run_id),
            holylog_correctness::ActorId::new(actor),
            sink,
        );
        let observer = Arc::new(Self::new(trace, default_loglet_id));
        scripture_service::RuntimeObservationSession::new(actor, observer)
    }
}

impl RuntimeObserver for TraceRuntimeObserver {
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
        let bytes = receipt.chunk_id.as_bytes();
        let loglet_id = ctx
            .loglet_id
            .clone()
            .or_else(|| self.default_loglet_id.clone())
            .unwrap_or_default();
        self.trace.emit(
            Some(OperationId::new(&ctx.operation_id)),
            EventKind::ScriptureCommittedAck {
                logical_offset: receipt.first_offset.get(),
                digest: payload_digest(&bytes),
                size: bytes.len(),
                loglet_id,
            },
        );
    }

    fn receipt_denied(&self, ctx: &OperationContext, reason: &str, _sequence: u64) {
        self.trace.emit(
            Some(OperationId::new(&ctx.operation_id)),
            EventKind::ScriptureDenied {
                reason: reason.to_owned(),
                digest: None,
            },
        );
    }
}
