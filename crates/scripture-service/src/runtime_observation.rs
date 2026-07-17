//! No-op-by-default runtime observation seam for correctness campaigns.
//!
//! Emits semantic outcomes at actual commit, admission/receipt denial, and
//! runtime lifecycle boundaries. Production builds default to
//! [`NoopRuntimeObserver`]; campaign tooling attaches a trace-backed observer.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use scripture::{DriverError, Receipt, ReceiptFuture, Submission};

/// Causal context stamped on every runtime-originated observation.
#[derive(Debug, Clone)]
pub struct OperationContext {
    /// Stable operation identity within one actor/run.
    pub operation_id: String,
    /// Optional active Loglet / generation identity at observation time.
    pub loglet_id: Option<String>,
    /// Optional causal parent operation (handoff, recovery, …).
    pub causal_parent: Option<String>,
    /// BLAKE3 digest of the submitted payload bytes (never payload plaintext).
    pub payload_digest: Option<String>,
    /// Total submitted payload bytes represented by `payload_digest`.
    pub payload_size: Option<usize>,
}

impl OperationContext {
    /// Builds a context for one logical operation.
    #[must_use]
    pub fn new(operation_id: impl Into<String>) -> Self {
        Self {
            operation_id: operation_id.into(),
            loglet_id: None,
            causal_parent: None,
            payload_digest: None,
            payload_size: None,
        }
    }

    /// Attaches the active Loglet identity.
    #[must_use]
    pub fn with_loglet_id(mut self, loglet_id: impl Into<String>) -> Self {
        self.loglet_id = Some(loglet_id.into());
        self
    }

    /// Attaches a causal parent operation id.
    #[must_use]
    pub fn with_causal_parent(mut self, parent: impl Into<String>) -> Self {
        self.causal_parent = Some(parent.into());
        self
    }

    /// Attaches redacted payload metadata calculated at the admission boundary.
    #[must_use]
    pub fn with_submission(mut self, submission: &Submission) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"scripture-runtime-observation-payload-v1\0");
        let mut size = 0_usize;
        for record in &submission.records {
            hasher.update(&(record.payload.len() as u64).to_le_bytes());
            hasher.update(&record.payload);
            size = size.saturating_add(record.payload.len());
        }
        self.payload_digest = Some(hasher.finalize().to_hex().to_string());
        self.payload_size = Some(size);
        self
    }
}

/// Monotonic local event sequence for one observed runtime session.
#[derive(Debug, Default)]
pub struct EventSequencer {
    next: AtomicU64,
}

impl EventSequencer {
    /// Returns the next sequence number (starts at 1).
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Runtime-originated semantic observations (no-op by default).
pub trait RuntimeObserver: Send + Sync {
    /// The runtime entered a serving/admitting phase for this actor.
    fn runtime_started(&self, _actor: &str, _sequence: u64) {}

    /// The runtime left serving (handoff, terminal, or shutdown).
    fn runtime_stopped(&self, _actor: &str, _sequence: u64, _reason: &str) {}

    /// Admission or an in-flight receipt resolved to a committed ACK.
    fn committed_ack(&self, _ctx: &OperationContext, _receipt: &Receipt, _sequence: u64) {}

    /// Admission or receipt resolution failed without a committed ACK.
    fn receipt_denied(&self, _ctx: &OperationContext, _reason: &str, _sequence: u64) {}
}

/// Production default: no observations.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopRuntimeObserver;

impl RuntimeObserver for NoopRuntimeObserver {}

/// Session binding one observer to one actor with monotonic event sequencing.
#[derive(Clone)]
pub struct RuntimeObservationSession {
    actor: String,
    observer: Arc<dyn RuntimeObserver>,
    sequencer: Arc<EventSequencer>,
    operation_counter: Arc<AtomicU64>,
}

impl RuntimeObservationSession {
    /// Creates a session for `actor` backed by `observer`.
    #[must_use]
    pub fn new(actor: impl Into<String>, observer: Arc<dyn RuntimeObserver>) -> Self {
        Self {
            actor: actor.into(),
            observer,
            sequencer: Arc::new(EventSequencer::default()),
            operation_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Allocates the next operation id within this session.
    #[must_use]
    pub fn next_operation_id(&self, label: &str) -> OperationContext {
        let ordinal = self.operation_counter.fetch_add(1, Ordering::Relaxed);
        OperationContext::new(format!("{}-{}", label, ordinal))
    }

    /// Allocates one submission operation context with redacted payload metadata.
    #[must_use]
    pub fn next_submission_operation(
        &self,
        label: &str,
        submission: &Submission,
    ) -> OperationContext {
        self.next_operation_id(label).with_submission(submission)
    }

    /// Emits `runtime_started`.
    pub fn runtime_started(&self) {
        self.observer
            .runtime_started(&self.actor, self.sequencer.next());
    }

    /// Emits `runtime_stopped`.
    pub fn runtime_stopped(&self, reason: &str) {
        self.observer
            .runtime_stopped(&self.actor, self.sequencer.next(), reason);
    }

    /// Records a committed ACK at the runtime boundary.
    pub fn emit_committed_ack(&self, ctx: &OperationContext, receipt: &Receipt) {
        self.observer
            .committed_ack(ctx, receipt, self.sequencer.next());
    }

    /// Wraps a receipt future so ACK/denial is observed at actual resolution.
    pub fn observe_receipt(&self, ctx: OperationContext, receipt: ReceiptFuture) -> ReceiptFuture {
        let observer = Arc::clone(&self.observer);
        let sequencer = Arc::clone(&self.sequencer);
        let (sender, receiver) = futures::channel::oneshot::channel();
        tokio::spawn(async move {
            match receipt.await {
                Ok(receipt) => {
                    observer.committed_ack(&ctx, &receipt, sequencer.next());
                    let _ = sender.send(Ok(receipt));
                }
                Err(error) => {
                    observer.receipt_denied(&ctx, &driver_error_label(&error), sequencer.next());
                    let _ = sender.send(Err(error));
                }
            }
        });
        ReceiptFuture::from_receiver(receiver)
    }

    /// Records an immediate admission denial (before a receipt future exists).
    pub fn admission_denied(&self, ctx: &OperationContext, reason: &str) {
        self.observer
            .receipt_denied(ctx, reason, self.sequencer.next());
    }
}

fn driver_error_label(error: &DriverError) -> String {
    format!("{error}")
}
