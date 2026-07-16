//! Receipt futures and the shared trace ledger helper.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::channel::oneshot;

use crate::trace::{Effect, Event, Ledger};

use super::DriverError;
use super::Receipt;

/// Future returned by [`super::ChunkDriverHandle::submit`].
///
/// Dropping it never cancels an accepted submission.
#[must_use = "receipts are learned by awaiting this future"]
pub struct ReceiptFuture {
    pub(super) receiver: oneshot::Receiver<Result<Receipt, DriverError>>,
}

impl ReceiptFuture {
    /// Wraps an existing oneshot receiver (spool / lab adapters).
    pub fn from_receiver(receiver: oneshot::Receiver<Result<Receipt, DriverError>>) -> Self {
        Self { receiver }
    }
}

impl Future for ReceiptFuture {
    type Output = Result<Receipt, DriverError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(context)
            .map(|result| result.unwrap_or(Err(DriverError::Unavailable)))
    }
}

/// Cloneable trace recorder shared by the actor and its handles.
///
/// It exists so deterministic integration tests can inspect a completed
/// `run(self)` without making the core's ledger globally mutable.
#[derive(Clone, Debug, Default)]
pub(super) struct SharedLedger(Arc<Mutex<Ledger>>);

impl SharedLedger {
    pub(super) fn event(&self, event: Event) {
        if let Ok(mut ledger) = self.0.lock() {
            ledger.event(event);
        }
    }

    pub(super) fn effect(&self, scope: crate::trace::CostScope, effect: Effect) {
        if let Ok(mut ledger) = self.0.lock() {
            ledger.effect(scope, effect);
        }
    }

    pub(super) fn snapshot(&self) -> Ledger {
        self.0
            .lock()
            .map(|ledger| ledger.clone())
            .unwrap_or_default()
    }
}
