//! Deterministic scripted LogDrive for composition campaign scenarios.
//!
//! Ported from Holylog's test `ScriptedDrive` so scripture-campaign can gate
//! writes and inject post-write failures without depending on Holylog's private
//! test harness.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use bytes::Bytes;
use futures::future::poll_fn;
use futures::task::AtomicWaker;
use holylog::drive::{DriveError, DriveFuture, LogDrive};
use holylog::logdrive::{Address, ReferenceLogDrive, TailDescription};

#[derive(Debug, thiserror::Error)]
#[error("injected scripted-drive failure")]
struct InjectedFailure;

/// Manually opened future gate.
#[derive(Debug, Default)]
pub(crate) struct PollGate {
    open: AtomicBool,
    waker: AtomicWaker,
}

impl PollGate {
    /// Opens the gate and wakes waiters.
    pub(crate) fn open(&self) {
        self.open.store(true, Ordering::Release);
        self.waker.wake();
    }

    async fn wait(&self) {
        poll_fn(|context| {
            if self.open.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            self.waker.register(context.waker());
            if self.open.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

/// Deterministic primitive LogDrive with explicit write gates and failures.
#[derive(Debug, Default)]
pub(crate) struct ScriptedDrive {
    model: Mutex<ReferenceLogDrive>,
    available: AtomicBool,
    writes: AtomicU64,
    write_gates: Mutex<HashMap<Address, Arc<PollGate>>>,
    failing_writes: Mutex<BTreeSet<Address>>,
    post_write_failures: Mutex<BTreeSet<Address>>,
}

impl ScriptedDrive {
    /// Creates an available empty drive.
    #[must_use]
    pub(crate) fn available() -> Arc<Self> {
        Arc::new(Self {
            available: AtomicBool::new(true),
            ..Self::default()
        })
    }

    /// Arms (or returns) a closed write gate for `address`.
    pub(crate) fn gate_write(&self, address: Address) -> Arc<PollGate> {
        let mut gates = self
            .write_gates
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(gates.entry(address).or_default())
    }

    /// Persist then return an error (durable-but-uncompleted slot).
    pub(crate) fn fail_after_write(&self, address: Address) {
        self.post_write_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(address);
    }

    /// Whether `address` is present in the reference model.
    #[must_use]
    pub(crate) fn contains(&self, address: Address) -> bool {
        self.model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .read(address)
            .is_some()
    }

    fn check_available(&self) -> Result<(), DriveError> {
        if self.available.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(DriveError::backend(InjectedFailure))
        }
    }

    fn write_gate(&self, address: Address) -> Option<Arc<PollGate>> {
        self.write_gates
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&address)
            .cloned()
    }
}

impl LogDrive for ScriptedDrive {
    fn write(&self, address: Address, value: Bytes) -> DriveFuture<'_, ()> {
        Box::pin(async move {
            self.check_available()?;
            if let Some(gate) = self.write_gate(address) {
                gate.wait().await;
            }
            self.check_available()?;
            if self
                .failing_writes
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))?
                .contains(&address)
            {
                return Err(DriveError::backend(InjectedFailure));
            }
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.model
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))?
                .write(address, value)?;
            if self
                .post_write_failures
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))?
                .contains(&address)
            {
                return Err(DriveError::backend(InjectedFailure));
            }
            Ok(())
        })
    }

    fn read(&self, address: Address) -> DriveFuture<'_, Option<Bytes>> {
        Box::pin(async move {
            self.check_available()?;
            Ok(self
                .model
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))?
                .read(address)
                .cloned())
        })
    }

    fn weak_tail(&self, k: u64) -> DriveFuture<'_, TailDescription> {
        Box::pin(async move {
            self.check_available()?;
            Ok(self
                .model
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))?
                .weak_tail(k))
        })
    }
}
