//! Deterministic scripted LogDrive for composition campaign scenarios.
//!
//! Ported from Holylog's test `ScriptedDrive` so scripture-campaign can gate
//! writes/tail scans and inject post-write failures without depending on
//! Holylog's private test harness.

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

/// Deterministic primitive LogDrive with explicit write/tail gates and failures.
#[derive(Debug)]
pub(crate) struct ScriptedDrive {
    model: Mutex<ReferenceLogDrive>,
    available: AtomicBool,
    writes: AtomicU64,
    write_gates: Mutex<HashMap<Address, Arc<PollGate>>>,
    failing_writes: Mutex<BTreeSet<Address>>,
    post_write_failures: Mutex<BTreeSet<Address>>,
    pending_tail_gate: Mutex<Option<Arc<PollGate>>>,
    last_tail_scan_written: Mutex<BTreeSet<Address>>,
}

impl Default for ScriptedDrive {
    fn default() -> Self {
        Self {
            model: Mutex::new(ReferenceLogDrive::new()),
            available: AtomicBool::new(true),
            writes: AtomicU64::new(0),
            write_gates: Mutex::new(HashMap::new()),
            failing_writes: Mutex::new(BTreeSet::new()),
            post_write_failures: Mutex::new(BTreeSet::new()),
            pending_tail_gate: Mutex::new(None),
            last_tail_scan_written: Mutex::new(BTreeSet::new()),
        }
    }
}

impl ScriptedDrive {
    /// Creates an available empty drive.
    #[must_use]
    pub(crate) fn available() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Creates an unavailable drive (all operations fail closed).
    #[must_use]
    #[allow(dead_code)] // reserved for additional unavailability schedules
    pub(crate) fn unavailable() -> Arc<Self> {
        Arc::new(Self {
            available: AtomicBool::new(false),
            ..Self::default()
        })
    }

    /// Toggles availability after construction.
    pub(crate) fn set_available(&self, available: bool) {
        self.available.store(available, Ordering::Release);
    }

    /// Arms (or returns) a closed write gate for `address`.
    pub(crate) fn gate_write(&self, address: Address) -> Arc<PollGate> {
        let mut gates = self
            .write_gates
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(gates.entry(address).or_default())
    }

    /// Arms a one-shot gate for the next `weak_tail` snapshot.
    pub(crate) fn gate_next_tail_scan(&self) -> Arc<PollGate> {
        let gate = Arc::new(PollGate::default());
        *self
            .pending_tail_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&gate));
        gate
    }

    /// Persist then return an error (durable-but-uncompleted slot).
    pub(crate) fn fail_after_write(&self, address: Address) {
        self.post_write_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(address);
    }

    /// Fail the write before persistence.
    pub(crate) fn fail_write(&self, address: Address) {
        self.failing_writes
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

    /// Written addresses observed by the most recent gated/ungated weak_tail.
    #[must_use]
    pub(crate) fn last_tail_scan_written(&self) -> BTreeSet<Address> {
        self.last_tail_scan_written
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Current durable written-address set.
    #[must_use]
    pub(crate) fn written_addresses(&self) -> BTreeSet<Address> {
        self.model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .written_addresses()
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
            let gate = self
                .pending_tail_gate
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))?
                .take();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            self.check_available()?;
            let model = self
                .model
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))?;
            *self
                .last_tail_scan_written
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))? = model.written_addresses();
            Ok(model.weak_tail(k))
        })
    }
}
