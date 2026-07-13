//! Test-only scripted [`LogDrive`] for fault injection at the storage boundary.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Poll;

use bytes::Bytes;
use futures::future::poll_fn;
use futures::task::AtomicWaker;
use holylog::drive::{DriveError, DriveFuture, LogDrive};
use holylog::logdrive::{Address, ReferenceLogDrive, TailDescription};

#[derive(Debug, thiserror::Error)]
#[error("injected scripted-drive failure")]
struct InjectedFailure;

/// A poll-driven gate that blocks a write until [`Self::open`] is called.
#[derive(Debug, Default)]
pub(crate) struct PollGate {
    open: AtomicBool,
    waker: AtomicWaker,
}

impl PollGate {
    /// Releases the gate so a blocked write may proceed.
    pub(crate) fn open(&self) {
        self.open.store(true, Ordering::Release);
        self.waker.wake();
    }

    pub(crate) async fn wait(&self) {
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

/// Scripted LogDrive: gate before durable write, fail before/after write.
#[derive(Debug, Default)]
pub(crate) struct ScriptedLogDrive {
    model: Mutex<ReferenceLogDrive>,
    writes: AtomicU64,
    write_gates: Mutex<HashMap<Address, Arc<PollGate>>>,
    fail_before_write: Mutex<BTreeSet<Address>>,
    fail_after_write: Mutex<BTreeSet<Address>>,
}

impl ScriptedLogDrive {
    /// Creates an empty scripted drive backed by an in-memory reference model.
    #[must_use]
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Blocks the write to `address` until [`PollGate::open`] is called on the
    /// returned gate.
    #[must_use]
    pub(crate) fn gate_write(&self, address: Address) -> Arc<PollGate> {
        let mut gates = self.write_gates.lock().expect("gates");
        Arc::clone(gates.entry(address).or_default())
    }

    /// Makes the write to `address` fail before any bytes are durably stored.
    pub(crate) fn fail_before_write(&self, address: Address) {
        self.fail_before_write
            .lock()
            .expect("fail-before")
            .insert(address);
    }

    /// Durably stores the write to `address`, then returns a backend error.
    pub(crate) fn fail_after_durable_write(&self, address: Address) {
        self.fail_after_write
            .lock()
            .expect("fail-after")
            .insert(address);
    }

    /// How many write attempts reached the drive (including gated retries).
    #[must_use]
    pub(crate) fn write_count(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    /// The address the next successful write attempt will target.
    #[must_use]
    pub(crate) fn next_write_address(&self, address: impl Fn(u64) -> Address) -> Address {
        address(self.write_count())
    }

    /// Whether a payload is durably present at `address`.
    #[must_use]
    pub(crate) fn contains(&self, address: Address) -> bool {
        self.model.lock().expect("model").read(address).is_some()
    }

    /// Reads a durably stored payload, if any.
    #[must_use]
    pub(crate) fn read(&self, address: Address) -> Option<Bytes> {
        self.model.lock().expect("model").read(address).cloned()
    }
}

impl LogDrive for ScriptedLogDrive {
    fn write(&self, address: Address, value: Bytes) -> DriveFuture<'_, ()> {
        Box::pin(async move {
            let gate = self
                .write_gates
                .lock()
                .expect("gates")
                .get(&address)
                .cloned();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            if self
                .fail_before_write
                .lock()
                .expect("fail-before")
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
                .fail_after_write
                .lock()
                .expect("fail-after")
                .contains(&address)
            {
                return Err(DriveError::backend(InjectedFailure));
            }
            Ok(())
        })
    }

    fn read(&self, address: Address) -> DriveFuture<'_, Option<Bytes>> {
        Box::pin(async move {
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
            Ok(self
                .model
                .lock()
                .map_err(|_| DriveError::backend(InjectedFailure))?
                .weak_tail(k)?)
        })
    }
}
