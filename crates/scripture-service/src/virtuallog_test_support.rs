//! Test-only helpers for lawful Holylog v0.2 VirtualLog fixtures.
//!
//! Not a production API. Converts Canon/VirtualLog harnesses that previously
//! smuggled `AtomicLog` through `LogletResolver` into receipt-gated provision.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use holylog::atomic::{InMemorySeal, InMemoryTrimPoint, Seal, TrimPoint};
use holylog::drive::LogDrive;
use holylog::memory::InMemoryLogDrive;
use holylog::provision::{
    BindTag, FreshWritableProvisionReceipt, InMemoryExclusiveClaimStore, LogletComponents,
    LogletObjectNamespaces, ProvisionAuthority, ProvisionerId, ReadSealView, ResolvedLoglet,
    WritableLoglet, resolve_read_seal,
};
use holylog::virtual_log::{
    ApplicationFence, ConditionalRegister, InMemoryConditionalRegister, LogletId, LogletResolver,
    ResolveFuture, VirtualLog,
};

use crate::ProvisionedSuccessor;

const TEST_ROOT: &str = "scripture-service-tests";

/// Capability-typed resolver test double.
#[derive(Default)]
pub struct TestResolver {
    loglets: Mutex<BTreeMap<LogletId, ResolvedLoglet>>,
}

impl TestResolver {
    pub fn insert_writable(&self, id: LogletId, writable: Arc<WritableLoglet>) {
        self.loglets
            .lock()
            .expect("lock")
            .insert(id, ResolvedLoglet::Writable(writable));
    }

    pub fn insert_read_seal(&self, id: LogletId, view: Arc<ReadSealView>) {
        self.loglets
            .lock()
            .expect("lock")
            .insert(id, ResolvedLoglet::ReadSeal(view));
    }

    pub fn insert(&self, id: LogletId, resolved: ResolvedLoglet) {
        self.loglets.lock().expect("lock").insert(id, resolved);
    }

    pub fn remove(&self, id: &LogletId) -> Option<ResolvedLoglet> {
        self.loglets.lock().expect("lock").remove(id)
    }

    pub fn is_writable(&self, id: &LogletId) -> bool {
        matches!(
            self.loglets.lock().expect("lock").get(id),
            Some(ResolvedLoglet::Writable(_))
        )
    }
}

impl LogletResolver for TestResolver {
    fn resolve(&self, id: &LogletId) -> ResolveFuture<'_, Option<ResolvedLoglet>> {
        let id = id.clone();
        Box::pin(async move { Ok(self.loglets.lock().expect("lock").get(&id).cloned()) })
    }
}

/// Owns a claim authority and installs provisioned generations into a resolver.
pub struct ProvisioningFleet {
    authority: ProvisionAuthority,
    resolver: Arc<TestResolver>,
}

impl ProvisioningFleet {
    pub fn new(provisioner: impl Into<String>) -> (Self, Arc<TestResolver>) {
        let resolver = Arc::new(TestResolver::default());
        let fleet = Self {
            authority: ProvisionAuthority::new(
                Arc::new(InMemoryExclusiveClaimStore::new()),
                ProvisionerId::new(provisioner),
            ),
            resolver: Arc::clone(&resolver),
        };
        (fleet, resolver)
    }

    pub fn with_claims(
        claims: Arc<InMemoryExclusiveClaimStore>,
        provisioner: impl Into<String>,
    ) -> (Self, Arc<TestResolver>) {
        let resolver = Arc::new(TestResolver::default());
        let fleet = Self {
            authority: ProvisionAuthority::new(claims, ProvisionerId::new(provisioner)),
            resolver: Arc::clone(&resolver),
        };
        (fleet, resolver)
    }

    pub fn bind(id: &LogletId) -> BindTag {
        BindTag::new(id.as_str().as_bytes().to_vec())
    }

    pub fn empty_components(k: u64) -> LogletComponents {
        LogletComponents::new(
            Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>,
            Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
            Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
            k,
        )
    }

    pub fn components_with_drive(drive: Arc<dyn LogDrive>, k: u64) -> LogletComponents {
        LogletComponents::new(
            drive,
            Arc::new(InMemorySeal::new()) as Arc<dyn Seal>,
            Arc::new(InMemoryTrimPoint::new()) as Arc<dyn TrimPoint>,
            k,
        )
    }

    pub fn resolver(&self) -> Arc<TestResolver> {
        Arc::clone(&self.resolver)
    }

    /// Provisions `id` fresh, installs Writable in the resolver, returns the move-only successor.
    pub async fn provision(&self, id: &LogletId, k: u64) -> ProvisionedSuccessor {
        self.provision_with_components(id, Self::empty_components(k))
            .await
    }

    /// Provisions with explicit components (custom drive, etc.).
    pub async fn provision_with_components(
        &self,
        id: &LogletId,
        components: LogletComponents,
    ) -> ProvisionedSuccessor {
        self.provision_with_namespaces(
            id,
            LogletObjectNamespaces::under_root(TEST_ROOT, id),
            components,
        )
        .await
    }

    /// Provisions under an alternate namespace root (second claim for tests).
    pub async fn provision_with_root(
        &self,
        id: &LogletId,
        root: &str,
        k: u64,
    ) -> ProvisionedSuccessor {
        self.provision_with_namespaces(
            id,
            LogletObjectNamespaces::under_root(root, id),
            Self::empty_components(k),
        )
        .await
    }

    async fn provision_with_namespaces(
        &self,
        id: &LogletId,
        namespaces: LogletObjectNamespaces,
        components: LogletComponents,
    ) -> ProvisionedSuccessor {
        let bind = Self::bind(id);
        let (receipt, writable) = self
            .authority
            .provision_fresh(id.clone(), namespaces, bind.clone(), components)
            .await
            .expect("provision fresh");
        let writable = Arc::new(writable);
        self.resolver
            .insert_writable(id.clone(), Arc::clone(&writable));
        ProvisionedSuccessor {
            receipt,
            writable,
            bind,
        }
    }

    /// Provisions without requiring the Arc resolver (caller installs).
    pub async fn provision_detached(
        &self,
        id: &LogletId,
        k: u64,
    ) -> (
        FreshWritableProvisionReceipt,
        Arc<WritableLoglet>,
        BindTag,
        LogletComponents,
    ) {
        let bind = Self::bind(id);
        let components = Self::empty_components(k);
        let (receipt, writable) = self
            .authority
            .provision_fresh(
                id.clone(),
                LogletObjectNamespaces::under_root(TEST_ROOT, id),
                bind.clone(),
                components.clone(),
            )
            .await
            .expect("provision fresh");
        (receipt, Arc::new(writable), bind, components)
    }

    /// Re-materializes sealed/historical namespaces as ReadSeal.
    pub async fn install_read_seal(&self, id: &LogletId, components: LogletComponents) {
        let view = resolve_read_seal(components)
            .await
            .expect("resolve read/seal");
        self.resolver.insert_read_seal(id.clone(), Arc::new(view));
    }
}

/// Shared VirtualLog test harness with receipt-gated bootstrap/reconfigure.
pub struct VirtualLogHarness {
    pub register: Arc<dyn ConditionalRegister>,
    pub fleet: ProvisioningFleet,
    pub resolver: Arc<TestResolver>,
    pub first: LogletId,
    pub second: LogletId,
    pub third: LogletId,
}

impl VirtualLogHarness {
    pub async fn memory() -> Self {
        Self::with_ids(
            "virt-first",
            "virt-second",
            "virt-third",
            Arc::new(InMemoryConditionalRegister::new()),
        )
        .await
    }

    pub async fn with_register(register: Arc<dyn ConditionalRegister>) -> Self {
        Self::with_ids("virt-first", "virt-second", "virt-third", register).await
    }

    pub async fn with_ids(
        first_name: &str,
        second_name: &str,
        third_name: &str,
        register: Arc<dyn ConditionalRegister>,
    ) -> Self {
        let (fleet, resolver) = ProvisioningFleet::new("virtuallog-harness");
        let first = LogletId::new(first_name).expect("first id");
        let second = LogletId::new(second_name).expect("second id");
        let third = LogletId::new(third_name).expect("third id");
        Self {
            register,
            fleet,
            resolver,
            first,
            second,
            third,
        }
    }

    pub async fn with_first_drive(_first_drive: Arc<dyn LogDrive>) -> Self {
        Self::memory().await
    }

    pub async fn bootstrap_first_with_drive(
        &self,
        drive: Arc<dyn LogDrive>,
        fence: ApplicationFence,
    ) {
        let successor = self
            .fleet
            .provision_with_components(
                &self.first,
                ProvisioningFleet::components_with_drive(drive, 0),
            )
            .await;
        self.virtual_log()
            .bootstrap_with_receipt(
                successor.receipt,
                successor.writable.as_ref(),
                &successor.bind,
                fence,
            )
            .await
            .expect("bootstrap");
    }

    pub fn virtual_log(&self) -> VirtualLog {
        VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        )
    }

    pub async fn bootstrap_first(&self, fence: ApplicationFence) {
        let successor = self.fleet.provision(&self.first, 0).await;
        self.virtual_log()
            .bootstrap_with_receipt(
                successor.receipt,
                successor.writable.as_ref(),
                &successor.bind,
                fence,
            )
            .await
            .expect("bootstrap");
    }

    pub async fn provision(&self, id: &LogletId, k: u64) -> ProvisionedSuccessor {
        self.fleet.provision(id, k).await
    }

    pub async fn provision_with_root(
        &self,
        id: &LogletId,
        root: &str,
        k: u64,
    ) -> ProvisionedSuccessor {
        self.fleet.provision_with_root(id, root, k).await
    }

    pub async fn reconfigure(&self, successor: ProvisionedSuccessor, fence: ApplicationFence) {
        let log = self.virtual_log();
        let observed = log.observe_membership().await.expect("observe");
        log.reconfigure_with_receipt(
            &observed,
            successor.receipt,
            successor.writable.as_ref(),
            &successor.bind,
            fence,
        )
        .await
        .expect("reconfigure");
    }

    pub async fn reconfigure_id(&self, id: &LogletId, fence: ApplicationFence) {
        let successor = self.provision(id, 0).await;
        self.reconfigure(successor, fence).await;
    }
}
