//! Durable object-store progress register for one binding record object.
//!
//! This is the smallest generic conditional-object seam needed for
//! `scripture-workload`: one value per `(workload_id, canon_id, verse_id)`, using
//! object-store `PutMode::{Create, Update}` with version witnesses.
//!
//! Non-claim: this adapter does **not** attest RustFS/provider conformance by
//! itself; callers must supply already-attested capabilities and run Holylog
//! conformance externally.

use std::sync::Arc;

use holylog_object_store_register::{ConditionalWrite, ReadConsistency, RegisterCapabilities};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use crate::progress::{
    AcquiredBinding, BindingKey, BindingToken, ConsumerBinding, ConsumerProgressStore,
    ProgressError, ProgressFuture, ProgressRegister, ProgressVersion,
};
use crate::types::{CanonRef, SourceOffset, VerseRef, WorkloadId};

/// Maximum UTF-8 bytes per key component (`workload_id`, `canon_id`, `verse_id`).
pub const MAX_PROGRESS_KEY_COMPONENT_BYTES: usize = 256;
/// Maximum UTF-8 bytes for `binding_token`.
pub const MAX_PROGRESS_TOKEN_BYTES: usize = 256;
/// Maximum UTF-8 bytes for `last_commit_ref`.
pub const MAX_PROGRESS_COMMIT_REF_BYTES: usize = 4096;
/// Fixed codec header bytes (`magic+version+flags+reserved+epoch+frontier+lens`).
pub const PROGRESS_CODEC_HEADER_BYTES: usize = 28;
/// Maximum encoded register record size.
pub const MAX_PROGRESS_RECORD_BYTES: usize =
    PROGRESS_CODEC_HEADER_BYTES + MAX_PROGRESS_TOKEN_BYTES + MAX_PROGRESS_COMMIT_REF_BYTES;

const CODEC_MAGIC: [u8; 4] = *b"sprg";
const CODEC_VERSION: u8 = 1;
const FLAG_HAS_COMMIT: u8 = 0b0000_0001;
const MAX_ACQUIRE_CAS_ATTEMPTS: usize = 32;

/// Construction failures for [`ObjectStoreProgressStore`].
#[derive(Debug, thiserror::Error)]
pub enum ProgressStoreConfigError {
    /// Register capability attestation is insufficient for safe CAS.
    #[error("unsupported register capability: {0}")]
    UnsupportedCapability(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConditionalVersion {
    e_tag: Option<String>,
    version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConditionalValue {
    bytes: Vec<u8>,
    version: ConditionalVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConditionalSwap {
    Applied(ConditionalVersion),
    Conflict,
    Unknown(String),
}

trait ConditionalObjectRegister: Send + Sync {
    fn read<'a>(
        &'a self,
        path: &'a ObjectPath,
    ) -> ProgressFuture<'a, Result<Option<ConditionalValue>, ProgressError>>;

    fn compare_and_swap<'a>(
        &'a self,
        path: &'a ObjectPath,
        expected: Option<&'a ConditionalVersion>,
        value: Vec<u8>,
    ) -> ProgressFuture<'a, Result<ConditionalSwap, ProgressError>>;
}

#[derive(Debug)]
struct ObjectStoreConditionalObjectRegister {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreConditionalObjectRegister {
    fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl ConditionalObjectRegister for ObjectStoreConditionalObjectRegister {
    fn read<'a>(
        &'a self,
        path: &'a ObjectPath,
    ) -> ProgressFuture<'a, Result<Option<ConditionalValue>, ProgressError>> {
        Box::pin(async move {
            let result = match self.store.get(path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(ProgressError::Io(format!("read {path}: {error}"))),
            };
            let version = ConditionalVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let bytes = result
                .bytes()
                .await
                .map_err(|error| ProgressError::Io(format!("read-bytes {path}: {error}")))?;
            Ok(Some(ConditionalValue {
                bytes: bytes.to_vec(),
                version,
            }))
        })
    }

    fn compare_and_swap<'a>(
        &'a self,
        path: &'a ObjectPath,
        expected: Option<&'a ConditionalVersion>,
        value: Vec<u8>,
    ) -> ProgressFuture<'a, Result<ConditionalSwap, ProgressError>> {
        Box::pin(async move {
            let mode = match expected {
                None => PutMode::Create,
                Some(version) => PutMode::Update(UpdateVersion {
                    e_tag: version.e_tag.clone(),
                    version: version.version.clone(),
                }),
            };
            let result = self
                .store
                .put_opts(
                    path,
                    PutPayload::from_bytes(value.into()),
                    PutOptions::from(mode),
                )
                .await;
            match result {
                Ok(put) => Ok(ConditionalSwap::Applied(ConditionalVersion {
                    e_tag: put.e_tag,
                    version: put.version,
                })),
                Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. }) => Ok(ConditionalSwap::Conflict),
                Err(error) => Ok(ConditionalSwap::Unknown(error.to_string())),
            }
        })
    }
}

struct ObjectStoreProgressInner {
    register: Arc<dyn ConditionalObjectRegister>,
    root: ObjectPath,
}

/// Object-store implementation of [`ConsumerProgressStore`].
#[derive(Clone)]
pub struct ObjectStoreProgressStore {
    inner: Arc<ObjectStoreProgressInner>,
}

impl ObjectStoreProgressStore {
    /// Creates a progress store rooted under one exclusive prefix.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        root: ObjectPath,
        capabilities: RegisterCapabilities,
    ) -> Result<Self, ProgressStoreConfigError> {
        require_attested_capabilities(capabilities)?;
        Ok(Self {
            inner: Arc::new(ObjectStoreProgressInner {
                register: Arc::new(ObjectStoreConditionalObjectRegister::new(store)),
                root,
            }),
        })
    }

    /// Maps the durable object-store witness into the opaque [`ProgressVersion`].
    ///
    /// This is derived from the conditional object's `e_tag`/`version`, never from
    /// a process-local counter. Lost-reply recovery must re-read the object and
    /// re-derive this witness; callers must not invent versions.
    fn progress_version_from_conditional(version: &ConditionalVersion) -> ProgressVersion {
        let mut data = Vec::with_capacity(64);
        data.extend_from_slice(b"sprg-v1\0");
        data.extend_from_slice(version.e_tag.as_deref().unwrap_or("").as_bytes());
        data.push(0);
        data.extend_from_slice(version.version.as_deref().unwrap_or("").as_bytes());
        let digest = blake3::hash(&data);
        let bytes = digest.as_bytes();
        ProgressVersion::new(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn register_path_for_key(&self, key: &BindingKey) -> Result<ObjectPath, ProgressError> {
        let workload = safe_hex_component(
            key.workload_id.as_str(),
            "workload_id",
            MAX_PROGRESS_KEY_COMPONENT_BYTES,
        )?;
        let canon = safe_hex_component(
            key.canon_id.as_str(),
            "canon_id",
            MAX_PROGRESS_KEY_COMPONENT_BYTES,
        )?;
        let verse = safe_hex_component(
            key.verse_id.as_str(),
            "verse_id",
            MAX_PROGRESS_KEY_COMPONENT_BYTES,
        )?;
        Ok(self
            .inner
            .root
            .clone()
            .join("scripture-workload")
            .join("progress-register")
            .join("v1")
            .join(format!("w-{workload}"))
            .join(format!("c-{canon}"))
            .join(format!("v-{verse}"))
            .join("register.bin"))
    }

    async fn read_register_for(
        &self,
        key: &BindingKey,
    ) -> Result<Option<(ProgressRegister, ConditionalVersion)>, ProgressError> {
        let path = self.register_path_for_key(key)?;
        let observed = self.inner.register.read(&path).await?;
        match observed {
            None => Ok(None),
            Some(value) => Ok(Some((
                decode_record(
                    &value.bytes,
                    key,
                    MAX_PROGRESS_TOKEN_BYTES,
                    MAX_PROGRESS_COMMIT_REF_BYTES,
                )?,
                value.version,
            ))),
        }
    }

    async fn reread_resolves_to_intended(
        &self,
        key: &BindingKey,
        intended: &ProgressRegister,
    ) -> Result<bool, ProgressError> {
        let reread = self.read_register_for(key).await?;
        Ok(reread.is_some_and(|(register, _)| register == *intended))
    }

    #[cfg(test)]
    fn with_test_register(register: Arc<dyn ConditionalObjectRegister>, root: ObjectPath) -> Self {
        Self {
            inner: Arc::new(ObjectStoreProgressInner { register, root }),
        }
    }
}

impl ConsumerProgressStore for ObjectStoreProgressStore {
    fn acquire_or_renew<'a>(
        &'a self,
        key: BindingKey,
        owner_token: &'a BindingToken,
    ) -> ProgressFuture<'a, Result<AcquiredBinding, ProgressError>> {
        let owner_token = owner_token.clone();
        Box::pin(async move {
            for _attempt in 0..MAX_ACQUIRE_CAS_ATTEMPTS {
                let path = self.register_path_for_key(&key)?;
                let observed = self.read_register_for(&key).await?;
                let (intended, expected_version) = match observed {
                    None => {
                        let binding = ConsumerBinding {
                            workload_id: key.workload_id.clone(),
                            canon_id: key.canon_id.clone(),
                            verse_id: key.verse_id.clone(),
                            binding_epoch: 1,
                        };
                        (
                            ProgressRegister {
                                binding,
                                binding_token: owner_token.clone(),
                                frontier: SourceOffset::new(0),
                                last_commit_ref: None,
                            },
                            None,
                        )
                    }
                    Some((current, current_version)) if current.binding_token == owner_token => {
                        (current, Some(current_version))
                    }
                    Some((current, current_version)) => {
                        let next_epoch = current
                            .binding
                            .binding_epoch
                            .checked_add(1)
                            .ok_or_else(|| ProgressError::Io("binding_epoch overflow".into()))?;
                        let binding = ConsumerBinding {
                            workload_id: key.workload_id.clone(),
                            canon_id: key.canon_id.clone(),
                            verse_id: key.verse_id.clone(),
                            binding_epoch: next_epoch,
                        };
                        (
                            ProgressRegister {
                                binding,
                                binding_token: owner_token.clone(),
                                frontier: current.frontier,
                                last_commit_ref: current.last_commit_ref,
                            },
                            Some(current_version),
                        )
                    }
                };
                let encoded = encode_record(
                    &intended,
                    MAX_PROGRESS_TOKEN_BYTES,
                    MAX_PROGRESS_COMMIT_REF_BYTES,
                )?;
                match self
                    .inner
                    .register
                    .compare_and_swap(&path, expected_version.as_ref(), encoded)
                    .await?
                {
                    ConditionalSwap::Applied(_new_version) => {
                        return Ok(AcquiredBinding {
                            binding: intended.binding,
                            owner_token: owner_token.clone(),
                        });
                    }
                    ConditionalSwap::Conflict => continue,
                    ConditionalSwap::Unknown(detail) => {
                        if self.reread_resolves_to_intended(&key, &intended).await? {
                            return Ok(AcquiredBinding {
                                binding: intended.binding,
                                owner_token: owner_token.clone(),
                            });
                        }
                        return Err(ProgressError::Indeterminate(format!(
                            "acquire_or_renew outcome unknown after dispatch: {detail}"
                        )));
                    }
                }
            }
            Err(ProgressError::CasConflict)
        })
    }

    fn observe<'a>(
        &'a self,
        workload_id: &'a WorkloadId,
        canon_id: &'a CanonRef,
        verse_id: &'a VerseRef,
    ) -> ProgressFuture<'a, Result<Option<(ProgressRegister, ProgressVersion)>, ProgressError>>
    {
        Box::pin(async move {
            let key = BindingKey::new(workload_id.clone(), canon_id.clone(), verse_id.clone());
            let observed = self.read_register_for(&key).await?;
            Ok(observed.map(|(register, version)| {
                (register, Self::progress_version_from_conditional(&version))
            }))
        })
    }

    fn advance<'a>(
        &'a self,
        fence: &'a AcquiredBinding,
        new_frontier: SourceOffset,
        last_commit_ref: String,
    ) -> ProgressFuture<'a, Result<(ProgressRegister, ProgressVersion), ProgressError>> {
        Box::pin(async move {
            fence.binding.validate()?;
            if last_commit_ref.trim().is_empty() {
                return Err(ProgressError::Io("empty last_commit_ref".into()));
            }
            let key = BindingKey::new(
                fence.binding.workload_id.clone(),
                fence.binding.canon_id.clone(),
                fence.binding.verse_id.clone(),
            );
            let path = self.register_path_for_key(&key)?;
            let (existing, expected_version) = self
                .read_register_for(&key)
                .await?
                .ok_or(ProgressError::StaleBinding)?;
            if existing.binding_token != fence.owner_token {
                return Err(ProgressError::FenceHeld);
            }
            if existing.binding.binding_epoch != fence.binding.binding_epoch {
                return Err(ProgressError::StaleBinding);
            }
            if new_frontier.get() <= existing.frontier.get() {
                return Err(ProgressError::FrontierRegression);
            }
            let intended = ProgressRegister {
                binding: existing.binding.clone(),
                binding_token: fence.owner_token.clone(),
                frontier: new_frontier,
                last_commit_ref: Some(last_commit_ref),
            };
            let encoded = encode_record(
                &intended,
                MAX_PROGRESS_TOKEN_BYTES,
                MAX_PROGRESS_COMMIT_REF_BYTES,
            )?;
            match self
                .inner
                .register
                .compare_and_swap(&path, Some(&expected_version), encoded)
                .await?
            {
                ConditionalSwap::Applied(new_version) => Ok((
                    intended,
                    Self::progress_version_from_conditional(&new_version),
                )),
                ConditionalSwap::Conflict => Err(ProgressError::CasConflict),
                ConditionalSwap::Unknown(detail) => {
                    let reread = self.read_register_for(&key).await?;
                    match reread {
                        Some((register, version)) if register == intended => {
                            Ok((intended, Self::progress_version_from_conditional(&version)))
                        }
                        Some((register, _)) if register == existing => {
                            Err(ProgressError::Indeterminate(format!(
                                "advance outcome unknown and register stayed unchanged: {detail}"
                            )))
                        }
                        Some(_) => Err(ProgressError::CasConflict),
                        None => Err(ProgressError::Indeterminate(format!(
                            "advance outcome unknown and register disappeared: {detail}"
                        ))),
                    }
                }
            }
        })
    }
}

fn require_attested_capabilities(
    capabilities: RegisterCapabilities,
) -> Result<(), ProgressStoreConfigError> {
    if capabilities.conditional_write != ConditionalWrite::VersionMatch {
        return Err(ProgressStoreConfigError::UnsupportedCapability(
            "conditional_write must be VersionMatch",
        ));
    }
    if capabilities.read_consistency != ReadConsistency::StronglyConsistent {
        return Err(ProgressStoreConfigError::UnsupportedCapability(
            "read_consistency must be StronglyConsistent",
        ));
    }
    Ok(())
}

fn encode_record(
    register: &ProgressRegister,
    max_token_bytes: usize,
    max_commit_ref_bytes: usize,
) -> Result<Vec<u8>, ProgressError> {
    register.binding.validate()?;
    let token = register.binding_token.as_str().as_bytes();
    if token.is_empty() || token.len() > max_token_bytes {
        return Err(ProgressError::MalformedRecord(format!(
            "binding_token byte length {} out of bounds 1..={max_token_bytes}",
            token.len()
        )));
    }
    let commit_ref = register
        .last_commit_ref
        .as_ref()
        .map(|s| s.as_bytes())
        .unwrap_or_default();
    if register
        .last_commit_ref
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(ProgressError::MalformedRecord(
            "last_commit_ref is present but empty".into(),
        ));
    }
    if commit_ref.len() > max_commit_ref_bytes {
        return Err(ProgressError::MalformedRecord(format!(
            "last_commit_ref byte length {} exceeds {max_commit_ref_bytes}",
            commit_ref.len()
        )));
    }
    let token_len = u16::try_from(token.len())
        .map_err(|_| ProgressError::MalformedRecord("binding_token > u16::MAX".into()))?;
    let commit_len = u16::try_from(commit_ref.len())
        .map_err(|_| ProgressError::MalformedRecord("last_commit_ref > u16::MAX".into()))?;
    let mut out = Vec::with_capacity(PROGRESS_CODEC_HEADER_BYTES + token.len() + commit_ref.len());
    out.extend_from_slice(&CODEC_MAGIC);
    out.push(CODEC_VERSION);
    let mut flags = 0u8;
    if !commit_ref.is_empty() {
        flags |= FLAG_HAS_COMMIT;
    }
    out.push(flags);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&register.binding.binding_epoch.to_le_bytes());
    out.extend_from_slice(&register.frontier.get().to_le_bytes());
    out.extend_from_slice(&token_len.to_le_bytes());
    out.extend_from_slice(&commit_len.to_le_bytes());
    out.extend_from_slice(token);
    out.extend_from_slice(commit_ref);
    if out.len() > MAX_PROGRESS_RECORD_BYTES {
        return Err(ProgressError::MalformedRecord(format!(
            "encoded register {} exceeds {MAX_PROGRESS_RECORD_BYTES}",
            out.len()
        )));
    }
    Ok(out)
}

fn decode_record(
    bytes: &[u8],
    key: &BindingKey,
    max_token_bytes: usize,
    max_commit_ref_bytes: usize,
) -> Result<ProgressRegister, ProgressError> {
    if bytes.len() > MAX_PROGRESS_RECORD_BYTES {
        return Err(ProgressError::MalformedRecord(format!(
            "record size {} exceeds {MAX_PROGRESS_RECORD_BYTES}",
            bytes.len()
        )));
    }
    if bytes.len() < PROGRESS_CODEC_HEADER_BYTES {
        return Err(ProgressError::MalformedRecord(format!(
            "record too short: {} bytes",
            bytes.len()
        )));
    }
    if bytes[..4] != CODEC_MAGIC {
        return Err(ProgressError::MalformedRecord("invalid codec magic".into()));
    }
    if bytes[4] != CODEC_VERSION {
        return Err(ProgressError::MalformedRecord(format!(
            "unsupported codec version {}",
            bytes[4]
        )));
    }
    let flags = bytes[5];
    if flags & !FLAG_HAS_COMMIT != 0 {
        return Err(ProgressError::MalformedRecord(format!(
            "unknown codec flags {flags:#x}"
        )));
    }
    let reserved = u16::from_le_bytes([bytes[6], bytes[7]]);
    if reserved != 0 {
        return Err(ProgressError::MalformedRecord(
            "reserved bits must be zero".into(),
        ));
    }
    let binding_epoch = u64::from_le_bytes(bytes[8..16].try_into().unwrap_or([0u8; 8]));
    if binding_epoch == 0 {
        return Err(ProgressError::MalformedRecord(
            "binding_epoch must be nonzero".into(),
        ));
    }
    let frontier = u64::from_le_bytes(bytes[16..24].try_into().unwrap_or([0u8; 8]));
    let token_len = usize::from(u16::from_le_bytes([bytes[24], bytes[25]]));
    let commit_len = usize::from(u16::from_le_bytes([bytes[26], bytes[27]]));
    if token_len == 0 || token_len > max_token_bytes {
        return Err(ProgressError::MalformedRecord(format!(
            "binding_token byte length {token_len} out of bounds 1..={max_token_bytes}"
        )));
    }
    if commit_len > max_commit_ref_bytes {
        return Err(ProgressError::MalformedRecord(format!(
            "last_commit_ref byte length {commit_len} exceeds {max_commit_ref_bytes}"
        )));
    }
    if (flags & FLAG_HAS_COMMIT) == 0 && commit_len != 0 {
        return Err(ProgressError::MalformedRecord(
            "commit flag unset but commit_len != 0".into(),
        ));
    }
    if (flags & FLAG_HAS_COMMIT) != 0 && commit_len == 0 {
        return Err(ProgressError::MalformedRecord(
            "commit flag set but commit_len == 0".into(),
        ));
    }
    let expected_len = PROGRESS_CODEC_HEADER_BYTES
        .checked_add(token_len)
        .and_then(|v| v.checked_add(commit_len))
        .ok_or_else(|| ProgressError::MalformedRecord("record length overflow".into()))?;
    if bytes.len() != expected_len {
        return Err(ProgressError::MalformedRecord(format!(
            "record length mismatch: got {}, expected {expected_len} (reject trailing/truncated)",
            bytes.len()
        )));
    }
    let token_start = PROGRESS_CODEC_HEADER_BYTES;
    let token_end = token_start + token_len;
    let commit_end = token_end + commit_len;
    let token = std::str::from_utf8(&bytes[token_start..token_end])
        .map_err(|error| ProgressError::MalformedRecord(format!("binding_token utf8: {error}")))?;
    let binding_token = BindingToken::new(token)
        .map_err(|error| ProgressError::MalformedRecord(format!("binding_token: {error}")))?;
    let last_commit_ref = if commit_len == 0 {
        None
    } else {
        let commit = std::str::from_utf8(&bytes[token_end..commit_end]).map_err(|error| {
            ProgressError::MalformedRecord(format!("last_commit_ref utf8: {error}"))
        })?;
        if commit.trim().is_empty() {
            return Err(ProgressError::MalformedRecord(
                "last_commit_ref present but empty".into(),
            ));
        }
        Some(commit.to_owned())
    };
    let binding = ConsumerBinding {
        workload_id: key.workload_id.clone(),
        canon_id: key.canon_id.clone(),
        verse_id: key.verse_id.clone(),
        binding_epoch,
    };
    binding.validate()?;
    Ok(ProgressRegister {
        binding,
        binding_token,
        frontier: SourceOffset::new(frontier),
        last_commit_ref,
    })
}

fn safe_hex_component(
    raw: &str,
    label: &'static str,
    max_bytes: usize,
) -> Result<String, ProgressError> {
    let bytes = raw.as_bytes();
    if bytes.is_empty() {
        return Err(ProgressError::Io(format!("{label} must be non-empty")));
    }
    if bytes.len() > max_bytes {
        return Err(ProgressError::Io(format!(
            "{label} length {} exceeds {max_bytes} bytes",
            bytes.len()
        )));
    }
    Ok(hex_encode(bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(char::from(HEX[(byte >> 4) as usize]));
        out.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use object_store::memory::InMemory;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn key() -> BindingKey {
        BindingKey::new(
            WorkloadId::new("wl-1").expect("workload"),
            CanonRef::new("canon/a").expect("canon"),
            VerseRef::new("verse:1").expect("verse"),
        )
    }

    fn store_with_memory_backend() -> ObjectStoreProgressStore {
        ObjectStoreProgressStore::new(
            Arc::new(InMemory::new()),
            ObjectPath::from("unit"),
            RegisterCapabilities::amazon_s3(),
        )
        .expect("store")
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    enum FaultMode {
        #[default]
        None,
        UnknownBeforeWrite,
        UnknownAfterWrite,
    }

    #[derive(Debug, Default)]
    struct FakeConditionalRegister {
        inner: Mutex<FakeInner>,
    }

    #[derive(Debug, Default)]
    struct FakeInner {
        objects: HashMap<String, ConditionalValue>,
        next_tag: u64,
        next_fault: FaultMode,
    }

    impl FakeConditionalRegister {
        fn new() -> Self {
            Self::default()
        }

        fn arm_fault(&self, mode: FaultMode) {
            let mut guard = self.inner.lock().expect("lock");
            guard.next_fault = mode;
        }
    }

    impl ConditionalObjectRegister for FakeConditionalRegister {
        fn read<'a>(
            &'a self,
            path: &'a ObjectPath,
        ) -> ProgressFuture<'a, Result<Option<ConditionalValue>, ProgressError>> {
            Box::pin(async move {
                let guard = self.inner.lock().expect("lock");
                Ok(guard.objects.get(path.as_ref()).cloned())
            })
        }

        fn compare_and_swap<'a>(
            &'a self,
            path: &'a ObjectPath,
            expected: Option<&'a ConditionalVersion>,
            value: Vec<u8>,
        ) -> ProgressFuture<'a, Result<ConditionalSwap, ProgressError>> {
            Box::pin(async move {
                let mut guard = self.inner.lock().expect("lock");
                let current = guard.objects.get(path.as_ref()).cloned();
                let matches = match (expected, current.as_ref()) {
                    (None, None) => true,
                    (None, Some(_)) => false,
                    (Some(_), None) => false,
                    (Some(expected), Some(current)) => &current.version == expected,
                };
                if !matches {
                    return Ok(ConditionalSwap::Conflict);
                }
                let fault = guard.next_fault;
                guard.next_fault = FaultMode::None;
                if fault == FaultMode::UnknownBeforeWrite {
                    return Ok(ConditionalSwap::Unknown(
                        "simulated lost reply (before write)".into(),
                    ));
                }
                guard.next_tag = guard.next_tag.saturating_add(1);
                let new_version = ConditionalVersion {
                    e_tag: Some(format!("etag-{}", guard.next_tag)),
                    version: None,
                };
                guard.objects.insert(
                    path.as_ref().to_owned(),
                    ConditionalValue {
                        bytes: value,
                        version: new_version.clone(),
                    },
                );
                if fault == FaultMode::UnknownAfterWrite {
                    Ok(ConditionalSwap::Unknown(
                        "simulated lost reply (after write)".into(),
                    ))
                } else {
                    Ok(ConditionalSwap::Applied(new_version))
                }
            })
        }
    }

    fn store_with_fake(fake: Arc<FakeConditionalRegister>) -> ObjectStoreProgressStore {
        ObjectStoreProgressStore::with_test_register(fake, ObjectPath::from("unit"))
    }

    #[test]
    fn codec_rejects_trailing_bytes() {
        let register = ProgressRegister {
            binding: ConsumerBinding {
                workload_id: WorkloadId::new("w").expect("id"),
                canon_id: CanonRef::new("c").expect("canon"),
                verse_id: VerseRef::new("v").expect("verse"),
                binding_epoch: 7,
            },
            binding_token: BindingToken::new("tok").expect("token"),
            frontier: SourceOffset::new(9),
            last_commit_ref: Some("commit-x".into()),
        };
        let mut encoded = encode_record(
            &register,
            MAX_PROGRESS_TOKEN_BYTES,
            MAX_PROGRESS_COMMIT_REF_BYTES,
        )
        .expect("encode");
        encoded.push(0xff);
        let err = decode_record(
            &encoded,
            &key(),
            MAX_PROGRESS_TOKEN_BYTES,
            MAX_PROGRESS_COMMIT_REF_BYTES,
        )
        .expect_err("reject trailing");
        assert!(matches!(err, ProgressError::MalformedRecord(_)));
    }

    #[test]
    fn acquire_race_takeover_bumps_epoch() {
        let store = store_with_memory_backend();
        let a = BindingToken::new("token-a").expect("token");
        let b = BindingToken::new("token-b").expect("token");
        let first = block_on(store.acquire_or_renew(key(), &a)).expect("first");
        let second = block_on(store.acquire_or_renew(key(), &b)).expect("second");
        assert_eq!(first.binding.binding_epoch, 1);
        assert_eq!(second.binding.binding_epoch, 2);
    }

    #[test]
    fn same_token_renew_keeps_epoch() {
        let store = store_with_memory_backend();
        let token = BindingToken::new("token-a").expect("token");
        let first = block_on(store.acquire_or_renew(key(), &token)).expect("first");
        let renew = block_on(store.acquire_or_renew(key(), &token)).expect("renew");
        assert_eq!(first.binding.binding_epoch, renew.binding.binding_epoch);
    }

    #[test]
    fn restart_takeover_carries_frontier_and_commit_ref() {
        let store = store_with_memory_backend();
        let a = BindingToken::new("proc-a").expect("token");
        let b = BindingToken::new("proc-b").expect("token");
        let fence_a = block_on(store.acquire_or_renew(key(), &a)).expect("acquire a");
        block_on(store.advance(&fence_a, SourceOffset::new(11), "commit-a".into()))
            .expect("advance a");
        let fence_b = block_on(store.acquire_or_renew(key(), &b)).expect("takeover b");
        assert_eq!(fence_b.binding.binding_epoch, 2);
        let observed =
            block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
                .expect("observe")
                .expect("present")
                .0;
        assert_eq!(observed.frontier, SourceOffset::new(11));
        assert_eq!(observed.last_commit_ref.as_deref(), Some("commit-a"));
        assert_eq!(observed.binding_token.as_str(), "proc-b");
    }

    #[test]
    fn stale_advance_rejected_and_cas_conflict_does_not_mutate() {
        let store = store_with_memory_backend();
        let a = BindingToken::new("host-a").expect("token");
        let b = BindingToken::new("host-b").expect("token");
        let fence_a = block_on(store.acquire_or_renew(key(), &a)).expect("acquire a");
        block_on(store.advance(&fence_a, SourceOffset::new(5), "commit-a".into()))
            .expect("advance a");
        let _fence_b = block_on(store.acquire_or_renew(key(), &b)).expect("takeover b");
        let err = block_on(store.advance(&fence_a, SourceOffset::new(6), "stale".into()))
            .expect_err("stale advance rejected");
        assert!(matches!(
            err,
            ProgressError::FenceHeld | ProgressError::StaleBinding
        ));
        let observed =
            block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
                .expect("observe")
                .expect("present")
                .0;
        assert_eq!(observed.frontier, SourceOffset::new(5));
        assert_eq!(observed.binding.binding_epoch, 2);
    }

    #[test]
    fn lost_reply_after_dispatch_is_resolved_only_by_reread() {
        let fake = Arc::new(FakeConditionalRegister::new());
        let store = store_with_fake(Arc::clone(&fake));
        let token = BindingToken::new("worker-a").expect("token");
        let fence = block_on(store.acquire_or_renew(key(), &token)).expect("acquire");
        fake.arm_fault(FaultMode::UnknownAfterWrite);
        // CAS reply is lost, but reread sees exact intended register and reports applied.
        let (register, _) =
            block_on(store.advance(&fence, SourceOffset::new(3), "commit-a".into()))
                .expect("reread resolves");
        assert_eq!(register.frontier, SourceOffset::new(3));
        assert_eq!(register.last_commit_ref.as_deref(), Some("commit-a"));
    }

    #[test]
    fn lost_reply_before_write_is_indeterminate_and_does_not_mutate() {
        let fake = Arc::new(FakeConditionalRegister::new());
        let store = store_with_fake(Arc::clone(&fake));
        let token = BindingToken::new("worker-a").expect("token");
        let fence = block_on(store.acquire_or_renew(key(), &token)).expect("acquire");
        fake.arm_fault(FaultMode::UnknownBeforeWrite);
        let err = block_on(store.advance(&fence, SourceOffset::new(3), "commit-a".into()))
            .expect_err("lost reply before write");
        assert!(matches!(err, ProgressError::Indeterminate(_)));
        let observed =
            block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
                .expect("observe")
                .expect("present")
                .0;
        assert_eq!(observed.frontier, SourceOffset::new(0));
        assert!(observed.last_commit_ref.is_none());
    }

    #[test]
    fn progress_version_tracks_object_store_witness_not_local_counter() {
        let fake = Arc::new(FakeConditionalRegister::new());
        let store = store_with_fake(Arc::clone(&fake));
        let token = BindingToken::new("worker-a").expect("token");
        let fence = block_on(store.acquire_or_renew(key(), &token)).expect("acquire");
        let (register, advance_version) =
            block_on(store.advance(&fence, SourceOffset::new(3), "commit-a".into()))
                .expect("advance");
        assert_eq!(register.frontier, SourceOffset::new(3));
        let (observed, observe_version) =
            block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
                .expect("observe")
                .expect("present");
        assert_eq!(observed, register);
        assert_eq!(
            advance_version, observe_version,
            "observe must re-derive the same durable witness as advance"
        );
        // Same etag must keep mapping; a new write must change it.
        let (_, second_version) =
            block_on(store.advance(&fence, SourceOffset::new(4), "commit-b".into()))
                .expect("second advance");
        assert_ne!(advance_version, second_version);
        let reobserve =
            block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
                .expect("reobserve")
                .expect("present")
                .1;
        assert_eq!(second_version, reobserve);
    }

    #[test]
    fn malformed_record_fails_closed() {
        let fake = Arc::new(FakeConditionalRegister::new());
        let store = store_with_fake(Arc::clone(&fake));
        let path = store.register_path_for_key(&key()).expect("path");
        {
            let mut guard = fake.inner.lock().expect("lock");
            guard.objects.insert(
                path.as_ref().to_owned(),
                ConditionalValue {
                    bytes: vec![0xde, 0xad, 0xbe, 0xef],
                    version: ConditionalVersion {
                        e_tag: Some("etag-1".into()),
                        version: None,
                    },
                },
            );
        }
        let err = block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
            .expect_err("malformed should fail closed");
        assert!(matches!(err, ProgressError::MalformedRecord(_)));
    }

    fn full_schedule_case(a_resumes_before_b_advance: bool) {
        let store = store_with_memory_backend();
        let token_a = BindingToken::new("zombie-a").expect("token");
        let token_b = BindingToken::new("worker-b").expect("token");
        let fence_a = block_on(store.acquire_or_renew(key(), &token_a)).expect("a epoch1");
        // A pauses after producing output (modeled here as having the commit ref ready).
        let commit_a = "commit-a-epoch1".to_owned();
        let fence_b = block_on(store.acquire_or_renew(key(), &token_b)).expect("b takeover epoch2");
        assert_eq!(fence_b.binding.binding_epoch, 2);
        if a_resumes_before_b_advance {
            let err = block_on(store.advance(&fence_a, SourceOffset::new(1), commit_a.clone()))
                .expect_err("stale before b advance");
            assert!(matches!(
                err,
                ProgressError::FenceHeld | ProgressError::StaleBinding
            ));
        }
        block_on(store.advance(&fence_b, SourceOffset::new(1), "commit-b-epoch2".into()))
            .expect("b wins canonical register");
        let err = block_on(store.advance(&fence_a, SourceOffset::new(2), commit_a))
            .expect_err("a still stale");
        assert!(matches!(
            err,
            ProgressError::FenceHeld | ProgressError::StaleBinding
        ));
        let final_register =
            block_on(store.observe(&key().workload_id, &key().canon_id, &key().verse_id))
                .expect("observe")
                .expect("register")
                .0;
        assert_eq!(final_register.binding.binding_epoch, 2);
        assert_eq!(final_register.binding_token.as_str(), "worker-b");
        assert_eq!(final_register.frontier, SourceOffset::new(1));
        assert_eq!(
            final_register.last_commit_ref.as_deref(),
            Some("commit-b-epoch2")
        );
    }

    #[test]
    fn full_a_epoch1_then_b_epoch2_then_a_resume_before_and_after() {
        full_schedule_case(true);
        full_schedule_case(false);
    }
}
