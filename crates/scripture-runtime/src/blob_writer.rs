//! Cross-Verse write-optimised blob batching.
//!
//! A Scribe accumulates sealed chunks from many Verse assignments into one
//! object, then appends a per-Verse [`DataRef`] under that Verse's authority
//! fence. The blob PUT is not a log append and grants nothing — a Verse that
//! loses authority between PUT and pointer append simply never commits its
//! DataRef, and those blob bytes stay unreferenced garbage.
//!
//! Staging objects under [`crate::blob_rewrite::DEFAULT_STAGING_PREFIX`] are
//! short-lived. A background rewrite (see [`crate::blob_rewrite`]) moves each
//! Verse's chunks into per-Verse read-optimised objects and appends superseding
//! pointers. Prefix-based retention applies to rewritten objects; staging
//! blobs become collectable only after every referenced [`ChunkId`] has a
//! durable superseding pointer in the log.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use scripture::{
    ChunkAppendAck, ChunkDigest, ChunkError, ChunkId, ChunkLogError, CohortId, DataRef,
    DataRefError, Frame, JournalId, Record, RecordOffset, SealedChunk, SubmissionRef, decode_index,
    encode_data_ref, encoded_chunk_len,
};

use self::clock_shim::{BlobClock, SystemBlobClock};

/// Default target object size: large enough to amortise PUTs on object storage.
pub const DEFAULT_TARGET_BLOB_BYTES: usize = 64 * 1024 * 1024;
/// Default linger: bounds latency when traffic is too slow to hit the size cut.
pub const DEFAULT_MAX_LINGER: Duration = Duration::from_millis(100);

/// Cut / accumulate configuration for one Scribe-local blob writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobWriterConfig {
    /// Cut when accumulated sealed-chunk bytes reach this size.
    pub target_blob_bytes: usize,
    /// Cut when the oldest buffered envelope has waited this long.
    pub max_linger: Duration,
    /// Object-store prefix for write-optimised blobs (`blobs/v1/...`).
    pub blob_prefix: String,
}

impl Default for BlobWriterConfig {
    fn default() -> Self {
        Self {
            target_blob_bytes: DEFAULT_TARGET_BLOB_BYTES,
            max_linger: DEFAULT_MAX_LINGER,
            blob_prefix: "blobs/v1".into(),
        }
    }
}

/// A stable, generation-free submission offered to the blob writer.
///
/// This is the spool seam.  It deliberately carries no generation-specific
/// sealed bytes: a successor must reseal this same evidence under its active
/// generation after a handoff.
#[derive(Debug, Clone)]
pub struct BlobEnvelope {
    /// Stable Verse / assignment identity used to group contiguous runs.
    pub verse_key: String,
    /// Stable chunk identity used for acknowledgements across resealing.
    pub chunk_id: ChunkId,
    /// Journal carried by this one-frame chunk.
    pub journal_id: JournalId,
    /// Cohort policy for this chunk.
    pub cohort_id: CohortId,
    /// Records to seal.
    pub records: Vec<Record>,
    /// Producer submission spans to seal with the records.
    pub submissions: Vec<SubmissionRef>,
}

/// Abstract source of sealed envelopes (ingress today; spool later).
#[async_trait]
pub trait BlobEnvelopeSource: Send {
    /// Returns the next envelope, or `None` when the source is drained.
    async fn next_envelope(&mut self) -> Result<Option<BlobEnvelope>, BlobWriterError>;
}

/// Seals a stable envelope under the currently active Verse generation.
///
/// Implementations allocate `base_offset` from their writer's current offset
/// and embed that writer's generation and id.  The trait is intentionally
/// called only at commit time, so a handoff replay never reuses old-generation
/// bytes.
#[async_trait]
pub trait VerseSealer: Send {
    /// Seal `envelope` under the active generation.
    async fn seal(&mut self, envelope: &BlobEnvelope) -> Result<SealedChunk, BlobWriterError>;
}

/// Per-Verse fenced DataRef append target.
///
/// Implementations typically wrap a [`scripture::ChunkLogWriter`]. Tests inject
/// a target that fails closed for one Verse to prove sibling isolation.
#[async_trait]
pub trait DataRefAppendTarget: Send {
    /// Appends `data_ref` for `sealed` under this Verse's authority fence.
    async fn append_data_ref(
        &mut self,
        sealed: &SealedChunk,
        data_ref: &DataRef,
    ) -> Result<ChunkAppendAck, BlobWriterError>;

    /// Appends one or more DataRefs under a single Verse fence.
    ///
    /// Default: one SRDF append when `items.len() == 1`, otherwise one SRRB
    /// ReferenceBatch. Override only when the target cannot speak SRRB yet.
    async fn append_data_refs(
        &mut self,
        items: &[(&SealedChunk, &DataRef)],
    ) -> Result<ChunkAppendAck, BlobWriterError> {
        match items {
            [] => Err(BlobWriterError::Invariant(
                "append_data_refs requires at least one DataRef".into(),
            )),
            [(sealed, data_ref)] => self.append_data_ref(sealed, data_ref).await,
            _ => Err(BlobWriterError::Invariant(
                "default append_data_refs cannot encode ReferenceBatch; override required".into(),
            )),
        }
    }
}

/// Why a blob was cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobCutReason {
    /// Accumulated bytes reached [`BlobWriterConfig::target_blob_bytes`].
    Size,
    /// Oldest envelope waited [`BlobWriterConfig::max_linger`].
    Linger,
    /// Source drained; flush remaining work.
    SourceDrained,
}

/// A cut before any generation binding has happened.
#[derive(Debug, Clone)]
pub struct CutPlan {
    /// Which trigger fired.
    pub reason: BlobCutReason,
    /// Stable envelopes to seal and commit.
    pub envelopes: Vec<BlobEnvelope>,
    blob_prefix: String,
}

/// Blob writer / commit failures.
#[derive(Debug, thiserror::Error)]
pub enum BlobWriterError {
    /// Configuration rejected.
    #[error("blob writer config: {0}")]
    Config(String),
    /// Object-store I/O failed.
    #[error("blob object store: {0}")]
    ObjectStore(String),
    /// DataRef codec failed.
    #[error(transparent)]
    DataRef(#[from] DataRefError),
    /// Chunk codec rejected a pre-seal size estimate or a sealed result.
    #[error(transparent)]
    Chunk(#[from] ChunkError),
    /// Fenced log append failed (authority moved, poison, etc.).
    #[error(transparent)]
    ChunkLog(#[from] ChunkLogError),
    /// Envelope source failed.
    #[error("blob envelope source: {0}")]
    Source(String),
    /// Internal invariant broken (should not happen on a correct writer).
    #[error("blob writer invariant: {0}")]
    Invariant(String),
    /// Existing or recovered blob bytes disagree with their content address.
    #[error("blob digest verification failed for {key}")]
    BlobDigestMismatch {
        /// Content-addressed object key.
        key: String,
    },
}

/// Outcome of one placement's post-PUT DataRef append.
#[derive(Debug)]
pub struct PlacementCommitOutcome {
    /// Verse / assignment key.
    pub verse_key: String,
    /// Immutable chunk identity, stable across a retry or handoff replay.
    pub chunk_id: ChunkId,
    /// Committed ack, or rejection (no producer ack).
    pub result: Result<ChunkAppendAck, BlobWriterError>,
}

/// Seals, verifies, and commits a cut plan.
///
/// A placement whose append fails does **not** roll back siblings that already
/// committed. Producers for the failed Verse must not be acknowledged.
pub async fn commit_cut_plan(
    store: &Arc<dyn ObjectStore>,
    plan: &CutPlan,
    sealers: &mut BTreeMap<String, Box<dyn VerseSealer>>,
    targets: &mut BTreeMap<String, Box<dyn DataRefAppendTarget>>,
) -> Result<Vec<PlacementCommitOutcome>, BlobWriterError> {
    struct Placement {
        verse_key: String,
        sealed: SealedChunk,
        offset: u64,
        length: u64,
        record_count: u32,
    }

    let mut groups: Vec<(String, Vec<SealedChunk>)> = Vec::new();
    for envelope in &plan.envelopes {
        let sealer = sealers.get_mut(&envelope.verse_key).ok_or_else(|| {
            BlobWriterError::Invariant(format!("no sealer for verse {}", envelope.verse_key))
        })?;
        let sealed = sealer.seal(envelope).await?;
        if sealed.chunk_id != envelope.chunk_id || ChunkDigest::of(&sealed.bytes) != sealed.digest {
            return Err(BlobWriterError::Invariant(
                "VerseSealer returned bytes that do not match stable identity".into(),
            ));
        }
        if let Some((_, chunks)) = groups
            .iter_mut()
            .find(|(key, _)| key == &envelope.verse_key)
        {
            chunks.push(sealed);
        } else {
            groups.push((envelope.verse_key.clone(), vec![sealed]));
        }
    }

    let mut bytes = BytesMut::new();
    let mut placements = Vec::with_capacity(plan.envelopes.len());
    for (verse_key, chunks) in groups {
        for sealed in chunks {
            let index = decode_index(&sealed.bytes)?;
            let [frame] = index.frames.as_slice() else {
                return Err(BlobWriterError::Invariant(
                    "sealed chunk has not exactly one frame".into(),
                ));
            };
            let offset = u64::try_from(bytes.len())
                .map_err(|_| BlobWriterError::Invariant("blob offset exceeds u64".into()))?;
            let length = u64::try_from(sealed.bytes.len())
                .map_err(|_| BlobWriterError::Invariant("chunk length exceeds u64".into()))?;
            bytes.extend_from_slice(&sealed.bytes);
            placements.push(Placement {
                verse_key: verse_key.clone(),
                sealed,
                offset,
                length,
                record_count: frame.record_count,
            });
        }
    }
    let bytes = bytes.freeze();
    let blob_digest = ChunkDigest::of(&bytes);
    let blob_key = format!("{}/{blob_digest}", plan_prefix(plan)?);
    put_and_verify(store, &blob_key, bytes.clone(), blob_digest).await?;

    // Group placements by Verse so many DataRefs share one lawful append
    // (ReferenceBatch). Never batch metadata across Verses.
    let mut by_verse: Vec<(String, Vec<Placement>)> = Vec::new();
    for placement in placements {
        if let Some((_, group)) = by_verse
            .iter_mut()
            .find(|(key, _)| key == &placement.verse_key)
        {
            group.push(placement);
        } else {
            by_verse.push((placement.verse_key.clone(), vec![placement]));
        }
    }

    let mut outcomes = Vec::new();
    for (verse_key, group) in by_verse {
        let mut prepared = Vec::with_capacity(group.len());
        for placement in &group {
            let data_ref = DataRef {
                blob_key: blob_key.clone(),
                offset: placement.offset,
                length: placement.length,
                record_count: placement.record_count,
                chunk_id: placement.sealed.chunk_id,
                chunk_digest: placement.sealed.digest,
                blob_digest,
            };
            let _ = encode_data_ref(&data_ref)?;
            prepared.push((placement, data_ref));
        }
        let items: Vec<(&SealedChunk, &DataRef)> = prepared
            .iter()
            .map(|(placement, data_ref)| (&placement.sealed, data_ref))
            .collect();
        let batch_result = match targets.get_mut(&verse_key) {
            Some(target) => target.append_data_refs(&items).await,
            None => Err(BlobWriterError::Invariant(format!(
                "no DataRef append target for verse {verse_key}"
            ))),
        };
        match batch_result {
            Ok(ack) => {
                let mut next = ack.first_offset;
                for (placement, data_ref) in prepared {
                    let placement_next = next
                        .checked_add(data_ref.record_count as usize)
                        .ok_or_else(|| {
                            BlobWriterError::Invariant("placement offset overflow".into())
                        })?;
                    outcomes.push(PlacementCommitOutcome {
                        verse_key: verse_key.clone(),
                        chunk_id: placement.sealed.chunk_id,
                        result: Ok(ChunkAppendAck {
                            slot: ack.slot,
                            chunk_id: placement.sealed.chunk_id,
                            first_offset: next,
                            next_offset: placement_next,
                            record_count: data_ref.record_count,
                        }),
                    });
                    next = placement_next;
                }
            }
            Err(error) => {
                let mut group = group.into_iter();
                if let Some(first) = group.next() {
                    outcomes.push(PlacementCommitOutcome {
                        verse_key: verse_key.clone(),
                        chunk_id: first.sealed.chunk_id,
                        result: Err(error),
                    });
                    for placement in group {
                        outcomes.push(PlacementCommitOutcome {
                            verse_key: verse_key.clone(),
                            chunk_id: placement.sealed.chunk_id,
                            result: Err(BlobWriterError::Invariant(
                                "verse ReferenceBatch rejected; sibling placements share the failure"
                                    .into(),
                            )),
                        });
                    }
                }
            }
        }
    }
    Ok(outcomes)
}

fn plan_prefix(plan: &CutPlan) -> Result<&str, BlobWriterError> {
    if plan.blob_prefix.is_empty() {
        return Err(BlobWriterError::Invariant(
            "cut plan has an empty blob prefix".into(),
        ));
    }
    Ok(&plan.blob_prefix)
}

pub(crate) async fn put_and_verify(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    bytes: Bytes,
    digest: ChunkDigest,
) -> Result<(), BlobWriterError> {
    let path = ObjectPath::from(key);
    for _ in 0..2 {
        match store
            .put_opts(
                &path,
                PutPayload::from_bytes(bytes.clone()),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(_) => {}
        }
        match store.get(&path).await {
            Ok(result) => {
                let actual = result
                    .bytes()
                    .await
                    .map_err(|error| BlobWriterError::ObjectStore(error.to_string()))?;
                if ChunkDigest::of(&actual) != digest {
                    return Err(BlobWriterError::BlobDigestMismatch { key: key.into() });
                }
                return Ok(());
            }
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(error) => return Err(BlobWriterError::ObjectStore(error.to_string())),
        }
    }
    Err(BlobWriterError::ObjectStore(format!(
        "object {key} absent after indeterminate PUT retry"
    )))
}

struct BufferedEnvelope {
    envelope: BlobEnvelope,
    buffered_at: Duration,
}

/// Accumulates generation-free envelopes across Verses and cuts shared blobs.
pub struct BlobWriter<C = SystemBlobClock> {
    config: BlobWriterConfig,
    clock: C,
    buffer: Vec<BufferedEnvelope>,
    buffered_bytes: usize,
}

impl BlobWriter<SystemBlobClock> {
    /// Creates a writer with the system clock.
    pub fn new(config: BlobWriterConfig) -> Result<Self, BlobWriterError> {
        Self::with_clock(config, SystemBlobClock)
    }
}

impl<C: BlobClock> BlobWriter<C> {
    /// Creates a writer with an injected clock (tests use a manual clock).
    pub fn with_clock(config: BlobWriterConfig, clock: C) -> Result<Self, BlobWriterError> {
        if config.target_blob_bytes == 0 {
            return Err(BlobWriterError::Config(
                "target_blob_bytes must be nonzero".into(),
            ));
        }
        if config.max_linger.is_zero() {
            return Err(BlobWriterError::Config("max_linger must be nonzero".into()));
        }
        if config.blob_prefix.trim().is_empty() {
            return Err(BlobWriterError::Config(
                "blob_prefix must be nonempty".into(),
            ));
        }
        Ok(Self {
            config,
            clock,
            buffer: Vec::new(),
            buffered_bytes: 0,
        })
    }

    /// Pushes one envelope and returns a cut when a trigger fires.
    pub fn push(&mut self, envelope: BlobEnvelope) -> Result<Option<CutPlan>, BlobWriterError> {
        let len = estimate_envelope_len(&envelope)?;
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(len)
            .ok_or_else(|| BlobWriterError::Invariant("buffered_bytes overflow".into()))?;
        self.buffer.push(BufferedEnvelope {
            envelope,
            buffered_at: self.clock.now(),
        });
        if self.buffered_bytes >= self.config.target_blob_bytes {
            return Ok(Some(self.cut(BlobCutReason::Size)?));
        }
        if self.linger_due() {
            return Ok(Some(self.cut(BlobCutReason::Linger)?));
        }
        Ok(None)
    }

    /// Polls linger without a new envelope (call from a timer loop).
    pub fn poll_linger(&mut self) -> Result<Option<CutPlan>, BlobWriterError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        if self.linger_due() {
            return Ok(Some(self.cut(BlobCutReason::Linger)?));
        }
        Ok(None)
    }

    /// Flushes any buffered envelopes because the source drained.
    pub fn flush_drained(&mut self) -> Result<Option<CutPlan>, BlobWriterError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.cut(BlobCutReason::SourceDrained)?))
    }

    fn linger_due(&self) -> bool {
        let Some(oldest) = self.buffer.first() else {
            return false;
        };
        self.clock.now().saturating_sub(oldest.buffered_at) >= self.config.max_linger
    }

    fn cut(&mut self, reason: BlobCutReason) -> Result<CutPlan, BlobWriterError> {
        let buffered = std::mem::take(&mut self.buffer);
        self.buffered_bytes = 0;
        if buffered.is_empty() {
            return Err(BlobWriterError::Invariant("cut with empty buffer".into()));
        }

        Ok(CutPlan {
            reason,
            envelopes: buffered.into_iter().map(|item| item.envelope).collect(),
            blob_prefix: self.config.blob_prefix.clone(),
        })
    }
}

fn estimate_envelope_len(envelope: &BlobEnvelope) -> Result<usize, BlobWriterError> {
    encoded_chunk_len(&[Frame {
        journal_id: envelope.journal_id,
        base_offset: RecordOffset::new(0),
        records: envelope.records.clone(),
        submissions: envelope.submissions.clone(),
    }])
    .map_err(BlobWriterError::from)
}

/// Clock shim so blob linger tests stay deterministic without pulling scripture's
/// full Clock trait into the runtime public surface.
pub mod clock_shim {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    /// Monotonic clock used only for blob linger decisions.
    pub trait BlobClock: Send + Sync {
        /// Elapsed monotonic time from an arbitrary origin.
        fn now(&self) -> Duration;
    }

    /// Production clock.
    #[derive(Debug, Clone, Copy)]
    pub struct SystemBlobClock;

    impl BlobClock for SystemBlobClock {
        fn now(&self) -> Duration {
            // Process-local origin is fine: linger is relative, not wall-clock.
            static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
            ORIGIN.get_or_init(Instant::now).elapsed()
        }
    }

    /// Manual clock for linger-trigger tests.
    #[derive(Debug, Default)]
    pub struct ManualBlobClock {
        nanos: AtomicU64,
    }

    impl ManualBlobClock {
        /// Creates a clock at zero.
        #[must_use]
        pub const fn new() -> Self {
            Self {
                nanos: AtomicU64::new(0),
            }
        }

        /// Advances the clock without sleeping.
        pub fn advance(&self, duration: Duration) {
            let add = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
            self.nanos.fetch_add(add, Ordering::Relaxed);
        }
    }

    impl BlobClock for ManualBlobClock {
        fn now(&self) -> Duration {
            Duration::from_nanos(self.nanos.load(Ordering::Relaxed))
        }
    }

    impl BlobClock for std::sync::Arc<ManualBlobClock> {
        fn now(&self) -> Duration {
            ManualBlobClock::now(self)
        }
    }
}
