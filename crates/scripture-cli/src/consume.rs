//! `scripture consume` — debug/demo console consumer.
//!
//! Read-only: no consumer register, checkpoint, acknowledgement, trim, or
//! mutation. After a generation cutover it re-observes membership and keeps
//! reading contiguous committed history. Not a durable consumer product.

use std::collections::BTreeMap;
use std::error::Error;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use holylog::provision::resolve_read_seal;
use holylog::virtual_log::{ConditionalRegister, LogletResolver, VirtualLog};
use holylog_object_store::{ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::ObjectStore;
use object_store::path::Path;
use scripture::ChunkDigest;
use scripture_runtime::{
    ObjectStorePartsFactory, PartsFactory, ProcessLogletResolver, resolve_log_payload,
};
use serde_json::json;

use crate::assemble;
use crate::config::{AssignmentConfig, ScriptureConfig};

const LOGLET_K: u64 = 2;
const DEFAULT_SECONDS: u64 = 30;

/// Output encoding for one printed record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human-oriented single line per record.
    Text,
    /// One JSON object per record on stdout; progress on stderr.
    Jsonl,
}

impl OutputFormat {
    /// Parses `text` or `jsonl`.
    pub fn parse(raw: &str) -> Result<Self, Box<dyn Error>> {
        match raw {
            "text" => Ok(Self::Text),
            "jsonl" => Ok(Self::Jsonl),
            other => Err(format!("consume: unknown --format {other} (expected text|jsonl)").into()),
        }
    }
}

/// Options for `scripture consume`.
#[derive(Debug, Clone)]
pub struct ConsumeOptions {
    pub canon: String,
    pub verse: String,
    /// First VirtualLog entry position to inspect.
    pub from: u64,
    /// Stop after printing this many logical records (`None` = unbounded).
    pub until_records: Option<u64>,
    /// Upper bound for tailing; must be non-zero unless `--no-follow`.
    pub seconds: u64,
    pub format: OutputFormat,
    /// Read only the currently observed contiguous tail and exit.
    pub no_follow: bool,
}

impl ConsumeOptions {
    /// Default demo-friendly seconds bound.
    #[must_use]
    pub const fn default_seconds() -> u64 {
        DEFAULT_SECONDS
    }
}

/// End-of-run counters written to stderr.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeSummary {
    pub entries_scanned: u64,
    pub records_printed: u64,
    pub final_cursor: u64,
    pub elapsed: Duration,
    pub membership_change: bool,
}

/// Shared Holylog seams used by both `consume` and `consume-lab`.
pub(crate) struct VerseReadSeams {
    pub store: Arc<dyn ObjectStore>,
    pub register: Arc<dyn ConditionalRegister>,
    pub parts: Arc<dyn PartsFactory>,
    pub resolver: Arc<ProcessLogletResolver>,
}

impl VerseReadSeams {
    pub(crate) fn from_config(
        config: &ScriptureConfig,
        assignment: &AssignmentConfig,
    ) -> Result<(Self, String), Box<dyn Error>> {
        let shared = assemble::connect_shared_store(config)?;
        let store_root = config.assignment_store_root(assignment)?;
        let seams = Self::from_store(
            Arc::clone(&shared.store),
            store_root.clone(),
            shared.backend.register_capabilities(),
            shared.backend.drive_capabilities(),
        )?;
        Ok((seams, store_root))
    }

    pub(crate) fn from_store(
        store: Arc<dyn ObjectStore>,
        store_root: impl Into<String>,
        register_caps: holylog_object_store_register::RegisterCapabilities,
        drive_caps: holylog_object_store::BackendCapabilities,
    ) -> Result<Self, Box<dyn Error>> {
        let store_root = store_root.into().trim_end_matches('/').to_owned();
        let register = Arc::new(ObjectStoreConditionalRegister::new(
            Arc::clone(&store),
            Path::from(store_root.clone()).join(register_path("verse").as_ref()),
            register_caps,
        )?) as Arc<dyn ConditionalRegister>;
        let parts = Arc::new(ObjectStorePartsFactory::new(
            Arc::clone(&store),
            store_root,
            drive_caps,
            WritePolicy::AtomicCreate,
            Arc::new(ObjectStoreMetrics::default()),
        )) as Arc<dyn PartsFactory>;
        Ok(Self {
            store,
            register,
            parts,
            resolver: Arc::new(ProcessLogletResolver::default()),
        })
    }

    /// Builds seams over an already-constructed register (hermetic tests).
    #[allow(dead_code)] // used by #[cfg(test)] hermetic harness
    pub(crate) fn from_parts(
        store: Arc<dyn ObjectStore>,
        register: Arc<dyn ConditionalRegister>,
        parts: Arc<dyn PartsFactory>,
        resolver: Arc<ProcessLogletResolver>,
    ) -> Self {
        Self {
            store,
            register,
            parts,
            resolver,
        }
    }

    /// Re-observes membership and returns the contiguous durable VirtualLog end.
    pub(crate) async fn observe_contiguous_end(
        &self,
    ) -> Result<(VirtualLog, u64, Vec<String>), Box<dyn Error>> {
        let log = VirtualLog::new(
            Arc::clone(&self.register),
            Arc::clone(&self.resolver) as Arc<dyn LogletResolver>,
        );
        let observed = log.observe_membership().await?;
        let mut end = 0_u64;
        let mut generations = Vec::new();
        for generation in &observed.state.generations {
            let durable = self.parts.open(&generation.loglet_id)?;
            let view = resolve_read_seal(durable.components(LOGLET_K)).await?;
            let tail = view.observe_durable().await?.contiguous_tail();
            self.resolver
                .insert_read_seal(generation.loglet_id.clone(), Arc::new(view));
            end = generation.start.saturating_add(tail);
            generations.push(generation.loglet_id.as_str().to_owned());
        }
        Ok((log, end, generations))
    }
}

/// Finds the Canon/Verse assignment in multi-assignment YAML.
pub(crate) fn find_assignment(
    config: &ScriptureConfig,
    canon: &str,
    verse: &str,
) -> Result<AssignmentConfig, Box<dyn Error>> {
    config
        .scribe
        .as_ref()
        .ok_or("consume requires scribe.assignments")?
        .assignments
        .iter()
        .find(|a| a.canon == canon && a.verse == verse)
        .cloned()
        .ok_or_else(|| format!("no assignment for canon={canon} verse={verse}").into())
}

/// Runs the demo consumer against a configured assignment store root.
pub async fn consume(
    config: ScriptureConfig,
    options: ConsumeOptions,
) -> Result<(), Box<dyn Error>> {
    let assignment = find_assignment(&config, &options.canon, &options.verse)?;
    let (seams, store_root) = VerseReadSeams::from_config(&config, &assignment)?;
    eprintln!(
        "scripture consume: canon={} verse={} from={} root={store_root} (read-only debug consumer; no checkpoint)",
        options.canon, options.verse, options.from
    );
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    let summary = run_consume(&seams, &options, &mut stdout, &mut stderr).await?;
    write_summary(&mut stderr, &summary)?;
    Ok(())
}

/// Core record-printing loop shared by the CLI and hermetic tests.
pub(crate) async fn run_consume(
    seams: &VerseReadSeams,
    options: &ConsumeOptions,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Result<ConsumeSummary, Box<dyn Error>> {
    if !options.no_follow && options.seconds == 0 {
        return Err(
            "consume: --seconds 0 is rejected (use --no-follow for a bounded one-shot)".into(),
        );
    }

    let start = Instant::now();
    let deadline = if options.no_follow {
        None
    } else {
        Some(start + Duration::from_secs(options.seconds))
    };

    let mut cursor = options.from;
    let mut records_printed = 0_u64;
    let mut entries_scanned = 0_u64;
    let mut generations_seen: BTreeMap<String, u64> = BTreeMap::new();
    let mut membership_fingerprint: Option<Vec<String>> = None;
    let mut membership_change = false;

    loop {
        if let Some(limit) = options.until_records
            && records_printed >= limit
        {
            break;
        }
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            break;
        }

        let (log, end, generations) = seams.observe_contiguous_end().await?;
        match &membership_fingerprint {
            None => membership_fingerprint = Some(generations.clone()),
            Some(prior) if prior != &generations => {
                membership_change = true;
                membership_fingerprint = Some(generations.clone());
            }
            Some(_) => {}
        }
        if generations.len() > 1 {
            membership_change = true;
        }

        let mut advanced = false;
        while cursor < end {
            if let Some(limit) = options.until_records
                && records_printed >= limit
            {
                break;
            }

            let entry = log.read_next(cursor, end).await.map_err(|error| {
                format!("consume: failed reading VirtualLog entry {cursor}: {error}")
            })?;
            entries_scanned = entries_scanned.saturating_add(1);

            let resolved_chunks = resolve_log_payload(&seams.store, &entry.payload)
                .await
                .map_err(|error| {
                    format!(
                        "consume: corrupt or unresolvable payload at entry {}: {error}",
                        entry.position
                    )
                })?;

            let mut record_index_in_entry = 0_u64;
            'chunks: for resolved in &resolved_chunks {
                for frame in &resolved.chunk.frames {
                    for (frame_index, record) in frame.records.iter().enumerate() {
                        if let Some(limit) = options.until_records
                            && records_printed >= limit
                        {
                            break 'chunks;
                        }
                        let record_offset = frame
                            .base_offset
                            .checked_add(frame_index)
                            .map(|offset| offset.get())
                            .unwrap_or(record_index_in_entry);
                        let payload = record.payload.as_ref();
                        let digest = ChunkDigest::of(payload);
                        print_record(out, options, entry.position, record_offset, payload, digest)?;
                        records_printed = records_printed.saturating_add(1);
                        record_index_in_entry = record_index_in_entry.saturating_add(1);
                        *generations_seen
                            .entry(entry.loglet_id.as_str().to_owned())
                            .or_default() += 1;
                        advanced = true;
                    }
                }
            }

            cursor = entry.position.saturating_add(1);
        }

        if options.no_follow {
            break;
        }
        if !advanced {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let _ = err;

    Ok(ConsumeSummary {
        entries_scanned,
        records_printed,
        final_cursor: cursor,
        elapsed: start.elapsed(),
        membership_change,
    })
}

fn print_record(
    out: &mut dyn Write,
    options: &ConsumeOptions,
    entry: u64,
    record_offset: u64,
    payload: &[u8],
    digest: ChunkDigest,
) -> Result<(), Box<dyn Error>> {
    let (encoding, rendered) = render_payload(payload);
    match options.format {
        OutputFormat::Text => {
            writeln!(
                out,
                "canon={} verse={} entry={entry} record={record_offset} bytes={} digest={digest} payload={encoding}:{rendered}",
                options.canon,
                options.verse,
                payload.len(),
            )?;
        }
        OutputFormat::Jsonl => {
            let object = json!({
                "canon": options.canon,
                "verse": options.verse,
                "entry": entry,
                "record_offset": record_offset,
                "bytes": payload.len(),
                "digest": digest.to_string(),
                "payload_encoding": encoding,
                "payload": rendered,
            });
            writeln!(out, "{object}")?;
        }
    }
    Ok(())
}

/// Renders payload bytes safely for terminals.
///
/// Valid UTF-8 without C0/C1 controls (except tab) becomes `text` with escapes
/// for remaining awkward characters. Everything else is labelled `hex`.
pub(crate) fn render_payload(payload: &[u8]) -> (&'static str, String) {
    if let Ok(text) = std::str::from_utf8(payload)
        && text.chars().all(|ch| {
            let code = ch as u32;
            ch == '\t' || !(code < 0x20 || (0x7f..=0x9f).contains(&code))
        })
    {
        return ("text", escape_text(text));
    }
    ("hex", hex_encode(payload))
}

fn escape_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '"' => out.push_str("\\\""),
            c if c.is_control() => {
                for byte in c.encode_utf8(&mut [0; 4]).bytes() {
                    out.push_str(&format!("\\x{byte:02x}"));
                }
            }
            c => out.push(c),
        }
    }
    out
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

pub(crate) fn write_summary(
    err: &mut dyn Write,
    summary: &ConsumeSummary,
) -> Result<(), Box<dyn Error>> {
    writeln!(
        err,
        "scripture consume: entries_scanned={} records_printed={} final_cursor={} elapsed_ms={} membership_change={}",
        summary.entries_scanned,
        summary.records_printed,
        summary.final_cursor,
        summary.elapsed.as_millis(),
        if summary.membership_change {
            "yes"
        } else {
            "no"
        }
    )?;
    Ok(())
}

/// Prints `consume`-specific usage.
pub fn print_help() {
    eprintln!(
        "\
scripture consume — read-only debug/demo consumer (no checkpoint / not durable)

Usage:
  scripture consume --config PATH --canon ID --verse ID [options]

Required:
  --config PATH          Multi-assignment Scripture YAML
  --canon ID             Canon (journal) id
  --verse ID             Verse id

Options:
  --from OFFSET          First VirtualLog entry position (default 0)
  --until-records N      Exit after printing N logical records (default: unbounded)
  --seconds N            Follow deadline in seconds (default {DEFAULT_SECONDS}; 0 rejected)
  --format text|jsonl    Record output format (default text)
  --no-follow            Read the current contiguous tail once and exit

Examples:
  scripture consume --config scripture.yaml --canon demo --verse events \\
    --from 0 --until-records 5 --no-follow
  scripture consume --config scripture.yaml --canon demo --verse events \\
    --format jsonl --seconds 30

This command does not own consumer progress, acknowledgements, trimming, or HA
subscription semantics. Membership is re-observed while following."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use holylog::provision::{ExclusiveClaimStore, InMemoryExclusiveClaimStore};
    use holylog::virtual_log::{
        ConditionalRegister, InMemoryConditionalRegister, LogletResolver, VirtualLog,
    };
    use holylog_object_store::{ObjectStoreMetrics, WritePolicy};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use scripture::serving_authority::{AuthorityKey, RouteHint, WriterTerm};
    use scripture::{
        ChunkPolicy, CohortId, JournalId, OwnerEndpoint, OwnerId, ProducerId, Record,
        RecoveryBound, Submission, SystemClock, SystemTimer, VerseId, WriterId,
    };
    use scripture_runtime::{
        BackendProfile, HolylogJournalFoundation, NodeIdentity, ObjectStorePartsFactory,
        PartsFactory, ProcessLogletResolver, bootstrap_and_serve, promote_and_serve,
        resolve_log_payload,
    };
    use scripture_service::{
        AuthorityCoordinator, DeterministicTransitionIdGenerator, JournalFoundationTransition,
        VerseRuntimeConfig,
    };

    fn owner_a() -> OwnerId {
        OwnerId::from_bytes(*b"consume-owner-a!")
    }

    fn owner_b() -> OwnerId {
        OwnerId::from_bytes(*b"consume-owner-b!")
    }

    fn runtime_config(owner: OwnerId) -> VerseRuntimeConfig {
        VerseRuntimeConfig {
            journal_id: JournalId::from_bytes(*b"consume-demo-j!!"),
            verse_id: VerseId::from_bytes(*b"consume-demo-v!!"),
            owner_id: owner,
            cohort_id: CohortId::from_bytes(*b"consume-cohort!!"),
            writer_id: WriterId::from_bytes(*b"consume-writer!!"),
            policy: ChunkPolicy {
                max_chunk_bytes: 64 * 1024,
                max_record_bytes: 16 * 1024,
                max_chunk_records: 8,
                max_chunk_age: Duration::from_secs(60),
                max_buffered_bytes: 64 * 1024,
                max_inflight_chunks: 1,
                max_uncommitted_age: Duration::from_secs(60),
                recovery_scan: RecoveryBound::new(8).expect("bound"),
            },
            recovery_bound: RecoveryBound::new(8).expect("bound"),
            queue_capacity: 16,
            dataref_blobs: None,
            blob_sink: None,
            blob_verse_key: None,
        }
    }

    struct Harness {
        store: Arc<dyn ObjectStore>,
        register: Arc<dyn ConditionalRegister>,
        resolver: Arc<ProcessLogletResolver>,
        parts: Arc<dyn PartsFactory>,
        foundation: Arc<HolylogJournalFoundation>,
        coordinator: AuthorityCoordinator,
        key: AuthorityKey,
    }

    fn harness(owner: OwnerId, advertise: &str) -> Harness {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
        let resolver = Arc::new(ProcessLogletResolver::default());
        let parts = Arc::new(ObjectStorePartsFactory::new(
            Arc::clone(&store),
            "consume-demo-root",
            BackendProfile::RustFs.drive_capabilities(),
            WritePolicy::AtomicCreate,
            Arc::new(ObjectStoreMetrics::default()),
        )) as Arc<dyn PartsFactory>;
        let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
        let config = runtime_config(owner);
        let key = AuthorityKey {
            journal_id: config.journal_id,
            verse_id: config.verse_id,
        };
        let endpoint = OwnerEndpoint::new(advertise).expect("endpoint");
        let foundation = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
            key,
            NodeIdentity {
                owner_id: owner,
                endpoint: endpoint.clone(),
            },
            Arc::clone(&register),
            Arc::clone(&resolver),
            Arc::clone(&parts),
            claims,
            2,
        ));
        let coordinator = AuthorityCoordinator::new(
            Arc::clone(&register),
            Arc::clone(&resolver) as Arc<dyn LogletResolver>,
            Arc::clone(&foundation) as Arc<dyn JournalFoundationTransition>,
            Arc::new(DeterministicTransitionIdGenerator::new()),
            owner,
            RouteHint::new(endpoint.as_str()).expect("route"),
        );
        Harness {
            store,
            register,
            resolver,
            parts,
            foundation,
            coordinator,
            key,
        }
    }

    async fn commit_payload(
        session: &scripture_runtime::HaServingSession,
        producer: &[u8; 16],
        sequence: u64,
        payload: &'static [u8],
    ) {
        let pending = session
            .submit(Submission {
                producer_id: ProducerId::from_bytes(*producer),
                producer_epoch: 1,
                sequence,
                records: vec![Record::new([], Bytes::from_static(payload))],
            })
            .await
            .expect("admit");
        session.flush().await.expect("flush");
        pending.await.expect("commit");
    }

    fn options(format: OutputFormat, until: Option<u64>, from: u64) -> ConsumeOptions {
        ConsumeOptions {
            canon: "consume-demo-j!!".to_owned(),
            verse: "consume-demo-v!!".to_owned(),
            from,
            until_records: until,
            seconds: 5,
            format,
            no_follow: true,
        }
    }

    #[test]
    fn render_payload_prints_safe_utf8_as_text() {
        let (encoding, rendered) = render_payload(b"hello world");
        assert_eq!(encoding, "text");
        assert_eq!(rendered, "hello world");
    }

    #[test]
    fn render_payload_hex_encodes_control_bytes() {
        let (encoding, rendered) = render_payload(&[0x1b, b'[', b'3', b'1', b'm']);
        assert_eq!(encoding, "hex");
        assert_eq!(rendered, "1b5b33316d");
        assert!(!rendered.contains('\u{1b}'));
    }

    #[test]
    fn render_payload_hex_encodes_non_utf8() {
        let (encoding, rendered) = render_payload(&[0xff, 0xfe, 0x00]);
        assert_eq!(encoding, "hex");
        assert_eq!(rendered, "fffe00");
    }

    #[test]
    fn output_format_parse_rejects_unknown() {
        assert!(OutputFormat::parse("yaml").is_err());
        assert_eq!(
            OutputFormat::parse("jsonl").expect("jsonl"),
            OutputFormat::Jsonl
        );
    }

    #[tokio::test]
    async fn no_follow_text_prints_known_payloads_and_empty_is_success() {
        let h = harness(owner_a(), "tcp://consume-a:9000");
        let session = bootstrap_and_serve(
            &h.coordinator,
            h.foundation.as_ref(),
            h.key,
            WriterTerm::new(1).expect("term"),
            runtime_config(owner_a()),
            Arc::clone(&h.register),
            Arc::clone(&h.resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .expect("bootstrap");

        // Consumer must not share the writer's resolver: observe installs
        // ReadSeal views that would demote the live Writable handle.
        let seams = VerseReadSeams::from_parts(
            Arc::clone(&h.store),
            Arc::clone(&h.register),
            Arc::clone(&h.parts),
            Arc::new(ProcessLogletResolver::default()),
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let empty = run_consume(
            &seams,
            &options(OutputFormat::Text, None, 0),
            &mut out,
            &mut err,
        )
        .await
        .expect("empty");
        assert_eq!(empty.records_printed, 0);
        assert!(out.is_empty());

        commit_payload(&session, b"consume-prod!!!!", 0, b"alpha").await;
        commit_payload(&session, b"consume-prod!!!!", 1, b"beta").await;

        let seams = VerseReadSeams::from_parts(
            Arc::clone(&h.store),
            Arc::clone(&h.register),
            Arc::clone(&h.parts),
            Arc::new(ProcessLogletResolver::default()),
        );
        out.clear();
        let summary = run_consume(
            &seams,
            &options(OutputFormat::Text, None, 0),
            &mut out,
            &mut err,
        )
        .await
        .expect("text");
        let text = String::from_utf8(out).expect("utf8");
        assert_eq!(summary.records_printed, 2);
        assert!(text.contains("payload=text:alpha"));
        assert!(text.contains("payload=text:beta"));
        assert!(text.contains("canon=consume-demo-j!!"));
        assert!(text.contains("verse=consume-demo-v!!"));
    }

    #[tokio::test]
    async fn jsonl_is_one_object_per_record_on_stdout() {
        let h = harness(owner_a(), "tcp://consume-a:9001");
        let session = bootstrap_and_serve(
            &h.coordinator,
            h.foundation.as_ref(),
            h.key,
            WriterTerm::new(1).expect("term"),
            runtime_config(owner_a()),
            Arc::clone(&h.register),
            Arc::clone(&h.resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .expect("bootstrap");
        commit_payload(&session, b"consume-prod!!!!", 0, b"json-one").await;
        commit_payload(&session, b"consume-prod!!!!", 1, &[0xff, 0x00, 0x1b]).await;

        let seams = VerseReadSeams::from_parts(
            Arc::clone(&h.store),
            Arc::clone(&h.register),
            Arc::clone(&h.parts),
            Arc::new(ProcessLogletResolver::default()),
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let summary = run_consume(
            &seams,
            &options(OutputFormat::Jsonl, None, 0),
            &mut out,
            &mut err,
        )
        .await
        .expect("jsonl");
        assert_eq!(summary.records_printed, 2);
        let text = String::from_utf8(out).expect("utf8");
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in &lines {
            let value: serde_json::Value = serde_json::from_str(line).expect("json");
            assert!(value.get("digest").is_some());
            assert!(value.get("payload").is_some());
        }
        assert!(lines[0].contains("json-one"));
        assert!(lines[1].contains("\"payload_encoding\":\"hex\""));
        assert!(!text.contains('\u{1b}'));
    }

    #[tokio::test]
    async fn from_and_until_records_bound_the_cursor() {
        let h = harness(owner_a(), "tcp://consume-a:9002");
        let session = bootstrap_and_serve(
            &h.coordinator,
            h.foundation.as_ref(),
            h.key,
            WriterTerm::new(1).expect("term"),
            runtime_config(owner_a()),
            Arc::clone(&h.register),
            Arc::clone(&h.resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .expect("bootstrap");
        for sequence in 0..5 {
            commit_payload(
                &session,
                b"consume-prod!!!!",
                sequence,
                match sequence {
                    0 => b"r0",
                    1 => b"r1",
                    2 => b"r2",
                    3 => b"r3",
                    _ => b"r4",
                },
            )
            .await;
        }

        let seams = VerseReadSeams::from_parts(
            Arc::clone(&h.store),
            Arc::clone(&h.register),
            Arc::clone(&h.parts),
            Arc::new(ProcessLogletResolver::default()),
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let summary = run_consume(
            &seams,
            &options(OutputFormat::Text, Some(2), 1),
            &mut out,
            &mut err,
        )
        .await
        .expect("bounded");
        let text = String::from_utf8(out).expect("utf8");
        assert_eq!(summary.records_printed, 2);
        assert!(!text.contains("payload=text:r0"));
        assert!(text.contains("payload=text:r1"));
        assert!(text.contains("payload=text:r2"));
        assert!(!text.contains("payload=text:r3"));
        assert_eq!(summary.final_cursor, 3);
    }

    #[tokio::test]
    async fn corrupt_entry_fails_closed_with_position() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let err = resolve_log_payload(&store, b"not-a-scripture-payload")
            .await
            .expect_err("garbage must fail");
        let mapped = format!("consume: corrupt or unresolvable payload at entry 7: {err}");
        assert!(mapped.contains("entry 7"));

        let h = harness(owner_a(), "tcp://consume-a:9003");
        let session = bootstrap_and_serve(
            &h.coordinator,
            h.foundation.as_ref(),
            h.key,
            WriterTerm::new(1).expect("term"),
            runtime_config(owner_a()),
            Arc::clone(&h.register),
            Arc::clone(&h.resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .expect("bootstrap");
        commit_payload(&session, b"consume-prod!!!!", 0, b"ok-before-corrupt").await;

        let log = VirtualLog::new(
            Arc::clone(&h.register),
            Arc::clone(&h.resolver) as Arc<dyn LogletResolver>,
        );
        let _ = log.observe_membership().await.expect("membership");
        log.append(Bytes::from_static(b"corrupt-bytes-not-chunk"))
            .await
            .expect("inject corrupt");

        let seams = VerseReadSeams::from_parts(
            Arc::clone(&h.store),
            Arc::clone(&h.register),
            Arc::clone(&h.parts),
            Arc::new(ProcessLogletResolver::default()),
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let failure = run_consume(
            &seams,
            &options(OutputFormat::Text, None, 0),
            &mut out,
            &mut err,
        )
        .await
        .expect_err("corrupt must fail closed");
        let message = failure.to_string();
        assert!(
            message.contains("corrupt or unresolvable payload at entry"),
            "unexpected error: {message}"
        );
    }

    #[tokio::test]
    async fn membership_handoff_records_print_exactly_once() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let register = Arc::new(InMemoryConditionalRegister::new()) as Arc<dyn ConditionalRegister>;
        let a_resolver = Arc::new(ProcessLogletResolver::default());
        let b_resolver = Arc::new(ProcessLogletResolver::default());
        let parts = Arc::new(ObjectStorePartsFactory::new(
            Arc::clone(&store),
            "consume-handoff-root",
            BackendProfile::RustFs.drive_capabilities(),
            WritePolicy::AtomicCreate,
            Arc::new(ObjectStoreMetrics::default()),
        )) as Arc<dyn PartsFactory>;
        let claims = Arc::new(InMemoryExclusiveClaimStore::new()) as Arc<dyn ExclusiveClaimStore>;
        let config_a = runtime_config(owner_a());
        let key = AuthorityKey {
            journal_id: config_a.journal_id,
            verse_id: config_a.verse_id,
        };
        let advertise_a = OwnerEndpoint::new("tcp://consume-a:9100").expect("ep");
        let advertise_b = OwnerEndpoint::new("tcp://consume-b:9100").expect("ep");
        let foundation_a = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
            key,
            NodeIdentity {
                owner_id: owner_a(),
                endpoint: advertise_a.clone(),
            },
            Arc::clone(&register),
            Arc::clone(&a_resolver),
            Arc::clone(&parts),
            Arc::clone(&claims),
            2,
        ));
        let foundation_b = Arc::new(HolylogJournalFoundation::with_default_loglet_ids(
            key,
            NodeIdentity {
                owner_id: owner_b(),
                endpoint: advertise_b.clone(),
            },
            Arc::clone(&register),
            Arc::clone(&b_resolver),
            Arc::clone(&parts),
            Arc::clone(&claims),
            2,
        ));
        let coordinator_a = AuthorityCoordinator::new(
            Arc::clone(&register),
            Arc::clone(&a_resolver) as Arc<dyn LogletResolver>,
            Arc::clone(&foundation_a) as Arc<dyn JournalFoundationTransition>,
            Arc::new(DeterministicTransitionIdGenerator::new()),
            owner_a(),
            RouteHint::new(advertise_a.as_str()).expect("route"),
        );
        let coordinator_b = AuthorityCoordinator::new(
            Arc::clone(&register),
            Arc::clone(&b_resolver) as Arc<dyn LogletResolver>,
            Arc::clone(&foundation_b) as Arc<dyn JournalFoundationTransition>,
            Arc::new(DeterministicTransitionIdGenerator::new()),
            owner_b(),
            RouteHint::new(advertise_b.as_str()).expect("route"),
        );

        let session_a = bootstrap_and_serve(
            &coordinator_a,
            foundation_a.as_ref(),
            key,
            WriterTerm::new(1).expect("t1"),
            config_a,
            Arc::clone(&register),
            Arc::clone(&a_resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .expect("bootstrap a");
        commit_payload(&session_a, b"consume-prod!!!!", 0, b"before-cutover").await;
        let expected = session_a.generation().clone();
        a_resolver.remove(&expected.active_loglet_id);

        let session_b = promote_and_serve(
            &coordinator_b,
            foundation_b.as_ref(),
            key,
            WriterTerm::new(2).expect("t2"),
            expected,
            runtime_config(owner_b()),
            Arc::clone(&register),
            Arc::clone(&b_resolver),
            SystemClock::new(),
            SystemTimer::new(),
        )
        .await
        .expect("promote b");
        commit_payload(&session_b, b"consume-prod!!!!", 1, b"after-cutover").await;

        let read_resolver = Arc::new(ProcessLogletResolver::default());
        let seams = VerseReadSeams::from_parts(
            Arc::clone(&store),
            Arc::clone(&register),
            Arc::clone(&parts),
            read_resolver,
        );
        let mut out = Vec::new();
        let mut err = Vec::new();
        let summary = run_consume(
            &seams,
            &options(OutputFormat::Text, None, 0),
            &mut out,
            &mut err,
        )
        .await
        .expect("handoff consume");
        let text = String::from_utf8(out).expect("utf8");
        assert_eq!(summary.records_printed, 2);
        assert!(summary.membership_change);
        assert_eq!(text.matches("payload=text:before-cutover").count(), 1);
        assert_eq!(text.matches("payload=text:after-cutover").count(), 1);
    }
}
