//! `scripture consume-lab` — a tailing reader for measuring throughput out.
//!
//! A lab instrument, paired with `produce-lab`. The point is continuity during
//! a rolling restart: a Verse moving between Scribes must not stall reads, and
//! the only way to know is to read while it moves.
//!
//! Re-observes membership each pass, because a handoff adds a generation and a
//! reader holding the old chain would simply stop at the old tail and look
//! healthy while falling behind.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use holylog::provision::resolve_read_seal;
use holylog::virtual_log::{ConditionalRegister, LogletResolver, VirtualLog};
use holylog_object_store::{ObjectStoreMetrics, WritePolicy};
use holylog_object_store_register::{ObjectStoreConditionalRegister, register_path};
use object_store::path::Path;
use scripture_runtime::{
    ObjectStorePartsFactory, PartsFactory, ProcessLogletResolver, resolve_log_payload,
};

use crate::assemble;
use crate::config::{AssignmentConfig, ScriptureConfig};

const LOGLET_K: u64 = 2;

/// Options for one consume run.
pub struct ConsumeOptions {
    pub canon: String,
    pub verse: String,
    /// Stop after this many seconds.
    pub seconds: u64,
    /// Stop early once this many records have been read (0 = no limit).
    pub until_records: u64,
}

/// Tails a Verse and reports read throughput and stalls.
pub async fn consume_lab(
    config: ScriptureConfig,
    options: ConsumeOptions,
) -> Result<(), Box<dyn Error>> {
    let shared = assemble::connect_shared_store(&config)?;
    let assignment = find_assignment(&config, &options)?;
    let store_root = config.assignment_store_root(&assignment)?;

    println!(
        "scripture consume-lab: canon={} verse={} seconds={} root={store_root}",
        options.canon, options.verse, options.seconds
    );

    let register = Arc::new(ObjectStoreConditionalRegister::new(
        Arc::clone(&shared.store),
        Path::from(store_root.clone()).join(register_path("verse").as_ref()),
        shared.backend.register_capabilities(),
    )?) as Arc<dyn ConditionalRegister>;

    let parts = Arc::new(ObjectStorePartsFactory::new(
        Arc::clone(&shared.store),
        store_root,
        shared.backend.drive_capabilities(),
        WritePolicy::AtomicCreate,
        Arc::new(ObjectStoreMetrics::default()),
    ));
    let resolver = Arc::new(ProcessLogletResolver::default());

    let start = Instant::now();
    let deadline = start + Duration::from_secs(options.seconds);
    let mut cursor = 0_u64;
    let mut records = 0_u64;
    let mut generations_seen: BTreeMap<String, u64> = BTreeMap::new();
    let mut last_progress = Instant::now();
    let mut stalls: Vec<f64> = Vec::new();
    let mut samples: Vec<(f64, u64)> = Vec::new();

    while Instant::now() < deadline {
        if options.until_records > 0 && records >= options.until_records {
            break;
        }

        // Re-resolve every pass: a handoff publishes a new generation, and a
        // reader on a stale chain stops at the old tail while looking healthy.
        let log = VirtualLog::new(
            Arc::clone(&register),
            Arc::clone(&resolver) as Arc<dyn LogletResolver>,
        );
        let observed = log.observe_membership().await?;

        let mut end = 0_u64;
        for generation in &observed.state.generations {
            let durable = parts.open(&generation.loglet_id)?;
            let view = resolve_read_seal(durable.components(LOGLET_K)).await?;
            // Contiguous tail: a gap-tolerant tail would read past records that
            // are not durably present yet and report phantom progress.
            let tail = view.observe_durable().await?.contiguous_tail();
            resolver.insert_read_seal(generation.loglet_id.clone(), Arc::new(view));
            end = generation.start.saturating_add(tail);
        }

        let mut advanced = false;
        while cursor < end {
            let entry = log.read_next(cursor, end).await?;
            let resolved = resolve_log_payload(&shared.store, &entry.payload).await?;
            let in_entry: u64 = resolved
                .chunk
                .frames
                .iter()
                .map(|frame| frame.records.len() as u64)
                .sum();
            records += in_entry;
            *generations_seen
                .entry(entry.loglet_id.as_str().to_owned())
                .or_default() += in_entry;
            cursor = entry.position + 1;
            advanced = true;

            if options.until_records > 0 && records >= options.until_records {
                break;
            }
        }

        let now = Instant::now();
        if advanced {
            let gap = now.duration_since(last_progress).as_secs_f64();
            if gap > 1.0 {
                stalls.push(gap);
            }
            last_progress = now;
            samples.push((now.duration_since(start).as_secs_f64(), records));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let wall = start.elapsed().as_secs_f64();
    println!("\n=== read throughput ===");
    println!("wall_seconds={wall:.2} records_read={records}");
    if wall > 0.0 {
        println!("records_per_second={:.1}", records as f64 / wall);
    }
    println!("final_cursor={cursor}");

    println!("\n=== generations read ===");
    for (loglet, count) in &generations_seen {
        println!("  {loglet}: {count} records");
    }
    if generations_seen.len() > 1 {
        println!("  (read spans a cutover)");
    }

    println!("\n=== read stalls (>1s with no progress) ===");
    if stalls.is_empty() {
        println!("  none");
    } else {
        for stall in stalls.iter().take(10) {
            println!("  {stall:.1}s");
        }
    }
    Ok(())
}

fn find_assignment(
    config: &ScriptureConfig,
    options: &ConsumeOptions,
) -> Result<AssignmentConfig, Box<dyn Error>> {
    config
        .scribe
        .as_ref()
        .ok_or("consume-lab requires scribe.assignments")?
        .assignments
        .iter()
        .find(|a| a.canon == options.canon && a.verse == options.verse)
        .cloned()
        .ok_or_else(|| {
            format!(
                "no assignment for canon={} verse={}",
                options.canon, options.verse
            )
            .into()
        })
}
