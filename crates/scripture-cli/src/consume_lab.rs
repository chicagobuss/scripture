//! `scripture consume-lab` — a tailing reader for measuring throughput out.
//!
//! Compatibility / lab instrument, paired with `produce-lab`. Prefer
//! `scripture consume` for the debug/demo record console. The point of this
//! command is continuity during a rolling restart: a Verse moving between
//! Scribes must not stall reads, and the only way to know is to read while it
//! moves.
//!
//! Re-observes membership each pass via the shared [`crate::consume`] reader
//! seams, because a handoff adds a generation and a reader holding the old
//! chain would simply stop at the old tail and look healthy while falling
//! behind.

use std::collections::BTreeMap;
use std::error::Error;
use std::time::{Duration, Instant};

use crate::config::ScriptureConfig;
use crate::consume::{self, VerseReadSeams};

/// Options for one consume-lab run.
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
    let assignment = consume::find_assignment(&config, &options.canon, &options.verse)?;
    let (seams, store_root) = VerseReadSeams::from_config(&config, &assignment)?;

    println!(
        "scripture consume-lab: canon={} verse={} seconds={} root={store_root} (lab alias; prefer `scripture consume` for record output)",
        options.canon, options.verse, options.seconds
    );

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

        let (log, end, _) = seams.observe_contiguous_end().await?;

        let mut advanced = false;
        while cursor < end {
            let entry = log.read_next(cursor, end).await?;
            let resolved_chunks =
                scripture_runtime::resolve_log_payload(&seams.store, &entry.payload).await?;
            let in_entry: u64 = resolved_chunks
                .iter()
                .flat_map(|resolved| resolved.chunk.frames.iter())
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
