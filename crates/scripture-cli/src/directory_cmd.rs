//! `scripture directory` — read the fleet directory (decision 0014).
//!
//! Discovery only. Nothing here decides authority: the printed dispositions
//! are publication-time hints that may be stale in either direction, and a
//! client acting on them must still expect the serving Scribe's authority gate
//! to refuse.

use std::error::Error;

use scripture_runtime::directory::{self, DirectoryRecord};

use crate::assemble;
use crate::config::ScriptureConfig;

/// Lists directory records, optionally ranking endpoints for one `(canon, verse)`.
pub async fn directory(
    config: ScriptureConfig,
    canon: Option<&str>,
    verse: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let shared = assemble::connect_shared_store(&config)?;
    let records = directory::list_all(&shared.store, &config.store.prefix).await?;
    let now = directory::now_ms();

    if records.is_empty() {
        println!("scripture: directory empty prefix={}", config.store.prefix);
        println!(
            "scripture: no node has published; this is distinct from every node having expired"
        );
        return Ok(());
    }

    print_roster(&records, now);

    if let (Some(canon), Some(verse)) = (canon, verse) {
        println!();
        let ranked = directory::rank_candidates(&records, canon, verse, now);
        if ranked.is_empty() {
            println!("scripture: no directory candidate for canon={canon} verse={verse}");
            return Ok(());
        }
        println!("scripture: candidates for canon={canon} verse={verse} (best first)");
        for (index, candidate) in ranked.iter().enumerate() {
            println!(
                "  {}. endpoint={} owner={} disposition={} admits_acks={} fresh={} age_ms={}",
                index + 1,
                candidate.endpoint,
                candidate.owner_id,
                candidate.disposition,
                candidate.claims_serving,
                candidate.fresh,
                candidate.age_ms,
            );
        }
        println!("scripture: ranking is a hint; authority is decided by the serving Scribe's gate");
    }
    Ok(())
}

fn print_roster(records: &[DirectoryRecord], now: u64) {
    let fresh = records.iter().filter(|r| r.is_fresh_at(now)).count();
    println!(
        "scripture: directory nodes={} fresh={} stale={}",
        records.len(),
        fresh,
        records.len() - fresh,
    );
    for record in records {
        println!(
            "node owner={} advertise={} fresh={} age_ms={} assignments={}",
            record.owner_id,
            record.node_advertise,
            record.is_fresh_at(now),
            record.age_ms_at(now),
            record.assignments.len(),
        );
        for assignment in &record.assignments {
            println!(
                "  canon={} verse={} advertise={} posture={} disposition={} admits_acks={}",
                assignment.canon,
                assignment.verse,
                assignment.advertise,
                assignment.posture,
                assignment.disposition,
                assignment.admits_committed_acks,
            );
        }
    }
}
