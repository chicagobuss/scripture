//! Component test: fake raw-lines Scribe + induced disconnect duplicates.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use scripture_telemetry_producer::{
    AckStatus, DropOldestBuffer, ProducerConfig, RawLinesClient, SeqAllocator, SourceKind,
    dedup_committed_lines, enqueue, prepare_scrape,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};

const SAMPLE_BODY: &str = r#"
# TYPE node_cpu_seconds_total counter
node_cpu_seconds_total{cpu="0",mode="idle"} 12.5
# TYPE node_memory_MemAvailable_bytes gauge
node_memory_MemAvailable_bytes 4096
"#;

#[derive(Default)]
struct FakeState {
    committed: Vec<String>,
    /// When true, accept one line then drop the connection before OK.
    drop_before_ack: bool,
}

async fn run_fake_scribe(
    listener: TcpListener,
    state: Arc<Mutex<FakeState>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    let mut reader = BufReader::new(reader);
                    let mut line = String::new();
                    let mut next_offset = {
                        let guard = state.lock().await;
                        guard.committed.len() as u64
                    };
                    loop {
                        line.clear();
                        match reader.read_line(&mut line).await {
                            Ok(0) => break,
                            Ok(_) => {}
                            Err(_) => break,
                        }
                        let payload = line.trim_end_matches(['\r', '\n']).to_owned();
                        if payload.is_empty() {
                            continue;
                        }
                        let drop_before_ack = {
                            let mut guard = state.lock().await;
                            let drop = guard.drop_before_ack;
                            if drop {
                                guard.drop_before_ack = false;
                            }
                            drop
                        };
                        if drop_before_ack {
                            // Simulate commit landing server-side but ACK lost.
                            {
                                let mut guard = state.lock().await;
                                guard.committed.push(payload);
                            }
                            // Drop connection without writing OK.
                            break;
                        }
                        let first = next_offset;
                        next_offset += 1;
                        {
                            let mut guard = state.lock().await;
                            guard.committed.push(payload);
                        }
                        let ok = format!("OK {first} {next_offset}\n");
                        if writer.write_all(ok.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        }
    }
}

#[tokio::test]
async fn induced_disconnect_duplicates_share_dedup_key() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = listener.local_addr().expect("addr").to_string();
    let state = Arc::new(Mutex::new(FakeState {
        drop_before_ack: true,
        ..FakeState::default()
    }));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(run_fake_scribe(listener, Arc::clone(&state), shutdown_rx));

    let config = ProducerConfig::phase1_single_source(
        "fleet-collector-0",
        "http://127.0.0.1/metrics",
        &endpoint,
    );
    config.validate().expect("valid");

    let mut seqs = SeqAllocator::default();
    let (prepared, dropped_series, dropped_unlisted) = prepare_scrape(
        &config,
        "node-node-a",
        SourceKind::NodeExporter,
        SAMPLE_BODY,
        &mut seqs,
    )
    .expect("prepare");
    assert_eq!(dropped_series, 0);
    assert_eq!(dropped_unlisted, 0);
    assert_eq!(prepared.len(), 2);

    let mut buffer = DropOldestBuffer::new(
        "node-node-a",
        config.buffer.max_records_per_verse,
        config.buffer.max_bytes_per_verse,
    );
    assert_eq!(enqueue(&mut buffer, &prepared), 0);

    let mut client = RawLinesClient::new(&endpoint, Duration::from_secs(2), Duration::from_secs(2));
    let mut send_log = Vec::new();
    let mut acked = BTreeSet::new();

    // First record: server drops before ACK → Unacked; payload may already be committed.
    let first = buffer.pop_front().expect("first");
    let status = client.send_await_ack(&first).await.expect("send");
    assert_eq!(status, AckStatus::Unacked);
    send_log.push((first.seq, status, first.payload_digest.clone()));

    // Resend same seq/line (at-least-once).
    client.disconnect();
    let status = client.send_await_ack(&first).await.expect("resend");
    assert!(matches!(status, AckStatus::Committed { .. }));
    acked.insert(first.seq);
    send_log.push((first.seq, status, first.payload_digest.clone()));

    // Remaining record commits cleanly.
    let second = buffer.pop_front().expect("second");
    let status = client.send_await_ack(&second).await.expect("second");
    assert!(matches!(status, AckStatus::Committed { .. }));
    acked.insert(second.seq);

    let committed = state.lock().await.committed.clone();
    // At least one duplicate of seq 0 should exist in the Canon log.
    let seq0_count = committed
        .iter()
        .filter(|line| line.contains("\"seq\":0"))
        .count();
    assert!(
        seq0_count >= 2,
        "expected duplicate seq=0, got {committed:?}"
    );

    let deduped = dedup_committed_lines(&committed);
    assert_eq!(deduped.len(), 2, "dedup should collapse to sent set");
    let dedup_seqs: BTreeSet<u64> = deduped
        .iter()
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|value| value.get("seq")?.as_u64())
        })
        .collect();
    assert_eq!(dedup_seqs, acked);

    // All duplicates share the same producer_id/verse/seq key for seq 0.
    let keys: BTreeSet<(String, String, u64)> = committed
        .iter()
        .filter_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            if value.get("seq")?.as_u64()? != 0 {
                return None;
            }
            Some((
                value.get("producer_id")?.as_str()?.to_owned(),
                value.get("verse")?.as_str()?.to_owned(),
                0,
            ))
        })
        .collect();
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys.iter().next().expect("key"),
        &("fleet-collector-0".into(), "node-node-a".into(), 0)
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
    let _ = send_log;
}
