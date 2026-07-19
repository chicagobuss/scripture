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
    /// When true, reply `ERR not-owner` once then accept.
    deny_once: bool,
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
                        let (drop_before_ack, deny_once) = {
                            let mut guard = state.lock().await;
                            let drop = guard.drop_before_ack;
                            if drop {
                                guard.drop_before_ack = false;
                            }
                            let deny = guard.deny_once;
                            if deny {
                                guard.deny_once = false;
                            }
                            (drop, deny)
                        };
                        if deny_once {
                            let _ = writer.write_all(b"ERR not-owner\n").await;
                            continue;
                        }
                        if drop_before_ack {
                            // Simulate commit landing server-side but ACK lost.
                            {
                                let mut guard = state.lock().await;
                                guard.committed.push(payload);
                            }
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

    let mut seqs = SeqAllocator::with_incarnation(1001);
    let (prepared, counters) = prepare_scrape(
        &config,
        "node-node-a",
        SourceKind::NodeExporter,
        SAMPLE_BODY,
        &mut seqs,
    )
    .expect("prepare");
    assert_eq!(counters.dropped_series, 0);
    assert_eq!(counters.dropped_metric_not_allowlisted, 0);
    assert_eq!(counters.unparseable_lines, 0);
    assert_eq!(prepared.len(), 2);
    assert!(prepared[0].envelope.collected_at.ends_with('Z'));
    assert!(prepared[0].envelope.collected_at.contains('T'));
    assert_eq!(prepared[0].envelope.incarnation, 1001);

    let mut buffer = DropOldestBuffer::new(
        "node-node-a",
        config.buffer.max_records_per_verse,
        config.buffer.max_bytes_per_verse,
    );
    assert_eq!(enqueue(&mut buffer, &prepared), 0);

    let mut client = RawLinesClient::new(&endpoint, Duration::from_secs(2), Duration::from_secs(2));
    let mut send_log = Vec::new();
    let mut acked = BTreeSet::new();

    let first = buffer.pop_front().expect("first");
    let status = client.send_await_ack(&first).await.expect("send");
    assert_eq!(status, AckStatus::Unacked);
    send_log.push((first.seq, status, first.payload_digest.clone()));

    client.disconnect();
    let status = client.send_await_ack(&first).await.expect("resend");
    assert!(matches!(status, AckStatus::Committed { .. }));
    acked.insert(first.seq);
    send_log.push((first.seq, status, first.payload_digest.clone()));

    let second = buffer.pop_front().expect("second");
    let status = client.send_await_ack(&second).await.expect("second");
    assert!(matches!(status, AckStatus::Committed { .. }));
    acked.insert(second.seq);

    let committed = state.lock().await.committed.clone();
    let seq0_count = committed
        .iter()
        .filter(|line| line.contains("\"seq\":0"))
        .count();
    assert!(
        seq0_count >= 2,
        "expected duplicate seq=0, got {committed:?}"
    );

    let deduped = dedup_committed_lines(&committed);
    assert_eq!(deduped.unparseable_committed, 0);
    assert_eq!(deduped.lines.len(), 2, "dedup should collapse to sent set");
    let dedup_seqs: BTreeSet<u64> = deduped
        .lines
        .iter()
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|value| value.get("seq")?.as_u64())
        })
        .collect();
    assert_eq!(dedup_seqs, acked);

    let keys: BTreeSet<(String, String, u64, u64)> = committed
        .iter()
        .filter_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            if value.get("seq")?.as_u64()? != 0 {
                return None;
            }
            Some((
                value.get("producer_id")?.as_str()?.to_owned(),
                value.get("verse")?.as_str()?.to_owned(),
                value.get("incarnation")?.as_u64()?,
                0,
            ))
        })
        .collect();
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys.iter().next().expect("key"),
        &("fleet-collector-0".into(), "node-node-a".into(), 1001, 0)
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
    assert_eq!(send_log.len(), 2);
}

#[tokio::test]
async fn denied_ack_reconnects_and_resends() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = listener.local_addr().expect("addr").to_string();
    let state = Arc::new(Mutex::new(FakeState {
        deny_once: true,
        ..FakeState::default()
    }));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(run_fake_scribe(listener, Arc::clone(&state), shutdown_rx));

    let config = ProducerConfig::phase1_single_source(
        "fleet-collector-0",
        "http://127.0.0.1/metrics",
        &endpoint,
    );
    let mut seqs = SeqAllocator::with_incarnation(42);
    let (prepared, _) = prepare_scrape(
        &config,
        "node-node-a",
        SourceKind::NodeExporter,
        SAMPLE_BODY,
        &mut seqs,
    )
    .expect("prepare");
    let line = prepared[0].buffered.clone();

    let mut client = RawLinesClient::new(&endpoint, Duration::from_secs(2), Duration::from_secs(2));
    let first = client.send_await_ack(&line).await.expect("deny");
    assert_eq!(first, AckStatus::Denied);
    client.disconnect();
    let second = client.send_await_ack(&line).await.expect("resend");
    assert!(matches!(second, AckStatus::Committed { .. }));

    let committed = state.lock().await.committed.clone();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0], line.line);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn restart_incarnation_keeps_seq_zero_distinct() {
    let config = ProducerConfig::phase1_single_source(
        "fleet-collector-0",
        "http://127.0.0.1/metrics",
        "127.0.0.1:9",
    );
    let mut first_life = SeqAllocator::with_incarnation(1);
    let mut second_life = SeqAllocator::with_incarnation(2);
    let (a, _) = prepare_scrape(
        &config,
        "node-node-a",
        SourceKind::NodeExporter,
        SAMPLE_BODY,
        &mut first_life,
    )
    .expect("first");
    let (b, _) = prepare_scrape(
        &config,
        "node-node-a",
        SourceKind::NodeExporter,
        SAMPLE_BODY,
        &mut second_life,
    )
    .expect("second");
    assert_eq!(a[0].envelope.seq, 0);
    assert_eq!(b[0].envelope.seq, 0);
    assert_ne!(a[0].envelope.incarnation, b[0].envelope.incarnation);
    assert_ne!(a[0].envelope.dedup_key(), b[0].envelope.dedup_key());

    let mut committed = vec![a[0].buffered.line.clone(), b[0].buffered.line.clone()];
    // Same seq across incarnations must both survive dedup.
    let deduped = dedup_committed_lines(&committed);
    assert_eq!(deduped.lines.len(), 2);

    committed.push("not-json".into());
    let with_garbage = dedup_committed_lines(&committed);
    assert_eq!(with_garbage.unparseable_committed, 1);
    assert_eq!(with_garbage.lines.len(), 2);
}
