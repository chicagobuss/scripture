//! Phase 3: A→B promotion keeps identity; authority ledger is per-Verse.

use std::sync::Arc;
use std::time::Duration;

use scripture_telemetry_producer::{
    AckStatus, ProducerConfig, RawLinesClient, RunOptions, SeqAllocator, SourceKind,
    dedup_committed_lines, prepare_scrape, promotion_message, run_producer,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};

const SAMPLE_BODY: &str = r#"
# TYPE node_cpu_seconds_total counter
node_cpu_seconds_total{cpu="0",mode="idle"} 12.5
# TYPE node_memory_MemAvailable_bytes gauge
node_memory_MemAvailable_bytes 4096
"#;

#[derive(Default)]
struct SinkState {
    committed: Vec<String>,
    /// After this many commits, reply Denied forever.
    deny_after: Option<usize>,
}

async fn run_sink(
    listener: TcpListener,
    state: Arc<Mutex<SinkState>>,
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
                        let deny = {
                            let guard = state.lock().await;
                            guard.deny_after.is_some_and(|n| guard.committed.len() >= n)
                        };
                        if deny {
                            let _ = writer.write_all(b"ERR not-owner\n").await;
                            continue;
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

async fn serve_metrics(listener: TcpListener, mut shutdown: oneshot::Receiver<()>) {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((mut stream, _)) = accepted else { break };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    let _ = stream.read(&mut buf).await;
                    let body = SAMPLE_BODY;
                    let response = format!(
                        "HTTP/1.0 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        }
    }
}

#[tokio::test]
async fn a_to_b_promotion_records_authority_and_keeps_dedup_key() {
    let metrics = TcpListener::bind("127.0.0.1:0").await.expect("metrics");
    let metrics_addr = metrics.local_addr().expect("addr");
    let (m_tx, m_rx) = oneshot::channel();
    let metrics_task = tokio::spawn(serve_metrics(metrics, m_rx));

    let sink_a = TcpListener::bind("127.0.0.1:0").await.expect("a");
    let sink_b = TcpListener::bind("127.0.0.1:0").await.expect("b");
    let addr_a = sink_a.local_addr().expect("a").to_string();
    let addr_b = sink_b.local_addr().expect("b").to_string();
    let state_a = Arc::new(Mutex::new(SinkState {
        deny_after: Some(1),
        ..SinkState::default()
    }));
    let state_b = Arc::new(Mutex::new(SinkState::default()));
    let (a_tx, a_rx) = oneshot::channel();
    let (b_tx, b_rx) = oneshot::channel();
    let task_a = tokio::spawn(run_sink(sink_a, Arc::clone(&state_a), a_rx));
    let task_b = tokio::spawn(run_sink(sink_b, Arc::clone(&state_b), b_rx));

    let mut config = ProducerConfig::phase1_single_source(
        "fleet-collector-0",
        &format!("http://{metrics_addr}/metrics"),
        &addr_a,
    );
    config.endpoints[0].failover_connects = vec![addr_b.clone()];
    config.scrape.interval = Duration::from_millis(50);
    config.scrape.timeout = Duration::from_secs(2);
    config.drain_deadline = Duration::from_secs(2);
    config.retry_initial_backoff = Duration::from_millis(10);
    config.retry_max_backoff = Duration::from_millis(50);

    let ledger_dir = std::env::temp_dir().join(format!(
        "scripture-promo-{}-{}",
        std::process::id(),
        metrics_addr.port()
    ));
    let _ = std::fs::create_dir_all(&ledger_dir);
    let ledger_path = ledger_dir.join("send-ledger.jsonl");

    let counters = run_producer(
        config,
        &ledger_path,
        RunOptions {
            max_iterations: Some(3),
            ack_timeout: Duration::from_millis(500),
        },
    )
    .await
    .expect("run");

    assert!(counters.committed >= 2, "counters={counters:?}");
    assert!(counters.promotions >= 1, "expected A→B promotion");

    let ledger = std::fs::read_to_string(&ledger_path).expect("ledger");
    let expected = promotion_message("node-node-a", &addr_a, &addr_b);
    assert!(
        ledger.contains(&expected),
        "missing authority message {expected} in {ledger}"
    );
    assert!(ledger.contains("\"row_type\":\"authority\""));

    let mut committed = state_a.lock().await.committed.clone();
    committed.extend(state_b.lock().await.committed.clone());
    assert!(!committed.is_empty());
    let deduped = dedup_committed_lines(&committed);
    assert_eq!(deduped.unparseable_committed, 0);
    // All records share the same incarnation (process-stable across promotion).
    let incarnations: std::collections::BTreeSet<_> = committed
        .iter()
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()?
                .get("incarnation")?
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(incarnations.len(), 1, "identity must not fork across A→B");

    let _ = m_tx.send(());
    let _ = a_tx.send(());
    let _ = b_tx.send(());
    let _ = metrics_task.await;
    let _ = task_a.await;
    let _ = task_b.await;
    let _ = std::fs::remove_dir_all(&ledger_dir);
}

#[tokio::test]
async fn drain_deadline_exits_with_pending_when_endpoint_down() {
    let metrics = TcpListener::bind("127.0.0.1:0").await.expect("metrics");
    let metrics_addr = metrics.local_addr().expect("addr");
    let (m_tx, m_rx) = oneshot::channel();
    let metrics_task = tokio::spawn(serve_metrics(metrics, m_rx));

    // Bind then drop so the port is closed (connect fails → Unacked).
    let dead = TcpListener::bind("127.0.0.1:0").await.expect("dead");
    let dead_addr = dead.local_addr().expect("addr").to_string();
    drop(dead);

    let mut config = ProducerConfig::phase1_single_source(
        "fleet-collector-0",
        &format!("http://{metrics_addr}/metrics"),
        &dead_addr,
    );
    config.scrape.interval = Duration::from_millis(20);
    config.drain_deadline = Duration::from_millis(200);
    config.retry_initial_backoff = Duration::from_millis(10);
    config.retry_max_backoff = Duration::from_millis(20);
    config.connect_timeout = Duration::from_millis(50);

    let ledger_path = std::env::temp_dir().join(format!(
        "scripture-drain-{}-{}.jsonl",
        std::process::id(),
        metrics_addr.port()
    ));

    let started = std::time::Instant::now();
    let counters = run_producer(
        config,
        &ledger_path,
        RunOptions {
            max_iterations: Some(1),
            ack_timeout: Duration::from_millis(50),
        },
    )
    .await
    .expect("run must exit");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "drain hung: {:?}",
        started.elapsed()
    );
    assert!(
        counters.abandoned_on_drain_deadline > 0 || counters.unacked_attempts > 0,
        "counters={counters:?}"
    );

    let _ = m_tx.send(());
    let _ = metrics_task.await;
    let _ = std::fs::remove_file(&ledger_path);
}

#[tokio::test]
async fn client_retarget_switches_endpoint() {
    let a = TcpListener::bind("127.0.0.1:0").await.expect("a");
    let b = TcpListener::bind("127.0.0.1:0").await.expect("b");
    let addr_a = a.local_addr().expect("a").to_string();
    let addr_b = b.local_addr().expect("b").to_string();
    let state_a = Arc::new(Mutex::new(SinkState {
        deny_after: Some(0), // deny immediately
        ..SinkState::default()
    }));
    let state_b = Arc::new(Mutex::new(SinkState::default()));
    let (a_tx, a_rx) = oneshot::channel();
    let (b_tx, b_rx) = oneshot::channel();
    let task_a = tokio::spawn(run_sink(a, state_a, a_rx));
    let task_b = tokio::spawn(run_sink(b, Arc::clone(&state_b), b_rx));

    let config = ProducerConfig::phase1_single_source(
        "fleet-collector-0",
        "http://127.0.0.1/metrics",
        &addr_a,
    );
    let mut seqs = SeqAllocator::with_incarnation("promo-inc");
    let (prepared, _) = prepare_scrape(
        &config,
        "node-node-a",
        SourceKind::NodeExporter,
        SAMPLE_BODY,
        &mut seqs,
    )
    .expect("prepare");
    let line = prepared[0].buffered.clone();

    let mut client = RawLinesClient::new(&addr_a, Duration::from_secs(1), Duration::from_secs(1));
    assert_eq!(
        client.send_await_ack(&line).await.expect("deny"),
        AckStatus::Denied
    );
    client.retarget(&addr_b);
    assert!(matches!(
        client.send_await_ack(&line).await.expect("b"),
        AckStatus::Committed { .. }
    ));
    assert_eq!(state_b.lock().await.committed.len(), 1);

    let _ = a_tx.send(());
    let _ = b_tx.send(());
    let _ = task_a.await;
    let _ = task_b.await;
}
