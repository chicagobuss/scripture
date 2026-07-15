//! Verifies `--inflight-per-connection` writes ahead of the first ACK.

use std::time::Duration;

use scripture_load::{LoadConfig, NamedChunkPolicy, run_load};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inflight_writes_precede_first_ack() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("addr");
    let (ready_tx, ready_rx) = oneshot::channel::<usize>();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let mut seen = 0_usize;
        while seen < 4 {
            line.clear();
            let n = reader.read_line(&mut line).await.expect("read");
            assert!(n > 0, "unexpected EOF before 4 writes");
            seen += 1;
        }
        let _ = ready_tx.send(seen);
        for _ in 0..4 {
            writer.write_all(b"OK 0 1\n").await.expect("ack");
        }
        // Drain any trailing EOF.
        let _ = reader.read_line(&mut line).await;
    });

    let report = run_load(LoadConfig {
        endpoint: address.to_string(),
        connections: 1,
        record_bytes: 32,
        duration: Duration::from_secs(2),
        max_bytes: 1024,
        target_records_per_sec: None,
        run_id: "inflight-test".into(),
        ack_timeout: Duration::from_secs(2),
        chunk_policy: NamedChunkPolicy::fleet_lab_default(),
        backend: "test".into(),
        inflight_per_connection: 4,
    })
    .await
    .expect("load");

    let seen = ready_rx.await.expect("server signal");
    assert_eq!(seen, 4, "server must observe 4 writes before any ACK");
    assert_eq!(report.accepted_records, 4);
    assert_eq!(report.errors, 0);
    server.await.expect("server");
}
