//! Lab-grade network adapters for Scripture.
//!
//! The first adapter is deliberately small: newline-delimited raw bytes in,
//! one durable acknowledgement line out. It proves the transport can stay
//! thin over [`scripture_service::JournalHandle`]; it is not a production
//! listener, schema registry, or restart-safe daemon.

use std::collections::BTreeMap;
use std::io;

use bytes::Bytes;
use scripture::{AttributeValue, Record};
use scripture_service::JournalHandle;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// Fixed configuration for one raw-lines listener.
///
/// Listener configuration is intentionally immutable for the connection. A
/// future schema registry selects a versioned parser before a connection is
/// accepted; it must not change parsing halfway through a byte stream.
#[derive(Debug, Clone, PartialEq)]
pub struct RawLinesConfig {
    /// Largest accepted line in bytes, excluding the terminating newline.
    pub max_line_bytes: usize,
    /// Static attributes attached to each accepted line.
    pub attributes: BTreeMap<String, AttributeValue>,
}

impl Default for RawLinesConfig {
    fn default() -> Self {
        Self {
            max_line_bytes: 8 * 1024,
            attributes: BTreeMap::new(),
        }
    }
}

/// Serves one newline-delimited, durable raw-lines connection.
///
/// Each input line becomes one record whose payload is the exact line bytes
/// excluding `\n` and an optional preceding `\r`. For every accepted line the
/// server writes `OK <first-offset> <next-offset>\n` in FIFO input order. An
/// error is reported as `ERR <reason>\n` and closes the connection; a client
/// that disconnects before an `OK` has an unknown durable outcome and must
/// retry at-least-once.
pub async fn serve_raw_lines_connection(
    stream: TcpStream,
    journal: JournalHandle,
    config: RawLinesConfig,
) -> io::Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    loop {
        match read_line(&mut reader, config.max_line_bytes).await {
            Ok(Some(line)) => {
                let record = Record {
                    attributes: config.attributes.clone(),
                    payload: Bytes::from(line),
                };
                let acknowledgement = match journal.submit(vec![record]).await {
                    Ok(acknowledgement) => acknowledgement,
                    Err(error) => {
                        write_error(&mut writer, &error.to_string()).await?;
                        return Ok(());
                    }
                };
                match acknowledgement.await {
                    Ok(acknowledgement) => {
                        writer
                            .write_all(
                                format!(
                                    "OK {} {}\n",
                                    acknowledgement.first_offset.get(),
                                    acknowledgement.next_offset.get()
                                )
                                .as_bytes(),
                            )
                            .await?;
                        writer.flush().await?;
                    }
                    Err(error) => {
                        write_error(&mut writer, &error.to_string()).await?;
                        return Ok(());
                    }
                }
            }
            Ok(None) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                write_error(&mut writer, &error.to_string()).await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }
}

async fn write_error<W: AsyncWrite + Unpin>(writer: &mut W, reason: &str) -> io::Result<()> {
    writer
        .write_all(format!("ERR {reason}\n").as_bytes())
        .await?;
    writer.flush().await
}

async fn read_line<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_line_bytes: usize,
) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        match reader.read_exact(&mut byte).await {
            Ok(_) if byte[0] == b'\n' => {
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(Some(line));
            }
            Ok(_) if line.len() == max_line_bytes => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "line exceeds configured byte limit",
                ));
            }
            Ok(_) => line.push(byte[0]),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof && line.is_empty() => {
                return Ok(None);
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "connection ended before newline-delimited record completed",
                ));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use holylog::atomic::AtomicLog;
    use holylog::drive::LogDrive;
    use holylog::memory::InMemoryLogDrive;
    use scripture::{JournalId, JournalReader, JournalWriter, ReadEvent, RecordOffset};
    use scripture_service::JournalActor;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::{RawLinesConfig, serve_raw_lines_connection};

    #[tokio::test]
    async fn raw_lines_is_a_thin_durable_fifo_adapter() {
        let drive = Arc::new(InMemoryLogDrive::new()) as Arc<dyn LogDrive>;
        let log = AtomicLog::builder(drive, 4).build().expect("build log");
        let journal_id = JournalId::from_bytes(*b"raw-lines-test!!");
        let writer = JournalWriter::new(journal_id, log.clone(), RecordOffset::new(0));
        let (handle, actor) = JournalActor::new(writer, 4);
        let actor = tokio::spawn(actor.run());

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server_handle = handle.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            serve_raw_lines_connection(stream, server_handle, RawLinesConfig::default())
                .await
                .expect("serve")
        });

        let mut client = TcpStream::connect(address).await.expect("connect");
        client.write_all(b"first\r\nsecond\n").await.expect("write");
        client.shutdown().await.expect("finish input");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("read acks");
        assert_eq!(response, b"OK 0 1\nOK 1 2\n");
        server.await.expect("server joins");

        let mut reader = JournalReader::from_start(journal_id, log);
        reader.refresh_tail().await.expect("tail");
        let ReadEvent::Record(first) = reader.read_next().await.expect("first record") else {
            panic!("expected first record");
        };
        let ReadEvent::Record(second) = reader.read_next().await.expect("second record") else {
            panic!("expected second record");
        };
        assert_eq!(first.payload.as_ref(), b"first");
        assert_eq!(second.payload.as_ref(), b"second");

        drop(handle);
        actor.await.expect("actor joins");
    }
}
