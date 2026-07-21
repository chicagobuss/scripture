//! Runtime half of experimental Producer Wire v1.
//!
//! The core codec is in `scripture::producer_wire`. This module binds a Hello
//! identity to the exact `Submission` identity understood by the current
//! driver. It is not exposed by a configured listener yet.

use std::io;

use scripture::{
    ProducerWireErrorCode, ProducerWireFrame, ReceiptFuture, Record, Submission,
    decode_producer_wire_frame, encode_producer_wire_frame,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// One machine-readable admission refusal for Producer Wire.
#[derive(Debug, Clone)]
pub(crate) struct ProducerWireRejection {
    pub(crate) code: ProducerWireErrorCode,
    pub(crate) message: String,
}

impl ProducerWireRejection {
    pub(crate) fn new(code: ProducerWireErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Admission/flush surface deliberately separate from raw-lines.
///
/// Raw-lines predates a machine-readable producer contract and collapses
/// errors to text. Producer Wire must retain the bounded error class all the
/// way to its frame encoder.
pub(crate) trait ProducerWireSink {
    async fn submit(&self, submission: Submission) -> Result<ReceiptFuture, ProducerWireRejection>;
    async fn flush(&self) -> Result<(), ProducerWireRejection>;
}

/// Serves one v1 connection over an already accepted transport.
///
/// Each submission is flushed before its receipt is awaited in this first,
/// correctness-focused path. A bounded pending pipeline can later improve
/// batching without changing stable identity or receipt meaning.
pub(crate) async fn serve_producer_wire_connection<R, W, S>(
    mut reader: R,
    mut writer: W,
    sink: S,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    S: ProducerWireSink,
{
    let Some(ProducerWireFrame::Hello {
        producer_id,
        producer_epoch,
    }) = read_frame(&mut reader).await?
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "producer-wire requires Hello as first frame",
        ));
    };

    while let Some(frame) = read_frame(&mut reader).await? {
        match frame {
            ProducerWireFrame::Submit { sequence, records } => {
                let submission = Submission {
                    producer_id,
                    producer_epoch,
                    sequence,
                    records: records
                        .into_iter()
                        .map(|payload| Record {
                            attributes: Default::default(),
                            payload,
                        })
                        .collect(),
                };
                let receipt = match sink.submit(submission).await {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        write_error(
                            &mut writer,
                            producer_epoch,
                            sequence,
                            error.code,
                            &error.message,
                        )
                        .await?;
                        continue;
                    }
                };
                if let Err(error) = sink.flush().await {
                    write_error(
                        &mut writer,
                        producer_epoch,
                        sequence,
                        error.code,
                        &error.message,
                    )
                    .await?;
                    continue;
                }
                match receipt.await {
                    Ok(receipt) => {
                        write_frame(
                            &mut writer,
                            ProducerWireFrame::Ack {
                                producer_epoch,
                                sequence,
                                first_offset: receipt.first_offset.get(),
                                next_offset: receipt.next_offset.get(),
                            },
                        )
                        .await?;
                    }
                    Err(_) => {
                        write_error(
                            &mut writer,
                            producer_epoch,
                            sequence,
                            ProducerWireErrorCode::Ambiguous,
                            "receipt unavailable",
                        )
                        .await?;
                    }
                }
            }
            ProducerWireFrame::Close => return Ok(()),
            ProducerWireFrame::Hello { .. }
            | ProducerWireFrame::Ack { .. }
            | ProducerWireFrame::Error { .. } => {
                write_error(
                    &mut writer,
                    producer_epoch,
                    0,
                    ProducerWireErrorCode::Unsupported,
                    "unexpected client frame",
                )
                .await?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<ProducerWireFrame>> {
    let mut prefix = [0_u8; 4];
    match reader.read_exact(&mut prefix).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes(prefix) as usize;
    if length > scripture::MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "producer-wire frame exceeds absolute cap",
        ));
    }
    let mut bytes = vec![0_u8; length + 4];
    bytes[..4].copy_from_slice(&prefix);
    reader.read_exact(&mut bytes[4..]).await?;
    decode_producer_wire_frame(&bytes)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: ProducerWireFrame,
) -> io::Result<()> {
    let bytes = encode_producer_wire_frame(&frame)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    writer.write_all(&bytes).await?;
    writer.flush().await
}

async fn write_error<W: AsyncWrite + Unpin>(
    writer: &mut W,
    producer_epoch: u32,
    sequence: u64,
    code: ProducerWireErrorCode,
    message: &str,
) -> io::Result<()> {
    write_frame(
        writer,
        ProducerWireFrame::Error {
            producer_epoch,
            sequence,
            code,
            message: message.to_owned(),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use futures::channel::oneshot;
    use scripture::{
        AckLevel, ChunkId, JournalId, ProducerId, Receipt, ReceiptFuture, RecordOffset,
    };
    use tokio::io::{AsyncWriteExt, duplex};

    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        submissions: Mutex<Vec<Submission>>,
        flushes: Mutex<u64>,
    }

    impl ProducerWireSink for Arc<RecordingSink> {
        async fn submit(
            &self,
            submission: Submission,
        ) -> Result<ReceiptFuture, ProducerWireRejection> {
            self.submissions
                .lock()
                .expect("submissions")
                .push(submission);
            let (sender, receiver) = oneshot::channel();
            let _ = sender.send(Ok(Receipt {
                level: AckLevel::Committed,
                journal_id: JournalId::from_bytes(*b"wire-journal-001"),
                first_offset: RecordOffset::new(40),
                next_offset: RecordOffset::new(42),
                chunk_id: ChunkId::from_bytes(*b"wire-chunk-00001"),
                slot: 1,
                canon_revision: 2,
                deduplicated: false,
            }));
            Ok(ReceiptFuture::from_receiver(receiver))
        }

        async fn flush(&self) -> Result<(), ProducerWireRejection> {
            *self.flushes.lock().expect("flushes") += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn hello_identity_is_preserved_and_ack_echoes_submission() {
        let sink = Arc::new(RecordingSink::default());
        let producer_id = ProducerId::from_bytes(*b"producer-wire-01");
        let (mut client, server) = duplex(4096);
        let (reader, writer) = tokio::io::split(server);
        let task = {
            let sink = Arc::clone(&sink);
            tokio::spawn(async move { serve_producer_wire_connection(reader, writer, sink).await })
        };
        client
            .write_all(
                &encode_producer_wire_frame(&ProducerWireFrame::Hello {
                    producer_id,
                    producer_epoch: 4,
                })
                .expect("hello"),
            )
            .await
            .expect("hello write");
        client
            .write_all(
                &encode_producer_wire_frame(&ProducerWireFrame::Submit {
                    sequence: 9,
                    records: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
                })
                .expect("submit"),
            )
            .await
            .expect("submit write");
        let response = read_frame(&mut client).await.expect("read").expect("ack");
        assert_eq!(
            response,
            ProducerWireFrame::Ack {
                producer_epoch: 4,
                sequence: 9,
                first_offset: 40,
                next_offset: 42,
            }
        );
        {
            let submissions = sink.submissions.lock().expect("submissions");
            assert_eq!(submissions.len(), 1);
            assert_eq!(submissions[0].producer_id, producer_id);
            assert_eq!(submissions[0].producer_epoch, 4);
            assert_eq!(submissions[0].sequence, 9);
            assert_eq!(
                submissions[0].records[0].payload,
                Bytes::from_static(b"one")
            );
        }
        assert_eq!(*sink.flushes.lock().expect("flushes"), 1);
        client.shutdown().await.expect("shutdown");
        task.await.expect("join").expect("serve");
    }

    #[tokio::test]
    async fn submit_before_hello_cannot_reach_admission() {
        let sink = Arc::new(RecordingSink::default());
        let (mut client, server) = duplex(4096);
        let (reader, writer) = tokio::io::split(server);
        let task = {
            let sink = Arc::clone(&sink);
            tokio::spawn(async move { serve_producer_wire_connection(reader, writer, sink).await })
        };
        client
            .write_all(
                &encode_producer_wire_frame(&ProducerWireFrame::Submit {
                    sequence: 1,
                    records: vec![Bytes::from_static(b"no-hello")],
                })
                .expect("submit"),
            )
            .await
            .expect("write");
        client.shutdown().await.expect("shutdown");
        assert!(task.await.expect("join").is_err());
        assert!(sink.submissions.lock().expect("submissions").is_empty());
    }
}
