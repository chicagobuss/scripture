//! Experimental `rsyslog omprog` output helper for native Scripture Wire.
//!
//! Rsyslog owns the source queue and retries. This helper emits `OK` only once
//! a raw input line has a committed Producer Wire ACK. A reply lost after that
//! point remains an at-least-once source boundary: without a stable rsyslog
//! event id, a retried identical line may become a new submission. Consumers
//! must retain their normal deterministic deduplication discipline.

use std::error::Error;
use std::path::PathBuf;

use bytes::Bytes;
use scripture::{
    MAX_FRAME_BYTES, PendingWireSubmission, ProducerId, ProducerOutbox, ProducerOutboxIdentity,
    ProducerWireFrame, decode_producer_wire_frame, encode_producer_wire_frame,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

struct Config {
    scribe: String,
    outbox: PathBuf,
    target: String,
    producer_id: ProducerId,
    epoch: u32,
}

fn usage() -> &'static str {
    "usage: scripture-rsyslog-omprog --scribe HOST:PORT --outbox PATH --target CANON_VERSE_LABEL --producer-id 16_ASCII_BYTES [--epoch N]"
}

fn next_argument(arguments: &mut impl Iterator<Item = String>) -> Result<String, Box<dyn Error>> {
    arguments.next().ok_or_else(|| usage().into())
}

fn parse_config() -> Result<Config, Box<dyn Error>> {
    let mut scribe = None;
    let mut outbox = None;
    let mut target = None;
    let mut producer_id = None;
    let mut epoch = 1_u32;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--scribe" => scribe = Some(next_argument(&mut arguments)?),
            "--outbox" => outbox = Some(PathBuf::from(next_argument(&mut arguments)?)),
            "--target" => target = Some(next_argument(&mut arguments)?),
            "--producer-id" => producer_id = Some(next_argument(&mut arguments)?),
            "--epoch" => epoch = next_argument(&mut arguments)?.parse()?,
            _ => return Err(usage().into()),
        }
    }
    let raw_id = producer_id.ok_or_else(|| Box::<dyn Error>::from(usage()))?;
    let fixed: [u8; 16] = raw_id
        .as_bytes()
        .try_into()
        .map_err(|_| "--producer-id must be exactly 16 ASCII bytes")?;
    if epoch == 0 {
        return Err("--epoch must be nonzero".into());
    }
    Ok(Config {
        scribe: scribe.ok_or_else(|| Box::<dyn Error>::from(usage()))?,
        outbox: outbox.ok_or_else(|| Box::<dyn Error>::from(usage()))?,
        target: target.ok_or_else(|| Box::<dyn Error>::from(usage()))?,
        producer_id: ProducerId::from_bytes(fixed),
        epoch,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_config()?;
    let mut outbox = ProducerOutbox::open(
        config.outbox,
        ProducerOutboxIdentity {
            producer_id: config.producer_id,
            producer_epoch: config.epoch,
            target: config.target,
        },
        1024 * 1024 * 1024,
    )?;
    let hello = outbox.hello_frame()?;

    // `omprog confirmMessages` waits for this ready confirmation before it
    // starts feeding source messages. Refuse readiness if prior durable work
    // cannot be forwarded; rsyslog keeps its action queue instead of creating
    // a second helper instance over the same outbox.
    drain_pending(&mut outbox, &config.scribe, &hello).await?;
    let mut stdout = tokio::io::stdout();
    stdout.write_all(b"OK\n").await?;
    stdout.flush().await?;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let sequence = outbox.next_sequence()?;
        let submit = encode_producer_wire_frame(&ProducerWireFrame::Submit {
            sequence,
            records: vec![Bytes::from(line.into_bytes())],
        })?;
        let outcome = async {
            outbox.stage_submit(&submit)?;
            drain_pending(&mut outbox, &config.scribe, &hello).await
        }
        .await;
        match outcome {
            Ok(()) => stdout.write_all(b"OK\n").await?,
            Err(error) => {
                eprintln!("rsyslog omprog: deferred: {error}");
                // Any non-OK line makes rsyslog retain/retry its source entry.
                // Do not print diagnostics on stdout: it is the confirmation
                // channel, not a human log.
                stdout.write_all(b"DEFER_COMMIT\n").await?;
            }
        }
        stdout.flush().await?;
    }
    Ok(())
}

async fn drain_pending(
    outbox: &mut ProducerOutbox,
    scribe: &str,
    hello: &[u8],
) -> Result<(), Box<dyn Error>> {
    for pending in outbox.pending_submissions() {
        forward_one(scribe, hello, &pending).await?;
        outbox.mark_committed(outbox.identity().producer_epoch, pending.sequence)?;
    }
    Ok(())
}

async fn forward_one(
    scribe: &str,
    hello: &[u8],
    pending: &PendingWireSubmission,
) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(scribe).await?;
    stream.write_all(hello).await?;
    stream.write_all(&pending.encoded_submit).await?;
    stream.flush().await?;
    matching_ack(read_frame(&mut stream).await?, pending).map_err(|error| error.into())
}

fn matching_ack(frame: ProducerWireFrame, pending: &PendingWireSubmission) -> Result<(), String> {
    match frame {
        ProducerWireFrame::Ack {
            sequence,
            first_offset,
            next_offset,
            ..
        } if sequence == pending.sequence && first_offset < next_offset => Ok(()),
        ProducerWireFrame::Ack { .. } => Err("Scribe ACK identity/range mismatch".into()),
        ProducerWireFrame::Error { code, message, .. } => Err(format!(
            "Scribe refused pending submission: {code:?}: {message}"
        )),
        frame => Err(format!("expected Ack/Error, received {frame:?}")),
    }
}

async fn read_frame(stream: &mut TcpStream) -> Result<ProducerWireFrame, Box<dyn Error>> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_FRAME_BYTES {
        return Err("Scribe declared oversized Producer Wire frame".into());
    }
    let mut bytes = vec![0_u8; length + 4];
    bytes[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut bytes[4..]).await?;
    Ok(decode_producer_wire_frame(&bytes)?)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use scripture::{ProducerWireErrorCode, ProducerWireFrame};

    use super::{PendingWireSubmission, matching_ack};

    fn pending() -> PendingWireSubmission {
        PendingWireSubmission {
            sequence: 4,
            encoded_submit: Bytes::from_static(b"not inspected here").to_vec(),
        }
    }

    #[test]
    fn omprog_only_confirms_a_matching_nonempty_committed_range() {
        assert!(
            matching_ack(
                ProducerWireFrame::Ack {
                    producer_epoch: 1,
                    sequence: 4,
                    first_offset: 10,
                    next_offset: 11,
                },
                &pending()
            )
            .is_ok()
        );
        assert!(
            matching_ack(
                ProducerWireFrame::Ack {
                    producer_epoch: 1,
                    sequence: 5,
                    first_offset: 10,
                    next_offset: 11,
                },
                &pending()
            )
            .is_err()
        );
        assert!(
            matching_ack(
                ProducerWireFrame::Ack {
                    producer_epoch: 1,
                    sequence: 4,
                    first_offset: 11,
                    next_offset: 11,
                },
                &pending()
            )
            .is_err()
        );
        assert!(
            matching_ack(
                ProducerWireFrame::Error {
                    producer_epoch: 1,
                    sequence: 4,
                    code: ProducerWireErrorCode::Ambiguous,
                    message: "lost reply".into(),
                },
                &pending()
            )
            .is_err()
        );
    }
}
