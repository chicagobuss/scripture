//! Direct experimental Producer Wire v1 example client.
//!
//! This sends one submission. If the connection is lost before an Ack, retry
//! the *same* producer id, epoch, sequence, and bytes; do not allocate a new
//! sequence merely because the reply is missing.

use std::error::Error;

use bytes::Bytes;
use scripture::{
    ProducerId, ProducerWireFrame, decode_producer_wire_frame, encode_producer_wire_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn usage() -> &'static str {
    "usage: scripture-producer-wire-client HOST PORT PAYLOAD [PRODUCER_ID EPOCH SEQUENCE]"
}

fn producer_id(raw: &str) -> Result<ProducerId, Box<dyn Error>> {
    let fixed: [u8; 16] = raw
        .as_bytes()
        .try_into()
        .map_err(|_| "PRODUCER_ID must be exactly 16 ASCII bytes")?;
    Ok(ProducerId::from_bytes(fixed))
}

async fn read_frame(stream: &mut TcpStream) -> Result<ProducerWireFrame, Box<dyn Error>> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > scripture::MAX_FRAME_BYTES {
        return Err("peer declared oversized frame".into());
    }
    let mut bytes = vec![0_u8; length + 4];
    bytes[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut bytes[4..]).await?;
    Ok(decode_producer_wire_frame(&bytes)?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 || args.len() > 6 {
        return Err(usage().into());
    }
    let host = &args[0];
    let port: u16 = args[1].parse()?;
    let payload = Bytes::copy_from_slice(args[2].as_bytes());
    let id = producer_id(args.get(3).map_or("producer-rust-01", String::as_str))?;
    let epoch: u32 = args.get(4).map_or(Ok(1), |raw| raw.parse())?;
    let requested_sequence: u64 = args.get(5).map_or(Ok(0), |raw| raw.parse())?;
    if epoch == 0 {
        return Err("EPOCH must be nonzero".into());
    }

    let hello = encode_producer_wire_frame(&ProducerWireFrame::Hello {
        producer_id: id,
        producer_epoch: epoch,
    })?;
    let submit = encode_producer_wire_frame(&ProducerWireFrame::Submit {
        sequence: requested_sequence,
        records: vec![payload],
    })?;
    let mut stream = TcpStream::connect((host.as_str(), port)).await?;
    stream.write_all(&hello).await?;
    stream.write_all(&submit).await?;
    stream.flush().await?;
    match read_frame(&mut stream).await? {
        ProducerWireFrame::Ack {
            producer_epoch,
            sequence,
            first_offset,
            next_offset,
        } if producer_epoch == epoch && sequence == requested_sequence => {
            println!(
                "{{\"verdict\":\"ack\",\"epoch\":{producer_epoch},\"sequence\":{sequence},\"first_offset\":{first_offset},\"next_offset\":{next_offset}}}"
            );
            Ok(())
        }
        ProducerWireFrame::Ack { .. } => Err("Scribe ACK identity mismatch".into()),
        ProducerWireFrame::Error {
            producer_epoch,
            sequence,
            code,
            message,
        } => Err(format!(
            "Scribe error epoch={producer_epoch} sequence={sequence} code={code:?}: {message}"
        )
        .into()),
        frame => Err(format!("expected Ack or Error, got {frame:?}").into()),
    }
}
