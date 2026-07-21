#!/usr/bin/env node
// Experimental Scripture Producer Wire v1 reference client + codec.
// Run `node producer_wire_v1.mjs --self-test`, or provide --host, --port, and
// --payload to contact a direct experimental Scribe Wire endpoint. This is not
// a production SDK.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import net from "node:net";

const MAGIC = Buffer.from("SPW1");
const HELLO = 1;
const SUBMIT = 2;
const ACK = 3;
const MAX_FRAME_BYTES = 4 * 1024 * 1024;

function frame(kind, body) {
  const message = Buffer.concat([MAGIC, Buffer.from([kind]), body]);
  if (message.length > MAX_FRAME_BYTES) throw new Error("frame exceeds 4 MiB");
  const prefix = Buffer.alloc(4);
  prefix.writeUInt32BE(message.length);
  return Buffer.concat([prefix, message]);
}

function hello(producerId, epoch) {
  if (producerId.length !== 16 || epoch < 1 || epoch > 0xffffffff) throw new Error("invalid Hello");
  const body = Buffer.alloc(20);
  producerId.copy(body);
  body.writeUInt32BE(epoch, 16);
  return frame(HELLO, body);
}

function submit(sequence, records) {
  if (records.length < 1 || records.length > 1024) throw new Error("invalid record count");
  const head = Buffer.alloc(12);
  head.writeBigUInt64BE(BigInt(sequence));
  head.writeUInt32BE(records.length, 8);
  const body = [head];
  for (const record of records) {
    const length = Buffer.alloc(4);
    length.writeUInt32BE(record.length);
    body.push(length, record);
  }
  return frame(SUBMIT, Buffer.concat(body));
}

function ack(epoch, sequence, first, nextOffset) {
  const body = Buffer.alloc(28);
  body.writeUInt32BE(epoch);
  body.writeBigUInt64BE(BigInt(sequence), 4);
  body.writeBigUInt64BE(BigInt(first), 12);
  body.writeBigUInt64BE(BigInt(nextOffset), 20);
  return frame(ACK, body);
}

function selfTest() {
  const here = dirname(fileURLToPath(import.meta.url));
  const vectors = JSON.parse(readFileSync(join(here, "..", "producer-wire-v1-vectors.json")));
  const h = vectors.hello;
  if (hello(Buffer.from(h.producer_id_hex, "hex"), h.producer_epoch).toString("hex") !== h.frame_hex) throw new Error("Hello vector mismatch");
  const s = vectors.submit;
  if (submit(s.sequence, s.records_hex.map((x) => Buffer.from(x, "hex"))).toString("hex") !== s.frame_hex) throw new Error("Submit vector mismatch");
  const a = vectors.ack;
  if (ack(a.producer_epoch, a.sequence, a.first_offset, a.next_offset).toString("hex") !== a.frame_hex) throw new Error("Ack vector mismatch");
  console.log("producer-wire-v1 node vectors: PASS");
}

function readFrame(socket) {
  return new Promise((resolve, reject) => {
    let buffered = Buffer.alloc(0);
    const onData = (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      if (buffered.length < 4) return;
      const length = buffered.readUInt32BE(0);
      if (length > MAX_FRAME_BYTES) {
        cleanup();
        reject(new Error("peer declared oversized frame"));
        return;
      }
      if (buffered.length >= length + 4) {
        const frameBytes = buffered.subarray(0, length + 4);
        cleanup();
        resolve(frameBytes);
      }
    };
    const onClose = () => { cleanup(); reject(new Error("Scribe closed before a complete frame")); };
    const onError = (error) => { cleanup(); reject(error); };
    const cleanup = () => {
      socket.off("data", onData);
      socket.off("close", onClose);
      socket.off("error", onError);
    };
    socket.on("data", onData);
    socket.once("close", onClose);
    socket.once("error", onError);
  });
}

async function sendOnce({ host, port, producerId, epoch, sequence, payload }) {
  if (producerId.length !== 16) throw new Error("--producer-id must be exactly 16 ASCII bytes");
  const socket = net.createConnection({ host, port });
  await new Promise((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  // Connection loss or timeout is intentionally ambiguous. Retry the exact
  // producer identity/epoch/sequence/payload tuple; do not increment sequence.
  const reply = readFrame(socket);
  socket.write(Buffer.concat([hello(producerId, epoch), submit(sequence, [payload])]));
  const [kind, body] = decode(await reply);
  socket.end();
  if (kind === ACK) {
    const ackEpoch = body.readUInt32BE(0);
    const ackSequence = body.readBigUInt64BE(4);
    if (ackEpoch !== epoch || ackSequence !== BigInt(sequence)) throw new Error("Scribe ACK identity mismatch");
    console.log(JSON.stringify({ verdict: "ack", epoch: ackEpoch, sequence: String(ackSequence),
      first_offset: String(body.readBigUInt64BE(12)), next_offset: String(body.readBigUInt64BE(20)) }));
    return;
  }
  if (kind === 4) {
    if (body.length < 17) throw new Error("truncated Error");
    const length = body.readUInt32BE(13);
    if (body.length !== 17 + length) throw new Error("invalid Error length");
    throw new Error(`Scribe error epoch=${body.readUInt32BE(0)} sequence=${body.readBigUInt64BE(4)} code=${body[12]}: ${body.subarray(17).toString("utf8")}`);
  }
  throw new Error(`expected Ack or Error, got frame type ${kind}`);
}

const args = process.argv.slice(2);
if (args.length === 1 && args[0] === "--self-test") {
  selfTest();
} else {
  const value = (flag, fallback = undefined) => {
    const index = args.indexOf(flag);
    return index === -1 ? fallback : args[index + 1];
  };
  const host = value("--host");
  const port = Number(value("--port"));
  const payload = value("--payload");
  if (!host || !Number.isInteger(port) || port < 1 || port > 65535 || payload === undefined) {
    console.error("usage: node producer_wire_v1.mjs --self-test | --host HOST --port PORT --payload TEXT [--producer-id ID --epoch N --sequence N]");
    process.exit(2);
  }
  sendOnce({ host, port, payload: Buffer.from(payload, "utf8"),
    producerId: Buffer.from(value("--producer-id", "producer-node-01"), "ascii"),
    epoch: Number(value("--epoch", "1")), sequence: BigInt(value("--sequence", "0")) })
    .catch((error) => { console.error(`producer-wire: ${error.message}`); process.exitCode = 1; });
}
