#!/usr/bin/env node
// Experimental Scripture Producer Wire v1 reference client + codec.
// Run `node producer_wire_v1.mjs --self-test`, or provide --host, --port, and
// --payload to contact a direct experimental Scribe Wire endpoint. This is not
// a production SDK.

import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import net from "node:net";
import { tmpdir } from "node:os";

const MAGIC = Buffer.from("SPW1");
const HELLO = 1;
const SUBMIT = 2;
const ACK = 3;
const MAX_FRAME_BYTES = 4 * 1024 * 1024;
const OUTBOX_FILE = "producer-wire-outbox.json";
const OUTBOX_LOCK = "producer-wire-outbox.lock";

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

function decode(encoded) {
  if (encoded.length < 9) throw new Error("truncated frame");
  const length = encoded.readUInt32BE(0);
  if (length > MAX_FRAME_BYTES || encoded.length !== length + 4) throw new Error("invalid frame length");
  if (!encoded.subarray(4, 8).equals(MAGIC)) throw new Error("unsupported magic/version");
  const kind = encoded[8];
  if (![HELLO, SUBMIT, ACK, 4, 5].includes(kind)) throw new Error("unknown frame type");
  return [kind, encoded.subarray(9)];
}

class ProducerOutbox {
  // This is a small durable reference implementation for Node. It persists
  // complete Submit frames, not reconstructed payloads. Its local JSON format
  // is intentionally not an interchange promise with Rust's append-only WAL.
  constructor(root, producerId, epoch, target) {
    if (!target) throw new Error("--target must name one logical Canon/Verse");
    this.root = root; this.producerId = producerId; this.epoch = epoch; this.target = target;
    mkdirSync(root, { recursive: true, mode: 0o700 });
    this.lock = join(root, OUTBOX_LOCK);
    try {
      const fd = openSync(this.lock, "wx", 0o600);
      writeFileSync(fd, String(process.pid)); fsyncSync(fd); closeSync(fd);
    } catch (error) { throw new Error(`outbox already owned: ${this.lock} (${error.code ?? error.message})`); }
    try { this.state = this.loadOrCreate(); }
    catch (error) { this.close(); throw error; }
  }
  identity() { return { producer_id_hex: this.producerId.toString("hex"), epoch: this.epoch, target: this.target }; }
  loadOrCreate() {
    const path = join(this.root, OUTBOX_FILE);
    if (!existsSync(path)) {
      const state = { format: "spw-reference-outbox-v1", identity: this.identity(), entries: {} };
      this.store(state); return state;
    }
    const state = JSON.parse(readFileSync(path, "utf8"));
    if (state.format !== "spw-reference-outbox-v1" || JSON.stringify(state.identity) !== JSON.stringify(this.identity()) || typeof state.entries !== "object" || state.entries === null) {
      throw new Error("outbox durable identity/state does not match requested producer target");
    }
    return state;
  }
  store(state) {
    const path = join(this.root, OUTBOX_FILE); const temporary = join(this.root, `.outbox-${process.pid}-${Date.now()}`);
    const fd = openSync(temporary, "wx", 0o600);
    try { writeFileSync(fd, `${JSON.stringify(state)}\n`); fsyncSync(fd); }
    finally { closeSync(fd); }
    renameSync(temporary, path);
    const directory = openSync(this.root, "r"); try { fsyncSync(directory); } finally { closeSync(directory); }
  }
  stage(encodedSubmit) {
    const [kind, body] = decode(encodedSubmit); if (kind !== SUBMIT) throw new Error("outbox only stages Submit frames");
    const sequence = body.readBigUInt64BE(0); const key = sequence.toString(); const encoded = encodedSubmit.toString("base64");
    if (this.state.entries[key]) {
      if (this.state.entries[key].submit_b64 !== encoded) throw new Error(`IdentityConflict at sequence ${key}`);
      return;
    }
    const expected = Object.keys(this.state.entries).reduce((max, value) => { const n = BigInt(value); return n > max ? n : max; }, -1n) + 1n;
    if (sequence !== expected) throw new Error(`outbox expected sequence ${expected}, got ${sequence}`);
    this.state.entries[key] = { submit_b64: encoded, acknowledged: false }; this.store(this.state);
  }
  staged(sequence) {
    const entry = this.state.entries[BigInt(sequence).toString()]; if (!entry) throw new Error(`outbox sequence ${sequence} was not staged`);
    return Buffer.from(entry.submit_b64, "base64");
  }
  acknowledge(epoch, sequence) {
    if (epoch !== this.epoch) throw new Error("outbox received an ACK for another epoch");
    const entry = this.state.entries[BigInt(sequence).toString()]; if (!entry) throw new Error(`outbox received an ACK for unknown sequence ${sequence}`);
    if (!entry.acknowledged) { entry.acknowledged = true; this.store(this.state); }
  }
  close() { try { unlinkSync(this.lock); } catch (error) { if (error.code !== "ENOENT") throw error; } }
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
  const root = join(tmpdir(), `scripture-node-outbox-${process.pid}-${Date.now()}`);
  const outbox = new ProducerOutbox(root, Buffer.from("producer-node-01"), 1, "canon/demo/verse/one");
  const first = submit(0, [Buffer.from("one")]); outbox.stage(first);
  if (!outbox.staged(0).equals(first)) throw new Error("outbox did not preserve exact Submit bytes");
  try { outbox.stage(submit(0, [Buffer.from("changed")])); throw new Error("changed retry unexpectedly staged"); }
  catch (error) { if (!String(error.message).includes("IdentityConflict")) throw error; }
  outbox.close();
  rmSync(root, { recursive: true, force: true });
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

async function sendOnce({ host, port, producerId, epoch, sequence, payload, outbox }) {
  if (producerId.length !== 16) throw new Error("--producer-id must be exactly 16 ASCII bytes");
  const socket = net.createConnection({ host, port });
  await new Promise((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  // Connection loss or timeout is intentionally ambiguous. Retry the exact
  // producer identity/epoch/sequence/payload tuple; do not increment sequence.
  let encodedSubmit = submit(sequence, [payload]);
  if (outbox) { outbox.stage(encodedSubmit); encodedSubmit = outbox.staged(sequence); }
  const reply = readFrame(socket);
  socket.write(Buffer.concat([hello(producerId, epoch), encodedSubmit]));
  const [kind, body] = decode(await reply);
  socket.end();
  if (kind === ACK) {
    const ackEpoch = body.readUInt32BE(0);
    const ackSequence = body.readBigUInt64BE(4);
    if (ackEpoch !== epoch || ackSequence !== BigInt(sequence)) throw new Error("Scribe ACK identity mismatch");
    if (outbox) outbox.acknowledge(ackEpoch, ackSequence);
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
  const outboxPath = value("--outbox");
  const target = value("--target");
  if (!host || !Number.isInteger(port) || port < 1 || port > 65535 || payload === undefined) {
    console.error("usage: node producer_wire_v1.mjs --self-test | --host HOST --port PORT --payload TEXT [--producer-id ID --epoch N --sequence N --outbox PATH --target CANON_VERSE_LABEL]");
    process.exit(2);
  }
  if (Boolean(outboxPath) !== Boolean(target)) { console.error("--outbox and --target must be used together"); process.exit(2); }
  const producerId = Buffer.from(value("--producer-id", "producer-node-01"), "ascii");
  const epoch = Number(value("--epoch", "1"));
  if (producerId.length !== 16 || !Number.isInteger(epoch) || epoch < 1 || epoch > 0xffffffff) { console.error("invalid producer identity or epoch"); process.exit(2); }
  const outbox = outboxPath ? new ProducerOutbox(outboxPath, producerId, epoch, target) : null;
  sendOnce({ host, port, payload: Buffer.from(payload, "utf8"), producerId, epoch,
    sequence: BigInt(value("--sequence", "0")), outbox })
    .catch((error) => { console.error(`producer-wire: ${error.message}`); process.exitCode = 1; })
    .finally(() => { if (outbox) outbox.close(); });
}
