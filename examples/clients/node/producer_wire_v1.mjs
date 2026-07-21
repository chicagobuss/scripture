#!/usr/bin/env node
// Experimental Scripture Producer Wire v1 reference codec.
// Run `node producer_wire_v1.mjs --self-test`. A Scribe Wire listener has not
// landed, so this verifies framing only and is not a production SDK.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

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

if (process.argv.length !== 3 || process.argv[2] !== "--self-test") {
  console.error("usage: node producer_wire_v1.mjs --self-test");
  process.exit(2);
}
selfTest();
