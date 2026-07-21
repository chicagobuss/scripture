#!/usr/bin/env node
/**
 * Read-only Operations Cockpit adapter for scripture-telemetry-producer JSONL.
 *
 * This is deliberately an evidence viewer, not a control plane. It translates
 * producer observations into the cockpit snapshot contract and leaves Scribe
 * authority, Holylog readback, and object-store state as explicitly not run.
 */
import { readFile } from "node:fs/promises";
import { basename, resolve } from "node:path";

const ledgerPath = process.env.SCRIPTURE_TELEMETRY_LEDGER;

function fail(message) {
  process.stderr.write(`telemetry-ledger-adapter: ${message}\n`);
  process.exit(2);
}

function shortTime(index) {
  return `row ${index + 1}`;
}

function parseLedger(text) {
  const rows = [];
  for (const [index, line] of text.split(/\r?\n/).entries()) {
    if (!line.trim()) continue;
    let row;
    try {
      row = JSON.parse(line);
    } catch (error) {
      fail(`invalid JSON at ${shortTime(index)}: ${error.message}`);
    }
    if (!row || typeof row !== "object" || typeof row.row_type !== "string") {
      fail(`missing row_type at ${shortTime(index)}`);
    }
    rows.push({ ...row, _index: index });
  }
  return rows;
}

function producerKey(row) {
  return `${row.producer_id ?? "unknown-producer"}\u0000${row.verse ?? "unknown-verse"}`;
}

function snapshot(rows) {
  const producers = new Map();
  const events = [];
  let committed = 0;
  let unacked = 0;
  let failovers = 0;

  for (const row of rows) {
    if (row.row_type === "send") {
      const id = String(row.producer_id ?? "unknown-producer");
      const verse = String(row.verse ?? "unknown-verse");
      const key = producerKey(row);
      const previous = producers.get(key) ?? {
        id: `${id} → ${verse}`,
        kind: "otel-shaped-json / raw-lines",
        state: "observed",
        sequence: -1,
        ack: "unknown",
        source: "telemetry send ledger"
      };
      previous.sequence = Math.max(previous.sequence, Number.isSafeInteger(row.seq) ? row.seq : -1);
      previous.ack = String(row.ack_status ?? "unknown");
      previous.state = row.unacked ? "retrying_or_unacked" : "committed";
      producers.set(key, previous);
      if (row.unacked) unacked += 1;
      else if (String(row.ack_status ?? "").startsWith("committed:")) committed += 1;
      continue;
    }
    if (row.row_type === "failover") {
      failovers += 1;
      events.push({
        at: shortTime(row._index),
        kind: "producer_failover_observed",
        text: String(row.message ?? `Verse ${row.verse ?? "unknown"} advanced ${row.from_endpoint ?? "?"} → ${row.to_endpoint ?? "?"}`)
      });
    }
  }

  events.unshift({
    at: "ledger",
    kind: "observed",
    text: `Producer ledger: ${committed} committed observations, ${unacked} unacked observations, ${failovers} connect-chain failovers.`
  });
  return {
    schemaVersion: 1,
    mode: "telemetry-ledger",
    title: "Telemetry producer evidence",
    runId: basename(ledgerPath),
    observedAt: new Date().toISOString(),
    capabilities: [],
    scribes: [],
    producers: [...producers.values()].sort((left, right) => left.id.localeCompare(right.id)),
    consumers: [],
    objectStores: [],
    events,
    evidence: [
      { label: "telemetry send ledger", verdict: "observed", source: resolve(ledgerPath) },
      { label: "producer failover", verdict: failovers ? "observed" : "not_run", source: resolve(ledgerPath) },
      { label: "Scribe authority / Holylog readback", verdict: "not_run", source: "not supplied by a producer-side ledger" },
      { label: "object-store durability", verdict: "not_run", source: "not supplied by a producer-side ledger" }
    ]
  };
}

if (process.argv[2] !== "status") fail("only read-only `status` is supported");
if (!ledgerPath) fail("set SCRIPTURE_TELEMETRY_LEDGER to a telemetry producer JSONL ledger");

try {
  const text = await readFile(ledgerPath, "utf8");
  process.stdout.write(`${JSON.stringify(snapshot(parseLedger(text)))}\n`);
} catch (error) {
  fail(error.message);
}
