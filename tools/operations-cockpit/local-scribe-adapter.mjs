#!/usr/bin/env node
/**
 * Fixed-action controller for one local Scripture Scribe process.
 *
 * Required environment:
 *   SCRIPTURE_LOCAL_CONFIG  absolute or relative Scripture config path
 *   SCRIPTURE_LOCAL_CANON   Canon passed to the fixed producer batch
 *   SCRIPTURE_LOCAL_VERSE   Verse passed to the fixed producer batch
 *
 * Credentials stay in the Cockpit process environment (for example, sourced
 * from a local ignored .env); they are never read, written, or returned here.
 */
import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { createConnection } from "node:net";
import { dirname, join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";

const repoRoot = resolve(dirname(new URL(import.meta.url).pathname), "../..");
const config = process.env.SCRIPTURE_LOCAL_CONFIG ? resolve(process.env.SCRIPTURE_LOCAL_CONFIG) : null;
const canon = process.env.SCRIPTURE_LOCAL_CANON;
const verse = process.env.SCRIPTURE_LOCAL_VERSE;
const binary = process.env.SCRIPTURE_LOCAL_BINARY ?? join(repoRoot, "target/debug/scripture");
const stateDir = resolve(process.env.SCRIPTURE_LOCAL_STATE_DIR ?? join(tmpdir(), "scripture-operations-cockpit-local"));
const pidPath = join(stateDir, "scribe.pid.json");
const logPath = join(stateDir, "scribe.log");
const eventPath = join(stateDir, "events.jsonl");

function fail(message) { throw new Error(message); }
function requireConfig() {
  if (!config) fail("SCRIPTURE_LOCAL_CONFIG is required");
  if (!canon) fail("SCRIPTURE_LOCAL_CANON is required");
  if (!verse) fail("SCRIPTURE_LOCAL_VERSE is required");
  if (!existsSync(config)) fail(`configured Scripture file does not exist: ${config}`);
}

async function event(kind, text) {
  await mkdir(stateDir, { recursive: true });
  await appendFile(eventPath, `${JSON.stringify({ at: new Date().toISOString(), kind, text })}\n`);
}

async function readPid() {
  try { return JSON.parse(await readFile(pidPath, "utf8")); } catch { return null; }
}

async function processIsOurs(record) {
  if (!record?.pid || !Number.isInteger(record.pid)) return false;
  try {
    process.kill(record.pid, 0);
    const command = await readFile(`/proc/${record.pid}/cmdline`, "utf8");
    return command.includes("scripture") && command.includes("bootstrap") && command.includes(config);
  } catch { return false; }
}

async function parseConfig() {
  const contents = await readFile(config, "utf8");
  const value = (name) => contents.match(new RegExp(`^\\s*${name}:\\s*[\"']?([^\\s\"']+)`, "m"))?.[1] ?? null;
  return { prefix: value("prefix") ?? "configured prefix", bind: value("bind") ?? null };
}

function portOpen(bind) {
  if (!bind) return Promise.resolve(false);
  const separator = bind.lastIndexOf(":");
  if (separator < 1) return Promise.resolve(false);
  const host = bind.slice(0, separator);
  const port = Number(bind.slice(separator + 1));
  if (!Number.isInteger(port)) return Promise.resolve(false);
  return new Promise((resolvePromise) => {
    const socket = createConnection({ host, port });
    const done = (value) => { socket.destroy(); resolvePromise(value); };
    socket.setTimeout(300, () => done(false));
    socket.once("connect", () => done(true));
    socket.once("error", () => done(false));
  });
}

async function startScribe() {
  requireConfig();
  if (!existsSync(binary)) fail(`Scripture binary not found: ${binary}; build it with cargo build -p scripture-cli --bin scripture`);
  const current = await readPid();
  if (await processIsOurs(current)) return current;
  await mkdir(stateDir, { recursive: true });
  await writeFile(logPath, "", { mode: 0o600 });
  const log = await import("node:fs").then(({ openSync }) => openSync(logPath, "a"));
  const child = spawn(binary, ["bootstrap", "--config", config], {
    detached: true,
    stdio: ["ignore", log, log],
    env: process.env
  });
  child.unref();
  const record = { pid: child.pid, config, started_at: new Date().toISOString() };
  await writeFile(pidPath, `${JSON.stringify(record)}\n`, { mode: 0o600 });
  await event("action", `Started local Scribe process ${child.pid}.`);
  return record;
}

async function stopScribe() {
  requireConfig();
  const record = await readPid();
  if (!await processIsOurs(record)) {
    await event("action", "Stop requested, but no managed local Scribe process is running.");
    return;
  }
  process.kill(record.pid, "SIGTERM");
  await event("action", `Stopped local Scribe process ${record.pid}.`);
}

function run(argumentsList, timeoutMs = 30_000) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(binary, argumentsList, { env: process.env, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    const timer = setTimeout(() => child.kill("SIGTERM"), timeoutMs);
    child.stdout.on("data", (chunk) => { output += chunk; });
    child.stderr.on("data", (chunk) => { output += chunk; });
    child.on("error", rejectPromise);
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code === 0) resolvePromise(output.trim());
      else rejectPromise(new Error(output.trim() || `scripture exited ${code}`));
    });
  });
}

async function produce() {
  requireConfig();
  const record = await readPid();
  if (!await processIsOurs(record)) fail("local Scribe is not running; start it before sending records");
  const result = await run(["produce-lab", "--config", config, "--canon", canon, "--verse", verse, "--workers", "1", "--per-worker", "3", "--payload-bytes", "96", "--records-per-submission", "1"]);
  await event("action", `Sent three records. ${result.replaceAll("\n", " ").slice(0, 280)}`);
}

async function events() {
  try {
    return (await readFile(eventPath, "utf8")).trim().split("\n").filter(Boolean).slice(-12).map(JSON.parse);
  } catch { return []; }
}

async function status() {
  requireConfig();
  const [record, details, recent, log] = await Promise.all([
    readPid(), parseConfig(), events(), readFile(logPath, "utf8").catch(() => "")
  ]);
  const running = await processIsOurs(record);
  const listening = running && await portOpen(details.bind);
  const state = !running || log.includes("disposition=FailClosed") ? "down" : listening ? "available" : "suspected";
  return {
    schemaVersion: 1,
    mode: "local-control",
    title: "Local Scripture control",
    runId: `local:${canon}/${verse}`,
    observedAt: new Date().toISOString(),
    capabilities: ["produce", "kill-scribe-a", "restart-scribe-a"],
    scribes: [{ id: "local-scribe", node: "this machine", verse, posture: state, reported_posture: state, reachable: listening, route: details.bind ?? "configured ingress", term: "local", source: config }],
    producers: [],
    consumers: [],
    objectStores: [{ id: "configured-store", provider: "configured", objects: "—", state: "observed", source: details.prefix }],
    events: recent,
    evidence: [{ label: "managed local Scribe", verdict: "observed", source: config }, { label: "Scribe process log", verdict: "observed", source: logPath }]
  };
}

async function main() {
  const [command, action] = process.argv.slice(2);
  if (command === "status") { console.log(JSON.stringify(await status())); return; }
  if (command !== "action") fail("usage: local-scribe-adapter.mjs status | action FIXED_ACTION");
  if (action === "produce") await produce();
  else if (action === "kill-scribe-a") await stopScribe();
  else if (action === "restart-scribe-a") { await stopScribe(); await new Promise((resolvePromise) => setTimeout(resolvePromise, 250)); await startScribe(); }
  else fail("unknown fixed cockpit action");
  console.log(JSON.stringify(await status()));
}

main().catch((error) => { process.stderr.write(`${error.message ?? error}\n`); process.exitCode = 1; });
