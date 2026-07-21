#!/usr/bin/env node
// Safe interactive demonstration adapter. It mutates only a temporary JSON
// file; it never contacts Kubernetes, Scribes, or object storage.
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const statePath = process.env.SCRIPTURE_OPS_DEMO_STATE ?? join(tmpdir(), "scripture-operations-cockpit-demo.json");
const fixture = join(root, "fixture-state.json");
const [command, action] = process.argv.slice(2);

async function load() {
  try { return JSON.parse(await readFile(statePath, "utf8")); }
  catch { await mkdir(dirname(statePath), { recursive: true }); await copyFile(fixture, statePath); return JSON.parse(await readFile(statePath, "utf8")); }
}
function event(state, kind, text) {
  state.observedAt = new Date().toISOString();
  state.events.unshift({ at: state.observedAt.slice(11, 19), kind, text });
  state.events = state.events.slice(0, 24);
}
const state = await load();
state.mode = "demo";
state.capabilities = ["produce", "pause-producer", "resume-producer", "kill-scribe-a", "restart-scribe-a", "promote-scribe-b", "cut-store-b", "restore-store-b", "cleanup", "refresh"];
if (command === "action") {
  const a = state.scribes.find((scribe) => scribe.id === "scribe-a");
  const b = state.scribes.find((scribe) => scribe.id === "scribe-b");
  const storeB = state.objectStores.find((store) => store.id === "s3-isolated-prefix");
  switch (action) {
    case "produce": state.producers.forEach((producer) => { producer.sequence += 1; }); event(state, "observed", "Producer submitted a bounded batch through the demo Wire path."); break;
    case "pause-producer": state.producers.forEach((producer) => { producer.state = "paused"; }); event(state, "observed", "Producers paused; no implied loss or ACK outcome."); break;
    case "resume-producer": state.producers.forEach((producer) => { producer.state = "sending"; }); event(state, "observed", "Producers resumed from their existing sequence state."); break;
    case "kill-scribe-a": a.posture = "down"; a.reachable = false; event(state, "observed", "Scribe A stopped; B remains a candidate until explicit promotion."); break;
    case "restart-scribe-a": a.posture = "standby"; a.reachable = true; event(state, "observed", "Scribe A restarted as a non-serving candidate."); break;
    case "promote-scribe-b": if (!b.reachable) throw new Error("Scribe B is unreachable"); a.posture = a.reachable ? "sealed" : "down"; b.posture = "serving"; b.term = Math.max(a.term, b.term) + 1; event(state, "oracle_pass", "Demo promotion: B is canonical serving route; A is fenced/sealed."); break;
    case "cut-store-b": storeB.state = "isolated"; event(state, "observed", "Demo directional store fault enabled for S3 path."); break;
    case "restore-store-b": storeB.state = "healthy"; event(state, "observed", "Demo directional store fault restored."); break;
    case "cleanup": await copyFile(fixture, statePath); console.log(await readFile(statePath, "utf8")); process.exit(0); break;
    case "refresh": event(state, "observed", "Demo state refreshed."); break;
    default: throw new Error("unsupported fixed action");
  }
  await writeFile(statePath, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
} else if (command !== "status") {
  throw new Error("usage: demo-adapter.mjs status | action NAME");
}
console.log(JSON.stringify(state));
