#!/usr/bin/env node
/** Read-only live status adapter for a fixed local Scribe fleet. */
import { get } from "node:http";

const rawTargets = process.env.SCRIPTURE_LOCAL_FLEET;
if (!rawTargets) throw new Error("SCRIPTURE_LOCAL_FLEET is required");
const targets = JSON.parse(rawTargets);
if (!Array.isArray(targets) || !targets.length) throw new Error("SCRIPTURE_LOCAL_FLEET must be a non-empty JSON array");

function ready(url) {
  return new Promise((resolvePromise) => {
    const request = get(url, { timeout: 800 }, (response) => {
      response.resume();
      resolvePromise(response.statusCode === 200);
    });
    request.once("timeout", () => request.destroy());
    request.once("error", () => resolvePromise(false));
  });
}

const health = await Promise.all(targets.map(async (target) => ({ ...target, healthy: await ready(target.readyz) })));
console.log(JSON.stringify({
  schemaVersion: 1,
  mode: "local-fleet",
  title: "Local Scripture fleet",
  runId: "cockpit-two-scribes-20260721-001",
  observedAt: new Date().toISOString(),
  capabilities: [],
  scribes: health.map((target) => ({
    id: target.id,
    node: "this machine",
    verse: target.verse,
    posture: target.healthy ? "available" : "down",
    reported_posture: target.healthy ? "available" : "down",
    reachable: target.healthy,
    route: target.route,
    term: "local",
    source: target.readyz
  })),
  producers: [],
  consumers: [],
  objectStores: [{ id: "R2 demo prefix", provider: "R2", objects: "—", state: "observed", source: "scripture/demos/cockpit-two-scribes-20260721-001" }],
  events: health.map((target) => ({ at: new Date().toISOString(), kind: target.healthy ? "observed" : "incomplete", text: `${target.id} ${target.healthy ? "ready" : "not ready"} at ${target.route}` })),
  evidence: health.map((target) => ({ label: `${target.id} readiness`, verdict: target.healthy ? "observed" : "incomplete", source: target.readyz }))
}));
