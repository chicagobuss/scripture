#!/usr/bin/env node
/**
 * Telemetry Canon → Parquet evidence vertical — local preflight runner.
 *
 * Default mode is render/preflight only. It never contacts cloud, Kubernetes,
 * SSH, or live Scribes. `--execute` is intentionally unimplemented until Joshua
 * supplies an explicit approved live command.
 *
 * Usage:
 *   node tools/telemetry-evidence-vertical/preflight.mjs
 *   node tools/telemetry-evidence-vertical/preflight.mjs --execute   # refused
 */
const args = new Set(process.argv.slice(2));

const plan = {
  schema: "telemetry-evidence-vertical-preflight-v1",
  default_mode: "render_preflight",
  live_execute: "refused_until_explicit_approval",
  steps: [
    "isolated namespace + object prefix (not created in preflight)",
    "two Scribes + telemetry producer + materializer (not started)",
    "Scribe A→B cutover observation",
    "materializer crash/restart + register takeover",
    "emit complete run-bundle-v1 (producer ledger, Scribe logs, Canon readback, register/manifests/Parquet summary, inventory, iceberg=absent|not_run)",
    "cleanup inventory"
  ],
  correctness_rules: [
    "one register record {binding_epoch, binding_token, frontier, last_commit_ref}",
    "every acquire/takeover bumps binding_epoch",
    "epoch in every output key; stale epoch never canonical",
    "canonical selection via register+manifest chain — never prefix LIST",
    "Canon source is read/seal-only",
    "Iceberg remains absent unless a real reconciled table commit is proven"
  ],
  local_proof: "cargo test -p scripture-workload --locked --test telemetry_vertical",
  iceberg: "absent unless proven",
  cloud_writes: "not_run"
};

if (args.has("--execute") || args.has("--execute-live")) {
  console.error(JSON.stringify({
    ok: false,
    error: "live --execute is refused by default; no R2/S3/GCS/k0s mutation without Joshua's explicit approved command",
    plan
  }, null, 2));
  process.exit(2);
}

process.stdout.write(`${JSON.stringify({ ok: true, mode: "preflight", plan }, null, 2)}\n`);
