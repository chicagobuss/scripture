#!/usr/bin/env node
/**
 * Automated fail-closed checks for run-bundle-v1 + fixture smoke.
 */
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import {
  BundleError,
  bundleToSnapshot,
  loadRunBundle,
  boundedPath
} from "./lib/run-bundle.mjs";

const root = dirname(fileURLToPath(import.meta.url));
const fixture = join(root, "fixtures", "run-bundle-v1");
let passed = 0;
let failed = 0;

function ok(label) {
  passed += 1;
  process.stdout.write(`ok  - ${label}\n`);
}

function bad(label, error) {
  failed += 1;
  process.stdout.write(`FAIL - ${label}: ${error}\n`);
}

async function expectReject(label, fn, codeIncludes) {
  try {
    await fn();
    bad(label, "expected rejection");
  } catch (error) {
    const message = String(error.message ?? error);
    const code = error instanceof BundleError ? error.code : "";
    if (codeIncludes && !(message.includes(codeIncludes) || code.includes(codeIncludes) || code === codeIncludes)) {
      bad(label, `rejected for wrong reason: ${message} (${code})`);
      return;
    }
    ok(label);
  }
}

async function copyTree(src, dest) {
  const { readdir, stat } = await import("node:fs/promises");
  await mkdir(dest, { recursive: true });
  for (const name of await readdir(src)) {
    const from = join(src, name);
    const to = join(dest, name);
    const info = await stat(from);
    if (info.isDirectory()) await copyTree(from, to);
    else await copyFile(from, to);
  }
}

// 1. Valid fixture loads and exposes explorer sections.
try {
  const bundle = await loadRunBundle(fixture);
  const snap = bundleToSnapshot(bundle);
  if (snap.mode !== "run-bundle") throw new Error("mode");
  if (snap.capabilities.length !== 0) throw new Error("capabilities must be empty");
  if (!snap.explorer?.messages?.length) throw new Error("messages");
  if (!snap.explorer?.scribe_timelines?.["scribe-a"]) throw new Error("scribe-a");
  if (!snap.explorer?.scribe_timelines?.["scribe-b"]) throw new Error("scribe-b");
  if (snap.explorer.objects.label !== "inventory observation") throw new Error("objects label");
  if (snap.explorer.outputs.iceberg.state !== "absent") throw new Error("iceberg absent");
  if (snap.explorer.layers.holylog_oracle !== "not_supplied") throw new Error("oracle layer");
  if (!snap.explorer.matrix.some((row) => row.claim.includes("Holylog") && row.verdict === "not_run")) {
    throw new Error("missing oracle cannot be pass");
  }
  if (snap.explorer.outputs.manifests.some((m) => m.binding_epoch === 1 && m.canonical === true)) {
    throw new Error("stale epoch marked canonical");
  }
  if (snap.scribes.some((scribe) => scribe.posture === "serving")) {
    throw new Error("process logs must not become serving-authority evidence");
  }
  if (snap.events.some((event) => event.kind === "oracle_pass")) {
    throw new Error("process-log promotion must remain an observation");
  }
  if (!snap.explorer.outputs.manifests.some((m) => m.binding_epoch === 2 && m.canonical === true)) {
    throw new Error("canonical epoch missing");
  }
  if (!snap.explorer.cross_links.some((link) => link.complete)) throw new Error("cross links");
  ok("valid fixture renders explorer sections");
} catch (error) {
  bad("valid fixture renders explorer sections", error.message ?? error);
}

// 2. Path traversal / absolute paths rejected.
await expectReject("rejects path traversal", async () => {
  boundedPath(fixture, "../secret", "test");
}, "path_rejected");

await expectReject("rejects absolute path", async () => {
  boundedPath(fixture, "/etc/passwd", "test");
}, "path_rejected");

// 3. Malformed / oversized / unreferenced fail closed.
{
  const tmp = await mkdtemp(join(tmpdir(), "run-bundle-bad-"));
  try {
    await copyTree(fixture, tmp);
    await writeFile(join(tmp, "messages.jsonl"), "{not-json\n");
    await expectReject("malformed JSONL fails closed", () => loadRunBundle(tmp), "malformed");
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}

{
  const tmp = await mkdtemp(join(tmpdir(), "run-bundle-over-"));
  try {
    await copyTree(fixture, tmp);
    const big = `${"x".repeat(2_000_001)}`;
    await writeFile(join(tmp, "messages.jsonl"), `${JSON.stringify({ digest: "x", pad: big })}\n`);
    await expectReject("oversized artifact fails closed", () => loadRunBundle(tmp), "oversized");
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}

{
  const tmp = await mkdtemp(join(tmpdir(), "run-bundle-verdict-"));
  try {
    await copyTree(fixture, tmp);
    const manifest = JSON.parse(await readFile(join(tmp, "manifest.json"), "utf8"));
    manifest.verdicts.push({ label: "bogus pass", verdict: "pass", source: "not supplied" });
    await writeFile(join(tmp, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
    await expectReject("pass without source fails closed", () => loadRunBundle(tmp), "verdict_source");
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}

{
  const tmp = await mkdtemp(join(tmpdir(), "run-bundle-oracle-pass-"));
  try {
    await copyTree(fixture, tmp);
    const manifest = JSON.parse(await readFile(join(tmp, "manifest.json"), "utf8"));
    for (const item of manifest.verdicts) {
      if (item.label === "Holylog oracle") {
        item.verdict = "pass";
        item.source = "holylog-oracle.json";
      }
    }
    await writeFile(join(tmp, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
    await expectReject("missing oracle cannot render as pass", () => loadRunBundle(tmp), "verdict_source");
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}

// 4. Adapter smoke: status JSON fields.
await new Promise((resolvePromise) => {
  const child = spawn(
    process.execPath,
    [join(root, "run-bundle-adapter.mjs"), "status"],
    {
      cwd: root,
      env: { ...process.env, SCRIPTURE_OPS_BUNDLE: fixture },
      stdio: ["ignore", "pipe", "pipe"]
    }
  );
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.on("close", (code) => {
    try {
      if (code !== 0) throw new Error(stderr || `exit ${code}`);
      const snap = JSON.parse(stdout);
      if (snap.mode !== "run-bundle") throw new Error("mode");
      if (snap.capabilities.length !== 0) throw new Error("actions not empty");
      if (snap.explorer.outputs.iceberg.state === "verified") throw new Error("iceberg must not be verified");
      if (!Array.isArray(snap.explorer.matrix)) throw new Error("matrix");
      ok("adapter status exposes key API fields");
    } catch (error) {
      bad("adapter status exposes key API fields", error.message ?? error);
    }
    resolvePromise();
  });
});

// 5. HTML escape helper used by UI (inline check mirrors app.js).
{
  const escapeHtml = (value) => String(value).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  })[c]);
  const raw = `<img src=x onerror=alert(1)> & "quotes"`;
  const escaped = escapeHtml(raw);
  if (escaped.includes("<") || escaped.includes(">") || !escaped.includes("&lt;img") || !escaped.includes("&amp;")) {
    bad("HTML in messages/logs is escaped", escaped);
  } else {
    ok("HTML in messages/logs is escaped");
  }
}

// 6. Iceberg absent cannot look like a table/snapshot in snapshot payload.
try {
  const snap = bundleToSnapshot(await loadRunBundle(fixture));
  const ice = snap.explorer.outputs.iceberg;
  if (ice.state !== "absent") throw new Error(ice.state);
  if (ice.snapshot_id != null) throw new Error("snapshot present");
  if (ice.table_ident != null) throw new Error("table present");
  ok("Iceberg absent cannot render as table/snapshot");
} catch (error) {
  bad("Iceberg absent cannot render as table/snapshot", error.message ?? error);
}

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exit(failed ? 1 : 0);
