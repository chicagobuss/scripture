#!/usr/bin/env node
/**
 * Local-only collector stub: validates/copies explicitly provided artifacts into
 * a new run-bundle-v1 directory. Does not scrape live systems.
 *
 * Usage:
 *   node bundle-collect.mjs --out DIR --manifest manifest.json \
 *     --file relative/path=/absolute/or/relative/source ...
 */
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { BundleError, collectLocalArtifacts } from "./lib/run-bundle.mjs";

function usage() {
  process.stderr.write(`usage: bundle-collect.mjs --out DIR --manifest PATH [--file rel=src]...\n`);
  process.exit(2);
}

const args = process.argv.slice(2);
let outDir = null;
let manifestPath = null;
const files = {};

for (let i = 0; i < args.length; i += 1) {
  const arg = args[i];
  if (arg === "--out") outDir = args[++i];
  else if (arg === "--manifest") manifestPath = args[++i];
  else if (arg === "--file") {
    const spec = args[++i] ?? "";
    const eq = spec.indexOf("=");
    if (eq <= 0) usage();
    files[spec.slice(0, eq)] = spec.slice(eq + 1);
  } else usage();
}

if (!outDir || !manifestPath) usage();

try {
  const manifest = JSON.parse(await readFile(resolve(manifestPath), "utf8"));
  const bundle = await collectLocalArtifacts({ outDir: resolve(outDir), manifest, files });
  process.stdout.write(`${JSON.stringify({
    ok: true,
    run_id: bundle.manifest.run_id,
    root: bundle.root,
    layers: bundle.layers
  }, null, 2)}\n`);
} catch (error) {
  process.stderr.write(`${error instanceof BundleError ? error.message : error}\n`);
  process.exit(1);
}
