#!/usr/bin/env node
/**
 * Read-only Operations Cockpit adapter for an immutable run-bundle-v1 directory.
 * Accepts only `status`. Never scrapes cloud, Kubernetes, or SSH.
 */
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { bundleToSnapshot, loadRunBundle, BundleError } from "./lib/run-bundle.mjs";

const root = dirname(fileURLToPath(import.meta.url));
const bundlePath = resolve(
  process.env.SCRIPTURE_OPS_BUNDLE ?? join(root, "fixtures", "run-bundle-v1")
);

function fail(message) {
  process.stderr.write(`run-bundle-adapter: ${message}\n`);
  process.exit(2);
}

if (process.argv[2] !== "status") {
  fail("only read-only `status` is supported (run-bundle mode has empty action capabilities)");
}

try {
  const bundle = await loadRunBundle(bundlePath);
  process.stdout.write(`${JSON.stringify(bundleToSnapshot(bundle))}\n`);
} catch (error) {
  fail(error instanceof BundleError ? error.message : String(error.message ?? error));
}
