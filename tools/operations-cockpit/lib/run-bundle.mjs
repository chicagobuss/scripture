/**
 * run-bundle-v1 — immutable local evidence directory for the Operations Cockpit.
 *
 * The UI and adapters read only this bundle. Paths are relative, bounded, and
 * fail-closed. Missing layers render as not_run / not_supplied — never healthy.
 */
import { readFile, readdir, stat } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, normalize, relative, resolve, sep } from "node:path";

export const SCHEMA_VERSION = 1;
export const MAX_FILE_BYTES = 2_000_000;
export const MAX_JSONL_ROWS = 20_000;
export const VERDICTS = new Set(["pass", "fail", "inconclusive", "not_run", "observed"]);
export const ICEBERG_STATES = new Set(["verified", "configured_not_verified", "absent", "not_run"]);

export class BundleError extends Error {
  constructor(message, code = "bundle_invalid") {
    super(message);
    this.name = "BundleError";
    this.code = code;
  }
}

function fail(message, code) {
  throw new BundleError(message, code);
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** Resolve a relative path inside root; reject absolute, traversal, and escapes. */
export function boundedPath(root, relativePath, label = "path") {
  if (typeof relativePath !== "string" || !relativePath.trim()) {
    fail(`${label} must be a non-empty relative path`);
  }
  if (isAbsolute(relativePath) || relativePath.includes("\0")) {
    fail(`${label} must be a relative path inside the bundle: ${relativePath}`, "path_rejected");
  }
  const normalized = normalize(relativePath);
  if (normalized.startsWith("..") || normalized.split(sep).includes("..")) {
    fail(`${label} path traversal rejected: ${relativePath}`, "path_rejected");
  }
  const absolute = resolve(root, normalized);
  const rel = relative(root, absolute);
  if (rel.startsWith("..") || isAbsolute(rel)) {
    fail(`${label} escapes bundle root: ${relativePath}`, "path_rejected");
  }
  return { absolute, relative: rel.split(sep).join("/") };
}

async function readBoundedFile(absolute, label) {
  let info;
  try {
    info = await stat(absolute);
  } catch {
    fail(`${label} not found: ${absolute}`, "missing_file");
  }
  if (!info.isFile()) fail(`${label} is not a file`, "missing_file");
  if (info.size > MAX_FILE_BYTES) {
    fail(`${label} exceeds ${MAX_FILE_BYTES} bytes (${info.size})`, "oversized");
  }
  return readFile(absolute, "utf8");
}

export function parseJsonl(text, label) {
  const rows = [];
  const lines = text.split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (!line.trim()) continue;
    let row;
    try {
      row = JSON.parse(line);
    } catch (error) {
      fail(`${label} malformed JSONL at line ${index + 1}: ${error.message}`, "malformed_jsonl");
    }
    if (!isPlainObject(row)) fail(`${label} row ${index + 1} must be a JSON object`, "malformed_jsonl");
    rows.push(row);
    if (rows.length > MAX_JSONL_ROWS) {
      fail(`${label} exceeds ${MAX_JSONL_ROWS} rows`, "oversized");
    }
  }
  return rows;
}

function validateManifest(manifest) {
  if (!isPlainObject(manifest)) fail("manifest.json must be an object");
  if (manifest.schema_version !== SCHEMA_VERSION) {
    fail(`unsupported schema_version ${manifest.schema_version}; expected ${SCHEMA_VERSION}`, "schema_version");
  }
  if (typeof manifest.run_id !== "string" || !manifest.run_id.trim()) fail("run_id required");
  if (typeof manifest.collected_at !== "string" || !manifest.collected_at.trim()) fail("collected_at required");
  if (!isPlainObject(manifest.revisions)) fail("revisions object required");
  if (typeof manifest.revisions.scripture !== "string" || typeof manifest.revisions.holylog !== "string") {
    fail("revisions.scripture and revisions.holylog required");
  }
  if (!isPlainObject(manifest.scope) || !Array.isArray(manifest.scope.object_prefixes)) {
    fail("scope.object_prefixes array required");
  }
  if (typeof manifest.scope.namespace !== "string") fail("scope.namespace required");
  if (!isPlainObject(manifest.inputs)) fail("inputs object required");
  if (!Array.isArray(manifest.verdicts) || manifest.verdicts.length === 0) {
    fail("verdicts must be a non-empty array");
  }
  for (const [index, item] of manifest.verdicts.entries()) {
    if (!isPlainObject(item) || typeof item.label !== "string") fail(`verdicts[${index}].label required`);
    if (!VERDICTS.has(item.verdict)) fail(`verdicts[${index}].verdict invalid: ${item.verdict}`);
    if (typeof item.source !== "string" || !item.source.trim()) {
      fail(`verdicts[${index}] missing source — UI cannot invent correctness claims`, "verdict_source");
    }
  }
  const policy = manifest.policy ?? {};
  if (policy.payload_previews !== undefined && policy.payload_previews !== "lab_nonsecret" && policy.payload_previews !== "off") {
    fail("policy.payload_previews must be off or lab_nonsecret");
  }
  return manifest;
}

function secretScan(text, label) {
  const patterns = [
    /AKIA[0-9A-Z]{16}/,
    /AWS_SECRET_ACCESS_KEY/i,
    /Bearer\s+[A-Za-z0-9\-._~+/]+=*/,
    /BEGIN (RSA |OPENSSH )?PRIVATE KEY/,
    /"SecretAccessKey"\s*:/,
    /X-Amz-Signature=/i
  ];
  for (const pattern of patterns) {
    if (pattern.test(text)) fail(`${label} appears to contain a secret pattern; rejected`, "secret_rejected");
  }
}

async function optionalJson(root, relativePath, label) {
  if (!relativePath) return { status: "not_supplied", path: null, value: null };
  const { absolute, relative: rel } = boundedPath(root, relativePath, label);
  try {
    const text = await readBoundedFile(absolute, label);
    secretScan(text, label);
    return { status: "present", path: rel, value: JSON.parse(text) };
  } catch (error) {
    if (error instanceof BundleError && error.code === "missing_file") {
      return { status: "not_supplied", path: rel, value: null };
    }
    throw error;
  }
}

async function optionalJsonl(root, relativePath, label) {
  if (!relativePath) return { status: "not_supplied", path: null, rows: [] };
  const { absolute, relative: rel } = boundedPath(root, relativePath, label);
  try {
    const text = await readBoundedFile(absolute, label);
    secretScan(text, label);
    return { status: "present", path: rel, rows: parseJsonl(text, label) };
  } catch (error) {
    if (error instanceof BundleError && error.code === "missing_file") {
      return { status: "not_supplied", path: rel, rows: [] };
    }
    throw error;
  }
}

function layerStatus(file) {
  return file.status === "present" ? "present" : "not_supplied";
}

function normalizeIceberg(value, status) {
  if (status !== "present") return { state: "not_run", detail: "iceberg.json not supplied" };
  if (!isPlainObject(value) || !ICEBERG_STATES.has(value.state)) {
    fail("outputs/iceberg.json.state must be verified|configured_not_verified|absent|not_run");
  }
  const tableIdent = typeof value.table_ident === "string" && value.table_ident.trim()
    ? value.table_ident
    : null;
  const snapshotId = value.snapshot_id != null && String(value.snapshot_id).trim()
    ? String(value.snapshot_id)
    : null;
  if (value.state === "verified" && (!tableIdent || !snapshotId)) {
    fail(
      "outputs/iceberg.json.state=verified requires non-empty table_ident and snapshot_id",
      "iceberg_evidence"
    );
  }
  return {
    state: value.state,
    detail: typeof value.detail === "string" ? value.detail : null,
    table_ident: tableIdent,
    snapshot_id: snapshotId
  };
}

function collectDeclaredInputPaths(inputs) {
  const declared = new Set();
  const add = (value) => {
    if (value == null) return;
    if (Array.isArray(value)) {
      value.forEach(add);
      return;
    }
    if (typeof value === "string" && value.trim()) {
      declared.add(value.split(sep).join("/"));
    }
  };
  add(inputs.producer_ledger);
  add(inputs.messages);
  add(inputs.console_readback);
  add(inputs.scribe_logs);
  add(inputs.object_inventory);
  add(inputs.outputs_register);
  add(inputs.outputs_manifests);
  add(inputs.parquet_summary);
  add(inputs.iceberg);
  add(inputs.holylog_oracle);
  add(inputs.authority_observation);
  return declared;
}

async function validateEvidenceVerdicts(root, manifest) {
  const declared = collectDeclaredInputPaths(manifest.inputs);
  for (const item of manifest.verdicts) {
    if (item.verdict !== "observed" && item.verdict !== "pass") continue;
    const source = item.source;
    if (source === "not supplied" || source === "not_run") {
      fail(
        `verdict "${item.label}" is ${item.verdict} but source is synthetic`,
        "verdict_source"
      );
    }
    const { absolute, relative: rel } = boundedPath(root, source, `verdict ${item.label} source`);
    if (!declared.has(rel) && !declared.has(source)) {
      fail(
        `verdict "${item.label}" source is not declared in manifest.inputs: ${source}`,
        "verdict_source"
      );
    }
    try {
      const info = await stat(absolute);
      if (!info.isFile()) {
        fail(`verdict "${item.label}" source is not a file: ${source}`, "verdict_source");
      }
    } catch {
      fail(`verdict "${item.label}" is ${item.verdict} but source is missing: ${source}`, "verdict_source");
    }
  }
}

/** Load and validate a run-bundle-v1 directory. */
export async function loadRunBundle(bundleRoot) {
  const root = resolve(bundleRoot);
  const manifestPath = join(root, "manifest.json");
  const manifestText = await readBoundedFile(manifestPath, "manifest.json");
  secretScan(manifestText, "manifest.json");
  let manifest;
  try {
    manifest = validateManifest(JSON.parse(manifestText));
  } catch (error) {
    if (error instanceof BundleError) throw error;
    fail(`manifest.json parse error: ${error.message}`, "malformed_json");
  }

  // Bound every referenced input path before reading.
  const inputs = manifest.inputs;
  const pathRefs = [];
  const pushRef = (value, label) => {
    if (value == null) return;
    if (Array.isArray(value)) {
      value.forEach((item, index) => pushRef(item, `${label}[${index}]`));
      return;
    }
    pathRefs.push(boundedPath(root, value, label));
  };
  pushRef(inputs.producer_ledger, "inputs.producer_ledger");
  pushRef(inputs.messages, "inputs.messages");
  pushRef(inputs.console_readback, "inputs.console_readback");
  pushRef(inputs.scribe_logs, "inputs.scribe_logs");
  pushRef(inputs.object_inventory, "inputs.object_inventory");
  pushRef(inputs.outputs_register, "inputs.outputs_register");
  pushRef(inputs.outputs_manifests, "inputs.outputs_manifests");
  pushRef(inputs.parquet_summary, "inputs.parquet_summary");
  pushRef(inputs.iceberg, "inputs.iceberg");
  pushRef(inputs.holylog_oracle, "inputs.holylog_oracle");
  pushRef(inputs.authority_observation, "inputs.authority_observation");

  for (const item of manifest.verdicts) {
    // Synthetic sources like "not supplied" are allowed only when verdict is not_run/inconclusive.
    if (item.source === "not supplied" || item.source === "not_run") {
      if (item.verdict !== "not_run" && item.verdict !== "inconclusive") {
        fail(`verdict "${item.label}" uses synthetic source but verdict is ${item.verdict}`, "verdict_source");
      }
      continue;
    }
    boundedPath(root, item.source, `verdict ${item.label} source`);
  }

  const producerLedger = await optionalJsonl(root, inputs.producer_ledger, "producer-ledger");
  const messages = await optionalJsonl(root, inputs.messages, "messages");
  const consoleReadback = await optionalJsonl(root, inputs.console_readback, "console-readback");
  const objectInventory = await optionalJson(root, inputs.object_inventory, "objects");
  const register = await optionalJson(root, inputs.outputs_register, "outputs/register");
  const parquetSummary = await optionalJson(root, inputs.parquet_summary, "outputs/parquet-summary");
  const icebergFile = await optionalJson(root, inputs.iceberg, "outputs/iceberg");
  const holylogOracle = await optionalJson(root, inputs.holylog_oracle, "holylog-oracle");
  const authorityObservation = await optionalJson(root, inputs.authority_observation, "authority-observation");

  const scribeLogPaths = Array.isArray(inputs.scribe_logs) ? inputs.scribe_logs : [];
  const scribeLogs = {};
  for (const relativePath of scribeLogPaths) {
    const loaded = await optionalJsonl(root, relativePath, `scribe log ${relativePath}`);
    const id = basename(relativePath, ".jsonl");
    scribeLogs[id] = loaded;
  }

  const manifestPaths = Array.isArray(inputs.outputs_manifests) ? inputs.outputs_manifests : [];
  const outputManifests = [];
  for (const relativePath of manifestPaths) {
    const loaded = await optionalJson(root, relativePath, `output manifest ${relativePath}`);
    if (loaded.status === "present") {
      outputManifests.push({ path: loaded.path, ...loaded.value });
    }
  }

  const previewsAllowed = (manifest.policy?.payload_previews ?? "off") === "lab_nonsecret";
  const iceberg = normalizeIceberg(icebergFile.value, icebergFile.status);

  await validateEvidenceVerdicts(root, manifest);

  return {
    schema_version: SCHEMA_VERSION,
    root,
    manifest,
    previewsAllowed,
    layers: {
      producer_ledger: layerStatus(producerLedger),
      messages: layerStatus(messages),
      console_readback: layerStatus(consoleReadback),
      scribe_logs: Object.keys(scribeLogs).length
        ? Object.values(scribeLogs).every((entry) => entry.status === "present") ? "present" : "partial"
        : "not_supplied",
      object_inventory: layerStatus(objectInventory),
      outputs_register: layerStatus(register),
      outputs_manifests: outputManifests.length ? "present" : "not_supplied",
      parquet_summary: layerStatus(parquetSummary),
      iceberg: layerStatus(icebergFile),
      holylog_oracle: layerStatus(holylogOracle),
      authority_observation: layerStatus(authorityObservation)
    },
    producerLedger: producerLedger.rows,
    producerLedgerPath: producerLedger.path,
    messages: messages.rows.map((row) => ({
      ...row,
      preview: previewsAllowed && row.preview_allowed === true ? row.preview ?? null : null
    })),
    messagesPath: messages.path,
    consoleReadback: consoleReadback.rows,
    consoleReadbackPath: consoleReadback.path,
    scribeLogs,
    objects: objectInventory.value,
    objectsPath: objectInventory.path,
    register: register.value,
    registerPath: register.path,
    outputManifests,
    parquetSummary: parquetSummary.value,
    parquetSummaryPath: parquetSummary.path,
    iceberg,
    icebergPath: icebergFile.path,
    holylogOracle: holylogOracle.value,
    authorityObservation: authorityObservation.value,
    pathRefs
  };
}

/** Build the cockpit snapshot + explorer payload from a loaded bundle. */
export function bundleToSnapshot(bundle) {
  const { manifest } = bundle;
  const producers = summarizeProducers(bundle.producerLedger);
  const scribes = summarizeScribes(bundle);
  const consumers = summarizeConsumers(bundle);
  const objectStores = summarizeStores(bundle);
  const events = summarizeEvents(bundle);

  return {
    schemaVersion: 1,
    mode: "run-bundle",
    title: `Run bundle · ${manifest.run_id}`,
    runId: manifest.run_id,
    observedAt: manifest.collected_at,
    capabilities: [],
    scribes,
    producers,
    consumers,
    objectStores,
    events,
    evidence: manifest.verdicts.map((item) => ({
      label: item.label,
      verdict: mapVerdict(item.verdict),
      source: item.source
    })),
    explorer: {
      schema_version: SCHEMA_VERSION,
      run_id: manifest.run_id,
      collected_at: manifest.collected_at,
      revisions: manifest.revisions,
      scope: manifest.scope,
      policy: {
        payload_previews: bundle.previewsAllowed ? "lab_nonsecret" : "off"
      },
      layers: bundle.layers,
      messages: bundle.messages,
      console_readback: {
        status: bundle.layers.console_readback === "present" ? "present" : "not_supplied",
        path: bundle.consoleReadbackPath,
        rows: bundle.consoleReadback
      },
      producer_ledger: bundle.producerLedger,
      scribe_timelines: Object.fromEntries(
        Object.entries(bundle.scribeLogs).map(([id, entry]) => [
          id,
          { status: entry.status, path: entry.path, rows: entry.rows }
        ])
      ),
      objects: bundle.objects
        ? {
            label: "inventory observation",
            note: "Object inventory is an observation, not a committed-history index.",
            provider: bundle.objects.provider ?? "unknown",
            prefixes: bundle.objects.prefixes ?? [],
            objects: bundle.objects.objects ?? []
          }
        : { status: "not_supplied" },
      outputs: {
        register: bundle.register
          ? { status: "present", path: bundle.registerPath, ...bundle.register }
          : { status: "not_supplied" },
        manifests: bundle.outputManifests,
        parquet_summary: bundle.parquetSummary
          ? { status: "present", path: bundle.parquetSummaryPath, ...bundle.parquetSummary }
          : { status: "not_supplied" },
        iceberg: bundle.iceberg
      },
      matrix: buildMatrix(bundle),
      cross_links: buildCrossLinks(bundle)
    }
  };
}

function mapVerdict(verdict) {
  if (verdict === "pass") return "oracle_pass";
  if (verdict === "fail") return "oracle_fail";
  if (verdict === "inconclusive") return "checker_inconclusive";
  return verdict;
}

function summarizeProducers(rows) {
  const map = new Map();
  for (const row of rows) {
    if (row.row_type !== "send") continue;
    const id = `${row.producer_id ?? "unknown"} → ${row.verse ?? "unknown"}`;
    const previous = map.get(id) ?? {
      id,
      kind: "run-bundle producer ledger",
      state: "observed",
      sequence: -1,
      ack: "unknown",
      source: "producer-ledger.jsonl"
    };
    previous.sequence = Math.max(previous.sequence, Number.isFinite(row.seq) ? row.seq : -1);
    previous.ack = String(row.ack_status ?? "unknown");
    previous.state = row.unacked ? "retrying_or_unacked" : "committed";
    map.set(id, previous);
  }
  return [...map.values()];
}

function summarizeScribes(bundle) {
  const ids = Object.keys(bundle.scribeLogs);
  if (!ids.length) return [];
  return ids.map((id) => {
    const rows = bundle.scribeLogs[id].rows;
    const available = [...rows].reverse().find((row) =>
      row.event_kind === "available" || row.event_kind === "recovered" || row.event_kind === "serving"
    );
    const down = [...rows].reverse().find((row) => row.event_kind === "down" || row.event_kind === "denied");
    const last = rows[rows.length - 1];
    // Process logs are observations, not Serving Authority evidence. Keep the
    // reported posture for drill-down, but never turn it into the overview's
    // canonical/effective-writer state.
    const reportedPosture = last?.posture
      ?? (down && (!available || String(down.at) > String(available.at)) ? "down" : available ? "available" : "unknown");
    return {
      id,
      node: last?.node ?? "fixture",
      verse: last?.verse ?? bundle.manifest.scope.namespace,
      posture: "observed",
      reported_posture: reportedPosture,
      reachable: reportedPosture !== "down",
      route: last?.route ?? "bundle://scribe",
      term: last?.term ?? 0,
      source: bundle.scribeLogs[id].path ?? `scribes/${id}.jsonl`
    };
  });
}

function summarizeConsumers(bundle) {
  if (!bundle.register) {
    return [{
      id: "consumer-register",
      state: "not_supplied",
      frontier: "—",
      output: "register not supplied",
      source: "not supplied"
    }];
  }
  const canonical = bundle.outputManifests.find((item) => item.canonical === true);
  return [{
    id: bundle.register.workload_id ?? "consumer-register",
    // The initial bundle contract carries manifest observations. Canonical
    // chain validation belongs to the materializer/vertical verifier, not an
    // arbitrary boolean in a presentation bundle.
    state: "observed",
    frontier: bundle.register.frontier ?? "—",
    output: canonical ? `declared epoch ${canonical.binding_epoch} manifest` : "manifest observation present",
    source: bundle.registerPath ?? "outputs/register.json"
  }];
}

function summarizeStores(bundle) {
  if (!bundle.objects) {
    return [{
      id: "object-inventory",
      provider: "—",
      state: "not_supplied",
      objects: 0,
      source: "not supplied"
    }];
  }
  const count = Array.isArray(bundle.objects.objects) ? bundle.objects.objects.length : 0;
  return [{
    id: "inventory-observation",
    provider: bundle.objects.provider ?? "unknown",
    state: "observed",
    objects: count,
    source: bundle.objectsPath ?? "objects.json"
  }];
}

function summarizeEvents(bundle) {
  const events = [];
  for (const row of bundle.producerLedger) {
    if (row.row_type === "failover") {
      events.push({
        at: row.at ?? "ledger",
        kind: "observed",
        text: row.message ?? `Producer connect-chain ${row.from_endpoint}→${row.to_endpoint}`
      });
    }
  }
  for (const [id, entry] of Object.entries(bundle.scribeLogs)) {
    for (const row of entry.rows) {
      if (row.event_kind === "recovered" || row.event_kind === "down" || row.level === "error") {
        events.push({
          at: (row.at ?? "").slice(11, 19) || id,
          kind: "observed",
          text: `[Scribe log ${id}] ${row.message ?? row.event_kind}`
        });
      }
    }
  }
  events.push({
    at: "bundle",
    kind: "observed",
    text: `Loaded run-bundle-v1; Iceberg state=${bundle.iceberg.state}; holylog oracle=${bundle.layers.holylog_oracle}.`
  });
  return events.slice(0, 32);
}

function buildMatrix(bundle) {
  const rows = bundle.manifest.verdicts.map((item) => ({
    claim: item.label,
    verdict: item.verdict,
    source: item.source,
    layer: guessLayer(item.source)
  }));
  rows.push({
    claim: "Iceberg table presence",
    verdict: bundle.iceberg.state === "verified"
      ? "pass"
      : bundle.iceberg.state === "absent"
        ? "observed"
        : "not_run",
    source: bundle.icebergPath ?? "not supplied",
    layer: "iceberg",
    detail: bundle.iceberg.detail
  });
  rows.push({
    claim: "Holylog oracle",
    verdict: bundle.layers.holylog_oracle === "present" ? "observed" : "not_run",
    source: bundle.manifest.inputs.holylog_oracle ?? "not supplied",
    layer: "holylog_oracle"
  });
  return rows;
}

function guessLayer(source) {
  if (!source || source === "not supplied") return "missing";
  if (source.includes("producer")) return "producer";
  if (source.includes("scribe")) return "scribe";
  if (source.includes("object")) return "objects";
  if (source.includes("parquet")) return "parquet";
  if (source.includes("iceberg")) return "iceberg";
  if (source.includes("manifest") || source.includes("register")) return "consumer";
  return "bundle";
}

function buildCrossLinks(bundle) {
  const byDigest = new Map();
  for (const row of bundle.messages) {
    if (!row.digest) continue;
    byDigest.set(row.digest, { message: row, producer: null, readback: null, output: null });
  }
  for (const row of bundle.producerLedger) {
    if (row.row_type !== "send" || !row.payload_digest) continue;
    const entry = byDigest.get(row.payload_digest) ?? {
      message: null, producer: null, readback: null, output: null
    };
    entry.producer = row;
    byDigest.set(row.payload_digest, entry);
  }
  for (const row of bundle.consoleReadback) {
    if (!row.digest) continue;
    const entry = byDigest.get(row.digest) ?? {
      message: null, producer: null, readback: null, output: null
    };
    entry.readback = row;
    byDigest.set(row.digest, entry);
  }
  const digests = bundle.parquetSummary?.source_digests;
  if (Array.isArray(digests)) {
    for (const digest of digests) {
      const entry = byDigest.get(digest);
      if (entry) entry.output = { path: bundle.parquetSummaryPath, digest };
    }
  }
  return [...byDigest.entries()]
    .filter(([, link]) => link.message && link.producer)
    .map(([digest, link]) => ({
      digest,
      message: link.message,
      producer: link.producer,
      readback: link.readback,
      output: link.output,
      complete: Boolean(link.message && link.producer && link.output)
    }));
}

/** Validate/copy explicitly provided local artifacts into a new bundle directory. No live scrape. */
export async function collectLocalArtifacts({ outDir, manifest, files }) {
  const { mkdir, copyFile, writeFile } = await import("node:fs/promises");
  const root = resolve(outDir);
  await mkdir(root, { recursive: true });
  const validated = validateManifest(structuredClone(manifest));
  for (const [relativePath, sourcePath] of Object.entries(files)) {
    const dest = boundedPath(root, relativePath, "collect dest").absolute;
    await mkdir(dirname(dest), { recursive: true });
    const absSource = resolve(sourcePath);
    const text = await readBoundedFile(absSource, `collect source ${relativePath}`);
    secretScan(text, relativePath);
    await writeFile(dest, text);
  }
  await writeFile(join(root, "manifest.json"), `${JSON.stringify(validated, null, 2)}\n`);
  return loadRunBundle(root);
}

export async function listBundleFiles(root) {
  const entries = await readdir(resolve(root), { withFileTypes: true, recursive: true });
  return entries.filter((entry) => entry.isFile()).map((entry) => join(entry.parentPath ?? entry.path, entry.name));
}
