const el = (id) => document.getElementById(id);
const actions = [
  ["produce", "Send batch"], ["pause-producer", "Pause producers"], ["resume-producer", "Resume producers"],
  ["kill-scribe-a", "Stop Scribe"], ["restart-scribe-a", "Restart Scribe"],
  ["cut-store-b", "Cut S3 path"], ["restore-store-b", "Restore S3"], ["cleanup", "Reset run"]
];
const verdictLabel = (value) => value.replaceAll("_", " ");
const escapeHtml = (value) => String(value).replace(/[&<>"']/g, (c) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" })[c]);

function row(title, detail, state) {
  return `<div class="row"><div><strong>${escapeHtml(title)}</strong><small>${escapeHtml(detail)}</small></div><span class="state ${escapeHtml(state)}">${escapeHtml(state.replaceAll("_", " "))}</span></div>`;
}
function empty(label) { return `<div class="empty">${escapeHtml(label)}</div>`; }
function matches(value, filter) {
  if (!filter) return true;
  return String(value ?? "").toLowerCase().includes(filter.toLowerCase());
}

function scribeLiveness(scribe) {
  const reported = String(scribe.reported_posture ?? scribe.posture ?? "").toLowerCase();
  if (["down", "sealed", "dead"].includes(reported)) return { state: "down", label: "down" };
  if (!scribe.reachable || ["suspected", "suspected_dead", "unreachable", "unknown"].includes(reported)) {
    return { state: "suspected", label: "suspected" };
  }
  return { state: "online", label: "online" };
}

let lastState = null;

function filterValue(id) {
  return (el(id)?.value ?? "").trim();
}

function renderMessages(explorer) {
  const messages = explorer.messages ?? [];
  const canon = filterValue("filter-canon");
  const verse = filterValue("filter-verse");
  const producer = filterValue("filter-producer");
  const outcome = filterValue("filter-outcome");
  const digest = filterValue("filter-digest");
  const filtered = messages.filter((message) =>
    matches(message.canon, canon)
    && matches(message.verse, verse)
    && matches(message.producer_id, producer)
    && matches(message.outcome ?? message.phase, outcome)
    && matches(message.digest, digest)
  );
  if (!filtered.length) {
    el("messages").innerHTML = empty(messages.length ? "No messages match local filters." : "Message index not supplied.");
    return;
  }
  el("messages").innerHTML = filtered.map((message) => {
    const preview = message.preview
      ? `<details><summary>message details</summary><pre>${escapeHtml(JSON.stringify(message.preview, null, 2))}</pre></details>`
      : `<small>message details not included</small>`;
    return `<div class="row message-row">
      <div>
        <strong>${escapeHtml(message.digest ?? "no-digest")}</strong>
        <small>${escapeHtml(message.at ?? "")} · ${escapeHtml(message.canon ?? "")}/${escapeHtml(message.verse ?? "")} · producer ${escapeHtml(message.producer_id ?? "?")} seq ${escapeHtml(message.seq ?? "—")}</small>
        <small>phase ${escapeHtml(message.phase ?? "—")} · outcome ${escapeHtml(message.outcome ?? "—")} · ${escapeHtml(message.byte_length ?? "?")} bytes${message.offset != null ? ` · offset ${escapeHtml(message.offset)}` : ""}</small>
        ${preview}
      </div>
      <span class="state ${escapeHtml(message.outcome ?? message.phase ?? "observed")}">${escapeHtml((message.outcome ?? message.phase ?? "observed").replaceAll("_", " "))}</span>
    </div>`;
  }).join("");
}

function renderConsoleReadback(explorer) {
  const readback = explorer.console_readback ?? { status: "not_supplied", rows: [] };
  if (readback.status !== "present") {
    el("console-readback").innerHTML = empty("Console readback not supplied.");
    return;
  }
  const rows = readback.rows ?? [];
  if (!rows.length) {
    el("console-readback").innerHTML = empty("Console consumer observed zero records.");
    return;
  }
  const entries = rows.map((row) => Number(row.entry)).filter(Number.isFinite);
  const cursor = entries.length ? Math.max(...entries) + 1 : "—";
  const previewsAllowed = explorer.policy?.payload_previews === "lab_nonsecret";
  el("console-readback").innerHTML = `
    <p class="note">${rows.length} record${rows.length === 1 ? "" : "s"} printed · next entry ${escapeHtml(cursor)} · read-only, no checkpoint.</p>
    ${rows.map((row) => {
      const payload = previewsAllowed && row.payload != null
        ? ` · payload=${escapeHtml(row.payload_encoding ?? "text")}:${escapeHtml(row.payload)}`
        : " · payload not included";
      return `<div class="row"><div><strong>${escapeHtml(row.digest ?? "no-digest")}</strong><small>${escapeHtml(row.canon ?? "?")}/${escapeHtml(row.verse ?? "?")} · entry ${escapeHtml(row.entry ?? "—")} · record ${escapeHtml(row.record_offset ?? "—")} · ${escapeHtml(row.bytes ?? "?")} bytes${payload}</small></div><span class="state observed">printed</span></div>`;
    }).join("")}
  `;
}

function renderScribeTimeline(explorer) {
  const timelines = explorer.scribe_timelines ?? {};
  const ids = Object.keys(timelines);
  const scribeFilter = filterValue("filter-scribe");
  const levelFilter = filterValue("filter-level");
  if (!ids.length) {
    el("scribe-timeline").innerHTML = empty("Scribe logs not supplied.");
    return;
  }
  el("scribe-timeline").innerHTML = ids.map((id) => {
    const entry = timelines[id];
    if (entry.status !== "present") {
      return `<div class="scribe-block"><strong>${escapeHtml(id)}</strong><div class="empty">not supplied</div></div>`;
    }
    if (scribeFilter && !matches(id, scribeFilter)) return "";
    const list = (entry.rows ?? []).filter((row) =>
      !levelFilter || matches(row.level, levelFilter) || matches(row.event_kind, levelFilter)
    );
    if (!list.length) {
      return `<div class="scribe-block"><strong>${escapeHtml(id)}</strong><div class="empty">No rows match filters.</div></div>`;
    }
    return `<div class="scribe-block"><strong>${escapeHtml(id)}</strong><small>${escapeHtml(entry.path ?? "")}</small>${list.map((row) =>
      `<div class="row"><div><strong>${escapeHtml(row.event_kind ?? row.level ?? "log")}</strong><small>${escapeHtml(row.at ?? "")} · ${escapeHtml(row.level ?? "")}</small><p>${escapeHtml(row.message ?? "")}</p></div><span class="state ${escapeHtml(row.posture ?? row.level ?? "observed")}">${escapeHtml((row.posture ?? row.level ?? "observed").replaceAll("_", " "))}</span></div>`
    ).join("")}</div>`;
  }).join("") || empty("No Scribe timelines match filters.");
}

function renderObjects(explorer) {
  const objects = explorer.objects;
  if (!objects || objects.status === "not_supplied") {
    el("object-explorer").innerHTML = empty("Object inventory not supplied.");
    return;
  }
  const list = objects.objects ?? [];
  const total = list.length;
  const byPrefix = new Map();
  for (const item of list) {
    const prefix = item.prefix ?? "(none)";
    byPrefix.set(prefix, (byPrefix.get(prefix) ?? 0) + 1);
  }
  el("object-explorer").innerHTML = `
    <p class="note">${escapeHtml(objects.note ?? "Object inventory — not a log.")}</p>
    <div class="row"><div><strong>${escapeHtml(objects.provider ?? "unknown")}</strong><small>${total} objects across ${byPrefix.size} prefixes</small></div><span class="state observed">inventory</span></div>
    ${[...byPrefix.entries()].map(([prefix, count]) => `<div class="row"><div><strong>${escapeHtml(prefix)}</strong><small>${count} keys</small></div></div>`).join("")}
    ${list.map((item) => `<div class="row"><div><strong>${escapeHtml(item.key)}</strong><small>${escapeHtml(item.size)} B · ${escapeHtml(item.version_or_etag ?? "—")} · ${escapeHtml(item.observed_at ?? "")}${item.note ? ` · ${escapeHtml(item.note)}` : ""}</small></div></div>`).join("")}
  `;
}

function renderConsumerOutput(explorer) {
  const outputs = explorer.outputs ?? {};
  const register = outputs.register ?? { status: "not_supplied" };
  const manifests = outputs.manifests ?? [];
  const parquet = outputs.parquet_summary ?? { status: "not_supplied" };
  const iceberg = outputs.iceberg ?? { state: "not_run" };

  const registerHtml = register.status === "not_supplied"
    ? empty("Consumer register not supplied.")
    : row(
      register.workload_id ?? "register",
      `epoch ${register.binding_epoch} · frontier ${register.frontier} · ${register.last_commit_ref ?? ""}`,
      "observed"
    );

  const manifestsHtml = manifests.length
    ? manifests.map((manifest) => row(
      `epoch ${manifest.binding_epoch}${manifest.canonical ? " · canonical" : ""}${manifest.stale ? " · STALE/noncanonical" : ""}`,
      `${manifest.manifest_digest ?? ""} · rows ${manifest.row_count ?? "—"} · ${(manifest.data_objects ?? []).join(", ")}`,
      manifest.canonical ? "caught_up" : "isolated"
    )).join("")
    : empty("Output manifests not supplied.");

  const parquetHtml = parquet.status === "not_supplied"
    ? empty("Parquet summary not supplied.")
    : row(
      `Parquet summary · epoch ${parquet.binding_epoch ?? "—"}`,
      `${parquet.row_count ?? 0} rows · digests ${(parquet.source_digests ?? []).join(", ")}`,
      "observed"
    );

  let icebergHtml;
  if (iceberg.state === "absent") {
    icebergHtml = `<div class="row iceberg-absent"><div><strong>Iceberg</strong><small>${escapeHtml(iceberg.detail ?? "absent")}</small></div><span class="state not_run">absent</span></div>`;
  } else if (iceberg.state === "verified") {
    icebergHtml = row(`Iceberg ${iceberg.table_ident ?? ""}`, `snapshot ${iceberg.snapshot_id ?? "—"}`, "caught_up");
  } else {
    icebergHtml = row("Iceberg", iceberg.detail ?? iceberg.state, iceberg.state === "configured_not_verified" ? "paused" : "not_run");
  }

  el("consumer-output").innerHTML = `
    <h3>Register frontier</h3>${registerHtml}
    <h3>Manifest chain</h3>${manifestsHtml}
    <h3>Parquet summary</h3>${parquetHtml}
    <h3>Iceberg status</h3>${icebergHtml}
  `;
}

function renderMatrix(explorer) {
  const matrix = explorer.matrix ?? [];
  el("evidence-matrix").innerHTML = matrix.length
    ? `<table class="matrix"><thead><tr><th>Claim</th><th>Verdict</th><th>Source</th></tr></thead><tbody>${
      matrix.map((item) => `<tr><td>${escapeHtml(item.claim)}</td><td><span class="verdict ${escapeHtml(item.verdict)}">${escapeHtml(verdictLabel(item.verdict))}</span></td><td><code>${escapeHtml(item.source)}</code></td></tr>`).join("")
    }</tbody></table>`
    : empty("No run status was collected.");

  const links = explorer.cross_links ?? [];
  el("cross-links").innerHTML = links.length
    ? `<h3>Cross-links (digest match only)</h3>${links.map((link) =>
      `<div class="row"><div><strong>${escapeHtml(link.digest)}</strong><small>message ${link.message ? "✓" : "✗"} · producer ${link.producer ? "✓" : "✗"} · parquet ${link.output ? "✓" : "✗"}${link.complete ? " · complete" : ""}</small></div></div>`
    ).join("")}`
    : "";
}

function renderExplorer(state) {
  const section = el("explorer");
  if (!state.explorer) {
    section.classList.add("hidden");
    return;
  }
  section.classList.remove("hidden");
  const layers = state.explorer.layers ?? {};
  el("explorer-meta").textContent = `${state.explorer.run_id} · ${state.explorer.messages?.length ?? 0} messages · console readback ${layers.console_readback ?? "not supplied"}`;
  renderMessages(state.explorer);
  renderConsoleReadback(state.explorer);
  renderScribeTimeline(state.explorer);
  renderObjects(state.explorer);
  renderConsumerOutput(state.explorer);
  renderMatrix(state.explorer);
}

function render(state) {
  lastState = state;
  el("title").textContent = state.title;
  el("subtitle").textContent = `${state.runId} · ${state.mode === "fixture" ? "read-only fixture — no live claim" : state.mode === "run-bundle" ? "immutable run-bundle-v1 — no credentials, no actions" : `${state.mode} adapter`}`;
  el("mode").textContent = state.mode;
  el("mode").className = `pill ${state.mode}`;
  el("observed").textContent = new Date(state.observedAt).toLocaleTimeString();
  el("route-note").textContent = state.adapterError ? `adapter stale: ${state.adapterError}` : "current fleet routes";
  const topology = el("topology"); topology.replaceChildren();
  if (!state.scribes.length) topology.innerHTML = empty("No Scribe information was collected.");
  for (const scribe of state.scribes) {
    const liveness = scribeLiveness(scribe);
    const node = document.createElement("div"); node.className = `scribe ${liveness.state}`;
    node.innerHTML = `<div class="scribe-top"><strong>${escapeHtml(scribe.id)}</strong><span class="state ${liveness.state}">${liveness.label}</span></div><small>${escapeHtml(scribe.node)} · ${escapeHtml(scribe.verse)}</small><code>${escapeHtml(scribe.route)}</code><div>reported ${escapeHtml(scribe.reported_posture ?? scribe.posture)} · term ${escapeHtml(scribe.term)} · ${scribe.reachable ? "reachable" : "unreachable"}</div>`;
    topology.append(node);
  }
  el("evidence-count").textContent = `${state.evidence.length} sources`;
  el("evidence-list").innerHTML = state.evidence.map((item) => `<div class="evidence-row"><span class="verdict ${escapeHtml(item.verdict)}">${escapeHtml(verdictLabel(item.verdict))}</span><div><strong>${escapeHtml(item.label)}</strong><small>${escapeHtml(item.source)}</small></div></div>`).join("");
  const available = state.scribes.filter((scribe) => scribeLiveness(scribe).state === "online").length;
  const committed = state.producers.filter((producer) => producer.ack === "committed" || String(producer.ack).startsWith("committed:")).length;
  el("metrics").innerHTML = [["Available Scribes", available, "fleet members"], ["Producers", state.producers.length, `${committed} committed`], ["Consumers", state.consumers.length, "current progress"], ["Stores", state.objectStores.length, "current run prefix"]].map(([label, value, note]) => `<article class="metric"><span>${label}</span><strong>${value}</strong><small>${note}</small></article>`).join("");
  el("producers").innerHTML = state.producers.length ? state.producers.map((producer) => row(producer.id, `${producer.kind} · seq ${producer.sequence} · ${producer.ack}`, producer.state)).join("") : empty("No producer activity.");
  el("consumers").innerHTML = state.consumers.length ? state.consumers.map((consumer) => row(consumer.id, `${consumer.output} · frontier ${consumer.frontier}`, consumer.state)).join("") : empty("No consumer information was collected.");
  el("stores").innerHTML = state.objectStores.length ? state.objectStores.map((store) => row(store.id, `${store.provider} · ${store.objects} objects`, store.state)).join("") : empty("No object storage information was collected.");
  const capabilities = new Set(state.capabilities);
  el("control-notice").textContent = capabilities.size
    ? "Actions are fixed adapter tokens. The browser cannot run arbitrary commands."
    : state.mode === "run-bundle"
      ? "Run-bundle mode is read-only: action capability list is empty. No kubectl, aws, rclone, or shell."
      : "Fixture mode has no mutation authority. Use npm run demo for a safe simulated control loop.";
  el("controls").replaceChildren(...actions.map(([action, label]) => {
    const button = document.createElement("button"); button.textContent = label; button.disabled = !capabilities.has(action); button.className = action.includes("kill") || action.includes("cut") || action === "cleanup" ? "danger" : "";
    button.onclick = () => invoke(action, button); return button;
  }));
  el("events").innerHTML = state.events.map((event) => `<li><time>${escapeHtml(event.at)}</time><span class="verdict ${escapeHtml(event.kind)}">${escapeHtml(verdictLabel(event.kind))}</span><p>${escapeHtml(event.text)}</p></li>`).join("");
  renderExplorer(state);
}

async function invoke(action, button) {
  button.disabled = true; const previous = button.textContent; button.textContent = "Working…";
  try { const response = await fetch(`/api/action/${encodeURIComponent(action)}`, { method: "POST" }); const body = await response.json(); if (!response.ok) throw new Error(body.error); render(body); }
  catch (error) { alert(`Operation refused: ${error.message}`); }
  finally { button.textContent = previous; }
}
async function refresh() { const response = await fetch("/api/state"); if (!response.ok) throw new Error("cockpit state unavailable"); render(await response.json()); }

for (const id of ["filter-canon", "filter-verse", "filter-producer", "filter-outcome", "filter-digest", "filter-scribe", "filter-level"]) {
  el(id)?.addEventListener("input", () => { if (lastState?.explorer) renderExplorer(lastState); });
}

const stream = new EventSource("/api/events"); stream.addEventListener("state", (event) => render(JSON.parse(event.data))); stream.addEventListener("error", () => { el("subtitle").textContent = "Live event stream reconnecting…"; });
refresh().catch((error) => { el("title").textContent = "Cockpit unavailable"; el("subtitle").textContent = error.message; });
