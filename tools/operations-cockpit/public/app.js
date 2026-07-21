const el = (id) => document.getElementById(id);
const actions = [
  ["produce", "Send batch"], ["pause-producer", "Pause producers"], ["resume-producer", "Resume producers"],
  ["kill-scribe-a", "Stop Scribe A"], ["restart-scribe-a", "Restart A"], ["promote-scribe-b", "Promote B"],
  ["cut-store-b", "Cut S3 path"], ["restore-store-b", "Restore S3"], ["cleanup", "Reset run"]
];
const verdictLabel = (value) => value.replaceAll("_", " ");
const escapeHtml = (value) => String(value).replace(/[&<>"']/g, (c) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" })[c]);

function row(title, detail, state) { return `<div class="row"><div><strong>${escapeHtml(title)}</strong><small>${escapeHtml(detail)}</small></div><span class="state ${escapeHtml(state)}">${escapeHtml(state.replaceAll("_", " "))}</span></div>`; }
function empty(label) { return `<div class="empty">${escapeHtml(label)}</div>`; }
function render(state) {
  el("title").textContent = state.title;
  el("subtitle").textContent = `${state.runId} · ${state.mode === "fixture" ? "read-only fixture — no live claim" : `${state.mode} adapter`}`;
  el("mode").textContent = state.mode;
  el("mode").className = `pill ${state.mode}`;
  el("observed").textContent = new Date(state.observedAt).toLocaleTimeString();
  el("route-note").textContent = state.adapterError ? `adapter stale: ${state.adapterError}` : "route discovery ≠ authority";
  const topology = el("topology"); topology.replaceChildren();
  if (!state.scribes.length) topology.innerHTML = empty("No Scribe topology was supplied by this evidence source.");
  for (const scribe of state.scribes) {
    const node = el("div"); node.className = `scribe ${scribe.posture}`;
    node.innerHTML = `<div class="scribe-top"><strong>${escapeHtml(scribe.id)}</strong><span class="state ${escapeHtml(scribe.posture)}">${escapeHtml(scribe.posture)}</span></div><small>${escapeHtml(scribe.node)} · ${escapeHtml(scribe.verse)}</small><code>${escapeHtml(scribe.route)}</code><div>writer term ${escapeHtml(scribe.term)} · ${scribe.reachable ? "reachable" : "unreachable"}</div>`;
    topology.append(node);
  }
  el("evidence-count").textContent = `${state.evidence.length} sources`;
  el("evidence-list").innerHTML = state.evidence.map((item) => `<div class="evidence-row"><span class="verdict ${escapeHtml(item.verdict)}">${escapeHtml(verdictLabel(item.verdict))}</span><div><strong>${escapeHtml(item.label)}</strong><small>${escapeHtml(item.source)}</small></div></div>`).join("");
  const serving = state.scribes.filter((scribe) => scribe.posture === "serving").length;
  const committed = state.producers.filter((producer) => producer.ack === "committed").length;
  el("metrics").innerHTML = [["Serving scribes", serving, "effective writer only"], ["Producers", state.producers.length, `${committed} committed profile`], ["Consumers", state.consumers.length, "independent checkpoints"], ["Stores", state.objectStores.length, "prefix-scoped evidence"]].map(([label, value, note]) => `<article class="metric"><span>${label}</span><strong>${value}</strong><small>${note}</small></article>`).join("");
  el("producers").innerHTML = state.producers.length ? state.producers.map((producer) => row(producer.id, `${producer.kind} · seq ${producer.sequence} · ${producer.ack}`, producer.state)).join("") : empty("No producer observations.");
  el("consumers").innerHTML = state.consumers.length ? state.consumers.map((consumer) => row(consumer.id, `${consumer.output} · frontier ${consumer.frontier}`, consumer.state)).join("") : empty("No consumer evidence was supplied.");
  el("stores").innerHTML = state.objectStores.length ? state.objectStores.map((store) => row(store.id, `${store.provider} · ${store.objects} objects`, store.state)).join("") : empty("No object-store evidence was supplied.");
  const capabilities = new Set(state.capabilities);
  el("control-notice").textContent = capabilities.size ? "Actions are fixed adapter tokens. The browser cannot run arbitrary commands." : "Fixture mode has no mutation authority. Use npm run demo for a safe simulated control loop.";
  el("controls").replaceChildren(...actions.map(([action, label]) => {
    const button = document.createElement("button"); button.textContent = label; button.disabled = !capabilities.has(action); button.className = action.includes("kill") || action.includes("cut") || action === "cleanup" ? "danger" : "";
    button.onclick = () => invoke(action, button); return button;
  }));
  el("events").innerHTML = state.events.map((event) => `<li><time>${escapeHtml(event.at)}</time><span class="verdict ${escapeHtml(event.kind)}">${escapeHtml(verdictLabel(event.kind))}</span><p>${escapeHtml(event.text)}</p></li>`).join("");
}
async function invoke(action, button) {
  button.disabled = true; const previous = button.textContent; button.textContent = "Working…";
  try { const response = await fetch(`/api/action/${encodeURIComponent(action)}`, { method: "POST" }); const body = await response.json(); if (!response.ok) throw new Error(body.error); render(body); }
  catch (error) { alert(`Operation refused: ${error.message}`); }
  finally { button.textContent = previous; }
}
async function refresh() { const response = await fetch("/api/state"); if (!response.ok) throw new Error("cockpit state unavailable"); render(await response.json()); }
const stream = new EventSource("/api/events"); stream.addEventListener("state", (event) => render(JSON.parse(event.data))); stream.addEventListener("error", () => { el("subtitle").textContent = "Live event stream reconnecting…"; });
refresh().catch((error) => { el("title").textContent = "Cockpit unavailable"; el("subtitle").textContent = error.message; });
