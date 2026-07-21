#!/usr/bin/env node
/**
 * Local-only Scripture Operations Cockpit.
 *
 * The server has no dependency on the Scribe runtime. In live mode an
 * operator-provided adapter is the sole capability boundary: `status` returns
 * one JSON snapshot and `action NAME` accepts a fixed action token. Browser
 * input is never interpolated into a shell command.
 */
import { createReadStream, existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { basename, dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const root = dirname(fileURLToPath(import.meta.url));
const publicRoot = join(root, "public");
const port = Number.parseInt(process.env.SCRIPTURE_OPS_PORT ?? "7777", 10);
const bind = process.env.SCRIPTURE_OPS_BIND ?? "127.0.0.1";
const adapter = process.env.SCRIPTURE_OPS_ADAPTER ? resolve(process.env.SCRIPTURE_OPS_ADAPTER) : null;
const allowedActions = new Set([
  "refresh",
  "produce",
  "pause-producer",
  "resume-producer",
  "kill-scribe-a",
  "restart-scribe-a",
  "promote-scribe-b",
  "cut-store-b",
  "restore-store-b",
  "cleanup"
]);
const subscribers = new Set();
let cachedState = null;

function json(response, status, value) {
  response.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
  response.end(JSON.stringify(value));
}

function sse(response, event, value) {
  response.write(`event: ${event}\ndata: ${JSON.stringify(value)}\n\n`);
}

function validState(value) {
  return value && typeof value === "object" && typeof value.mode === "string"
    && Array.isArray(value.scribes) && Array.isArray(value.producers)
    && Array.isArray(value.consumers) && Array.isArray(value.objectStores)
    && Array.isArray(value.events) && Array.isArray(value.evidence);
}

function adapterState(argumentsList, timeoutMs = 5_000) {
  return new Promise((resolvePromise, rejectPromise) => {
    if (!adapter) {
      rejectPromise(new Error("no local operations adapter configured"));
      return;
    }
    const child = spawn(adapter, argumentsList, { shell: false, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    const limit = 1_000_000;
    const timeout = setTimeout(() => child.kill("SIGTERM"), timeoutMs);
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      if (stdout.length > limit) child.kill("SIGTERM");
    });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", rejectPromise);
    child.on("close", (code) => {
      clearTimeout(timeout);
      if (code !== 0) {
        rejectPromise(new Error(`adapter exited ${code}: ${stderr.trim().slice(0, 512)}`));
        return;
      }
      try {
        const parsed = JSON.parse(stdout);
        if (!validState(parsed)) throw new Error("adapter did not return a valid operations snapshot");
        resolvePromise(parsed);
      } catch (error) {
        rejectPromise(error);
      }
    });
  });
}

async function fixtureState() {
  return JSON.parse(await readFile(join(root, "fixture-state.json"), "utf8"));
}

async function state() {
  try {
    cachedState = adapter ? await adapterState(["status"]) : await fixtureState();
  } catch (error) {
    if (cachedState) {
      cachedState = {
        ...cachedState,
        adapterError: String(error.message ?? error),
        observedAt: new Date().toISOString()
      };
    } else {
      throw error;
    }
  }
  return cachedState;
}

function broadcast(snapshot) {
  for (const response of subscribers) sse(response, "state", snapshot);
}

const mime = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml"
};

const server = createServer(async (request, response) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
  if (url.pathname === "/api/state" && request.method === "GET") {
    try { json(response, 200, await state()); } catch (error) { json(response, 503, { error: String(error.message ?? error) }); }
    return;
  }
  if (url.pathname === "/api/events" && request.method === "GET") {
    response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    response.write(": connected\n\n");
    subscribers.add(response);
    try { sse(response, "state", await state()); } catch (error) { sse(response, "error", { error: String(error.message ?? error) }); }
    request.on("close", () => subscribers.delete(response));
    return;
  }
  if (url.pathname.startsWith("/api/action/") && request.method === "POST") {
    const action = decodeURIComponent(url.pathname.slice("/api/action/".length));
    if (!allowedActions.has(action)) { json(response, 404, { error: "unknown fixed cockpit action" }); return; }
    if (!adapter) { json(response, 409, { error: "read-only fixture mode; configure SCRIPTURE_OPS_ADAPTER for actions" }); return; }
    try {
      const snapshot = await adapterState(["action", action], 20_000);
      cachedState = snapshot;
      broadcast(snapshot);
      json(response, 200, snapshot);
    } catch (error) {
      json(response, 409, { error: String(error.message ?? error) });
    }
    return;
  }
  if (request.method !== "GET") { json(response, 405, { error: "method not allowed" }); return; }
  const requested = url.pathname === "/" ? "index.html" : basename(url.pathname);
  const path = join(publicRoot, requested);
  if (!path.startsWith(publicRoot) || !existsSync(path)) { response.writeHead(404); response.end("not found"); return; }
  response.writeHead(200, { "content-type": mime[extname(path)] ?? "application/octet-stream", "cache-control": "no-store" });
  createReadStream(path).pipe(response);
});

setInterval(async () => {
  try { broadcast(await state()); } catch { /* individual GET exposes adapter failure */ }
}, 2_000).unref();

server.listen(port, bind, () => {
  console.log(`Scripture Operations Cockpit: http://${bind}:${port} (${adapter ? `adapter ${adapter}` : "read-only fixture"})`);
});
