# Scripture Operations Cockpit

Local-only Node UI for showing a Scripture topology, producer/consumer outcomes,
object-store evidence, and tightly bounded operational actions. It is a
development/campaign tool—not Scribe runtime code and not a production control
plane.

## Run a safe interactive demo

Requires Node 20+ and no `npm install`:

```sh
cd tools/operations-cockpit
npm run demo
```

Open <http://127.0.0.1:7777>. The `demo` badge is deliberate: the controls
modify only `/tmp/scripture-operations-cockpit-demo.json`. They do not contact
Kubernetes, a Scribe, R2, S3, or RustFS.

For a static, read-only visual fixture:

```sh
npm start
```

## Run-bundle evidence explorer (`run-bundle-v1`)

Turn one isolated campaign into a **drill-down evidence viewer**. The UI reads
only an immutable local directory; it never receives cloud credentials and never
runs kubectl/aws/rclone/shell.

```sh
cd tools/operations-cockpit
npm run bundle          # fixture run-bundle-v1
npm run test:bundle     # fail-closed schema + fixture smoke
```

Point at a collected bundle:

```sh
SCRIPTURE_OPS_BUNDLE=/absolute/path/to/run-bundle \
  SCRIPTURE_OPS_ADAPTER="$PWD/run-bundle-adapter.mjs" \
  npm start
```

### Bundle contract

Root `manifest.json`:

```json
{
  "schema_version": 1,
  "run_id": "...",
  "collected_at": "RFC3339",
  "revisions": {"scripture": "git SHA", "holylog": "git SHA"},
  "scope": {"namespace": "...", "object_prefixes": ["..."]},
  "policy": {"payload_previews": "off|lab_nonsecret"},
  "inputs": {
    "producer_ledger": "producer-ledger.jsonl",
    "messages": "messages.jsonl",
    "console_readback": "console-readback.jsonl",
    "scribe_logs": ["scribes/scribe-a.jsonl"],
    "object_inventory": "objects.json",
    "outputs_register": "outputs/register.json",
    "outputs_manifests": ["outputs/manifests/*.json"],
    "parquet_summary": "outputs/parquet-summary.json",
    "iceberg": "outputs/iceberg.json"
  },
  "verdicts": [{"label": "...", "verdict": "pass|fail|inconclusive|not_run|observed", "source": "relative/path"}]
}
```

Rules enforced by `lib/run-bundle.mjs`:

- relative paths only; traversal and absolute paths rejected;
- unknown `schema_version`, oversized files, malformed JSONL fail closed;
- every verdict needs a source; `pass` cannot point at a missing file;
- payload previews stay off unless `policy.payload_previews=lab_nonsecret` and
  the message row sets `preview_allowed: true`;
- Iceberg is shown verbatim as `verified|configured_not_verified|absent|not_run`;
- missing layers render as `not_supplied` / `not_run`, never healthy;
- run-bundle mode advertises an empty action capability list.

`console_readback` is optional JSONL emitted by `scripture consume --format jsonl`.
The Cockpit displays it as a read-only observation of records printed by that
command; it is not a checkpoint, acknowledgement, or durability verdict.

Optional local collector stub (copy/validate only — no live scrape):

```sh
node bundle-collect.mjs --out /tmp/new-bundle --manifest ./fixtures/run-bundle-v1/manifest.json \
  --file producer-ledger.jsonl=./fixtures/run-bundle-v1/producer-ledger.jsonl
```

## Show a real telemetry-producer run

`scripture-telemetry-producer` writes an append-only JSONL send ledger. The
included adapter turns that producer-side evidence into a **read-only** cockpit
view; it does not infer Scribe authority, Holylog readback, or object-store
durability from a producer acknowledgement.

```sh
cd tools/operations-cockpit
SCRIPTURE_TELEMETRY_LEDGER=/absolute/path/to/send-ledger.jsonl \
  SCRIPTURE_TELEMETRY_TOPOLOGY=/absolute/path/to/declared-topology.json \
  SCRIPTURE_OPS_ADAPTER="$PWD/telemetry-ledger-adapter.mjs" \
  npm start
```

For a safe visual rehearsal with the same adapter, point
`SCRIPTURE_TELEMETRY_LEDGER` at `telemetry-ledger.fixture.jsonl` and
`SCRIPTURE_TELEMETRY_TOPOLOGY` at `telemetry-topology.fixture.json`. The
topology file is an operator inventory, not evidence that either Scribe has
authority. The adapter accepts only `status`; cockpit action buttons remain
unavailable.

## Live adapter contract

The browser calls only the local Node server. To connect a lab, copy
`live-adapter.example.sh` under ignored `config/local/`, implement a
scenario-owned adapter, and start with:

```sh
SCRIPTURE_OPS_ADAPTER="$PWD/../../config/local/operations-cockpit/control.sh" npm start
```

The adapter gets either `status` or `action FIXED_ACTION` and emits one JSON
snapshot conforming to `fixture-state.json`. It is the only place that may
call a campaign runner, SSH, or Kubernetes. The Node process invokes it with
`shell: false`; action names are allow-listed in `server.mjs`; no browser value
is ever treated as a command.

Default live actions are `produce`, pause/resume, stop/restart a named Scribe,
cut/restore the named secondary-store path, and cleanup. Fleet availability is
observed as an automatic system behavior, never invoked as a promotion action.
A real adapter must enforce its own isolated namespace/prefix and explicit
live-run approval.

## Control one local Scribe

`local-scribe-adapter.mjs` is a concrete local controller for a configured
Scribe on this machine. It has exactly three actions: start/restart the managed
Scribe, stop it, and send a fixed three-record batch. It never accepts a command
or a path from the browser.

Build Scripture once, then start the Cockpit with the same environment that
supplies your object-store credentials:

```sh
cd /path/to/scripture
cargo build -p scripture-cli --bin scripture
set -a; . /path/to/credentials.env; set +a
SCRIPTURE_LOCAL_CONFIG="$PWD/config/local/consumer-e2e-r2.yaml" \
SCRIPTURE_LOCAL_CANON=demo-canon-00001 \
SCRIPTURE_LOCAL_VERSE=demo-verse-00001 \
SCRIPTURE_LOCAL_BINARY="$PWD/target/debug/scripture" \
npm --prefix tools/operations-cockpit run local:control
```

The adapter keeps its PID, event log, and Scribe output under
`/tmp/scripture-operations-cockpit-local` (override with
`SCRIPTURE_LOCAL_STATE_DIR`). The UI’s **Restart Scribe** button starts the
managed process when it is down; **Stop Scribe A** sends that managed process
SIGTERM. This is intentionally limited to the configured local process, not
SSH, Docker, Kubernetes, or remote machines.

For a live, read-only two-Scribe view, supply fixed readiness endpoints:

```sh
SCRIPTURE_LOCAL_FLEET='[
  {"id":"Scribe A","verse":"cockpit-verse-a!","route":"127.0.0.1:19201","readyz":"http://127.0.0.1:19301/readyz"},
  {"id":"Scribe B","verse":"cockpit-verse-b!","route":"127.0.0.1:19202","readyz":"http://127.0.0.1:19302/readyz"}
]' npm run local:fleet
```

## Evidence discipline

The UI deliberately distinguishes observed status, oracle verdicts, checker
verdicts, `not_run`, and `incomplete`. It may visualize a campaign, but it may
never turn a dashboard color into an HA or durability claim. A run bundle does
not itself establish HA or durability; object inventory is not committed
history; Parquet manifests do not prove Iceberg.
