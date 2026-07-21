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

Default live actions are `produce`, pause/resume, stop/restart A, promote B,
cut/restore the named secondary-store path, and cleanup. A real adapter must
enforce its own isolated namespace/prefix and explicit live-run approval.

## Evidence discipline

The UI deliberately distinguishes observed status, oracle verdicts, checker
verdicts, `not_run`, and `incomplete`. It may visualize a campaign, but it may
never turn a dashboard color into an HA or durability claim.
