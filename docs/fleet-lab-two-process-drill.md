# Fleet-lab two-process recovery and handoff drill

This is a local, non-production drill. It proves serving/standby composition,
bounded raw-lines load, and the soft-sequencer rule: a crashed owner must not
reattach an open Loglet.

## Safety gate

Read Holylog’s production Loglet / soft-sequencer safety gate before changing
runners. Ordinary node startup must not bootstrap, elect, or auto-replace.

## In-process evidence (always runnable)

These cover Parts C–D without RustFS:

```sh
cargo test -p scriptured --lib fleet_lab
cargo test -p scripture-load
```

Expected:

- A bootstraps Serving; B on the same register stays Standby and creates no actor
- After `crash_active_writer`, restart reports `RecoveryRequired`
- `scripture-load` integration test accepts bounded records with `OK` ACKs

## Two-process RustFS drill (opt-in)

Requires Holylog’s local S3 compose project and the `fleet-lab` feature.

### 0. Start RustFS and choose a run id

```sh
docker compose -f ../holylog/deploy/local-s3/compose.yaml up -d --wait
RUN_ID="drill-$(date -u +%Y%m%dT%H%M%SZ)"
# All objects live under fleet-lab/${RUN_ID}/ — never wipe the whole bucket.
```

### 1. Node A — bootstrap and serve

```sh
cargo run -p scriptured --features fleet-lab --bin fleet-lab-node -- \
  --run-id "$RUN_ID" \
  --bind 127.0.0.1:9000 \
  --owner 'fleet-lab-own-a!' \
  --advertise 'tcp://127.0.0.1:9000' \
  --bootstrap \
  --loglet-id "gen-a0"
```

### 2. Node B — standby on the same root

In a second terminal (same `RUN_ID`):

```sh
cargo run -p scriptured --features fleet-lab --bin fleet-lab-node -- \
  --run-id "$RUN_ID" \
  --bind 127.0.0.1:9001 \
  --owner 'fleet-lab-own-b!' \
  --advertise 'tcp://127.0.0.1:9001'
```

B must report standby / non-serving. It must not invent a writer for A’s open generation.

### 3. Load against A

```sh
cargo run -p scripture-load -- \
  --endpoint 127.0.0.1:9000 \
  --connections 4 \
  --record-bytes 256 \
  --duration-secs 10 \
  --max-bytes 4194304 \
  --run-id "$RUN_ID" \
  --backend rustfs \
  --chunk-policy-name fleet-lab-64kib-phase-one
```

Record the summary line (accepted records/bytes, ACK percentiles, errors).

### 4. Controlled handoff pause

1. Stop the producer (Ctrl-C is fine for this milestone; transparent reroute is later).
2. On A, perform an explicit drain/seal/publish to B (operator/control path; not
   automatic on listener shutdown). The in-process supervisor API is
   `VerseNodeSupervisor::drain_seal_publish` / `replace_after_lost_sequencer`.
3. Start or promote B only after Canon publishes B’s ownership.
4. Resume `scripture-load` against B’s bind address with the same `--run-id`.

Crash variant: kill A without handoff. Restarting A without `--bootstrap` must
not serve the open generation as a writer; seal-and-replace is an explicit
operator step that provisions a fresh Loglet id.

### 5. Cleanup (prefix only)

Delete only `fleet-lab/${RUN_ID}/` objects. Do not clear the `holylog-rustfs`
bucket by default.

## Chunk policy and backend naming

Reports must name:

- Backend: `in-memory` (unit/integration) or `rustfs` (this drill)
- Chunk policy: phase-one requires `max_inflight_chunks = 1`; the load tool’s
  default label is `fleet-lab-64kib-phase-one`

Do not promise Gbps targets. The first purpose is request amplification and the
saturation point of phase-one chunks.
