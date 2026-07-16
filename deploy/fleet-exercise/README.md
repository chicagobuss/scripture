# Fleet exercise harness (lab only)

Direct ZeroTier / SSH process orchestration for Scripture against RustFS or
Cloudflare R2. This is **not** Kubernetes packaging and does **not** claim HA,
automatic failover, restart-safe remote sequencing, or Decision 0012 recovery.

## Layout

| Path | Role |
| --- | --- |
| `inventory/hosts.example.env` | Non-secret host / ZeroTier placeholders |
| `bin/build-release.sh` | Build `fleet-lab-node` + `scripture-load` (native or documented cross) |
| `bin/preflight.sh` | SSH / arch / env-file presence / redacted R2 key checks |
| `bin/run-steady-state.sh` | Owner → standby → two remote loads → collect summaries |
| `bin/run-owner-crash.sh` | Kill -9 owner; assert standby does not promote |
| `bin/collect.sh` | Gather remote summary JSON into `results/<run-id>/` |
| `results/` | Local artifact directory (gitignored contents except `.gitkeep`) |

## Safety

- Secrets live only in a local `--env-file` or process environment. Never pass
  `--access-key` / `--secret-key`. Never copy secret values into Tracker,
  argv, or committed files.
- Cleanup deletes only `scripture-fleet-exercise/<run-id>/`.
- Banner/status/summary fields include `lab: true` and `ha_claim: false`.

## Typical local RustFS path

```sh
docker compose -f ../../holylog/deploy/local-s3/compose.yaml up -d --wait
./bin/build-release.sh
RUN_ID="drill-$(date -u +%Y%m%dT%H%M%SZ)"
# Terminal A
../target/release/fleet-lab-node --backend rustfs --run-id "$RUN_ID" \
  --bind 127.0.0.1:9000 --advertise tcp://127.0.0.1:9000 \
  --bootstrap --loglet-id gen-a0 --status-bind 127.0.0.1:9100 \
  --summary-dir "./results/$RUN_ID"
```

See `docs/fleet-lab-two-process-drill.md` and the Tracker folio guide
`scripture/real-r2-fleet-exercise-guide.md`.

## R2 (operator-authorized only)

```sh
# Provide a local env file with R2_* keys; do not paste values into shells that log history publicly.
./bin/preflight.sh --inventory inventory/hosts.env --env-file "$HOME/.config/scripture/r2.env"
# Joshua (or an authorized operator) then runs:
cargo test -p scriptured --features fleet-lab-r2-smoke --test fleet_lab_r2_smoke -- --ignored --exact
```
