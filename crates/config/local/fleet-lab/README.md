# Local product fleet projection

Personal orchestration for the Scripture **product** daemon (`scripture`), not a
parallel lab server. Git-ignored.

## Deleted

- `fleet-lab-node` / private server crate — removed; do not resurrect.

## Load client

Selected: `crates/scripture-load` — bounded-concurrency raw-lines producer with
committed-only ACK, duration/byte caps, and latency/error summary. It talks TCP
to the temporary product ingress (not a public protocol).

## Paths

- `deploy/fleet-exercise/` — SSH/ZeroTier build, preflight, steady-state, crash
- `deploy/k0s-fleet-lab/` — personal k0s overlays on the product image/Job

## Non-claims

No HA, auto-failover, Decision 0012 recovery, or public producer protocol.
