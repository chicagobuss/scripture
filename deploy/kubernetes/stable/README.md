# Scripture stable two-process examples (WP09)

Portable, non-secret Kubernetes examples for a release-grade two-process
Scripture drill. Personal settings, credentials, and image digests live under
the gitignored overlay `config/local/scripture-stable/` — never in this tree.

## Shape

| Resource | Role |
|----------|------|
| Namespace | `scripture-stable-<run-id>` during approved drills |
| Deployment A | `scripture serve` with `startup_role: bootstrap-if-empty` on `node-a` |
| Deployment B | `scripture serve` with `startup_role: standby` on `node-b` |
| Service `scripture-producer` | Selects **only** pods that pass `/readyz` (effective writer) |
| Service `scripture-admin-b` | Dedicated admin path to B’s promote port (not the producer Service) |
| RustFS | Ephemeral dedicated store in the same namespace on `bignlittles` |

## Hard rules

- Distinct `node.owner_id` for A and B.
- Liveness → `/livez`; readiness → `/readyz` (standby must be unready).
- Owner advertise DNS must resolve via per-owner Services `scripture-actor-a` / `scripture-actor-b` (not the producer Service).
- Producer ingress only from pods labeled `scripture.dev/client: producer`; admin ingress only from `scripture.dev/client: admin`.
- Those client labels are **NetworkPolicy selectors, not authentication**. Acceptable for this isolated drill only while the run namespace does not admit untrusted workload creators; stronger admin boundaries need namespace/RBAC separation later.
- No literal Secret data in tracked YAML.
- Release image must be built **without** `campaign-faults`.
- Publishable crates use `publish = ["fleet"]` (never crates.io).
- No live `kubectl apply` until Joshua approves the exact command sequence from the release-drill runner.

## Runner

```bash
./deploy/release/run-release-drill.sh
# default: contract checks + render + kubectl client-dry-run (syntax/render only)
#          → config/local/scripture-stable/runs/<run-id>/
# live: --execute --joshua-approved + APPROVAL file line "APPROVED <run-id>"
#       requires attested RC provenance first; installs cleanup trap; no partial apply mode
```

Four verdict classes are written to `verdicts.json` and never upgraded into each other.

## Overlay

Copy `config/local/scripture-stable/overlay.example.env` to an ignored
`overlay.env`, fill non-secret placement and image digest references, and keep
credential Secret manifests out of Git.
