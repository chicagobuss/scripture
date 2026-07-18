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
- No literal Secret data in tracked YAML.
- Release image must be built **without** `campaign-faults`.
- No live `kubectl apply` until Joshua approves the exact command sequence from the release-drill runner.

## Overlay

Copy `config/local/scripture-stable/overlay.example.env` to an ignored
`overlay.env`, fill non-secret placement and image digest references, and keep
credential Secret manifests out of Git.
