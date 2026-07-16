# Kubernetes correctness-campaign examples
#
# Reusable, reviewable campaign contract for Scripture k0s/RustFS correctness
# scenarios. Contains no personal hostnames, credential values, or ownership
# grants from scheduling.

> **Status: experimental scaffolding.** The initial campaign scenarios still
> need their runtime-originated receipt/denial trace seam before a live RustFS
> execution is approved. A green local checker verdict is not authorization to
> run this Job.
#
# Prerequisites (operator-supplied, outside this directory):
# - Namespaces from `namespaces.yaml` (or equivalents)
# - Secret `scripture-correctness-store` with env vars from the serve contract:
#   `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` (or AWS_* aliases)
# - Image `scripture-campaign:<version>` built from the separate experimental
#   campaign tool and explicitly imported on the target node (see `Dockerfile`)
# - REPLACE_WITH_* placeholders filled by the local runner
# - Existing RustFS Service DNS reachable from the campaign Job
#
# Ownership is never granted by Job/Pod/Lease/readiness. A completed Job or
# green probe is **not** a correctness verdict — only `checker-verdict.json`
# from the shared Holylog checker is.
#
# No automatic failover, operator, Helm chart, CRD beyond existing Serving
# Authority work, public producer protocol, or Decision 0012 recovery is
# claimed here.

## Evidence contract

Every run must produce a redacted artifact bundle under the Job's
`--artifact-dir` (collected by the local runner into
`config/local/correctness-testing/runs/<run_id>/`):

| Path | Meaning |
|---|---|
| `environment.json` | Redacted backend/topology identity (no secrets) |
| `traces/campaign.ndjson` | Shared Holylog correctness vocabulary |
| `observations/final-root.json` | Final Journal Foundation membership |
| `observations/final-authority.json` | Final Serving Authority observation (or null) |
| `checker-verdict.json` | Explicit `pass` / `fail` / `inconclusive` |
| `report.md` | Human-readable summary + non-claims |

Exit codes from `scripture-campaign`:

| Code | Meaning |
|---|---|
| 0 | Checker Pass |
| 1 | Execution failure |
| 2 | Checker Fail |
| 3 | Checker Inconclusive |

## Placement contract (examples)

Node affinity in the Job templates is illustrative. The local runner must fill
concrete hostnames and refuse to label a result a multi-node failure test when
placement cannot be honored.

| Role | Default intent | Why |
|---|---|---|
| RustFS S3 endpoint | distinct worker (e.g. storage node) | real remote S3-compatible semantics |
| Campaign driver Job | distinct from RustFS | process/node separation from the store |
| Checker/collector | distinct from an injected-fault writer when multi-process | evidence survives the fault |

The current `scripture-campaign` entrypoint runs A/B recovery roles **in-process**
inside one Job against a remote RustFS prefix. That validates the real SDK and
recovery path; it is **not** a multi-node process-separation proof. See the
non-claims in `environment.json`.

## Namespaces

| Namespace | Purpose | Lifetime |
|---|---|---|
| `scripture-correctness` | Ephemeral scenario Jobs and temporary configuration | per `run_id` |
| `scripture-load` | Optional producer/consumer Jobs | per `run_id` |
| `scripture-observe` | Optional checker/collection Jobs | per `run_id` |

Use one `run_id` everywhere: Kubernetes labels, object-store prefix, local
artifact directory, and trace bundle. Never recycle a completed run ID.
Never share prefixes with the persistent HA drill.

## Files

- `namespaces.yaml` — campaign-only namespaces
- `serviceaccount.yaml` — ServiceAccount for campaign Jobs
- `rbac.yaml` — narrowly scoped Role/RoleBinding (ConfigMap read in-namespace)
- `configmap-campaign.yaml` — non-secret endpoint/bucket/region template
- `job-campaign.yaml` — one-shot `scripture-campaign` Job
- `Dockerfile` — separate experimental campaign-tool image
- `topology.example.json` — non-secret local-runner topology shape (copy into
  gitignored `config/local/correctness-testing/rustfs/topology.json`)

## Image

```sh
DOCKER_BUILDKIT=1 docker build --ssh default \
  -f deploy/kubernetes/correctness/Dockerfile \
  -t scripture-campaign:0.1.0 \
  .
```

Home-fleet overlays, secret values, concrete node names, and run artifacts live
in the ignored local operator area (`config/local/correctness-testing/`) — not
here.

## Non-claims

- Kubernetes readiness is not authority or correctness evidence.
- Single-process A/B roles inside the campaign Job are not multi-node proof.
- RustFS single-node topology establishes no object-store replica independence,
  provider durability, multi-site availability, or equivalence with R2/S3/GCS.
- Campaign manifests create only correctness/load/observe namespaces; any
  persistent HA drill and its prefix are reference-only and untouched.
