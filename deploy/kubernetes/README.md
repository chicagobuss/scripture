# Generic Kubernetes examples for Scripture (product v0 deployment interface).
#
# These manifests contain no personal hostnames, ZeroTier addresses, real bucket
# names, credential values, or ownership grants from scheduling.
#
# Prerequisites (operator-supplied, outside this directory):
# - Namespace
# - Secret `scripture-store-credentials` with env vars from the serve contract:
#   R2_* or RUSTFS_*/AWS_*
# - Image `scripture:<version>` available to the cluster
# - REPLACE_WITH_* placeholders filled in the ConfigMaps
# - Existing Canon evidence for the configured Verse (see greenfield note)
#
# Ownership is never granted by Deployment/Pod/Lease/readiness. Canon
# disposition is reported by the process. Standby must not self-promote.
#
# No automatic failover, restart fencing, public producer protocol, or
# Decision 0012 recovery is claimed here.

## Greenfield bootstrap

1. Job (`job-bootstrap.yaml`): one-shot `scripture bootstrap --loglet-id …`
   (`restartPolicy: Never`, `backoffLimit: 0`).
2. Owner/standby Deployments use plain `scripture serve --config …` and omit
   bootstrap. They require existing Canon evidence.
3. After the bootstrap Job exits, Holylog forbids soft-sequencer reattach to the
   open generation. Ordinary owner `serve` may therefore observe
   `RecoveryRequired` until an **accepted** seal-and-replace / first-Serving
   decision exists. Do not smuggle recovery through Deployment args.

A fresh apply of only owner/standby Deployments without bootstrap does **not**
become Serving.


## Files

- `Dockerfile` — versioned image for the `scripture` binary
- `job-bootstrap.yaml` — one-shot greenfield Canon Job
- `configmap-owner.yaml` / `configmap-standby.yaml` — non-secret YAML
- `deployment-owner.yaml` / `deployment-standby.yaml` — independent candidates
- `service-owner.yaml` — selects ready/Serving owner Role only (never standby)

## Probes

| Path | Meaning |
|---|---|
| `/livez` | process/event-loop alive |
| `/readyz` | HTTP 200 only when disposition is Serving |
| `/status` | Canon disposition report |

The owner Service uses a readiness gate so Pods that are Standby or
RecoveryRequired are not endpoints, even if labeled `owner`.

## Image

```sh
DOCKER_BUILDKIT=1 docker build --ssh default \
  -f deploy/kubernetes/Dockerfile \
  -t scripture:0.1.0 \
  .
```

Home-fleet overlays, image import, and secret values live in the ignored local
operator area described by [`config/README.md`](../../config/README.md) — not
here. Tracker records the redacted source of intent and operational evidence.
