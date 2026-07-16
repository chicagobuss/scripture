# Kubernetes Serving Authority register

This directory defines the Kubernetes storage surface for Scripture's
`ServingAuthorityStore`. It is a namespaced conditional register, not
leadership, health, readiness, a lease, a route directory, or Journal
Foundation authority.

`ServingAuthority.spec.record` is the only authoritative value: base64 of the
bounded, canonical `ServingAuthorityRecord` codec. `spec.display` exists only
for `kubectl get` output. The adapter must decode `record`, derive the display
values itself, and fail closed when any supplied display copy disagrees.

The CRD intentionally does not expose a status subresource. A controller,
Pod, Service, or status writer must not be able to reinterpret authority.

## Why a dedicated CRD

| Rejected substrate | Why it is not Serving Authority |
| --- | --- |
| Lease | TTL/heartbeat leadership, not an opaque equality CAS register for a typed record |
| ConfigMap | Shared mutable config stampede; tools rewrite it; no typed authority schema |
| Pod / Service status | Scheduling and readiness are not writer grants |
| Deployment annotations | Restart/rollout metadata; not linearizable authority |
| Direct etcd | Bypasses RBAC/API attestation and couples Scripture to cluster internals |

Kubernetes here is only the durable backend for Scripture's backend-neutral
`ServingAuthorityStore` contract (`GET` + create / exact `resourceVersion`
replace). Holylog Journal Foundation remains on its own register path.

## Conditional-register mapping

| Scripture operation | Kubernetes operation |
| --- | --- |
| `observe` | current `GET` by deterministic object name; never a watch-cache authority decision |
| `CAS(None, record)` | `CREATE`; HTTP 409 is `Conflict` |
| `CAS(Some(resourceVersion), record)` | full `PUT`/replace with that exact `metadata.resourceVersion`; HTTP 409 is `Conflict` |
| connection loss after dispatch | `Indeterminate`, followed only by coordinator re-observation/reconciliation |

`resourceVersion` remains an opaque equality token. The adapter must not parse,
order, or synthesize it. A watch can later speed observation, but cannot
authorize a transition.

Object names are derived as `sa-` + hex of a domain-separated BLAKE3 digest of
the AuthorityKey's fixed journal∥verse bytes (`scripture-k8s-authority`).

## RBAC and example mounts

`rbac.yaml` is namespace-scoped and grants only the custom-resource verbs needed
by the register. For a one-authority deployment, render an additional
`resourceNames` restriction from the configured deterministic authority object
name before applying it.

`serviceaccount.yaml` is an example ServiceAccount plus a portable ConfigMap
shape. Personal endpoints, kube contexts, R2 credentials, and authority object
names do not belong in this base.

## Local k0s attestation (opt-in)

Register-only attestation requires explicit Joshua approval of the exact
commands. It may apply CRD/RBAC into an isolated namespace, exercise
create/GET/stale-CAS/conflict for one object, then delete that namespace. It
must not contact R2, create Scripture writers, start producer load, or claim HA.
