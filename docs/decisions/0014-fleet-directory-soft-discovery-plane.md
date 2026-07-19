# 0014 — Fleet directory: a soft discovery plane in object storage

## Status

Proposed, with a working implementation and a failure-mode test.

## Problem

A client that wants to write to a `(Canon, Verse)` must find a Scribe to talk
to. Today it cannot discover one.

The authoritative root register names exactly one endpoint — the current
writer's route hint — and it is the only durable record a Scribe publishes.
Observed on a live two-Scribe deployment, the entire durable state under a
Verse prefix is:

```
.../loglets/g0-t1-.../claim
.../virtual-log%2Fverse%2Fregister-pointer
```

Nothing records that a second Scribe exists. A standby does not open its
ingress listener at all, so it is not discoverable by probing either. This
produces three concrete gaps:

1. **Bootstrap.** A new client has no way to find any Scribe except operator
   configuration naming one.
2. **Liveness.** A client holding a route to a Scribe that has died has no
   source for an alternative. The root still names the dead owner until some
   candidate completes a recovery.
3. **Evidence for capability disclosure.** The durability/availability
   capability report must state `serving candidates for Verse X: N`, and its
   loudness rule forbids inferring a capability from configuration or a
   replica count. There is currently no cluster-wide evidence source for that
   number; a node knows only its own YAML.

## Decision

Add a **fleet directory**: a soft, self-published, TTL'd discovery plane in
object storage, strictly separate from the authoritative root.

The directory answers "which Scribes exist and which endpoints are worth
trying." It never answers "who may commit." That question keeps exactly one
answer — the conditional root register.

### Naming

This is deliberately **not** called membership. Holylog already uses
`observe_membership` / `cached_membership` for the *generation chain* — which
loglets constitute a virtual log. Reusing the word for node discovery would
collide with an existing, unrelated, load-bearing concept.

### Layout

```
<prefix>/directory/nodes/<owner_id_hex>.json
```

One object per node, keyed by owner id. Each node writes **only its own key**.

This layout is chosen so that publication needs no coordination: there is no
shared object to contend on, so no CAS, no retry loop, and no possibility of
two nodes racing. Listing the prefix yields the roster. A single roster object
would have required either multi-writer CAS on a hot key or a merge protocol,
and would have made a heartbeat capable of failing.

### Record

```json
{
  "format_version": 1,
  "owner_id": "scripture-own-a!",
  "node_advertise": "tcp://127.0.0.1:9201",
  "published_at_ms": 1784451322000,
  "valid_for_ms": 15000,
  "assignments": [
    {
      "canon": "telemetry-cnon!!",
      "verse": "telemetry-host-a",
      "advertise": "tcp://127.0.0.1:9201",
      "posture": "bootstrap-if-empty",
      "disposition": "Serving",
      "admits_committed_acks": true
    }
  ]
}
```

### Rules

1. **The directory is never consulted for authority.** It produces endpoints
   to *try*. Admission is decided by the serving Scribe's authority gate,
   which re-reads the root. A directory entry claiming `Serving` is a hint
   that may be stale, and a client must handle being wrong.
2. **Freshness is advisory.** `published_at_ms + valid_for_ms` yields an
   expiry. An expired record means "probably down" — never "is down." A
   partitioned-but-healthy node has a stale record and is still lawfully
   serving.
3. **No CAS, last-write-wins per key.** Safe because each key has exactly one
   writer. This is the reason the plane can be soft.
4. **Heartbeat interval must be well below the TTL** so ordinary scheduling
   jitter does not expire a live node.
5. **Withdraw on graceful shutdown**, expiry covers the ungraceful case.
6. **`disposition` is a ranking hint only.** Clients should prefer entries
   claiming `Serving` for the target `(canon, verse)`, then fall back to
   trying others, because the claim may be stale in either direction.

### Why not in the root register

The root is CAS'd on the authority-critical path. Folding heartbeats into it
would put a high-frequency, uncoordinated write stream onto the one object
whose version *is* the fence, creating contention on the safety-critical path
and invalidating cached generation state on every beat. It would also
reintroduce the cross-store reconciliation class that the one-record authority
experiment eliminated — two things to keep consistent instead of one.

Keeping them apart preserves the property that matters: losing or corrupting
the entire directory degrades discovery, and cannot produce two writers.

## Consequence for capability disclosure

`serving candidates for Verse X: N` becomes evidence-derived: list the
directory, count unexpired records naming that `(canon, verse)`. The report
must state it as *observed heartbeating candidates*, not as a guaranteed
count, because a live-but-partitioned node is invisible to the reader. That is
an honest capability statement; a count derived from local YAML is not.

## Non-goals

- Not a consensus system, not a failure detector, not a lease.
- Does not grant, transfer, or influence write authority.
- Does not replace the root register's route hint for the current writer.
- Does not implement a durable producer outbox or a public producer protocol.
- Does not make a partitioned node's absence from the roster authoritative.
