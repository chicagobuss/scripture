# Scripture HA models

These are deliberately small, executable specifications of product-level HA
contracts. They complement—rather than replace—the Holylog correctness suite.
They must stay above the implementation details of Kubernetes, Consul, object
store SDKs, and socket protocols.

## First scenario: two Scribes recover one Verse

`TwoScribeVerseRecovery.tla` models a single `(Canon, Verse)` with:

- Scribe `A` initially serving and Scribe `B` as an eligible recovery
  candidate;
- three independent clients (`C1`, `C2`, and `C3`), each with one locally
  durable event and a staleable cached route;
- a bounded three-attempt send budget per event, solely to bound the model;
- a fallible liveness observation: A's lease can expire even while A remains
  alive;
- an explicit recovery gap after A is fenced and before B's successor
  authority is published;
- successful writes, denied/time-out writes, committed writes with a lost
  reply, route refresh, and safe outbox reclamation.

`appendSet` records the observed `(event identity, writer, generation, term)`
evidence, rather than every physical byte-level duplicate. Repeated retries in
the same generation are intentionally collapsed to keep exhaustive checking
small; a retry crossing A→B remains visible as distinct generation evidence.

`TwoScribeVerseRecoveryNetwork.tla` is the next refinement. It makes route
snapshots, lease-expiry observations, producer sends, and ACKs explicit packet
objects. Their delivery order is arbitrary; any packet may be dropped, and a
producer may send a retry before an older request is delivered. Its TLC model
uses a two-packet in-flight bound to preserve exhaustive exploration. That is a
model-state-space bound, not a product network capacity.

The first complete network-model run checked 7,627,560 distinct states and
74,470,427 transitions without an invariant failure. It uses TLC symmetry
reduction for the three interchangeable clients and four checker workers.

The core algorithm in prose is:

```text
if control plane considers A absent:
    B may begin recovery, which fences A and creates a no-writer gap

if B completes the lawful Holylog/Scripture transition:
    B becomes the sole authoritative writer for the successor generation

on client send failure, stale route, or lost reply:
    retain the locally durable event; refresh route or observe prior commit;
    never reclaim it merely because a request timed out
```

Important: the lease is only an *attempt permission*. `PublishBSuccessor`,
which represents the fenced Holylog/Scripture transition, is the only action
that makes B the writer.

## Initial safety invariants

TLC checks these properties over every reachable bounded schedule:

1. Every generation has exactly one recorded authority.
2. Every physical append records the writer published for its generation.
3. A client acknowledgement implies a committed append exists.
4. A locally durable event is reclaimed only after commit acknowledgement or
   prior-commit observation.
5. The sealed/recovering interval has no writer.
6. After B's successor is published, A is not the authority for that latest
   generation.

This is a safety model, not a liveness proof. It deliberately does **not** yet
model a real Kubernetes Lease, Consul session, authority-store codec, route
TTL, producer protocol, durable outbox implementation, or multi-Verse
placement. Those belong in successive models after this vocabulary survives
review.

## Run TLC

Install the standard TLA+ tools, then run from this directory:

```sh
java -cp "$TLA_TOOLS_JAR" tlc2.TLC -config TwoScribeVerseRecovery.cfg \
  TwoScribeVerseRecovery.tla
```

`TLA_TOOLS_JAR` should point to the standard `tla2tools.jar`. The repository
does not vendor the checker. CI integration is intentionally deferred until
the first model and its intended bounded state space are reviewed.

Run the network refinement with:

```sh
java -XX:+UseParallelGC -cp "$TLA_TOOLS_JAR" tlc2.TLC -workers 4 \
  -config TwoScribeVerseRecoveryNetwork.cfg TwoScribeVerseRecoveryNetwork.tla
```
