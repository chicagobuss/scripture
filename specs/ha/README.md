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

`ThreeGenerationFencing.tla` is a small harness, not a product model. It exists
because the network model lets authority advance at most once (A -> B), which
makes endpoint identity an accidentally perfect proxy for epoch identity: a
route naming A is stale there exactly when A is not the writer. An
endpoint-only acceptance rule is therefore indistinguishable from an epoch
fence in that module, and no invariant it could carry would tell them apart.
The harness lets authority alternate, so A can lawfully regain writership and a
generation-0 route can name a Scribe that is once again the live writer while
describing a dead epoch.

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
7. Every append was authorised by the epoch that accepted it, compared against
   independently recorded packet provenance.

Invariant 2 is weaker than it appears on its own: appends were previously
stamped entirely from the state the acceptance guard had just read, so it
could not fail by construction. `AppendRecord` now also carries the
`routeGeneration`/`routeTerm` the packet was built from, which is what makes
invariant 7 able to fail.

Invariant 3 remains deliberately weak. It matches an acknowledgement to a
commit by client identity only, because `AppendRecord` still has no per-event
or per-attempt identity. It currently states "some commit exists for this
client", not "this acknowledgement belongs to the event it acknowledges", and
so expresses neither duplicate suppression nor exactly-once. Since
`ReclaimAcknowledged` is the action with real data-loss consequences, adding
that identity is the next correctness step.

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

The fencing harness runs in both directions. `EnforceEpochFence` selects the
acceptance rule under test, so the negative case is a configuration rather than
a forked copy of the module:

```sh
# Expect: no error.
java -cp "$TLA_TOOLS_JAR" tlc2.TLC -workers 4 \
  -config ThreeGenerationFencing.cfg ThreeGenerationFencing.tla

# Expect: CommitCarriesCurrentEpochRoute violated.
java -cp "$TLA_TOOLS_JAR" tlc2.TLC -workers 4 \
  -config ThreeGenerationFencingMutant.cfg ThreeGenerationFencing.tla
```

The mutant run is part of the evidence, not a curiosity: an invariant that has
never been observed to fail has not been shown to test anything. Its
counterexample commits a generation-0 route at generation 2 after A regains
writership, and notably does **not** violate invariant 2 — the recorded writer
still matches the published authority for that generation. If the mutant ever
passes, the harness has stopped testing the guard and must be repaired.

Measured runs (TLC 2.19, 4 workers): network model 7,627,560 distinct states at
depth 46, no violation; fencing harness 20,977 distinct states at depth 25, no
violation; mutant violated at 419 distinct states.
