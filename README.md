<p align="center">
  <img src="assets/logo.png" alt="Scripture logo" width="420">
</p>

**A correctness-first, object-store-native streaming system.** Scripture is
an early Rust implementation of a Kafka-like durable streaming service that
does not make the Kafka protocol its foundation. It is built on
[Holylog](https://github.com/chicagobuss/holylog), a shared-log kernel inspired
by the Conflux / LogDrive research lineage.

The idea is simple: immutable payload bytes live in durable object storage;
an ordered, fenced metadata history says which bytes form each stream. That
lets a small Scribe fleet provide durable streaming without placing a broker
replication protocol on every producer write.

Scripture is a build-week prototype with a serious correctness program—not a
finished Kafka replacement. The current source tree is intended to be easy to
run, inspect, and challenge.

## Start here: Docker Compose

This starts one local Scribe backed by a named Docker volume. It is the
smallest honest try-it: durable local-file storage in one Docker host, **not
HA** and not a cloud durability claim. It needs only Docker Compose; no cloud
credentials, Kubernetes cluster, or private registry.

```sh
git clone https://github.com/chicagobuss/scripture.git
cd scripture/examples/docker-compose
docker compose up --build --wait

# Send five bounded test records through the running Scribe.
docker compose exec scripture scripture produce-lab \
  --config /etc/scripture/scripture.yaml \
  --canon 'scripture-jrnl!!' --verse 'scripture-verse!' \
  --workers 1 --per-worker 5

# Read the same committed records back from the Canon.
docker compose exec scripture scripture consume \
  --config /etc/scripture/scripture.yaml \
  --canon 'scripture-jrnl!!' --verse 'scripture-verse!' \
  --from 0 --until-records 5 --no-follow

# Inspect process status.
curl -fsS http://127.0.0.1:9100/status
```

### Optional: see store-and-forward receipts

The Compose example also exposes the experimental native Producer Wire
listener on port `9001`. Its reference client uses the configured local edge
outbox and prints two distinct facts: `spooled` after it fsyncs the stable
envelope locally, then `committed` after the Scribe has acknowledged that same
producer identity into the Canon.

```sh
docker compose exec scripture scripture-producer-wire-client \
  127.0.0.1 9001 'edge receipt, then Canon commit' \
  --config /etc/scripture/scripture.yaml
```

This is deliberately a single-host demonstration. `spooled` means one named
local disk holds the envelope; it is useful for a temporary Scribe/object-store
outage and retry-safe forwarding, but it is **not** a committed Canon offset,
multi-Scribe HA, or protection against losing that Docker host. The client
keeps the envelope until it observes and durably checkpoints the committed ACK.

When you are done with either local exercise, remove the entire demo—including
its named-volume data—with `docker compose down -v`.

The image is built from source with the public, pinned Holylog tag. The
`scripture-data` volume deliberately survives ordinary `docker compose down`
so you can stop and restart the Scribe without losing local demo data.

Prefer Cargo? The same no-cloud walkthrough is available at
[`crates/scripture-cli/examples/scripture-local.yaml`](crates/scripture-cli/examples/scripture-local.yaml).

## What exists today

The useful core is already more than a diagram:

- **Canonical stream history.** Canon / Verse identities, dense logical
  offsets, deterministic records, immutable payload blobs, and ordered
  `DataRef` / reference-batch metadata.
- **Bounded producer path.** Multi-record submissions, stable producer
  identity, retry-safe deduplication, bounded batching, and an experimental
  edge outbox that follows fsync → `spooled` → forward → observed committed
  ACK → durable checkpoint → reclaim. `spooled` is explicitly one-local-disk
  evidence; `committed` remains the default and stronger receipt.
- **Scribe lifecycle.** A Scribe can serve a Verse, recover through a lawful
  seal-and-replace transition, and refuse work when it lacks authority. A
  route, health check, or discovery record is never authority.
- **Active-active fleet shape.** Multiple Scribes can actively serve distinct
  Verses at once. Each ordered Verse has one effective writer at a time; a
  successor is fenced through the durable Canon root rather than elected by
  gossip.
- **Useful ingress and examples.** Raw-lines is the current executable
  listener; Producer Wire v1 reference clients exist in Rust, Python, and
  Node.js. There is an rsyslog `omprog` bridge and an OpenMetrics-to-Scripture
  telemetry producer experiment.
- **Consumer/workload foundation.** A fenced consumer-progress register and a
  JSON → Arrow → Parquet materializer prove an ordered, restart-safe output
  chain. Iceberg is intentionally not claimed as implemented.
- **Inspection tools.** The Operations Cockpit renders an immutable evidence
  bundle: producer identities, Scribe observations, objects, committed output
  manifests, and explicit verdict provenance.

## A small vocabulary

- **Canon** — a named family of ordered streams and policy.
- **Verse** — one independently ordered, fenced stream within a Canon; the
  basic scaling and writer-authority unit.
- **Scribe** — a Scripture process. One Scribe can host multiple assignments;
  a fleet can distribute them.
- **Producer** — a client that retains a stable event identity across retry.
- **Consumer / workload** — an independently progressing reader or
  materializer. A slow consumer does not own or block a writer.

This is active-active at the fleet level, not “two writers append to the same
ordered Verse.” During a per-Verse handoff there can be a brief fail-closed
period with no writer; producer retry/outbox behavior exists so that a timeout
does not become a silent loss.

## Evidence, not vibes

Run the portable source-level gates from a clean checkout:

```sh
scripts/submission-check.sh
```

It runs formatting, strict Clippy, deterministic Rust tests for the core,
runtime, campaign, and workload crates, and writes a redacted local evidence
bundle under `.tmp/`. It does not contact a cluster or cloud provider.

The release-level active-active contract is also a focused, hermetic test:

```sh
cargo test -p scripture-runtime --test active_active_release --locked
```

It exercises one Canon with multiple Verses and Scribes, a per-Verse
promotion under producer traffic, producer-edge continuity, stale-receipt
rejection, sibling isolation, and contiguous readback. It is deliberately a
deterministic in-memory proof harness—not a claim that a Kubernetes cluster
has been production-attested.

The project also uses two complementary correctness methods:

| Method | What it checks | What it does **not** prove |
| --- | --- | --- |
| TLA+ model checking | Bounded authority, stale routes, dropped/reordered packets, recovery publication, epoch fencing, and conditional-register arbitration schedules | That the Rust implementation refines the model; unbounded liveness; a real Kubernetes or object-store deployment |
| Deterministic Rust fault tests | Concrete code paths for reply loss, writer death, seal/replace, outbox retention, retries, malformed inputs, and output-register recovery | Every distributed schedule or provider behavior |
| Local and hosted lab drills | Operational hypotheses on local RustFS and selected real object stores | A universal production-SLA claim |

The executable TLA+ specifications and their explicit scope are in
[`specs/ha/README.md`](specs/ha/README.md). They include passing bounded TLC
configurations **and deliberately broken mutant configurations that must
produce a counterexample**. The accurate claim is: *Scripture uses TLA+ model
checking to pressure-test bounded protocol models, paired with deterministic
implementation tests.* It is not “formally verified production software.”

## Architecture and direction

Holylog sits below Scripture as the durable shared-log kernel. Scripture adds
the product meaning: Canon / Verse identity, producer receipts, Scribe
authority, ingress, consumers, output workloads, and operational visibility.

The intended direction is a loosely coupled fleet:

```text
producer / local outbox
          |
          v
  Scribe fleet ── authoritative per Verse through a conditional root
          |
          v
 immutable blobs + ordered Verse references in object storage
          |
          +── console readers / transcribers / future queue and lakehouse outputs
```

Scribes do not need a peer protocol on the normal data path. The durable
conditional root fences a stale writer; service discovery merely helps clients
find candidates. Optional control substrates such as Kubernetes/etcd, Consul,
Postgres, or DynamoDB can make recovery faster and more ergonomic, but they
must never replace the fencing boundary.

The roadmap is intentionally incremental:

1. Strengthen active-active, per-Verse handoff and producer continuity proofs.
2. Finish the layered data plane: records → data blocks → immutable blobs →
   ordered reference batches, so object-store requests amortize at real scale.
3. Grow a small plugin surface for ingress and consumers: OpenTelemetry,
   common logging inputs, transforms, Parquet, and eventually Iceberg.
4. Add optional higher-order control and multi-store repair/automation without
   making external magic a requirement for the simple local experience.

Kafka compatibility is conceivable as an adapter, but it is not load-bearing
for the project. Scripture is trying to make the useful parts of modern
cloud-native streaming approachable without requiring a Kafka cluster or
starting every user with partitions and rebalance operations.

## What is deliberately not claimed

- no general Kafka-protocol compatibility;
- no end-to-end exactly-once promise;
- no completed Iceberg writer;
- no automatic multi-cloud repair product yet;
- no claim that the Compose example is HA or cloud durable;
- no claim that a one-disk `spooled` receipt survives losing that producer/edge
  host;
- no claim that model checking proves the deployed Rust program correct.

## Explore the project

- [`docs/decisions`](docs/decisions) — protocol and product decisions.
- [`specs/ha`](specs/ha) — small executable TLA+ authority/recovery models.
- [`crates/scripture`](crates/scripture) — core records, receipt, data-reference,
  and producer-outbox types.
- [`crates/scripture-runtime`](crates/scripture-runtime) — Scribe composition,
  object-store adapters, routing, and lifecycle code.
- [`crates/scripture-workload`](crates/scripture-workload) — consumer progress
  and the Arrow/Parquet proof path.
- [`tools/operations-cockpit`](tools/operations-cockpit) — read-only evidence
  explorer; run `npm run bundle` from that directory.
- [`examples`](examples) — Producer Wire and rsyslog bridge examples.

## How this project was built

Scripture and Holylog were built during a focused build week with Josh
directing work through Codex and other frontier-model coding sessions. Tracker,
a small document and task system built alongside the project, held work
packages, design notes, reviews, and evidence. The practical loop was simple:
state a concrete outcome, make the evidence boundary explicit, build a bounded
implementation, then review and repair the actual branch.

That process produced a lot of experiments. The repository keeps the useful
ones only when their claims are tied to executable tests, model configurations,
or clearly labeled lab evidence.

## License

MIT
