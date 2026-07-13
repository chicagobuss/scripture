# Scripture roadmap

Scripture is the log system built on the [holylog](../holylog) kernel: named
journals, payload batching, subscriptions, consumer checkpoints, and retention
policy, all strictly against holylog's public API.

Two product convictions organize this roadmap:

1. **Elasticity through journal ownership, not through storage machinery.**
   Scripture nodes pop in and out of existence to follow both ingest and
   consumption volume. The unit of ingest parallelism is the journal; nodes
   acquire and release journals through holylog's fenced seal-and-replace
   reconfiguration; readers scale freely because reads below a checked tail
   are coordination-free. No broker cluster, no consensus service in the data
   path.
2. **Bring your own backend.** A journal's durability lives on whatever
   attested store fits the operator's budget and trust: a free-tier database
   for a hobbyist, a flat-fee SQL instance for a small steady deployment,
   object storage at scale. Holylog's adapter kit makes a new backend a
   weekend project behind a mechanical conformance bar; Scripture's job is to
   make *choosing* one a documented, boring decision — and to make trying a
   creative one delightful.

## Where scripture stands

- **v0 contract implemented** (decisions 0001–0008): canonical self-contained
  batches, dense record offsets, one in-process durable writer with
  `committed` acknowledgement, direct pull readers with checkpoints and trim
  gaps, operator-configured journal identity, backend-neutral cost accounting.
- **Phase 1 (chunk driver) in flight** under decisions 0009–0011: chunk
  format and cohorts, acknowledgement levels and producer identity, spool
  epochs and the failure/handoff model. Its binding plan is
  `docs/phase-1-chunk-driver.md` and is unchanged by this roadmap.
- **Labs**: `protoscripture` (kernel pressure-test spike), `scripture-service`
  (single-owner submission actor), `scriptured` (raw-lines network lab), the
  two-process fleet drill, and an R2 smoke test through the holylog object
  store adapter.
- Cutovers use witnessed VirtualLog reconfiguration exclusively; recovery
  vocabulary and the fresh-provision boundary follow holylog decisions
  0009/0012.

## The deployment tier ladder

The same scripture, the same protocol, different attested backends. Tiers are
configuration, not editions. Data chunks (the heavy bytes) can go to object
storage in every tier; the *journal* — the ordered metadata log — sits on the
store whose billing geometry fits the workload:

| Tier | Journal backend | Why |
| --- | --- | --- |
| Free | D1-class (per-request, free tier) | A real deployment at $0; free-tier write budgets comfortably cover a batched hobby journal |
| Small & steady | Flat-fee SQL (Postgres-class) | Marginal requests free until saturation; predictable bill; ms-latency acks; one dependency (journal + seal + trim + root register colocated) |
| Scale | Object storage | Request-priced, size-independent: batching buys orders of magnitude; bottomless retention |
| Hot & latency-critical | DynamoDB-class (per-KB) | Cheapest small-append writes and ~10ms acks; the one tier where holylog's striping option matters |

Guidance, not law: the break-even arithmetic (append size after batching ×
rate × ack-latency target, priced in each provider's currency) gets documented
per tier, and holylog's conformance runs supply measured cost profiles rather
than estimates. Operators with a service they like and trust are explicitly
invited off this table — that is what the adapter kit is for.

**Scripture does not stripe by default.** Striping is holylog's per-log
bandwidth knob for byte-capped backends, gated behind holylog decision 0011;
scripture reaches for it only in the hot tier, and documents why.

## Phase plan

### Phase 1 — bounded single-owner chunk driver (in flight)

Unchanged; see `docs/phase-1-chunk-driver.md`. One journal per chunk, one
in-process owner, `committed`-only acknowledgement, reservation-enforced
limits, producer identity and dedup, deterministic tests.

### Phase 2 — backend tiers for real

Make the tier ladder true instead of aspirational.

- Consume holylog's LogDrive conformance kit as it lands; wire journal
  assembly so backend selection is pure configuration (journal, seal, trim,
  and root register per tier).
- Stand up one reference deployment per tier — free (D1), flat (Postgres),
  scale (object storage) — each with a measured cost sheet from real runs,
  extending `docs/cost-model.md` from object storage to all geometries.
- Document the selection arithmetic and the failure-domain honesty per tier
  (a single SQL instance or D1 database is one failure domain; quorum
  composition is available when an operator wants two stores to agree).
- "Add your own backend" guide: the scripture-side walkthrough of holylog's
  adapter authoring path, from creative idea to attested journal, including
  what the menagerie label does and does not promise.

### Phase 3 — the elastic fleet

Nodes pop in and out; the journal set is the stable thing.

- **Remote sequencer adoption** (holylog Track A.1 is the dependency): the
  journal owner's sequencer endpoint and epoch published through the
  application fence; non-owner nodes get linearizable `check_tail` and
  forwarded appends; epoch fencing turns owner restarts into clean
  seal-and-replace instead of silent reattachment.
- **Journal→owner mapping and rebalance**: a control-plane layer built
  entirely from fenced reconfiguration — claim, release, controlled handoff,
  crash takeover. Ownership changes are witnessed CAS transitions; there is
  no separate consensus system to operate.
- **Scale-out and scale-in drills**: extend the two-process fleet drill to N
  nodes joining and leaving under load, for both ingest (ownership movement)
  and consumption (reader churn), with the invariant suite proving no
  acknowledged record is lost or reordered across any transition.
- **Hot-journal remedies**: splitting and merging journals as reconfiguration
  patterns (a hot journal is sealed and succeeded by two), documented as the
  answer that node-count cannot provide.

### Phase 4 — subscriptions, retention, and the consumer surface

The v0 read path grows into the product surface: subscriptions and read
fan-out, consumer groups over checkpoint primitives, retention policy driving
holylog's trim (and eventually physical reclamation), and the query-facing
metadata a consumer ecosystem needs. Scoped by future decisions; nothing here
weakens the opaque-payload boundary with the kernel.

## Non-goals

- No broker-cluster control plane, service discovery, or consensus dependency
  in the data path — ownership and recovery ride the root register.
- No default striping; no backend assumed compatible without an attestation.
- No schema registry, transformation, or query engine inside scripture's
  core; those are consumers of the surface, not parts of it.

## Immediate recommended sequence

1. Finish Phase 1 per its binding plan.
2. Phase 2's Postgres-tier reference deployment as soon as holylog's SQL
   LogDrive adapter exists — it exercises the conformance kit end-to-end and
   proves the one-dependency small deployment.
3. Remote-sequencer design review jointly with holylog (the Phase 3
   dependency), so the fence/epoch schema lands once, correctly.
4. Free-tier (D1) reference deployment and the tier-selection doc.
5. Fleet drills at N>2 with ownership movement under load.
