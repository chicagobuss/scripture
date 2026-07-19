# Scripture v0 cost model

This model is backend-neutral. Provider request and storage prices are inputs;
they do not belong in the durable formula. Logical operations and physical
replica operations are reported separately.

Let:

- `R` = records in one batch;
- `B` = encoded batch bytes;
- `Qw` = replica writes required for an acknowledged LogDrive write;
- `Qr` = replica reads required for a LogDrive point read;
- `P` = tail polls per subscriber per billing interval;
- `S_get` = durable seal reads per tail poll (currently 1);
- `M_get` / `M_put` = other durable metadata reads/writes;
- `T` = retained encoded bytes over the billing interval.

## Append

One acknowledged v0 batch costs:

```text
logical appends       = 1 batch = R records
replica PUTs          = Qw
uploaded data bytes   = Qw × B
PUTs per record       = Qw / R
uploaded bytes/record = Qw × B / R
```

The current writer is sequential and permits only one outstanding batch, so:

```text
maximum batches/second <= 1 / durable_append_round_trip_seconds
```

K-window pipelining is not used by v0 and must not be included in throughput
or cost projections until the async writer driver exists.

The lab service actor preserves this depth-one behavior: each accepted
submission currently becomes one durable batch. Its bounded in-memory queue is
backpressure, not durable staging, and queued/failed submissions must be
counted separately from acknowledged batches. A future batching driver may
merge submissions behind the same acknowledgement boundary; no text or HTTP
transport should assume the present one-line/one-batch implementation.

The final seal check is a metadata read. With a durable LogDrive-backed seal,
add one seal GET per batch append that reaches its acknowledgement check.
Failed/abandoned writes may incur work without producing acknowledged records
and must be counted separately.

## Read

One previously unchecked batch read costs:

```text
replica GETs          = Qr (+ repair writes when required)
downloaded bytes      = Qr × B
GETs per record       = Qr / R
downloaded bytes/record = Qr × B / R
```

The v0 footer does not reduce these terms: Holylog currently returns complete
opaque values and exposes no range-read operation.

## Tail polling and idle subscribers

An open-log tail poll performs one sequencer call and one seal read. For a
durable LogDrive-backed seal:

```text
tail-poll GETs              = S_get = 1
idle-subscriber seal GETs   = P × S_get
idle-subscriber request cost = P × provider_GET_price
```

After sealing, the canonical check additionally performs `weakTail(K)`, whose
replica listing/point-repair work is backend- and history-dependent and must be
recorded by adapter metrics.

A future service tail cache can replace `P` per-session polls with one poll per
journal cadence plus local wakeups after a service-owned append. This is an
economic objective, not a present implementation claim: the current raw-lines
adapter only writes and performs no tail caching.

## Trim and retained storage

Logical trim includes a canonical tail check plus trim-point metadata work.
With `LogDriveTrimPoint`, a changing trim point performs bounded weak-tail,
one current-register GET, and one Qw-amplified register PUT. Logical trim does
not reduce `T`; physical reclamation and DELETE requests remain undecided.

```text
retained storage charge = T × provider_byte_interval_price
```

## Counter scope

Protoscripture's sample numbers are deterministic in-memory
`QuorumLogDrive` data-plane counters. They exclude durable seal and trim
metadata, provider request pricing, latency, and an idle polling cadence. A
provider-realistic experiment must put all of those operations in the same
ledger before quoting dollars.

## Physical Reclamation

In the cost analysis of log pruning and retention, several constraints must be observed:
- **Entries ≠ Slots:** Because of Scripture batching, the number of records (entries) $R$ does not equal the physical slots. Batching divides the S3/R2 request counts by $R$, making batching policy the first-class economic defense against API costs.
- **Durable Metadata Isolation:** Durable metadata registers (such as seal, trim, and future checkpoints) must be bound to and age out with the generations they describe, rather than accreting globally and creating unbounded scan/read overhead over time.
- **Physical purges:** Storage reclamation cannot rely on naïve slot-by-slot `DELETE` API loops, which are economically prohibitive. Reclamation strategies must utilize delegated provider lifecycle rules applied to sealed-generation prefixes to achieve zero-request bulk purges.

## Measured v0 request counts (2026-07-19)

Local rustfs, `scripture produce-lab`, counters read from `/status`
(`store_puts` / `store_gets`, sourced from the adapter that issued them).

| workers | records | PUT/record | GET/record | records/batch |
|---|---|---|---|---|
| 1 | 1000 | 1.000 | 1.000 | 1.0 |
| 8 | 1000 | 0.398 | 0.398 | 2.5 |
| 32 | 4000 | 0.331 | 0.331 | 3.0 |

So `R = 1` holds only at depth one. Under concurrency the driver does batch,
and `R` settles near 3 — records that arrive while the single outstanding batch
is in flight. `R` is therefore arrival-rate × append round-trip, self-limiting,
and not a configured value.

Throughput on the same runs rose 48 → 242 records/second between 1 and 48
workers while p50 latency rose 21ms → 210ms: saturation, not scaling. A 64x
payload increase (64B → 4KB) cost 5% throughput, so the path is round-trip
bound, not bandwidth bound. At `R ≈ 3` and ~240 records/second the writer is
completing roughly 80 durable batches/second, which is the depth-one bound this
document already states — `K`-window pipelining remains unimplemented.

### The counters are incomplete, and the gap is the interesting part

Per "Counter scope" above, these numbers cover the **LogDrive data path only**.
`ObjectStorePartsFactory` receives the shared `ObjectStoreMetrics`;
`ObjectStoreConditionalRegister` and `ObjectStoreExclusiveClaim` are
constructed without metrics and Holylog's register has no metrics support at
all. Authority work is therefore absent from the table.

By code path rather than measurement: `HaServingSession::submit` calls
`ensure_root_authority` twice per submission — once before admission and again
before the receipt resolves — and each call reaches `observe_membership` →
`read_register`, one register GET. That is **two register GETs per record**.

The consequence matters more than the number: those GETs are **per record and
do not amortise with batching**. At `R ≈ 3` the measured data path contributes
about 0.33 GET/record while authority contributes 2, so authority is roughly
85% of read requests and grows as a share whenever batching improves. Batching
is the stated economic defence against API cost, and it cannot touch the
dominant term.

Do not quote dollars from the table above. Instrumenting the register is the
prerequisite, and it is a Holylog change: `ObjectStoreConditionalRegister::new`
would need to accept an `ObjectStoreMetrics` the way `ObjectStorePartsFactory`
already does.

### Not a verdict against a target

This project has no numeric per-record request budget to pass or fail. This
document is a model, and the table is a first measured baseline to improve
against.
