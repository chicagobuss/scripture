# scripture

Scripture is the log-system layer being designed on top of the
[holylog](../holylog) kernel: named logs, payload batching, filtered
subscriptions, consumer checkpoints, queue semantics, and retention policy,
built strictly against holylog's public API.

**Status: v0 implementation phase.** The first eight decisions are accepted in
[`docs/decisions`](docs/decisions): canonical batches and typed attributes,
dense record offsets, one in-process durable writer, direct pull reads,
operator-configured stable journal identity, consumer-owned checkpoints/manual
retention, and backend-neutral cost accounting.

See [`ROADMAP.md`](ROADMAP.md) for the direction: the backend deployment-tier
ladder (free-tier, flat-fee SQL, object storage, hot per-KB stores), the
elastic journal-ownership fleet, and the bring-your-own-backend adapter story.

The `scripture` crate implements that bounded surface. It deliberately makes
no directory, cross-process writer fencing/restart, filtering, consumer-group,
queue, or physical-reclamation claim. Those remain later decisions rather
than implicit behavior.

## Protoscripture

`crates/protoscripture` is a **disposable spike** that
exercises the kernel from a consumer's seat: a versioned record envelope, a
batch codec, and a `Journal` over `AtomicLog`, driven end-to-end (append,
checked-tail reads, client-side filtering, prefix trim, seal, post-seal
recovery reads) by `cargo run -p protoscripture`. Its purpose is to
pressure-test the kernel surface, not to grow into the product; findings feed
[`docs/kernel-gap-report.md`](docs/kernel-gap-report.md).

## Scripture v0 crate

`crates/scripture` contains the code-bound v0 contract:

- canonical self-contained batches with stable `JournalId`, typed attributes,
  dense `RecordOffset`, and a validated footer index;
- a non-cloneable `JournalWriter` whose acknowledgements mean Holylog accepted
  the whole batch;
- a direct `JournalReader` with checkpoints defined as the next record to
  consume and explicit trim-gap events; and
- deterministic count/byte/monotonic-age batching policy without any claim
  that buffered records are durable.

## Service and product runtime

`crates/scripture-service` adds a bounded, cloneable submission handle over a
single task that owns the non-cloneable writer. An acknowledgement future
resolves only after the underlying durable append; after a kernel error the
actor resolves the failed and later requests as unavailable instead of leaving
callers pending. It does not turn the v0 writer into a restart-safe or
multi-process service.

`crates/scripture-runtime` owns product Verse-node composition: Canon-aware
startup, durable object-store parts, credential resolution, readiness/status
semantics, and the temporary Canon-gated ingress used for HA testing (not a
public producer protocol). Personal fleet tooling lives only under the ignored
[`config/local/`](config/README.md) operator area, not in the product workspace.

## Process command

`crates/scripture-cli` ships the product binary `scripture`:

```sh
scripture validate --config /path/to/scripture.yaml
scripture bootstrap --config /path/to/scripture.yaml --loglet-id <ID>
scripture serve --config /path/to/scripture.yaml
```

### Local console consumer

`scripture consume` is a **read-only debug/demo consumer**: it prints logical
Scripture records from a configured Canon/Verse to stdout. It owns no consumer
register or checkpoint and does not claim durable subscription semantics.

Against a local multi-assignment Scribe (for example RustFS-backed YAML under
`crates/scripture-cli/examples/`):

```sh
# terminal 1
cargo run -p scripture-cli -- serve --config /path/to/scripture.yaml

# terminal 2
cargo run -p scripture-cli -- produce-lab \
  --config /path/to/scripture.yaml \
  --canon demo --verse events \
  --workers 1 --per-worker 5

# terminal 3
cargo run -p scripture-cli -- consume \
  --config /path/to/scripture.yaml \
  --canon demo --verse events \
  --from 0 --until-records 5 --no-follow
```

Representative text output:

```text
canon=demo verse=events entry=0 record=0 bytes=64 digest=… payload=text:…
canon=demo verse=events entry=1 record=1 bytes=64 digest=… payload=text:…
scripture consume: entries_scanned=5 records_printed=5 final_cursor=5 elapsed_ms=12 membership_change=no
```

Use `--format jsonl` for one JSON object per record on stdout (summaries stay on
stderr). Hermetic coverage lives in `cargo test -p scripture-cli --locked`
(in-memory ObjectStore + VirtualLog; no cloud credentials).

Non-secret settings live in a versioned YAML file (`deny_unknown_fields`).
Credentials come only from the process environment (`RUSTFS_*`/`AWS_*` or
`R2_*`) or a Secret-mounted env — never YAML, argv, ConfigMap, or logs.
Generic Kubernetes examples and the image build live under
[`deploy/kubernetes`](deploy/kubernetes). Probes: `/livez`, `/readyz`
(Serving only), `/status`. This surface does not claim automatic failover,
restart fencing, a public producer protocol, or Decision 0012 recovery.

## Development

Pinned to the same Rust toolchain as holylog, which is consumed as a path
dependency during co-development.

```sh
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
cargo run -p protoscripture
```

The ignored R2 smoke test writes a unique temporary prefix, exercises durable
data/seal/trim objects through the Holylog adapter, and deletes the prefix on a
successful run. It requires `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, and
`R2_SECRET_ACCESS_KEY` (`R2_REGION` defaults to `auto`):

```sh
cargo test -p scripture --features r2-smoke --test r2 \
  scripture_v0_runs_against_r2 --locked -- --ignored --exact
```

## How this project was built

Scripture and its [holylog](../holylog) kernel were built during a focused
build week with Josh directing the work through Codex powered by GPT-5.6. That
was not a one-shot code-generation exercise: Codex was used as a working
partner to shape designs, implement Rust, run and interpret tests, inspect
real local services, and review or repair work produced in parallel sessions.

Josh used a lightweight document and task system called Tracker to keep the
work legible: design notes, work packages for other coding agents, reviews,
implementation notes, and follow-up fixes live there. The practical loop was
to state a concrete outcome, give an agent a bounded package, require it to
build and test the result, then have Codex review the actual branch and drive
the next correction. That made it possible to parallelize exploration without
losing the thread of the safety model.

The models were especially useful for moving between levels of the project:
formal and simulation-based checks around recovery/fencing, Rust workspace
tests, hermetic integration fixtures, and hands-on runs against local RustFS
and real object stores such as R2 and S3. The result is intentionally still a
work in progress, but its claims are tied to executable checks rather than a
dashboard or an architectural sketch.

## License

MIT
