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

## Service and raw-lines lab slice

`crates/scripture-service` adds a bounded, cloneable submission handle over a
single task that owns the non-cloneable writer. An acknowledgement future
resolves only after the underlying durable append; after a kernel error the
actor resolves the failed and later requests as unavailable instead of leaving
callers pending. It does not turn the v0 writer into a restart-safe or
multi-process service.

`crates/scriptured` contains the first deliberately thin network adapter:
newline-delimited raw bytes in, `OK <first-offset> <next-offset>` after each
durable line. It is a loopback-tested protocol harness, not a production
daemon, schema registry, or HTTP API. In particular a connection that closes
before its `OK` has an unknown durable outcome and must retry at-least-once.

## Development

Pinned to the same Rust toolchain as holylog, which is consumed as a path
dependency during co-development.

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
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

## License

MIT
