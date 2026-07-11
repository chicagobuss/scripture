# scripture

Scripture is the log-system layer being designed on top of the
[holylog](../holylog) kernel: named logs, payload batching, filtered
subscriptions, consumer checkpoints, queue semantics, and retention policy,
built strictly against holylog's public API.

**Status: design phase.** There is no product code here yet, and none may be
written until the corresponding design decisions are recorded. Scripture's
design obligations live in the project family's coordination notes; accepted
decisions migrate into `docs/decisions/` as the code that binds them lands.

## Protoscripture

The only crate, `crates/protoscripture`, is a **disposable spike** that
exercises the kernel from a consumer's seat: a versioned record envelope, a
batch codec, and a `Journal` over `AtomicLog`, driven end-to-end (append,
checked-tail reads, client-side filtering, prefix trim, seal, post-seal
recovery reads) by `cargo run -p protoscripture`. Its purpose is to
pressure-test the kernel surface, not to grow into the product; findings feed
[`docs/kernel-gap-report.md`](docs/kernel-gap-report.md).

## Development

Pinned to the same Rust toolchain as holylog, which is consumed as a path
dependency during co-development.

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
cargo run -p protoscripture
```

## License

MIT
