# Telemetry Canon → Parquet evidence vertical

Local preflight for the product vertical:

```text
OpenMetrics → OTel-shaped JSON → Scripture Canon/Verse
  → read-only committed history → Arrow/Parquet
  → progress register → run-bundle-v1
```

## Preflight (default)

```sh
node tools/telemetry-evidence-vertical/preflight.mjs
```

Prints the planned live drill. It does **not** create namespaces, contact
Scribes, or write to object stores.

`--execute` / `--execute-live` are refused until Joshua supplies an explicit
approved command.

## Deterministic local proof

```sh
cargo test -p scripture-workload --locked --test telemetry_vertical
```

Iceberg remains `absent` unless a real reconciled table commit is proven.
