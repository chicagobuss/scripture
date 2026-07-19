# scripture-workload

Consumer Workload Contract and reference materializers.

## Contract

```
acquire_or_renew(binding, owner_token)  → fenced ownership (loser: FenceHeld)
reconcile(range, fence) → Absent | AlreadyCommitted | Indeterminate
apply(range, fence)     → durable OutputCommit (only when Absent)
CAS checkpoint          → only after durable output / reconciled commit,
                          and only while holding the fence token
```

Indeterminate output fails closed: no checkpoint advance, no silent overwrite,
unknown partials are never deleted.

Product terms: **Canon**, **Verse**, **Scribe**, **Materializer**.  
Internal `JournalId` remains a substrate elsewhere; this crate speaks Canon.

## Bundled path

`json_arrow_parquet`: newline-JSON payloads → Arrow `RecordBatch` → Parquet +
adjacent `.commit.json` manifest (workload id, binding epoch, owner token,
schema ref, digest, exact range). Object names are blake3-derived and
workload-scoped so raw Canon/Verse strings cannot escape the output directory.

## Iceberg

See [`ICEBERG_FEASIBILITY.md`](./ICEBERG_FEASIBILITY.md). No Iceberg adapter is
shipped until metadata commit + provenance + crash reconciliation are proven.

## Config

Optional top-level `workloads:` list (see tests / examples). Existing
single-scribe configs remain valid with an empty or absent list. Declared
`max_records` / `max_bytes` are enforced; nonzero `max_wall_ms` is rejected
until implemented. `binding_epoch` must be nonzero.
