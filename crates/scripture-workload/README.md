# scripture-workload

Consumer Workload Contract and reference materializers.

## Contract

```
acquire_or_renew(binding_key, owner_token)
  → one register record { epoch, token, frontier, last_commit_ref }
  → fresh token always bumps epoch (carry frontier/commit ref);
    same-token renew retains epoch
reconcile(range, fence) → Absent | AlreadyCommitted | Indeterminate
apply(range, fence)     → durable OutputCommit (only when Absent)
advance(fence, frontier, last_commit_ref)
  → one CAS on the same record: epoch+token match, frontier strictly forward
```

Indeterminate output fails closed: no register advance, no silent overwrite,
unknown partials are never deleted. Epoch is embedded in output object keys so
stale-epoch PUTs cannot clobber. The in-memory store is a model/proof only —
not a durability claim.

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
until implemented. Config `binding_epoch` remains validated nonzero for now but
is not authoritative — the progress register assigns epochs on acquire.
