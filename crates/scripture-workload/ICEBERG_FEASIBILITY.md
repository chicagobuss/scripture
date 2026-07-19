# Iceberg adapter feasibility — WP consumer workload contract

**Status:** adapter **absent** from this package (honest non-claim).  
**Date:** 2026-07-19  
**Branch:** `cursor/workload-contract-arrow`

## What this package did prove

The Consumer Workload Contract and the bundled
`json_arrow_parquet` materializer prove:

- immutable `SourceRange` delivery;
- `reconcile` → durable output commit → CAS checkpoint;
- crash between Parquet/manifest commit and checkpoint advance is recovered by
  reconciliation without duplicating files;
- partial artifacts fail closed and do not advance progress.

Arrow/Parquet live only in `scripture-workload`, not in Scripture/Holylog core.

## Why Iceberg is not claimed here

The Apache Iceberg Rust crate (`iceberg` 0.9.x on crates.io) can create tables
and append data files in some catalog configurations, but this package does
**not** yet prove the seams required by the workload contract:

1. **Metadata commit as the durability boundary** — A real catalog transaction
   (REST / Hive / Glue / JDBC) must be the unit that `reconcile` recognizes.
   Writing data files alone is not an `OutputCommit`.
2. **Source-range provenance on the snapshot** — The exact
   `(canon_id, verse_id, first_offset, next_offset, workload_id, schema_ref)`
   must be attached to the committed snapshot (properties or equivalent) so
   restart reconciliation can match an immutable `SourceRange`.
3. **Restart reconciliation** — After a crash between data-file write and
   metadata commit, and after a crash between metadata commit and consumer
   checkpoint CAS, the adapter must classify Absent / AlreadyCommitted /
   Indeterminate without silent overwrite.
4. **Forced crash schedule** — No duplicate data-file publication under a
   scripted interrupt between file publish and snapshot commit.

Until those four are demonstrated against a real catalog (local or lab), an
Iceberg module would be a mock table claim. Prefer documenting the missing
seam over shipping a stub.

## Exact missing seams to close later

| Seam | Needed proof |
|------|----------------|
| Catalog transaction API | Commit returns a durable snapshot id; observe after restart |
| Provenance attachment | Snapshot properties (or Iceberg metadata extension) round-trip the `SourceRange` |
| Orphan file policy | Partial files after failed commit are not treated as committed |
| Checkpoint coupling | Consumer CAS only after snapshot observe succeeds |
| Duplicate suppression | Two apply attempts for one range yield one logical snapshot |

## Non-claims

- No Iceberg support in this branch.
- No R2/S3/GCS traffic by default.
- Parquet success does not imply Iceberg readiness.
