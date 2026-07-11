# Local prior-art checkouts

The complete SlateDB/OpenData repository is cloned locally at
`references/opendata/` for prior-art study. It contains the Buffer and Log
projects as well as their shared infrastructure, RFCs, and operational
context.

- Upstream: https://github.com/opendata-oss/opendata
- Checkout used for this study: `b14d86b6e62f0c44bddd82029b3e758f9fec2db9`
- Relevant paths: `buffer/`, `log/`, `common/`, and the repository-level `rfcs/`

The checkout is intentionally ignored by Scripture's Git repository. Refresh
it independently when studying upstream changes; do not copy its code into
the product without an explicit license/provenance review (upstream is MIT,
copyright opendata-oss).

## Questions this reference answers (studied 2026-07-11)

Per the family discipline, each reference exists to answer named questions.

1. **What does a production writer-actor loop look like?**
   `buffer/src/producer.rs`: `Producer::produce()` returns a `WriteHandle`
   whose `DurabilityWatcher::await_durable()` resolves only after the durable
   flush — the same submit → ack-future shape as Scripture's planned
   `JournalHandle`. `BatchWriterTask::run` is the canonical actor loop:
   `tokio::select!` over shutdown / message / deadline, with the deadline
   computed from the batch's `started_at + flush_interval`, size-triggered
   flush on add, and all notifiers resolved with one batch result
   (batch-level acks). **Do not copy** its direct `tokio::time::sleep` in the
   core loop: Scripture's core computes deadlines and lets the transport edge
   sleep, preserving deterministic testing.

2. **How do you serve binary Protobuf and ProtoJSON from one definition?**
   `log/src/server/proto.rs`: dual `#[derive(prost::Message, Serialize,
   Deserialize)]` with `serde_as(Base64)` for bytes and camelCase renames — no
   `.proto` files, no protoc/pbjson build step. Directly applicable to
   `scripture-proto`; hand-maintain `.proto` parity only when gRPC arrives.
   **Reject deliberately:** the `status: "success"` string field in response
   bodies and the coarse two-code error mapping in `server/error.rs` —
   Scripture uses HTTP status + a typed error taxonomy.

3. **What is the resume-cursor pattern for reads?** Log RFC 0007 + scan
   responses return an exclusive "resume from" sequence the client passes
   back — checkpoint-carrying reads with server-stateless sessions,
   validating Scripture's plane-2 design. `follow` + `timeout_ms` scan
   parameters are their long-poll surface; `view_tracker.rs` is the wakeup
   bookkeeping behind it (prior art for the tail-cache plane).

4. **Is there a multi-writer door that preserves dense offsets?** Buffer
   RFC 0001: stateless producers, any of which accepts any data, flushing
   unordered durable batches to object storage coordinated by a
   manifest-backed queue; a single consumer establishes database order later.
   Translation for Scripture: HA ingestion, if ever needed, is an *optional
   unordered buffer tier in front of the journal* — the single dense-offset
   writer drains it — not multiple journal writers. This answers open
   question Q1's "keep the door open" without weakening decision 0003.

5. **Clock abstractions.** `common/src/clock.rs` uses wall-clock
   `SystemTime`; Scripture's monotonic `Duration` clock (decision 0003 / A4)
   is deliberately different and better for batch aging. A third private
   clock trait in the ecosystem confirms A4's "extract a shared crate only
   when duplication is real" stance.

6. **Millions of logical streams?** Log's key-per-stream model over one LSM
   (no partition provisioning) is prior art for obligation 4's logical-stream
   multiplexing question — the answer lives in keyed data over one physical
   journal, not per-stream physical logs. Deferred with the directory
   decision; `gc.rs` (orphan batch cleanup) is also prior art for the
   lakehouse sink's orphan-file GC.
