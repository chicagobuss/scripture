# rsyslog → Scripture (experimental)

The first serious rsyslog integration is `omprog`, not ordinary network syslog
forwarding. Rsyslog owns the source-side action queue and invokes
`scripture-rsyslog-omprog` over stdin/stdout. The helper emits `OK` only after
Scripture Producer Wire returns a committed ACK. On an unavailable Scribe or
ambiguous outcome it emits `DEFER_COMMIT`, leaving rsyslog to retain and retry
the source message from its disk-assisted queue.

This yields a useful and honest contract: **no source ACK before a Scripture
committed ACK**. It remains at-least-once, not exactly-once, because rsyslog's
stdin callback does not give the helper a stable source event identifier. A
crash after the Scribe committed but before the helper wrote `OK` can make
rsyslog resend the line as a new native producer sequence. Scripture's native
Wire outbox makes Scribe-facing retries exact; consumers still need their usual
deterministic deduplication for this compatibility boundary.

## Configure rsyslog

See `rsyslog-omprog.conf`. Use `forceSingleInstance="on"`: one helper owns one
local durable outbox. The standard action queue has one worker and disk
spill/retention so a slow or unavailable Scribe does not discard messages.

## Remote syslog fan-in

For remote hosts, prefer rsyslog's `omrelp` → local rsyslog `imrelp` hop, then
the same `omprog` action. RELP gives application-level forwarding
acknowledgements and is substantially better than plain TCP, but its own
documentation still treats a small reply-loss duplicate window honestly.

Do not call this a stable public plugin yet. It needs explicit load, restart,
and failover evidence against a live two-Scribe endpoint before promotion from
experimental.
