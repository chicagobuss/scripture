# Scripture Producer Wire v1 (experimental)

## Status

This is an experimental native producer framing contract. The Rust codec and
cross-language reference codecs exist; a configured Scribe Wire listener does
not yet exist. Do not point production traffic at it or call it a stable SDK.

The current newline raw-lines listener is a separate compatibility/lab ingress.
It assigns identity per TCP connection and therefore cannot prove deduplicated
retry after a reconnect.

## Purpose

Producer Wire v1 carries the existing durable submission identity unchanged:

~~~text
ProducerId (16 bytes) + producer epoch (u32) + submission sequence (u64)
~~~

The eventual server creates Scripture Submission values from those fields. A
replay of the same canonical records under the same identity must return the
original committed offset range; changed records under the same identity must
fail closed.

One connection is bound to one already-selected Canon/Verse Scribe endpoint.
Directory lookup and failover select endpoints outside this framing; a route is
never authority.

## Framing

Every TCP message is exactly:

~~~text
u32_be body_length
body
~~~

The body is at most 4 MiB and begins with:

~~~text
bytes[4] magic = "SPW1"
u8 frame_type
~~~

All integers are unsigned big-endian. A decoder must reject truncated data,
unknown magic/type, lengths beyond the limit, semantic bound violations, and
trailing bytes. It must not allocate based on an unvalidated peer length.

| Type | Name | Body after type |
|---:|---|---|
| 1 | Hello | producer_id[16], producer_epoch u32 (nonzero) |
| 2 | Submit | sequence u64, record_count u32 (1–1024), then repeated record_len u32 + arbitrary bytes |
| 3 | Ack | producer_epoch u32, sequence u64, first_offset u64, next_offset u64; first < next |
| 4 | Error | producer_epoch u32, sequence u64, code u8, diagnostic_len u32 + UTF-8 diagnostic (≤1024 B) |
| 5 | Close | no bytes |

Error codes: 1 NotServing; 2 Backpressure; 3 IdentityConflict; 4 Unsupported;
5 Ambiguous. No response, transport close, or timeout is a negative ACK.

## Golden vectors

See `examples/clients/producer-wire-v1-vectors.json`. Python and Node reference
codecs run `--self-test` against it. The Rust codec has the same Hello vector
as a unit test.

## Compatibility mapping

| Source | v1 status | Important semantic difference |
|---|---|---|
| Native Rust/Python/Node client | planned client transport | retains producer identity/epoch/sequence |
| raw-lines | existing lab ingress | connection-scoped identity; reconnect retry is ambiguous |
| rsyslog TCP | future bridge | TCP delivery is not a source ACK protocol |
| OTel Collector | future bridge | no claim of OTLP compatibility until a concrete protocol is implemented |

