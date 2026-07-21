# Scripture Producer Wire v1 (experimental)

## Status

This is an experimental native producer framing contract. The Rust codec,
cross-language reference codecs, and a dedicated Scribe listener exist. The
listener is a direct endpoint only; fleet-directory routing and production SDKs
do not exist yet. Do not point production traffic at it or call it a stable
SDK.

The current newline raw-lines listener is a separate compatibility/lab ingress.
It assigns identity per TCP connection and therefore cannot prove deduplicated
retry after a reconnect.

## Scribe configuration

Keep raw-lines and Producer Wire on distinct sockets. Protocol selection is
never inferred from the first bytes of an arbitrary raw producer connection.

```yaml
scribe:
  assignments:
    - id: telemetry-host-a
      # Canon / Verse / authority fields omitted
      ingress:
        bind: "0.0.0.0:9000"                 # legacy raw-lines
        producer_wire_bind: "0.0.0.0:9001"   # experimental SPW1
```

The configured writer `advertise` route remains the raw-lines route in this
first slice. Producer Wire clients must be given their direct endpoint. A
later versioned directory record will carry protocol-specific endpoints rather
than pretending the two sockets are interchangeable.

## Purpose

Producer Wire v1 carries the existing durable submission identity unchanged:

~~~text
ProducerId (16 bytes) + producer epoch (u32) + submission sequence (u64)
~~~

Within an epoch, the first submission sequence is **0** and subsequent
submissions are dense. An exact retry repeats the same sequence *and exact
canonical records*; changing the records under an existing identity is an
`IdentityConflict`, never a replay of an earlier receipt.

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
clients run `--self-test` against it. The Rust codec has the same Hello vector
as a unit test.

Direct experimental client examples:

```sh
python3 examples/clients/python/producer_wire_v1.py \
  --host 127.0.0.1 --port 9001 --payload 'hello Scripture'
node examples/clients/node/producer_wire_v1.mjs \
  --host 127.0.0.1 --port 9001 --payload 'hello Scripture'
cargo run -p scripture-cli --bin scripture-producer-wire-client -- \
  127.0.0.1 9001 'hello Scripture'
```

All three accept a stable producer id, epoch, and sequence. On a lost reply,
retry the exact same tuple and bytes. A timeout is **ambiguous**, never a
license to advance the sequence.

## Compatibility mapping

| Source | v1 status | Important semantic difference |
|---|---|---|
| Native Rust/Python/Node client | experimental direct endpoint | retains producer identity/epoch/sequence |
| raw-lines | existing lab ingress | connection-scoped identity; reconnect retry is ambiguous |
| rsyslog TCP | future bridge | TCP delivery is not a source ACK protocol |
| OTel Collector | future bridge | no claim of OTLP compatibility until a concrete protocol is implemented |
