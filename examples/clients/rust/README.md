# Rust Producer Wire v1 example

The runnable Rust reference client is built with Scripture's workspace so its
codec version cannot silently drift:

```sh
cargo run -p scripture-cli --bin scripture-producer-wire-client -- \
  127.0.0.1 9001 "hello Scripture"
```

The endpoint must be a configured experimental
`ingress.producer_wire_bind`. On an ambiguous connection loss, retry the exact
same optional `PRODUCER_ID EPOCH SEQUENCE` values and payload; do not advance
the sequence until an ACK is received.
