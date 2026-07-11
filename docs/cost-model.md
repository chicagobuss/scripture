# Scripture v0 cost model

This model is backend-neutral. Provider request and storage prices are inputs;
they do not belong in the durable formula. Logical operations and physical
replica operations are reported separately.

Let:

- `R` = records in one batch;
- `B` = encoded batch bytes;
- `Qw` = replica writes required for an acknowledged LogDrive write;
- `Qr` = replica reads required for a LogDrive point read;
- `P` = tail polls per subscriber per billing interval;
- `S_get` = durable seal reads per tail poll (currently 1);
- `M_get` / `M_put` = other durable metadata reads/writes;
- `T` = retained encoded bytes over the billing interval.

## Append

One acknowledged v0 batch costs:

```text
logical appends       = 1 batch = R records
replica PUTs          = Qw
uploaded data bytes   = Qw × B
PUTs per record       = Qw / R
uploaded bytes/record = Qw × B / R
```

The final seal check is a metadata read. With a durable LogDrive-backed seal,
add one seal GET per batch append that reaches its acknowledgement check.
Failed/abandoned writes may incur work without producing acknowledged records
and must be counted separately.

## Read

One previously unchecked batch read costs:

```text
replica GETs          = Qr (+ repair writes when required)
downloaded bytes      = Qr × B
GETs per record       = Qr / R
downloaded bytes/record = Qr × B / R
```

The v0 footer does not reduce these terms: Holylog currently returns complete
opaque values and exposes no range-read operation.

## Tail polling and idle subscribers

An open-log tail poll performs one sequencer call and one seal read. For a
durable LogDrive-backed seal:

```text
tail-poll GETs              = S_get = 1
idle-subscriber seal GETs   = P × S_get
idle-subscriber request cost = P × provider_GET_price
```

After sealing, the canonical check additionally performs `weakTail(K)`, whose
replica listing/point-repair work is backend- and history-dependent and must be
recorded by adapter metrics.

## Trim and retained storage

Logical trim includes a canonical tail check plus trim-point metadata work.
With `LogDriveTrimPoint`, a changing trim point performs bounded weak-tail,
one current-register GET, and one Qw-amplified register PUT. Logical trim does
not reduce `T`; physical reclamation and DELETE requests remain undecided.

```text
retained storage charge = T × provider_byte_interval_price
```

## Counter scope

Protoscripture's sample numbers are deterministic in-memory
`QuorumLogDrive` data-plane counters. They exclude durable seal and trim
metadata, provider request pricing, latency, and an idle polling cadence. A
provider-realistic experiment must put all of those operations in the same
ledger before quoting dollars.
