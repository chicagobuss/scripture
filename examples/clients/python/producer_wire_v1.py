#!/usr/bin/env python3
"""Experimental Scripture Producer Wire v1 reference codec.

Run `python3 producer_wire_v1.py --self-test` to verify the shared golden
vectors. This is transport-ready but no Scribe Wire listener exists yet; it is
not a production SDK or a replacement for the current raw-lines harness.
"""

from __future__ import annotations

import argparse
import json
import struct
from pathlib import Path
from typing import Iterable

MAGIC = b"SPW1"
HELLO, SUBMIT, ACK, ERROR, CLOSE = range(1, 6)
MAX_FRAME_BYTES = 4 * 1024 * 1024
MAX_RECORDS = 1024


def _frame(kind: int, body: bytes) -> bytes:
    message = MAGIC + bytes([kind]) + body
    if len(message) > MAX_FRAME_BYTES:
        raise ValueError("frame exceeds 4 MiB")
    return struct.pack(">I", len(message)) + message


def hello(producer_id: bytes, epoch: int) -> bytes:
    if len(producer_id) != 16 or not 0 < epoch <= 0xFFFFFFFF:
        raise ValueError("Hello requires 16-byte producer id and nonzero u32 epoch")
    return _frame(HELLO, producer_id + struct.pack(">I", epoch))


def submit(sequence: int, records: Iterable[bytes]) -> bytes:
    values = list(records)
    if not 0 <= sequence <= 0xFFFFFFFFFFFFFFFF or not 1 <= len(values) <= MAX_RECORDS:
        raise ValueError("invalid sequence or record count")
    body = bytearray(struct.pack(">QI", sequence, len(values)))
    for record in values:
        if len(record) > 0xFFFFFFFF:
            raise ValueError("record too large")
        body.extend(struct.pack(">I", len(record)))
        body.extend(record)
    return _frame(SUBMIT, bytes(body))


def ack(epoch: int, sequence: int, first: int, next_offset: int) -> bytes:
    if not 0 < epoch <= 0xFFFFFFFF or not 0 <= first < next_offset <= 0xFFFFFFFFFFFFFFFF:
        raise ValueError("invalid Ack")
    return _frame(ACK, struct.pack(">IQQQ", epoch, sequence, first, next_offset))


def decode(frame: bytes) -> tuple[int, bytes]:
    if len(frame) < 9:
        raise ValueError("truncated frame")
    (length,) = struct.unpack(">I", frame[:4])
    if length > MAX_FRAME_BYTES or len(frame) != length + 4:
        raise ValueError("invalid frame length")
    if frame[4:8] != MAGIC:
        raise ValueError("unsupported magic/version")
    kind = frame[8]
    if kind not in (HELLO, SUBMIT, ACK, ERROR, CLOSE):
        raise ValueError("unknown frame type")
    return kind, frame[9:]


def self_test() -> None:
    vectors = json.loads(Path(__file__).parents[1].joinpath("producer-wire-v1-vectors.json").read_text())
    h = vectors["hello"]
    assert hello(bytes.fromhex(h["producer_id_hex"]), h["producer_epoch"]).hex() == h["frame_hex"]
    s = vectors["submit"]
    assert submit(s["sequence"], [bytes.fromhex(x) for x in s["records_hex"]]).hex() == s["frame_hex"]
    a = vectors["ack"]
    assert ack(a["producer_epoch"], a["sequence"], a["first_offset"], a["next_offset"]).hex() == a["frame_hex"]
    assert decode(bytes.fromhex(s["frame_hex"]))[0] == SUBMIT
    print("producer-wire-v1 python vectors: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if not args.self_test:
        parser.error("only --self-test is available until the Scribe Wire listener lands")
    self_test()
