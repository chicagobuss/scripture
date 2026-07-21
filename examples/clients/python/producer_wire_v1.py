#!/usr/bin/env python3
"""Experimental Scripture Producer Wire v1 reference codec.

Run `python3 producer_wire_v1.py --self-test` to verify the shared golden
vectors, or give `--host`, `--port`, and `--payload` to contact an experimental
direct Scribe Producer Wire endpoint. It is not a production SDK.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import struct
import tempfile
from pathlib import Path
from typing import Iterable

MAGIC = b"SPW1"
HELLO, SUBMIT, ACK, ERROR, CLOSE = range(1, 6)
MAX_FRAME_BYTES = 4 * 1024 * 1024
MAX_RECORDS = 1024
OUTBOX_FILE = "producer-wire-outbox.json"
OUTBOX_LOCK = "producer-wire-outbox.lock"


class ProducerOutbox:
    """Small crash-safe local outbox for this reference client.

    This stores complete Wire Submit frames, never decoded payloads, so retry
    after a process crash emits the exact byte claim. It is an SDK/reference
    convenience format, not a portable interchange format with Rust's
    append-only outbox implementation.
    """

    def __init__(self, root: Path, producer_id: bytes, epoch: int, target: str) -> None:
        if not target:
            raise ValueError("--target must name one logical Canon/Verse")
        self.root, self.producer_id, self.epoch, self.target = root, producer_id, epoch, target
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.lock = root / OUTBOX_LOCK
        self._acquire_lock()
        try:
            self.state = self._load_or_create()
        except Exception:
            self._release_lock()
            raise

    def _acquire_lock(self) -> None:
        try:
            fd = os.open(self.lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError as error:
            raise RuntimeError(f"outbox already owned: {self.lock}") from error
        with os.fdopen(fd, "w") as file:
            file.write(str(os.getpid()))
            file.flush()
            os.fsync(file.fileno())

    def _release_lock(self) -> None:
        try:
            self.lock.unlink()
        except FileNotFoundError:
            pass

    def close(self) -> None:
        self._release_lock()

    def _identity(self) -> dict[str, object]:
        return {"producer_id_hex": self.producer_id.hex(), "epoch": self.epoch, "target": self.target}

    def _load_or_create(self) -> dict[str, object]:
        path = self.root / OUTBOX_FILE
        if not path.exists():
            state: dict[str, object] = {"format": "spw-reference-outbox-v1", "identity": self._identity(), "entries": {}}
            self._store(state)
            return state
        state = json.loads(path.read_text())
        if state.get("format") != "spw-reference-outbox-v1" or state.get("identity") != self._identity() or not isinstance(state.get("entries"), dict):
            raise RuntimeError("outbox durable identity/state does not match requested producer target")
        return state

    def _store(self, state: dict[str, object]) -> None:
        payload = (json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n").encode()
        with tempfile.NamedTemporaryFile(dir=self.root, prefix=".outbox-", delete=False) as file:
            temporary = Path(file.name)
            os.chmod(temporary, 0o600)
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self.root / OUTBOX_FILE)
        directory_fd = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def stage(self, encoded_submit: bytes) -> None:
        kind, body = decode(encoded_submit)
        if kind != SUBMIT:
            raise ValueError("outbox only stages Submit frames")
        (sequence,) = struct.unpack(">Q", body[:8])
        entries: dict[str, dict[str, object]] = self.state["entries"]  # type: ignore[assignment]
        key = str(sequence)
        encoded = base64.b64encode(encoded_submit).decode("ascii")
        if key in entries:
            if entries[key].get("submit_b64") != encoded:
                raise RuntimeError(f"IdentityConflict at sequence {sequence}")
            return
        expected = max((int(value) for value in entries), default=-1) + 1
        if sequence != expected:
            raise RuntimeError(f"outbox expected sequence {expected}, got {sequence}")
        entries[key] = {"submit_b64": encoded, "acknowledged": False}
        self._store(self.state)

    def acknowledge(self, epoch: int, sequence: int) -> None:
        if epoch != self.epoch:
            raise RuntimeError("outbox received an ACK for another epoch")
        entries: dict[str, dict[str, object]] = self.state["entries"]  # type: ignore[assignment]
        entry = entries.get(str(sequence))
        if entry is None:
            raise RuntimeError(f"outbox received an ACK for unknown sequence {sequence}")
        if not entry.get("acknowledged"):
            entry["acknowledged"] = True
            self._store(self.state)

    def staged(self, sequence: int) -> bytes:
        entries: dict[str, dict[str, object]] = self.state["entries"]  # type: ignore[assignment]
        entry = entries.get(str(sequence))
        if entry is None:
            raise RuntimeError(f"outbox sequence {sequence} was not staged")
        return base64.b64decode(entry["submit_b64"])


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


def _read_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Scribe closed before a complete frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_frame(sock: socket.socket) -> bytes:
    prefix = _read_exact(sock, 4)
    (length,) = struct.unpack(">I", prefix)
    if length > MAX_FRAME_BYTES:
        raise ValueError("peer declared oversized frame")
    return prefix + _read_exact(sock, length)


def send_once(host: str, port: int, producer_id: bytes, epoch: int, sequence: int, payload: bytes,
              outbox: ProducerOutbox | None = None) -> None:
    """Submit one record and print only the received durable outcome.

    A timeout or connection loss is deliberately raised as ambiguous. A caller
    may reconnect and retry this exact `(producer_id, epoch, sequence, payload)`
    tuple; it must never advance the sequence merely because a reply was lost.
    """
    encoded_submit = submit(sequence, [payload])
    if outbox is not None:
        outbox.stage(encoded_submit)
        encoded_submit = outbox.staged(sequence)
    with socket.create_connection((host, port), timeout=10) as sock:
        sock.sendall(hello(producer_id, epoch) + encoded_submit)
        kind, body = decode(_read_frame(sock))
    if kind == ACK:
        ack_epoch, ack_sequence, first, next_offset = struct.unpack(">IQQQ", body)
        if ack_epoch != epoch or ack_sequence != sequence:
            raise ValueError("Scribe ACK does not match submitted identity")
        if outbox is not None:
            outbox.acknowledge(ack_epoch, ack_sequence)
        print(json.dumps({"verdict": "ack", "epoch": ack_epoch, "sequence": ack_sequence,
                          "first_offset": first, "next_offset": next_offset}))
        return
    if kind == ERROR:
        if len(body) < 17:
            raise ValueError("truncated Error")
        error_epoch, error_sequence = struct.unpack(">IQ", body[:12])
        code = body[12]
        (size,) = struct.unpack(">I", body[13:17])
        if len(body) != 17 + size:
            raise ValueError("invalid Error length")
        message = body[17:].decode("utf-8")
        raise RuntimeError(f"Scribe error epoch={error_epoch} sequence={error_sequence} code={code}: {message}")
    raise ValueError(f"expected Ack or Error, got frame type {kind}")


def self_test() -> None:
    vectors = json.loads(Path(__file__).parents[1].joinpath("producer-wire-v1-vectors.json").read_text())
    h = vectors["hello"]
    assert hello(bytes.fromhex(h["producer_id_hex"]), h["producer_epoch"]).hex() == h["frame_hex"]
    s = vectors["submit"]
    assert submit(s["sequence"], [bytes.fromhex(x) for x in s["records_hex"]]).hex() == s["frame_hex"]
    a = vectors["ack"]
    assert ack(a["producer_epoch"], a["sequence"], a["first_offset"], a["next_offset"]).hex() == a["frame_hex"]
    assert decode(bytes.fromhex(s["frame_hex"]))[0] == SUBMIT
    with tempfile.TemporaryDirectory() as directory:
        outbox = ProducerOutbox(Path(directory), b"producer-py-demo", 1, "canon/demo/verse/one")
        first = submit(0, [b"one"])
        outbox.stage(first)
        assert outbox.staged(0) == first
        try:
            outbox.stage(submit(0, [b"changed"]))
            raise AssertionError("changed retry must fail")
        except RuntimeError as error:
            assert "IdentityConflict" in str(error)
        outbox.close()
    print("producer-wire-v1 python vectors: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--payload")
    parser.add_argument("--producer-id", default="producer-py-demo")
    parser.add_argument("--epoch", type=int, default=1)
    parser.add_argument("--sequence", type=int, default=0)
    parser.add_argument("--outbox", type=Path)
    parser.add_argument("--target", help="stable logical Canon/Verse label required with --outbox")
    args = parser.parse_args()
    if args.self_test:
        self_test()
    elif args.host and args.port and args.payload is not None:
        producer_id = args.producer_id.encode("ascii")
        if len(producer_id) != 16:
            parser.error("--producer-id must be exactly 16 ASCII bytes")
        if (args.outbox is None) != (args.target is None):
            parser.error("--outbox and --target must be used together")
        outbox = ProducerOutbox(args.outbox, producer_id, args.epoch, args.target) if args.outbox else None
        try:
            send_once(args.host, args.port, producer_id, args.epoch, args.sequence,
                      args.payload.encode("utf-8"), outbox)
        finally:
            if outbox is not None:
                outbox.close()
    else:
        parser.error("use --self-test or --host HOST --port PORT --payload TEXT")
