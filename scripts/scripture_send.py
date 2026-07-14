#!/usr/bin/env python3
"""Tiny dependency-free producer for Scripture's provisional raw-lines lab.

Each input line is one opaque record.  The server returns one committed-only
``OK <first-offset> <next-offset>`` or ``ERR <reason>`` line per record.
This is a testing harness, not a stable client SDK or a schema-aware protocol.
"""

from __future__ import annotations

import argparse
import socket
import sys
import time
from collections import deque
from pathlib import Path
from typing import BinaryIO, Deque, Iterator


def endpoint(value: str) -> tuple[str, int]:
    """Parse HOST:PORT or [IPv6]:PORT without inventing a URL protocol."""
    if value.startswith("["):
        host, separator, port = value[1:].partition("]:")
        if not separator:
            raise argparse.ArgumentTypeError("IPv6 endpoint must be [host]:port")
    else:
        host, separator, port = value.rpartition(":")
        if not separator or not host:
            raise argparse.ArgumentTypeError("endpoint must be HOST:PORT")
    try:
        parsed_port = int(port)
    except ValueError as error:
        raise argparse.ArgumentTypeError("endpoint port must be an integer") from error
    if not 1 <= parsed_port <= 65535:
        raise argparse.ArgumentTypeError("endpoint port must be 1..65535")
    return host, parsed_port


def input_lines(stream: BinaryIO) -> Iterator[bytes]:
    for number, line in enumerate(stream, start=1):
        payload = line.rstrip(b"\r\n")
        if b"\n" in payload or b"\r" in payload:
            raise ValueError(f"input line {number} contains an embedded newline")
        yield payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="127.0.0.1:9000", type=endpoint)
    parser.add_argument("--file", type=Path, help="read newline-delimited records from this file")
    parser.add_argument(
        "--inflight",
        type=int,
        default=1,
        help="records to send before reading an ACK (default: 1)",
    )
    parser.add_argument(
        "--ack-timeout-secs",
        type=float,
        default=5.0,
        help="socket connect and ACK timeout (default: 5)",
    )
    parser.add_argument("--verbose", action="store_true", help="print each server response")
    arguments = parser.parse_args()
    if arguments.inflight < 1:
        parser.error("--inflight must be positive")
    if arguments.ack_timeout_secs <= 0:
        parser.error("--ack-timeout-secs must be positive")
    return arguments


def receive_ack(reader: BinaryIO, sent: bytes, timeout: float) -> tuple[bool, str]:
    # `socket.settimeout` also applies to the buffered file object's next read.
    response = reader.readline()
    if not response:
        raise ConnectionError("server closed the connection before an ACK")
    text = response.rstrip(b"\r\n").decode("utf-8", errors="replace")
    if text.startswith("OK "):
        return True, text
    if text.startswith("ERR "):
        return False, text
    raise ConnectionError(f"unexpected response for {len(sent)}-byte record: {text!r}")


def run(arguments: argparse.Namespace, stream: BinaryIO) -> int:
    accepted = rejected = sent_bytes = 0
    started = time.monotonic()
    pending: Deque[bytes] = deque()
    try:
        with socket.create_connection(arguments.endpoint, timeout=arguments.ack_timeout_secs) as conn:
            conn.settimeout(arguments.ack_timeout_secs)
            with conn.makefile("rb") as reader:
                for payload in input_lines(stream):
                    conn.sendall(payload + b"\n")
                    pending.append(payload)
                    sent_bytes += len(payload)
                    if len(pending) >= arguments.inflight:
                        ok, response = receive_ack(reader, pending.popleft(), arguments.ack_timeout_secs)
                        accepted += int(ok)
                        rejected += int(not ok)
                        if arguments.verbose:
                            print(response)
                while pending:
                    ok, response = receive_ack(reader, pending.popleft(), arguments.ack_timeout_secs)
                    accepted += int(ok)
                    rejected += int(not ok)
                    if arguments.verbose:
                        print(response)
    except (OSError, ValueError) as error:
        print(f"scripture-send: {error}", file=sys.stderr)
        return 3

    elapsed_ms = round((time.monotonic() - started) * 1000)
    print(
        f"scripture-send endpoint={arguments.endpoint[0]}:{arguments.endpoint[1]} "
        f"accepted_records={accepted} rejected_records={rejected} "
        f"sent_bytes={sent_bytes} elapsed_ms={elapsed_ms}"
    )
    return 2 if rejected else 0


def main() -> None:
    arguments = parse_args()
    if arguments.file is None:
        raise SystemExit(run(arguments, sys.stdin.buffer))
    try:
        with arguments.file.open("rb") as stream:
            raise SystemExit(run(arguments, stream))
    except OSError as error:
        print(f"scripture-send: {error}", file=sys.stderr)
        raise SystemExit(3) from error


if __name__ == "__main__":
    main()
