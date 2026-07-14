#!/usr/bin/env python3
"""Tiny dependency-free HTTP request harness for a future Scripture ingress API.

There is intentionally no HTTP ingest endpoint today.  This script is useful
for verifying that fact, and will send opaque bytes to an explicitly supplied
URL once an HTTP transport decision lands.  It is not a claim that a stable
HTTP protocol, schema, acknowledgement level, or authentication scheme exists.
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="explicit HTTP(S) endpoint to exercise")
    parser.add_argument("--file", type=Path, help="request body file; defaults to stdin")
    parser.add_argument("--method", default="POST", help="HTTP method (default: POST)")
    parser.add_argument(
        "--content-type",
        default="application/octet-stream",
        help="Content-Type sent unchanged (default: application/octet-stream)",
    )
    parser.add_argument("--timeout-secs", type=float, default=5.0)
    parser.add_argument(
        "--bearer-env",
        help="environment variable containing a bearer token; never put secrets on argv",
    )
    parser.add_argument("--show-body", action="store_true", help="print the response body")
    arguments = parser.parse_args()
    if arguments.timeout_secs <= 0:
        parser.error("--timeout-secs must be positive")
    return arguments


def body(arguments: argparse.Namespace) -> bytes:
    if arguments.file is None:
        return sys.stdin.buffer.read()
    return arguments.file.read_bytes()


def main() -> None:
    arguments = parse_args()
    headers = {"Content-Type": arguments.content_type}
    if arguments.bearer_env:
        token = os.environ.get(arguments.bearer_env)
        if not token:
            print(f"scripture-http: {arguments.bearer_env} is unset or empty", file=sys.stderr)
            raise SystemExit(3)
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        arguments.url,
        data=body(arguments),
        headers=headers,
        method=arguments.method.upper(),
    )
    try:
        with urllib.request.urlopen(request, timeout=arguments.timeout_secs) as response:
            response_body = response.read()
            print(f"scripture-http status={response.status} response_bytes={len(response_body)}")
            if arguments.show_body and response_body:
                sys.stdout.buffer.write(response_body)
                if not response_body.endswith(b"\n"):
                    sys.stdout.buffer.write(b"\n")
            raise SystemExit(0 if 200 <= response.status < 300 else 2)
    except urllib.error.HTTPError as error:
        response_body = error.read()
        print(f"scripture-http status={error.code} response_bytes={len(response_body)}", file=sys.stderr)
        if arguments.show_body and response_body:
            sys.stderr.buffer.write(response_body + (b"" if response_body.endswith(b"\n") else b"\n"))
        raise SystemExit(2) from error
    except OSError as error:
        print(f"scripture-http: {error}", file=sys.stderr)
        raise SystemExit(3) from error


if __name__ == "__main__":
    main()
