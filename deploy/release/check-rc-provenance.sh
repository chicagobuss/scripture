#!/usr/bin/env bash
# Exact RC provenance gate (WP09). Exit 0 only when identities agree.
# Usage: check-rc-provenance.sh <rc-manifest.toml> <scripture_image>
# Prints a one-line status on stdout; details on stderr.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
manifest="${1:?rc manifest path}"
image="${2:?scripture image ref}"

if [[ ! -f "$manifest" ]]; then
  echo "absent"
  exit 2
fi

if ! [[ "$image" =~ ^[a-z0-9._/-]+@sha256:[a-f0-9]{64}$ ]]; then
  echo "bad-image" >&2
  echo "SCRIPTURE_IMAGE must be name@sha256:<64 lowercase hex> (got non-digest or placeholder)" >&2
  echo "fail"
  exit 1
fi
image_digest="${image##*@}"

cd "$root"
if [[ -n "$(git status --porcelain)" ]]; then
  echo "dirty working tree" >&2
  echo "fail"
  exit 1
fi
head_commit="$(git rev-parse HEAD)"
lock_hash="sha256:$(sha256sum Cargo.lock | awk '{print $1}')"

python3 - "$manifest" "$head_commit" "$lock_hash" "$image_digest" <<'PY'
import sys, tomllib
path, head, lock_hash, image_digest = sys.argv[1:5]
with open(path, "rb") as f:
    data = tomllib.load(f)

def fail(msg: str) -> None:
    print(msg, file=sys.stderr)
    print("fail")
    sys.exit(1)

for key in ("REPLACE", "sha256:REPLACE", "REPLACE_WITH_CLEAN_COMMIT"):
    blob = repr(data)
    if key in blob:
        fail(f"rc manifest still contains placeholder {key}")

src = data.get("source_commit")
if not isinstance(src, str) or src != head:
    fail(f"source_commit mismatch: manifest={src!r} HEAD={head!r}")

cargo = data.get("cargo") or {}
mh = cargo.get("lockfile_hash")
if mh != lock_hash:
    fail(f"lockfile_hash mismatch: manifest={mh!r} computed={lock_hash!r}")

image = data.get("image") or {}
md = image.get("digest")
if md != image_digest:
    fail(f"image.digest mismatch: manifest={md!r} overlay={image_digest!r}")

packages = data.get("packages") or {}
required = ("scripture", "scripture-service", "scripture-runtime", "scripture-cli")
for name in required:
    entry = packages.get(name)
    if not isinstance(entry, dict):
        fail(f"packages.{name} missing")
    ver = entry.get("version")
    cksum = entry.get("checksum")
    if not isinstance(ver, str) or not ver:
        fail(f"packages.{name}.version missing")
    if not isinstance(cksum, str) or not cksum.startswith("sha256:") or "REPLACE" in cksum:
        fail(f"packages.{name}.checksum missing or placeholder")

holylog = data.get("holylog") or {}
if holylog.get("git_tag") != "v0.2.2":
    fail(f"holylog.git_tag must be v0.2.2 (got {holylog.get('git_tag')!r})")

print("pass", file=sys.stderr)
print("present")
sys.exit(0)
PY
