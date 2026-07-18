#!/usr/bin/env bash
# Verify release provenance against an operator-local registry-only attestation
# plus live containerd import checks on writer nodes.
#
# Usage:
#   check-rc-provenance.sh <rc-manifest.toml> <scripture_image> [attestation.toml]
#
# Env:
#   VERIFY_NODE_IMPORTS=0  skip ssh/ctr checks (render-only); live execute must leave default on
#
# Exit: 0 present/pass, 1 fail, 2 absent/incomplete
# Prints one status token on stdout; details on stderr.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
manifest="${1:?rc manifest path}"
image="${2:?scripture image ref}"
attestation="${3:-$root/config/local/kellnr/registry-build-attestation.toml}"
verify_imports="${VERIFY_NODE_IMPORTS:-1}"

if [[ ! -f "$manifest" ]]; then
  echo "absent"
  exit 2
fi
if [[ ! -f "$attestation" ]]; then
  echo "attestation absent: $attestation" >&2
  echo "absent"
  exit 2
fi

if ! [[ "$image" =~ ^[a-z0-9._/-]+@sha256:[a-f0-9]{64}$ ]]; then
  echo "SCRIPTURE_IMAGE must be name@sha256:<64 lowercase hex>" >&2
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
err="$(mktemp "${TMPDIR:-/tmp}/rc-prov.XXXXXX")"
trap 'rm -f "$err"' EXIT

set +e
python3 - "$manifest" "$attestation" "$head_commit" "$lock_hash" "$image_digest" <<'PY' >"$err" 2>&1
import sys, tomllib

manifest_path, attestation_path, head, lock_hash, image_digest = sys.argv[1:6]

def fail(msg: str, code: int = 1, status: str = "fail") -> None:
    print(msg, file=sys.stderr)
    print(status)
    sys.exit(code)

def load(path: str) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)

manifest = load(manifest_path)
att = load(attestation_path)

for blob_name, data in (("rc-manifest", manifest), ("attestation", att)):
    blob = repr(data)
    for key in ("REPLACE", "sha256:REPLACE", "REPLACE_WITH_CLEAN_COMMIT"):
        if key in blob:
            fail(f"{blob_name} still contains placeholder {key}")

if att.get("attestation_kind") != "clean-registry-only":
    fail("attestation_kind must be clean-registry-only")

builder = att.get("builder") or {}
for flag in (
    "no_path_overrides",
    "no_git_dependency_sources",
    "no_local_holylog_checkout",
):
    if builder.get(flag) is not True:
        fail(f"builder.{flag} must be true")
regs = builder.get("cargo_registries") or []
if "fleet" not in regs:
    fail("builder.cargo_registries must include fleet")

src = manifest.get("source_commit")
att_src = att.get("source_commit")
if src != head or att_src != head:
    fail(f"source_commit mismatch manifest={src!r} attestation={att_src!r} HEAD={head!r}")

cargo = manifest.get("cargo") or {}
if cargo.get("lockfile_hash") != lock_hash:
    fail(
        f"lockfile_hash mismatch: manifest={cargo.get('lockfile_hash')!r} computed={lock_hash!r}"
    )

image = manifest.get("image") or {}
att_image = att.get("image") or {}
if image.get("digest") != image_digest or att_image.get("digest") != image_digest:
    fail(
        "image.digest mismatch between overlay, rc-manifest, and attestation "
        f"(overlay={image_digest!r} manifest={image.get('digest')!r} att={att_image.get('digest')!r})"
    )

packages = manifest.get("packages") or {}
resolved = att.get("resolved_packages") or {}
required = (
    "scripture",
    "scripture-service",
    "scripture-runtime",
    "scripture-cli",
    "holylog",
    "holylog-correctness",
)
for name in required:
    ment = packages.get(name) if name.startswith("scripture") else None
    # Holylog may live only under attestation resolved_packages + [holylog] versions.
    rent = resolved.get(name)
    if name.startswith("scripture"):
        if not isinstance(ment, dict):
            fail(f"rc packages.{name} missing")
        if not isinstance(rent, dict):
            fail(f"attestation resolved_packages.{name} missing")
        if ment.get("checksum") != rent.get("checksum"):
            fail(f"{name} checksum mismatch between rc-manifest and attestation")
        if ment.get("version") != rent.get("version"):
            fail(f"{name} version mismatch between rc-manifest and attestation")
    else:
        if not isinstance(rent, dict):
            fail(f"attestation resolved_packages.{name} missing")

    if not isinstance(rent, dict):
        fail(f"resolved_packages.{name} missing")
    source = rent.get("source")
    registry = rent.get("registry")
    cksum = rent.get("checksum")
    if source != "registry+fleet" or registry != "fleet":
        fail(
            f"{name} must be registry+fleet/fleet (got source={source!r} registry={registry!r})"
        )
    if not isinstance(cksum, str) or not cksum.startswith("sha256:") or len(cksum) < 71:
        fail(f"{name} checksum missing or not sha256:<digest>")
    for bad in ("git", "path", "github.com", "file://"):
        joined = " ".join(str(v) for v in rent.values()).lower()
        if bad in joined and bad != "git":  # 'git' alone too broad? check source field
            pass
    # Explicitly reject git/path source labels.
    if isinstance(source, str) and (
        source.startswith("git+")
        or source.startswith("path+")
        or "git" == source
        or source.startswith("git")
    ):
        fail(f"{name} source must not be git/path (got {source!r})")

holylog = manifest.get("holylog") or {}
if holylog.get("git_tag") != "v0.2.2":
    # Tag is historical identity; packages must still resolve from fleet.
    fail(f"holylog.git_tag must be v0.2.2 (got {holylog.get('git_tag')!r})")

imports = att.get("node_imports") or {}
for node in ("node-a", "node-b"):
    entry = imports.get(node)
    if not isinstance(entry, dict):
        fail(f"node_imports.{node} missing")
    if entry.get("imported_digest") != image_digest:
        fail(
            f"node_imports.{node}.imported_digest must equal image digest "
            f"(got {entry.get('imported_digest')!r})"
        )

print("identity checks ok", file=sys.stderr)
print("ok")
sys.exit(0)
PY
py_rc=$?
set -e
cat "$err" >&2
status="$(tail -n1 "$err" | tr -d '\r' || true)"

if [[ "$py_rc" -ne 0 ]]; then
  case "$status" in
    fail|absent|incomplete) echo "$status" ;;
    *) echo "fail" ;;
  esac
  exit "$py_rc"
fi

if [[ "$verify_imports" != "0" ]]; then
  mapfile -t hosts < <(python3 - "$attestation" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as f:
    data = tomllib.load(f)
for name, entry in (data.get("node_imports") or {}).items():
    if isinstance(entry, dict):
        print(entry.get("host") or name)
PY
)
  if [[ "${#hosts[@]}" -eq 0 ]]; then
    echo "node_imports hosts empty" >&2
    echo "fail"
    exit 1
  fi
  for host in "${hosts[@]}"; do
    [[ -z "$host" ]] && continue
    if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "$host" \
      "sudo k0s ctr images ls" 2>/dev/null | grep -F "$image_digest" >/dev/null; then
      echo "image digest $image_digest not present in containerd on $host" >&2
      echo "fail"
      exit 1
    fi
    echo "node import ok: $host has $image_digest" >&2
  done
fi

echo "present"
exit 0
