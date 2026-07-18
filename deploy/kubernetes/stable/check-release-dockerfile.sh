#!/usr/bin/env bash
# Fail if the release Dockerfile enables campaign-faults or copies an uncommitted binary.
set -euo pipefail
root="$(cd "$(dirname "$0")/../../.." && pwd)"
dockerfile="$root/deploy/kubernetes/Dockerfile"
if grep -E '^[^#]*--features[[:space:]]+campaign-faults|^[^#]*campaign-faults' "$dockerfile" >/dev/null; then
  echo "release Dockerfile must not enable campaign-faults: $dockerfile" >&2
  exit 1
fi
if grep -E 'COPY[[:space:]].*deploy/bin/|COPY[[:space:]].*target/release/scripture[[:space:]]' "$dockerfile" | grep -v 'COPY --from=builder' >/dev/null; then
  echo "release Dockerfile must not copy a host-prebuilt scripture binary" >&2
  exit 1
fi
if grep -E 'mount=type=ssh' "$dockerfile" >/dev/null; then
  echo "note: release Dockerfile still uses BuildKit SSH for Holylog git until Kellnr RC resolution lands" >&2
fi
echo "release Dockerfile checks ok"
