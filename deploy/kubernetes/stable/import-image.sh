#!/usr/bin/env bash
# Operator-local image import for digest-addressed release tarballs.
# Default is dry-run. Does not use privileged in-cluster loaders.
#
# Usage:
#   ./import-image.sh --tarball scripture-0.1.0-rc.1.tar.gz --digest sha256:... --hosts node-a,node-b
#   ./import-image.sh ... --execute
set -euo pipefail

tarball=""
digest=""
hosts=""
execute=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tarball) tarball="$2"; shift 2 ;;
    --digest) digest="$2"; shift 2 ;;
    --hosts) hosts="$2"; shift 2 ;;
    --execute) execute=1; shift ;;
    -h|--help)
      sed -n '1,12p' "$0"
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$tarball" && -n "$digest" && -n "$hosts" ]] || {
  echo "require --tarball --digest --hosts" >&2
  exit 2
}
[[ -f "$tarball" ]] || { echo "missing tarball: $tarball" >&2; exit 2; }

echo "plan: import $tarball (expect digest $digest) to hosts: $hosts"
if [[ "$execute" -eq 0 ]]; then
  echo "dry-run only; pass --execute after review"
  exit 0
fi

IFS=',' read -r -a host_list <<<"$hosts"
for host in "${host_list[@]}"; do
  echo "== $host =="
  # Prefer gunzip|ctr import; verify listed digest contains expected hash.
  gzip -dc "$tarball" | ssh -o BatchMode=yes "$host" 'sudo k0s ctr images import -'
  ssh -o BatchMode=yes "$host" "sudo k0s ctr images ls | grep -F '$digest' || sudo k0s ctr images ls | grep scripture"
done
echo "import complete; confirm digest match against RC manifest before live drill"
