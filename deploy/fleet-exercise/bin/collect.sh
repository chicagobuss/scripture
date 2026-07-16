#!/usr/bin/env bash
# Collect remote summary JSON / logs into deploy/fleet-exercise/results/<run-id>/.
set -euo pipefail

INVENTORY=""
RUN_ID=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory) INVENTORY="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
[[ -n "$INVENTORY" && -n "$RUN_ID" ]] || {
  echo "usage: $0 --inventory PATH --run-id ID" >&2
  exit 2
}
# shellcheck disable=SC1090
source "$INVENTORY"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DEST="$ROOT/deploy/fleet-exercise/results/$RUN_ID"
mkdir -p "$DEST"

pull() {
  local host="$1"
  scp -q "${host}:${REMOTE_SUMMARY_DIR}/${RUN_ID}/*" "$DEST/" 2>/dev/null || true
}

pull "$OWNER_HOST"
pull "$STANDBY_HOST"
IFS=',' read -r -a producers <<< "${PRODUCER_HOSTS:-}"
for host in "${producers[@]}"; do
  [[ -n "$host" ]] || continue
  pull "$host"
done

echo "collected into $DEST"
ls -la "$DEST" || true
