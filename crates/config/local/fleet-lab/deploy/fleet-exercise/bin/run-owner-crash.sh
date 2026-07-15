#!/usr/bin/env bash
# Crash the remote owner process (SIGKILL). Observe RecoveryRequired locally.
# Does not claim HA or auto-failover.
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

echo "killing owner on $OWNER_HOST run_id=$RUN_ID"
# shellcheck disable=SC2029
ssh "$OWNER_HOST" "kill -9 \$(cat '${REMOTE_SUMMARY_DIR}/${RUN_ID}/owner.pid') || true"
sleep 1
# shellcheck disable=SC2029
ssh "$OWNER_HOST" "curl -sS --max-time 2 'http://127.0.0.1:${OWNER_STATUS_PORT:-9100}/status' || echo 'status unreachable (expected if probes died with process)'"
echo "owner crash issued (ha_claim=false; no auto promote)"
