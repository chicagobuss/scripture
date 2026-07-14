#!/usr/bin/env bash
# Owner crash experiment: kill -9 owner under load; standby must not self-promote.
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

echo "fleet-exercise owner-crash run_id=$RUN_ID (expect RecoveryRequired on restart; no auto-promote)"

# shellcheck disable=SC2029
ssh "$OWNER_HOST" "kill -9 \$(cat '${REMOTE_SUMMARY_DIR}/${RUN_ID}/owner.pid') || true"

sleep 2
# Restart old owner without bootstrap — must refuse open-log reattach.
# shellcheck disable=SC2029
ssh "$OWNER_HOST" "'${REMOTE_BIN_DIR}/fleet-lab-node' \
  --backend '${FLEET_EXERCISE_BACKEND}' --run-id '${RUN_ID}' \
  --bind '${OWNER_BIND}' --advertise '${OWNER_ADVERTISE}' \
  --owner '${OWNER_ID}' \
  --summary-dir '${REMOTE_SUMMARY_DIR}/${RUN_ID}' \
  --env-file '${REMOTE_BIN_DIR}/r2.env' \
  >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/owner-restart.log' 2>&1; echo exit=\$?" \
  | tee "/tmp/fleet-owner-restart-${RUN_ID}.log" || true

if grep -q 'RecoveryRequired' "/tmp/fleet-owner-restart-${RUN_ID}.log" \
  || ssh "$OWNER_HOST" "grep -q RecoveryRequired '${REMOTE_SUMMARY_DIR}/${RUN_ID}/owner-restart.log'"; then
  echo "ok: restart reported RecoveryRequired"
else
  echo "WARN: could not confirm RecoveryRequired text; inspect owner-restart.log" >&2
fi

echo "Confirm standby status endpoint still Standby (no ownership routes; curl localhost on standby):"
echo "  ssh ${STANDBY_HOST} 'curl -s http://127.0.0.1:9100/'"
echo "Expected: role Standby / NotOwner. Do NOT seal-and-replace in this script (Decision 0012 open)."
