#!/usr/bin/env bash
# Steady-state lab exercise: owner Serving, standby Standby, two producer loads.
# Does not claim HA. Secrets via remote env file path only.
set -euo pipefail

INVENTORY=""
ENV_FILE=""
RUN_ID=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory) INVENTORY="$2"; shift 2 ;;
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$INVENTORY" && -n "$RUN_ID" ]] || {
  echo "usage: $0 --inventory PATH --run-id ID [--env-file PATH]" >&2
  exit 2
}
# shellcheck disable=SC1090
source "$INVENTORY"
ENV_FILE="${ENV_FILE:-${R2_ENV_FILE:-}}"
BACKEND="${FLEET_EXERCISE_BACKEND:-rustfs}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
RESULT_DIR="$ROOT/deploy/fleet-exercise/results/$RUN_ID"
mkdir -p "$RESULT_DIR"

echo "fleet-exercise steady-state run_id=$RUN_ID backend=$BACKEND (lab; ha_claim=false)"

remote_start_owner() {
  local host="$1"
  ssh "$host" "mkdir -p '${REMOTE_BIN_DIR}' '${REMOTE_SUMMARY_DIR}/${RUN_ID}'"
  scp "$ROOT/deploy/fleet-exercise/bin/fleet-lab-node" "${host}:${REMOTE_BIN_DIR}/"
  if [[ "$BACKEND" == "r2" ]]; then
    scp "$ENV_FILE" "${host}:${REMOTE_BIN_DIR}/r2.env"
    ENV_ARGS=(--env-file "${REMOTE_BIN_DIR}/r2.env")
  else
    ENV_ARGS=()
  fi
  # shellcheck disable=SC2029
  ssh "$host" "nohup '${REMOTE_BIN_DIR}/fleet-lab-node' \
    --backend '${BACKEND}' --run-id '${RUN_ID}' \
    --bind '${OWNER_BIND}' --advertise '${OWNER_ADVERTISE}' \
    --owner '${OWNER_ID}' --bootstrap --loglet-id 'gen-a0' \
    --status-bind '${OWNER_STATUS_BIND}' \
    --summary-dir '${REMOTE_SUMMARY_DIR}/${RUN_ID}' \
    ${ENV_ARGS[*]+"${ENV_ARGS[@]}"} \
    >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/owner.log' 2>&1 & echo \$! >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/owner.pid'"
}

remote_start_standby() {
  local host="$1"
  ssh "$host" "mkdir -p '${REMOTE_BIN_DIR}' '${REMOTE_SUMMARY_DIR}/${RUN_ID}'"
  scp "$ROOT/deploy/fleet-exercise/bin/fleet-lab-node" "${host}:${REMOTE_BIN_DIR}/"
  if [[ "$BACKEND" == "r2" ]]; then
    scp "$ENV_FILE" "${host}:${REMOTE_BIN_DIR}/r2.env"
    ENV_ARGS=(--env-file "${REMOTE_BIN_DIR}/r2.env")
  else
    ENV_ARGS=()
  fi
  # shellcheck disable=SC2029
  ssh "$host" "nohup '${REMOTE_BIN_DIR}/fleet-lab-node' \
    --backend '${BACKEND}' --run-id '${RUN_ID}' \
    --bind '${STANDBY_BIND}' --advertise '${STANDBY_ADVERTISE}' \
    --owner '${STANDBY_ID}' \
    --status-bind '${STANDBY_STATUS_BIND}' \
    --summary-dir '${REMOTE_SUMMARY_DIR}/${RUN_ID}' \
    ${ENV_ARGS[*]+"${ENV_ARGS[@]}"} \
    >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/standby.log' 2>&1 & echo \$! >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/standby.pid'"
}

remote_start_owner "$OWNER_HOST"
sleep 3
remote_start_standby "$STANDBY_HOST"
sleep 2

IFS=',' read -r -a producers <<< "${PRODUCER_HOSTS:-}"
idx=0
for host in "${producers[@]}"; do
  [[ -n "$host" ]] || continue
  scp "$ROOT/deploy/fleet-exercise/bin/scripture-load" "${host}:${REMOTE_BIN_DIR}/"
  # shellcheck disable=SC2029
  ssh "$host" "'${REMOTE_BIN_DIR}/scripture-load' \
    --endpoint '${OWNER_ZT_IP}:9000' \
    --connections '${LOAD_CONNECTIONS}' \
    --record-bytes '${LOAD_RECORD_BYTES}' \
    --duration-secs '${LOAD_DURATION_SECS}' \
    --max-bytes '${LOAD_MAX_BYTES}' \
    --run-id '${RUN_ID}' \
    --backend '${BACKEND}' \
    --chunk-policy-name fleet-lab-64kib-phase-one \
    >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/load-${idx}.log' 2>&1" \
    | tee "$RESULT_DIR/load-${idx}.local.log" || true
  idx=$((idx + 1))
done

"$ROOT/deploy/fleet-exercise/bin/collect.sh" --inventory "$INVENTORY" --run-id "$RUN_ID"
echo "steady-state complete; artifacts in $RESULT_DIR"
