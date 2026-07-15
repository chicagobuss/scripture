#!/usr/bin/env bash
# Steady-state product path: one-shot bootstrap, then plain serve for owner/standby.
# Does NOT call seal-and-replace. After bootstrap, owner may be RecoveryRequired
# until an accepted first-Serving decision exists — do not authorize R2 on that gap.
set -euo pipefail

INVENTORY=""
ENV_FILE=""
RUN_ID=""
BOOTSTRAP_LOGLET="${BOOTSTRAP_LOGLET:-gen-a0}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory) INVENTORY="$2"; shift 2 ;;
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --bootstrap-loglet) BOOTSTRAP_LOGLET="$2"; shift 2 ;;
    --takeover-successor)
      echo "refusing --takeover-successor: recovery/replace is not an accepted product surface" >&2
      exit 2
      ;;
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
BIN="$ROOT/deploy/fleet-exercise/bin/scripture"
OWNER_CONFIG="${OWNER_CONFIG:-$ROOT/deploy/fleet-exercise/config/rendered/owner.yaml}"
STANDBY_CONFIG="${STANDBY_CONFIG:-$ROOT/deploy/fleet-exercise/config/rendered/standby.yaml}"
RESULT_DIR="$ROOT/deploy/fleet-exercise/results/$RUN_ID"
mkdir -p "$RESULT_DIR"

[[ -x "$BIN" ]] || { echo "missing $BIN (run build-release.sh)" >&2; exit 1; }
[[ -f "$OWNER_CONFIG" && -f "$STANDBY_CONFIG" ]] || {
  echo "missing rendered configs (run bin/render-config.sh PROFILE.env)" >&2
  exit 1
}
grep -E 'REPLACE_WITH_' "$OWNER_CONFIG" "$STANDBY_CONFIG" >/dev/null && {
  echo "rendered configs still contain placeholders" >&2
  exit 1
}
"$BIN" validate --config "$OWNER_CONFIG"
"$BIN" validate --config "$STANDBY_CONFIG"

echo "product steady-state run_id=$RUN_ID backend=$BACKEND (ha_claim=false)"
echo "WARNING: owner serve after bootstrap may be RecoveryRequired (open decision). Not R2-ready."

stage_config() {
  local host="$1" local_cfg="$2" remote_name="$3"
  ssh "$host" "mkdir -p '${REMOTE_BIN_DIR}' '${REMOTE_SUMMARY_DIR}/${RUN_ID}' '${REMOTE_CONFIG_DIR:-/tmp/scripture-config}'"
  scp "$BIN" "${host}:${REMOTE_BIN_DIR}/scripture"
  scp "$local_cfg" "${host}:${REMOTE_CONFIG_DIR:-/tmp/scripture-config}/${remote_name}"
  if [[ "$BACKEND" == "r2" ]]; then
    scp "$ENV_FILE" "${host}:${REMOTE_BIN_DIR}/r2.env"
  fi
}

remote_env() {
  if [[ "$BACKEND" == "r2" ]]; then
    echo "set -a; source '${REMOTE_BIN_DIR}/r2.env'; set +a;"
  else
    echo ""
  fi
}

stage_config "$OWNER_HOST" "$OWNER_CONFIG" "owner.yaml"
stage_config "$STANDBY_HOST" "$STANDBY_CONFIG" "standby.yaml"

# shellcheck disable=SC2029
ssh "$OWNER_HOST" "$(remote_env) '${REMOTE_BIN_DIR}/scripture' bootstrap \
  --config '${REMOTE_CONFIG_DIR:-/tmp/scripture-config}/owner.yaml' \
  --loglet-id '${BOOTSTRAP_LOGLET}' \
  >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/bootstrap.log' 2>&1"

# Plain serve — no takeover.
# shellcheck disable=SC2029
ssh "$OWNER_HOST" "$(remote_env) nohup '${REMOTE_BIN_DIR}/scripture' serve \
  --config '${REMOTE_CONFIG_DIR:-/tmp/scripture-config}/owner.yaml' \
  >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/owner.log' 2>&1 & echo \$! >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/owner.pid'"

sleep 3

# shellcheck disable=SC2029
ssh "$STANDBY_HOST" "$(remote_env) nohup '${REMOTE_BIN_DIR}/scripture' serve \
  --config '${REMOTE_CONFIG_DIR:-/tmp/scripture-config}/standby.yaml' \
  >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/standby.log' 2>&1 & echo \$! >'${REMOTE_SUMMARY_DIR}/${RUN_ID}/standby.pid'"

echo "started. Inspect owner /status for RecoveryRequired vs Serving before any load/R2 claim."
echo "collect with collect.sh --inventory … --run-id $RUN_ID"
