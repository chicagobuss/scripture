#!/usr/bin/env bash
# Preflight for product daemon fleet path. Never prints secret values.
set -euo pipefail

INVENTORY=""
ENV_FILE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory) INVENTORY="$2"; shift 2 ;;
    --env-file) ENV_FILE="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$INVENTORY" ]] || { echo "required: --inventory PATH" >&2; exit 2; }
# shellcheck disable=SC1090
source "$INVENTORY"

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPTURE_BIN="${SCRIPTURE_BIN:-$ROOT/deploy/fleet-exercise/bin/scripture}"
OWNER_CONFIG="${OWNER_CONFIG:-$ROOT/deploy/fleet-exercise/config/rendered/owner.yaml}"
STANDBY_CONFIG="${STANDBY_CONFIG:-$ROOT/deploy/fleet-exercise/config/rendered/standby.yaml}"

echo "product-fleet preflight (ha_claim=false)"
echo "owner=${OWNER_HOST:-} standby=${STANDBY_HOST:-}"

[[ -x "$SCRIPTURE_BIN" ]] || {
  echo "missing scripture binary at $SCRIPTURE_BIN (run build-release.sh)" >&2
  exit 1
}
echo "scripture binary: present"

need_host() {
  local host="$1"
  echo -n "ssh ${host}: "
  if ssh -o BatchMode=yes -o ConnectTimeout=5 "$host" 'uname -m'; then
    :
  else
    echo "UNREACHABLE"
    return 1
  fi
}

need_host "$OWNER_HOST"
need_host "$STANDBY_HOST"

for cfg in "$OWNER_CONFIG" "$STANDBY_CONFIG"; do
  [[ -f "$cfg" ]] || {
    echo "missing rendered config $cfg (run bin/render-config.sh)" >&2
    exit 1
  }
  if grep -E 'REPLACE_WITH_' "$cfg" >/dev/null; then
    echo "config still contains REPLACE_WITH_* placeholders: $cfg" >&2
    exit 1
  fi
  echo -n "validate $(basename "$cfg"): "
  "$SCRIPTURE_BIN" validate --config "$cfg"
done

if [[ -z "$ENV_FILE" ]]; then
  ENV_FILE="${R2_ENV_FILE:-}"
fi
BACKEND="${FLEET_EXERCISE_BACKEND:-rustfs}"
if [[ "$BACKEND" == "r2" ]]; then
  [[ -n "$ENV_FILE" && -f "$ENV_FILE" ]] || {
    echo "R2 env file missing (set R2_ENV_FILE or --env-file). Values not shown." >&2
    exit 1
  }
  echo "env-file: present ($(basename "$ENV_FILE"))"
  for key in R2_ENDPOINT R2_BUCKET R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY; do
    if grep -E "^${key}=" "$ENV_FILE" >/dev/null; then
      echo "  ${key}: present"
    else
      echo "  ${key}: MISSING" >&2
      exit 1
    fi
  done
fi

echo "preflight ok"
echo "NOTE: post-bootstrap owner serve may report RecoveryRequired until an accepted first-Serving decision exists. Do not authorize R2 on that gap."
