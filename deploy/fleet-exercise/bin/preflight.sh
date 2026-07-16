#!/usr/bin/env bash
# Preflight for the multi-machine fleet exercise. Never prints secret values.
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

if [[ -z "$ENV_FILE" ]]; then
  ENV_FILE="${R2_ENV_FILE:-}"
fi

echo "fleet-exercise preflight (lab)"
echo "backend=${FLEET_EXERCISE_BACKEND:-unset} owner=${OWNER_HOST:-} standby=${STANDBY_HOST:-}"

need_host() {
  local host="$1"
  echo -n "ssh ${host}: "
  if ssh -o BatchMode=yes -o ConnectTimeout=5 "$host" 'uname -m; command -v rustc >/dev/null && rustc -V || echo rustc=missing; command -v zerotier-cli >/dev/null && echo zerotier=present || echo zerotier=missing' ; then
    :
  else
    echo "UNREACHABLE"
    return 1
  fi
}

need_host "$OWNER_HOST"
need_host "$STANDBY_HOST"
IFS=',' read -r -a producers <<< "${PRODUCER_HOSTS:-}"
for host in "${producers[@]}"; do
  [[ -n "$host" ]] || continue
  need_host "$host"
done

if [[ "${FLEET_EXERCISE_BACKEND:-}" == "r2" ]]; then
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
