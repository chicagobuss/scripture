#!/usr/bin/env bash
# Render ignored per-environment YAML from templates + env profile.
# Never embeds secret values — only non-secret store endpoint/bucket/region/prefix.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${1:-}"
[[ -n "$PROFILE" && -f "$PROFILE" ]] || {
  echo "usage: $0 PATH_TO_PROFILE.env" >&2
  echo "profile must define: STORE_BACKEND STORE_ENDPOINT STORE_BUCKET STORE_REGION STORE_PREFIX" >&2
  echo "optional: OWNER_ADVERTISE STANDBY_ADVERTISE (tcp://…)" >&2
  exit 2
}

# shellcheck disable=SC1090
source "$PROFILE"
: "${STORE_BACKEND:?}" "${STORE_ENDPOINT:?}" "${STORE_BUCKET:?}" "${STORE_REGION:?}" "${STORE_PREFIX:?}"
OWNER_ADVERTISE="${OWNER_ADVERTISE:-tcp://scripture-owner:9000}"
STANDBY_ADVERTISE="${STANDBY_ADVERTISE:-tcp://scripture-standby:9000}"

OUT="$ROOT/deploy/fleet-exercise/config/rendered"
mkdir -p "$OUT"
TEMPLATE_OWNER="$ROOT/deploy/fleet-exercise/config/templates/owner.yaml"
TEMPLATE_STANDBY="$ROOT/deploy/fleet-exercise/config/templates/standby.yaml"
[[ -f "$TEMPLATE_OWNER" && -f "$TEMPLATE_STANDBY" ]] || {
  echo "missing templates under config/templates/" >&2
  exit 1
}

sed \
  -e "s|REPLACE_WITH_ENDPOINT|${STORE_ENDPOINT}|g" \
  -e "s|REPLACE_WITH_BUCKET|${STORE_BUCKET}|g" \
  -e "s|REPLACE_WITH_PREFIX|${STORE_PREFIX}|g" \
  -e "s|tcp://scripture-owner:9000|${OWNER_ADVERTISE}|g" \
  -e "s|backend: r2|backend: ${STORE_BACKEND}|g" \
  -e "s|region: auto|region: ${STORE_REGION}|g" \
  "$TEMPLATE_OWNER" >"$OUT/owner.yaml"
sed \
  -e "s|REPLACE_WITH_ENDPOINT|${STORE_ENDPOINT}|g" \
  -e "s|REPLACE_WITH_BUCKET|${STORE_BUCKET}|g" \
  -e "s|REPLACE_WITH_PREFIX|${STORE_PREFIX}|g" \
  -e "s|tcp://scripture-standby:9000|${STANDBY_ADVERTISE}|g" \
  -e "s|backend: r2|backend: ${STORE_BACKEND}|g" \
  -e "s|region: auto|region: ${STORE_REGION}|g" \
  "$TEMPLATE_STANDBY" >"$OUT/standby.yaml"

for f in "$OUT/owner.yaml" "$OUT/standby.yaml"; do
  if grep -E 'REPLACE_WITH_' "$f" >/dev/null; then
    echo "render left REPLACE_WITH_* in $f" >&2
    exit 1
  fi
done

echo "rendered $OUT/owner.yaml and $OUT/standby.yaml (non-secret only)"
