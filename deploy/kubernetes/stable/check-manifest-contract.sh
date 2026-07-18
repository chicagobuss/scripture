#!/usr/bin/env bash
# Assert stable example manifests keep owner DNS + least-privilege NetworkPolicy.
set -euo pipefail
root="$(cd "$(dirname "$0")/../../.." && pwd)"
stable="$root/deploy/kubernetes/stable"
failed=0

require() {
  local file="$1" pattern="$2" msg="$3"
  if ! grep -Eq "$pattern" "$file"; then
    echo "missing: $msg ($file)" >&2
    failed=1
  fi
}

require "$stable/services.yaml" 'name: scripture-actor-a' "owner Service scripture-actor-a"
require "$stable/services.yaml" 'name: scripture-actor-b' "owner Service scripture-actor-b"
require "$stable/configmaps.yaml" 'advertise: "tcp://scripture-actor-a:9000"' "A advertise uses owner Service DNS"
require "$stable/configmaps.yaml" 'advertise: "tcp://scripture-actor-b:9000"' "B advertise uses owner Service DNS"
require "$stable/networkpolicies.yaml" 'scripture.dev/client: producer' "producer NetworkPolicy from-selector"
require "$stable/networkpolicies.yaml" 'scripture.dev/client: admin' "admin NetworkPolicy from-selector"
if grep -A20 'name: producer-from-client' "$stable/networkpolicies.yaml" | grep -q 'port: 9200'; then
  echo "producer policy must not open admin port" >&2
  failed=1
fi
# Ensure admin policy has a from: block (not port-only).
if ! awk '/name: admin-from-client/,/^---$/ {print}' "$stable/networkpolicies.yaml" | grep -q 'from:'; then
  echo "admin-from-client must specify from:" >&2
  failed=1
fi
if ! awk '/name: producer-from-client/,/^---$/ {print}' "$stable/networkpolicies.yaml" | grep -q 'from:'; then
  echo "producer-from-client must specify from:" >&2
  failed=1
fi

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
echo "stable manifest contract ok"
