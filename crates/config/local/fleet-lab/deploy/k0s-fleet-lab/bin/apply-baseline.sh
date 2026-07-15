#!/usr/bin/env bash
# Apply generic product manifests (bootstrap Job + plain owner/standby + Service).
# Requires: filled ConfigMaps/Secret in-cluster, product image imported.
# Does NOT seal-and-replace. Owner may be RecoveryRequired after bootstrap Job.
set -euo pipefail
SCRIPTURE_DIR="${SCRIPTURE_DIR:-$(cd "$(dirname "$0")/../../../../../.." && pwd)}"
NS="${NAMESPACE:-scripture-lab}"
GENERIC="$SCRIPTURE_DIR/deploy/kubernetes"
[[ -d "$GENERIC" ]] || { echo "missing $GENERIC" >&2; exit 1; }

echo "applying product baseline from $GENERIC into namespace $NS (ha_claim=false)"
kubectl -n "$NS" apply -f "$GENERIC/configmap-owner.yaml"
kubectl -n "$NS" apply -f "$GENERIC/configmap-standby.yaml"
kubectl -n "$NS" apply -f "$GENERIC/service-owner.yaml"
kubectl -n "$NS" apply -f "$GENERIC/job-bootstrap.yaml"
kubectl -n "$NS" apply -f "$GENERIC/deployment-standby.yaml"
kubectl -n "$NS" apply -f "$GENERIC/deployment-owner.yaml"
echo "applied. Fill REPLACE_WITH_* in ConfigMaps before expect Serving."
echo "Post-bootstrap owner may be RecoveryRequired until an accepted first-Serving decision."
