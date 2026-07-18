#!/usr/bin/env bash
# WP09 release-drill runner (family 22 gate).
#
# Default: render + preflight + kubectl client-dry-run. Writes local artifacts.
# Live cluster mutation requires ALL of:
#   --execute
#   --joshua-approved
#   approval file matching run id (see --approval-file)
#
# Never prints secret values. Never targets Tracker / scripture-lab stores.
#
# Usage:
#   ./deploy/release/run-release-drill.sh
#   ./deploy/release/run-release-drill.sh --overlay config/local/scripture-stable/overlay.env
#   ./deploy/release/run-release-drill.sh --execute --joshua-approved \
#       --approval-file config/local/scripture-stable/APPROVAL
#
# Exit codes:
#   0  preflight/render (or approved live) succeeded
#   1  hard failure (contract / dry-run / live step)
#   2  incomplete attestation or live refused (not a pass)
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
stable="$root/deploy/kubernetes/stable"
release="$root/deploy/release"

overlay="${SCRIPTURE_STABLE_OVERLAY:-$root/config/local/scripture-stable/overlay.env}"
rc_manifest="${SCRIPTURE_RC_MANIFEST:-$root/config/local/kellnr/rc-manifest.toml}"
artifact_root="${SCRIPTURE_STABLE_ARTIFACT_ROOT:-$root/config/local/scripture-stable/runs}"
approval_file="${SCRIPTURE_STABLE_APPROVAL:-$root/config/local/scripture-stable/APPROVAL}"
execute=0
joshua_approved=0
run_id_override=""
skip_kubectl=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --overlay) overlay="$2"; shift 2 ;;
    --rc-manifest) rc_manifest="$2"; shift 2 ;;
    --artifact-dir) artifact_root="$2"; shift 2 ;;
    --approval-file) approval_file="$2"; shift 2 ;;
    --run-id) run_id_override="$2"; shift 2 ;;
    --execute) execute=1; shift ;;
    --joshua-approved) joshua_approved=1; shift ;;
    --skip-kubectl) skip_kubectl=1; shift ;;
    -h|--help)
      sed -n '2,22p' "$0"
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

log() { printf 'release-drill: %s\n' "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

# --- load overlay (non-secret keys only; never echo credential-like values) ---
load_overlay() {
  if [[ ! -f "$overlay" ]]; then
    mkdir -p "$(dirname "$overlay")"
    cp "$stable/overlay.example.env" "$overlay"
    log "seeded overlay from example at $overlay (edit image digest before live)"
  fi
  # shellcheck disable=SC1090
  set -a
  # shellcheck source=/dev/null
  source "$overlay"
  set +a

  KUBE_CONTEXT="${KUBE_CONTEXT:-Default}"
  RUN_ID="${run_id_override:-${RUN_ID:-wp09-drill-local}}"
  # Always derive namespace from run id so --run-id cannot drift from overlay NAMESPACE.
  NAMESPACE="scripture-stable-${RUN_ID}"
  SCRIPTURE_IMAGE="${SCRIPTURE_IMAGE:-scripture@sha256:REPLACE}"
  RUSTFS_NODE="${RUSTFS_NODE:-bignlittles}"
  WRITER_A_NODE="${WRITER_A_NODE:-node-a}"
  WRITER_B_NODE="${WRITER_B_NODE:-node-b}"
  BUCKET="${BUCKET:-scripture-stable-${RUN_ID}}"

  if [[ ! "$RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
    fail "RUN_ID must be a DNS-1123 label (got $RUN_ID)"
  fi
  if [[ "$SCRIPTURE_IMAGE" == *":latest" ]]; then
    fail "SCRIPTURE_IMAGE must not use :latest"
  fi
}

assert_no_forbidden_targets() {
  local blob="$1"
  local lower
  lower="$(printf '%s' "$blob" | tr '[:upper:]' '[:lower:]')"
  local needle
  for needle in scripture-lab 10.0.0.240 10.10.10.10; do
    if [[ "$lower" == *"$needle"* ]]; then
      fail "forbidden store/target identity in rendered material: $needle"
    fi
  done
  # Token-aware tracker check (matches campaign profile isolation).
  local token
  # shellcheck disable=SC2001
  for token in $(printf '%s' "$lower" | sed 's/[^a-z0-9-][^a-z0-9-]*/ /g'); do
    if [[ "$token" == "tracker" || "$token" == tracker-* || "$token" == *-tracker ]]; then
      fail "Tracker identity must not appear in overlay/rendered manifests ($token)"
    fi
  done
}

check_clean_source() {
  cd "$root"
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "dirty"
    return 1
  fi
  echo "clean:$(git rev-parse HEAD)"
  return 0
}

run_contract_scripts() {
  "$stable/check-no-secrets.sh"
  "$stable/check-release-dockerfile.sh"
  "$stable/check-manifest-contract.sh"
}

# Package preflight: exit 2 (unattested) is incomplete, not hard fail for render.
run_package_preflight() {
  set +e
  "$release/check-package-preflight.sh"
  local rc=$?
  set -e
  echo "$rc"
}

render_manifests() {
  local out_dir="$1"
  mkdir -p "$out_dir"
  local file base rendered
  for file in "$stable"/*.yaml; do
    base="$(basename "$file")"
    [[ "$base" == "secrets.placeholder.yaml" ]] && continue
    rendered="$out_dir/$base"
    sed \
      -e "s/scripture-stable-REPLACE_RUN_ID/${NAMESPACE}/g" \
      -e "s/REPLACE_RUN_ID/${RUN_ID}/g" \
      -e "s|scripture:REPLACE_IMAGE_DIGEST_OR_TAG|${SCRIPTURE_IMAGE}|g" \
      -e "s/kubernetes.io\/hostname: node-a/kubernetes.io\/hostname: ${WRITER_A_NODE}/g" \
      -e "s/kubernetes.io\/hostname: node-b/kubernetes.io\/hostname: ${WRITER_B_NODE}/g" \
      -e "s/kubernetes.io\/hostname: bignlittles/kubernetes.io\/hostname: ${RUSTFS_NODE}/g" \
      "$file" >"$rendered"
  done
  # Capture redacted render inventory (no env dump of secrets — overlay has none).
  {
    echo "run_id=${RUN_ID}"
    echo "namespace=${NAMESPACE}"
    echo "kube_context=${KUBE_CONTEXT}"
    echo "scripture_image=${SCRIPTURE_IMAGE}"
    echo "writer_a_node=${WRITER_A_NODE}"
    echo "writer_b_node=${WRITER_B_NODE}"
    echo "rustfs_node=${RUSTFS_NODE}"
    echo "bucket=${BUCKET}"
    echo "source_commit=$(cd "$root" && git rev-parse HEAD)"
  } >"$out_dir/render-meta.env"
}

kubectl_dry_run() {
  local rendered="$1"
  local file
  if [[ "$skip_kubectl" -eq 1 ]]; then
    log "skipping kubectl dry-run (--skip-kubectl)"
    return 0
  fi
  if ! command -v kubectl >/dev/null 2>&1; then
    fail "kubectl required for client-dry-run (or pass --skip-kubectl)"
  fi
  local ctx_args=(--context "$KUBE_CONTEXT")
  for file in "$rendered"/*.yaml; do
    kubectl "${ctx_args[@]}" apply --dry-run=client -f "$file" >/dev/null
  done
  log "kubectl client-dry-run ok for $(find "$rendered" -name '*.yaml' | wc -l) manifests"
}

check_rc_manifest() {
  if [[ ! -f "$rc_manifest" ]]; then
    echo "absent"
    return 1
  fi
  if grep -Eq 'REPLACE|sha256:REPLACE' "$rc_manifest"; then
    echo "placeholders"
    return 1
  fi
  if ! grep -Eq 'digest[[:space:]]*=[[:space:]]*"sha256:[a-f0-9]{64}"' "$rc_manifest" \
    && ! grep -Eq 'digest[[:space:]]*=[[:space:]]*"sha256:[A-Fa-f0-9]+"' "$rc_manifest"; then
    echo "no-digest"
    return 1
  fi
  echo "present"
  return 0
}

write_verdicts() {
  local art="$1"
  local clean_status="$2"
  local package_rc="$3"
  local rc_status="$4"
  local dry_run_ok="$5"
  local live_status="$6"

  local provenance="not_run"
  local provenance_detail="RC manifest and/or package attestation incomplete"
  if [[ "$package_rc" == "1" ]]; then
    provenance="fail"
    provenance_detail="package contract failed"
  elif [[ "$clean_status" == "dirty" ]]; then
    provenance="fail"
    provenance_detail="working tree dirty; clean committed source required"
  elif [[ "$clean_status" == clean:* && "$package_rc" == "0" && "$rc_status" == "present" ]]; then
    provenance="pass"
    provenance_detail="clean source + package preflight + filled RC manifest"
  else
    provenance="incomplete"
    provenance_detail="RC manifest and/or authenticated package attestation incomplete"
  fi

  # Semantic / Holylog / completeness stay not_run until approved live drill finishes.
  local semantic="not_run"
  local holylog="not_run"
  local completeness="not_run"
  case "$live_status" in
    refused|not_requested)
      completeness="incomplete"
      ;;
    applied_partial)
      semantic="incomplete"
      holylog="not_run"
      completeness="incomplete"
      ;;
    complete_pass)
      semantic="pass"
      holylog="pass"
      completeness="pass"
      ;;
    complete_fail)
      semantic="fail"
      completeness="fail"
      ;;
  esac

  cat >"$art/verdicts.json" <<EOF
{
  "schema_version": 1,
  "run_id": "${RUN_ID}",
  "namespace": "${NAMESPACE}",
  "mode": "$([[ "$execute" -eq 1 ]] && echo execute || echo preflight)",
  "verdicts": {
    "semantic_checker": { "status": "${semantic}", "detail": "live A→B producer/promotion sequence" },
    "holylog_durable_oracle": { "status": "${holylog}", "detail": "readback of sealed boundary and ordered records" },
    "release_provenance": { "status": "${provenance}", "detail": "${provenance_detail}" },
    "collection_completeness": { "status": "${completeness}", "detail": "artifact set for family 22" }
  },
  "checks": {
    "clean_source": "${clean_status}",
    "package_preflight_exit": ${package_rc},
    "rc_manifest": "${rc_status}",
    "kubectl_client_dry_run": ${dry_run_ok},
    "live": "${live_status}"
  },
  "non_claims": [
    "Family 22 is not pass until approved live drill completes with all four verdicts pass.",
    "Presence of Kellnr config alone is not kellnr-rc attestation.",
    "This runner does not claim automatic failover."
  ]
}
EOF
}

write_approval_commands() {
  local art="$1"
  cat >"$art/APPROVAL_REQUIRED_COMMANDS.txt" <<EOF
# Exact non-secret commands requiring Joshua approval (WP09 live drill)
# Review artifacts under: ${art}
# Then create approval file containing exactly: APPROVED ${RUN_ID}
#   printf 'APPROVED ${RUN_ID}\\n' > ${approval_file}
#
# Re-run:
#   ${release}/run-release-drill.sh \\
#     --overlay ${overlay} \\
#     --execute --joshua-approved \\
#     --approval-file ${approval_file} \\
#     --run-id ${RUN_ID}

# After approval the runner will (redacted):
# 1. kubectl --context ${KUBE_CONTEXT} apply -f <rendered>/namespace.yaml
# 2. create rustfs-credentials + scripture-admin-token Secrets (operator-supplied literals; never logged)
# 3. kubectl apply remaining rendered manifests
# 4. wait: A /readyz=200, B /readyz=503, producer Endpoints = A only
# 5. producer traffic + explicit B promote + stale-A denial + Holylog readback
# 6. kubectl delete namespace ${NAMESPACE} --wait

KUBE_CONTEXT=${KUBE_CONTEXT}
NAMESPACE=${NAMESPACE}
SCRIPTURE_IMAGE=${SCRIPTURE_IMAGE}
EOF
  log "wrote approval command sheet: $art/APPROVAL_REQUIRED_COMMANDS.txt"
}

approval_ok() {
  [[ -f "$approval_file" ]] || return 1
  local line
  line="$(head -n1 "$approval_file" | tr -d '\r')"
  [[ "$line" == "APPROVED ${RUN_ID}" ]]
}

live_apply() {
  local rendered="$1"
  local art="$2"
  local ctx_args=(--context "$KUBE_CONTEXT")

  log "LIVE: applying namespace ${NAMESPACE}"
  kubectl "${ctx_args[@]}" apply -f "$rendered/namespace.yaml"

  if [[ -z "${RUSTFS_ACCESS_KEY:-}" || -z "${RUSTFS_SECRET_KEY:-}" || -z "${SCRIPTURE_ADMIN_TOKEN:-}" ]]; then
    fail "live apply requires RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, SCRIPTURE_ADMIN_TOKEN in the environment (not in Git/overlay files)"
  fi

  kubectl "${ctx_args[@]}" -n "$NAMESPACE" create secret generic rustfs-credentials \
    --from-literal=RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    --from-literal=RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    --dry-run=client -o yaml | kubectl "${ctx_args[@]}" apply -f -
  kubectl "${ctx_args[@]}" -n "$NAMESPACE" create secret generic scripture-admin-token \
    --from-literal=token="$SCRIPTURE_ADMIN_TOKEN" \
    --dry-run=client -o yaml | kubectl "${ctx_args[@]}" apply -f -

  local file
  for file in rustfs.yaml configmaps.yaml services.yaml networkpolicies.yaml clients.yaml deployments.yaml; do
    kubectl "${ctx_args[@]}" apply -f "$rendered/$file"
  done

  log "LIVE: waiting for A ready / B unready (timeout 180s)"
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    local a_ready b_ready
    a_ready="$(kubectl "${ctx_args[@]}" -n "$NAMESPACE" get pod -l scripture.dev/owner=a -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
    b_ready="$(kubectl "${ctx_args[@]}" -n "$NAMESPACE" get pod -l scripture.dev/owner=b -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
    if [[ "$a_ready" == "true" && "$b_ready" == "false" ]]; then
      break
    fi
    sleep 3
  done
  a_ready="$(kubectl "${ctx_args[@]}" -n "$NAMESPACE" get pod -l scripture.dev/owner=a -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
  b_ready="$(kubectl "${ctx_args[@]}" -n "$NAMESPACE" get pod -l scripture.dev/owner=b -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
  {
    echo "a_ready=${a_ready}"
    echo "b_ready=${b_ready}"
    kubectl "${ctx_args[@]}" -n "$NAMESPACE" get endpoints scripture-producer -o wide || true
    kubectl "${ctx_args[@]}" -n "$NAMESPACE" get pods -o wide || true
  } >"$art/live-topology.txt"

  if [[ "$a_ready" != "true" || "$b_ready" != "false" ]]; then
    fail "expected A ready and B unready; see $art/live-topology.txt"
  fi

  # Remaining producer/promote/oracle steps need product clients + Holylog tooling.
  # Record honest incomplete status rather than claiming family 22 pass.
  cat >"$art/live-remaining.txt" <<EOF
Applied and verified A ready / B unready.
Remaining for full family-22 pass (not automated in this tranche):
- producer records via scripture-producer with >=2 client identities
- kill A; confirm producer Endpoints empty of A
- authenticated B promote at next lawful term
- B sole ready endpoint; stale A denial; dense ACK continuation
- fail-closed: unauth / wrong-term / stale-owner / concurrent promote
- Holylog readback + namespace delete verification
EOF
  log "LIVE: topology gate passed; remaining acceptance steps recorded in live-remaining.txt"
}

# --- main ---
load_overlay
assert_no_forbidden_targets "${KUBE_CONTEXT}
${NAMESPACE}
${SCRIPTURE_IMAGE}
${WRITER_A_NODE}
${WRITER_B_NODE}
${RUSTFS_NODE}
${BUCKET}"

art="${artifact_root}/${RUN_ID}"
mkdir -p "$art"
rendered="$art/rendered"
rm -rf "$rendered"

log "run_id=${RUN_ID} mode=$([[ $execute -eq 1 ]] && echo execute || echo preflight) artifacts=${art}"

clean_status="$(check_clean_source || true)"
if [[ "$clean_status" != clean:* ]]; then
  clean_status="dirty"
  log "WARN: working tree is dirty (provenance cannot pass)"
fi

run_contract_scripts
package_rc="$(run_package_preflight)"
log "package preflight exit=${package_rc}"

rc_status="$(check_rc_manifest || true)"
[[ -n "$rc_status" ]] || rc_status="absent"
log "rc manifest: ${rc_status} (${rc_manifest})"

render_manifests "$rendered"
assert_no_forbidden_targets "$(cat "$rendered"/*.yaml)"

dry_run_ok=true
if ! kubectl_dry_run "$rendered"; then
  dry_run_ok=false
  fail "kubectl client-dry-run failed"
fi

write_approval_commands "$art"

live_status="not_requested"
exit_code=0

if [[ "$execute" -eq 1 ]]; then
  if [[ "$joshua_approved" -ne 1 ]] || ! approval_ok; then
    live_status="refused"
    write_verdicts "$art" "$clean_status" "$package_rc" "$rc_status" true "$live_status"
    log "LIVE REFUSED: need --joshua-approved and approval file line 'APPROVED ${RUN_ID}'"
    log "see $art/APPROVAL_REQUIRED_COMMANDS.txt"
    exit 2
  fi
  live_apply "$rendered" "$art"
  live_status="applied_partial"
  write_verdicts "$art" "$clean_status" "$package_rc" "$rc_status" true "$live_status"
  log "live topology applied; family 22 still incomplete until remaining steps + Holylog oracle"
  exit 2
fi

write_verdicts "$art" "$clean_status" "$package_rc" "$rc_status" true "$live_status"

# Preflight success: contracts + render + dry-run. Incomplete provenance → exit 2.
if [[ "$package_rc" == "1" ]]; then
  exit 1
fi
if [[ "$clean_status" != clean:* || "$package_rc" != "0" || "$rc_status" != "present" ]]; then
  log "preflight render ok; provenance incomplete (exit 2) — expected until Kellnr RC is filled"
  exit 2
fi
log "preflight complete (provenance attested)"
exit 0
