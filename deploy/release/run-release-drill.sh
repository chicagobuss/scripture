#!/usr/bin/env bash
# WP09 release-drill runner (family 22 gate).
#
# Default: render + preflight + kubectl client-dry-run (syntax/render only).
# Live (--execute) runs the FULL acceptance state machine with EXIT/INT/TERM
# cleanup of the exact run namespace. There is no approved partial-topology mode.
#
# Live requires ALL of:
#   --execute
#   --joshua-approved
#   approval file line: APPROVED <run-id>
#   exact RC provenance pass (commit/lock/image/packages)
#   RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, SCRIPTURE_ADMIN_TOKEN in the environment
#
# Secrets never appear in kubectl argv (0600 env-files / stdin YAML only).
#
# Exit codes:
#   0  preflight attested, or live complete with all four verdicts pass
#   1  hard failure
#   2  incomplete attestation, live refused, or live incomplete after cleanup
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
keep_failed=0
live_namespace_created=0
secret_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --overlay) overlay="$2"; shift 2 ;;
    --rc-manifest) rc_manifest="$2"; shift 2 ;;
    --artifact-dir) artifact_root="$2"; shift 2 ;;
    --approval-file) approval_file="$2"; shift 2 ;;
    --run-id) run_id_override="$2"; shift 2 ;;
    --execute) execute=1; shift ;;
    --joshua-approved) joshua_approved=1; shift ;;
    --keep-failed) keep_failed=1; shift ;;
    --skip-kubectl) skip_kubectl=1; shift ;;
    -h|--help)
      sed -n '2,24p' "$0"
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

cleanup_secrets() {
  if [[ -n "${secret_dir:-}" && -d "$secret_dir" ]]; then
    find "$secret_dir" -type f -exec shred -u {} \; 2>/dev/null || rm -f "$secret_dir"/* || true
    rmdir "$secret_dir" 2>/dev/null || rm -rf "$secret_dir" || true
    secret_dir=""
  fi
}

cleanup_namespace() {
  cleanup_secrets
  if [[ "${live_namespace_created:-0}" -eq 1 && "${keep_failed:-0}" -eq 0 ]]; then
    log "cleanup: deleting namespace ${NAMESPACE}"
    kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" --wait=true --timeout=180s \
      >/dev/null 2>&1 || kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" --wait=false || true
    live_namespace_created=0
  elif [[ "${live_namespace_created:-0}" -eq 1 && "${keep_failed:-0}" -eq 1 ]]; then
    log "cleanup: retaining namespace ${NAMESPACE} (--keep-failed)"
  fi
}

on_exit() {
  local code=$?
  # Always scrub secret temp files; delete run namespace unless forensic retain.
  cleanup_namespace
  if [[ "${execute:-0}" -eq 1 && "$code" -ne 0 && -n "${art:-}" && -d "${art:-}" ]]; then
    write_verdicts "$art" "${clean_status:-dirty}" "${package_rc:-2}" "${rc_status:-fail}" true \
      "cleaned_fail" "fail" "fail" "fail" || true
  fi
  exit "$code"
}

# --- overlay: allow-listed KEY=VALUE only (never source as shell) ---
parse_overlay() {
  if [[ ! -f "$overlay" ]]; then
    mkdir -p "$(dirname "$overlay")"
    cp "$stable/overlay.example.env" "$overlay"
    log "seeded overlay from example at $overlay (edit image digest before live)"
  fi

  local -A values=()
  local line key val
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    if [[ ! "$line" =~ ^[A-Z][A-Z0-9_]*=. ]]; then
      fail "overlay rejects line (allow KEY=VALUE only): ${line:0:40}"
    fi
    key="${line%%=*}"
    val="${line#*=}"
    case "$key" in
      KUBE_CONTEXT|RUN_ID|SCRIPTURE_IMAGE|RUSTFS_NODE|WRITER_A_NODE|WRITER_B_NODE|BUCKET|RUSTFS_IMAGE|BUSYBOX_IMAGE|CURL_IMAGE|AWS_CLI_IMAGE) ;;
      *) fail "overlay unknown key: $key" ;;
    esac
    if [[ "$val" =~ [\$\`\;\|\&\<\>\(\)\{\}] ]]; then
      fail "overlay value for $key contains forbidden shell metacharacters"
    fi
    values["$key"]="$val"
  done <"$overlay"

  KUBE_CONTEXT="${values[KUBE_CONTEXT]:-Default}"
  RUN_ID="${run_id_override:-${values[RUN_ID]:-wp09-drill-local}}"
  NAMESPACE="scripture-stable-${RUN_ID}"
  SCRIPTURE_IMAGE="${values[SCRIPTURE_IMAGE]:-}"
  RUSTFS_NODE="${values[RUSTFS_NODE]:-bignlittles}"
  WRITER_A_NODE="${values[WRITER_A_NODE]:-node-a}"
  WRITER_B_NODE="${values[WRITER_B_NODE]:-node-b}"
  # Prefer explicit overlay bucket only when it matches this run; otherwise derive.
  if [[ -n "${values[BUCKET]:-}" && "${values[BUCKET]}" == *"${RUN_ID}"* ]]; then
    BUCKET="${values[BUCKET]}"
  else
    BUCKET="scripture-stable-${RUN_ID}"
  fi
  RUSTFS_IMAGE="${values[RUSTFS_IMAGE]:-rustfs/rustfs@sha256:fa19210ac4697c79d7ccca1ec9b0eb91aebacc6691991ffb14014bb3c67e6cc3}"
  BUSYBOX_IMAGE="${values[BUSYBOX_IMAGE]:-busybox@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662}"
  CURL_IMAGE="${values[CURL_IMAGE]:-curlimages/curl@sha256:08e466006f0860e54fc299378de998935333e0e130a15f6f98482e9f8dab3058}"
  AWS_CLI_IMAGE="${values[AWS_CLI_IMAGE]:-amazon/aws-cli@sha256:82acb165ccdc3e3f420b3753787847481d2f835d54582402774ec51fdbdc19ab}"

  if [[ ! "$RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
    fail "RUN_ID must be a DNS-1123 label (got $RUN_ID)"
  fi
  if (( ${#NAMESPACE} > 63 )); then
    fail "derived namespace exceeds 63 chars (${#NAMESPACE}): $NAMESPACE"
  fi
  if [[ -z "$SCRIPTURE_IMAGE" ]]; then
    fail "SCRIPTURE_IMAGE required in overlay"
  fi
  if [[ "$SCRIPTURE_IMAGE" == *":latest" ]]; then
    fail "SCRIPTURE_IMAGE must not use :latest"
  fi
  if [[ "$execute" -eq 1 ]]; then
    if [[ "$SCRIPTURE_IMAGE" == *REPLACE* ]]; then
      fail "SCRIPTURE_IMAGE still contains REPLACE; cannot live-execute"
    fi
    if ! [[ "$SCRIPTURE_IMAGE" =~ ^[a-z0-9._/-]+@sha256:[a-f0-9]{64}$ ]]; then
      fail "SCRIPTURE_IMAGE must match name@sha256:<64 hex> for live"
    fi
    for img_name in RUSTFS_IMAGE BUSYBOX_IMAGE CURL_IMAGE AWS_CLI_IMAGE; do
      img_val="${!img_name}"
      if ! [[ "$img_val" =~ @sha256:[a-f0-9]{64}$ ]]; then
        fail "$img_name must be digest-pinned (@sha256:<64 hex>) for live"
      fi
    done
  elif [[ "$SCRIPTURE_IMAGE" == *REPLACE* ]] || ! [[ "$SCRIPTURE_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]; then
    log "WARN: SCRIPTURE_IMAGE is not a final name@sha256:<64 hex>; provenance cannot pass"
  fi
  if ! [[ "$BUCKET" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    fail "BUCKET must be a valid S3 bucket name (got $BUCKET)"
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

run_package_preflight() {
  set +e
  "$release/check-package-preflight.sh" >&2
  local rc=$?
  set -e
  printf '%s\n' "$rc"
}

run_rc_provenance() {
  local errfile
  errfile="$(mktemp "${TMPDIR:-/tmp}/rc-prov.XXXXXX")"
  # Live execute always verifies containerd imports. Preflight skips ssh unless
  # VERIFY_NODE_IMPORTS=1 is set explicitly.
  local verify=0
  if [[ "$execute" -eq 1 || "${VERIFY_NODE_IMPORTS:-0}" == "1" ]]; then
    verify=1
  fi
  set +e
  local status
  status="$(VERIFY_NODE_IMPORTS="$verify" "$release/check-rc-provenance.sh" "$rc_manifest" "$SCRIPTURE_IMAGE" 2>"$errfile")"
  local rc=$?
  set -e
  cat "$errfile" >&2 || true
  rm -f "$errfile"
  if [[ "$rc" -eq 0 ]]; then
    echo "present"
  elif [[ "$status" == "absent" ]]; then
    echo "absent"
  else
    echo "${status:-fail}"
  fi
  return 0
}

render_manifests() {
  local out_dir="$1"
  mkdir -p "$out_dir"
  local file base rendered
  for file in "$stable"/*.yaml; do
    base="$(basename "$file")"
    [[ "$base" == "secrets.placeholder.yaml" ]] && continue
    # Example client pods are documentation only; live creates ephemeral Jobs.
    [[ "$base" == "clients.yaml" ]] && continue
    rendered="$out_dir/$base"
    sed \
      -e "s/scripture-stable-REPLACE_RUN_ID/${NAMESPACE}/g" \
      -e "s/REPLACE_RUN_ID/${RUN_ID}/g" \
      -e "s/REPLACE_BUCKET/${BUCKET}/g" \
      -e "s|scripture:REPLACE_IMAGE_DIGEST_OR_TAG|${SCRIPTURE_IMAGE}|g" \
      -e "s|REPLACE_RUSTFS_IMAGE|${RUSTFS_IMAGE}|g" \
      -e "s/kubernetes.io\/hostname: node-a/kubernetes.io\/hostname: ${WRITER_A_NODE}/g" \
      -e "s/kubernetes.io\/hostname: node-b/kubernetes.io\/hostname: ${WRITER_B_NODE}/g" \
      -e "s/kubernetes.io\/hostname: bignlittles/kubernetes.io\/hostname: ${RUSTFS_NODE}/g" \
      "$file" >"$rendered"
  done
  # ServiceAccounts only (example Pods are documentation; live uses ephemeral Jobs).
  awk '
    BEGIN { RS="---\n"; ORS="---\n" }
    /kind: ServiceAccount/ { print }
  ' "$stable/clients.yaml" \
    | sed \
      -e "s/scripture-stable-REPLACE_RUN_ID/${NAMESPACE}/g" \
      -e "s/REPLACE_RUN_ID/${RUN_ID}/g" \
    >"$out_dir/clients.yaml"
  # Drop a leading --- if file starts empty
  if [[ ! -s "$out_dir/clients.yaml" ]] || ! grep -q 'kind: ServiceAccount' "$out_dir/clients.yaml"; then
    fail "failed to render ServiceAccounts from clients.yaml"
  fi
  if grep -qE 'REPLACE_BUCKET|REPLACE_RUN_ID|scripture-stable-REPLACE|REPLACE_IMAGE|REPLACE_RUSTFS' "$out_dir"/*.yaml; then
    fail "rendered manifests still contain unsubstituted REPLACE_* placeholders"
  fi
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
    echo "client_labels_note=NetworkPolicy selectors only; not authentication. Drill namespace must not admit untrusted creators."
    echo "kubectl_dry_run_note=client-dry-run is syntax/render validation only; not connectivity or deployment validation."
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
  log "kubectl client-dry-run (syntax/render) ok for $(find "$rendered" -name '*.yaml' | wc -l) manifests"
}

write_verdicts() {
  local art="$1"
  local clean_status="$2"
  local package_rc="$3"
  local rc_status="$4"
  local dry_run_ok="$5"
  local live_status="$6"
  local semantic="${7:-not_run}"
  local holylog="${8:-not_run}"
  local completeness="${9:-incomplete}"

  local provenance="not_run"
  local provenance_detail="RC identities not yet proven"
  if [[ "$package_rc" == "1" ]]; then
    provenance="fail"
    provenance_detail="package contract failed"
  elif [[ "$clean_status" == "dirty" ]]; then
    provenance="fail"
    provenance_detail="working tree dirty; clean committed source required"
  elif [[ "$rc_status" == "fail" || "$rc_status" == "bad-image" ]]; then
    provenance="fail"
    provenance_detail="RC provenance identities disagree"
  elif [[ "$clean_status" == clean:* && "$package_rc" == "0" && "$rc_status" == "present" ]]; then
    provenance="pass"
    provenance_detail="registry-only attestation + RC identities + node containerd imports agree"
  else
    provenance="incomplete"
    provenance_detail="RC manifest and/or authenticated package attestation incomplete"
  fi

  case "$live_status" in
    refused|not_requested)
      completeness="incomplete"
      ;;
    cleaned_fail)
      completeness="fail"
      ;;
    complete_pass)
      semantic="pass"
      holylog="pass"
      completeness="pass"
      ;;
  esac

  cat >"$art/verdicts.json" <<EOF
{
  "schema_version": 2,
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
    "kubectl --dry-run=client is syntax/render validation only.",
    "Client pod labels are NetworkPolicy selectors, not authentication.",
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
# Prerequisites (operator-local, never commit):
#   - filled ${rc_manifest} matching HEAD + Cargo.lock + ${SCRIPTURE_IMAGE}
#   - config/local/kellnr/registry-build-attestation.toml from a clean registry-only builder
#     (no git/path Holylog sources; package checksums match Kellnr-resolved fleet sources)
#   - authenticated fleet package preflight exit 0
#   - image imported to writer nodes; attestation node_imports digests match (runner re-checks via ssh/ctr)
#   - digest-pinned helper images in overlay (RUSTFS/BUSYBOX/CURL/AWS_CLI)
#   - export RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY SCRIPTURE_ADMIN_TOKEN (env only)
#
# Approval file (exact line):
#   printf 'APPROVED ${RUN_ID}\\n' > ${approval_file}
#
# Execute full state machine (creates namespace, always cleans up unless --keep-failed):
#   ${release}/run-release-drill.sh \\
#     --overlay ${overlay} \\
#     --rc-manifest ${rc_manifest} \\
#     --execute --joshua-approved \\
#     --approval-file ${approval_file} \\
#     --run-id ${RUN_ID}
#
# Forensic retention after failure only:
#   add --keep-failed (explicit; default deletes the run namespace)

KUBE_CONTEXT=${KUBE_CONTEXT}
NAMESPACE=${NAMESPACE}
SCRIPTURE_IMAGE=${SCRIPTURE_IMAGE}
BUCKET=${BUCKET}
EOF
  log "wrote approval command sheet: $art/APPROVAL_REQUIRED_COMMANDS.txt"
}

approval_ok() {
  [[ -f "$approval_file" ]] || return 1
  local line
  line="$(head -n1 "$approval_file" | tr -d '\r')"
  [[ "$line" == "APPROVED ${RUN_ID}" ]]
}

ctx() { kubectl --context "$KUBE_CONTEXT" "$@"; }

create_secrets_safe() {
  secret_dir="$(mktemp -d "${TMPDIR:-/tmp}/scripture-drill-secrets.XXXXXX")"
  chmod 700 "$secret_dir"
  local rustfs_env="$secret_dir/rustfs.env"
  local admin_env="$secret_dir/admin.env"
  umask 077
  printf 'RUSTFS_ACCESS_KEY=%s\nRUSTFS_SECRET_KEY=%s\n' \
    "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" >"$rustfs_env"
  printf 'token=%s\n' "$SCRIPTURE_ADMIN_TOKEN" >"$admin_env"
  chmod 600 "$rustfs_env" "$admin_env"
  ctx -n "$NAMESPACE" create secret generic rustfs-credentials --from-env-file="$rustfs_env"
  ctx -n "$NAMESPACE" create secret generic scripture-admin-token --from-env-file="$admin_env"
  shred -u "$rustfs_env" "$admin_env" 2>/dev/null || rm -f "$rustfs_env" "$admin_env"
}

wait_jsonpath() {
  local desc="$1" want="$2" timeout="$3"
  shift 3
  local deadline=$((SECONDS + timeout)) got
  while (( SECONDS < deadline )); do
    got="$(ctx "$@" 2>/dev/null || true)"
    if [[ "$got" == "$want" ]]; then
      return 0
    fi
    sleep 2
  done
  fail "timeout waiting for $desc (want=$want last=$got)"
}

producer_endpoints_owners() {
  # Prints owner labels of pods backing scripture-producer Endpoints/EndpointSlice.
  local slices
  slices="$(ctx -n "$NAMESPACE" get endpointslices -l kubernetes.io/service-name=scripture-producer \
    -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.name}{"\n"}{end}' 2>/dev/null || true)"
  if [[ -z "$slices" ]]; then
    slices="$(ctx -n "$NAMESPACE" get endpoints scripture-producer \
      -o jsonpath='{range .subsets[*].addresses[*]}{.targetRef.name}{"\n"}{end}' 2>/dev/null || true)"
  fi
  local pod owner
  for pod in $slices; do
    [[ -z "$pod" ]] && continue
    owner="$(ctx -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.labels.scripture\.dev/owner}' 2>/dev/null || true)"
    printf '%s\n' "${owner:-unknown}"
  done
}

assert_producer_owners() {
  local expect="$1"
  local got
  got="$(producer_endpoints_owners | sort -u | tr '\n' ',' | sed 's/,$//')"
  if [[ "$got" != "$expect" ]]; then
    fail "producer Endpoints owners want=$expect got=$got"
  fi
}

run_bucket_init() {
  local yaml
  yaml="$(cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: rustfs-bucket-init
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 3
  template:
    metadata:
      labels:
        scripture.dev/role: bucket-init
    spec:
      restartPolicy: Never
      containers:
        - name: aws
          image: ${AWS_CLI_IMAGE}
          imagePullPolicy: IfNotPresent
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: rustfs-credentials
                  key: RUSTFS_ACCESS_KEY
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: rustfs-credentials
                  key: RUSTFS_SECRET_KEY
            - name: AWS_DEFAULT_REGION
              value: us-east-1
          command: ["/bin/sh","-eu","-c"]
          args:
            - |
              # RustFS is intentionally addressed through its in-cluster
              # Service, not a wildcard virtual-host endpoint.  Force
              # path-style S3 requests so the bucket name never becomes part
              # of DNS/Host routing during bootstrap.
              aws configure set default.s3.addressing_style path
              endpoint="http://rustfs.${NAMESPACE}.svc.cluster.local:9000"
              for i in \$(seq 1 60); do
                if aws --endpoint-url "\$endpoint" s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
                  exit 0
                fi
                aws --endpoint-url "\$endpoint" s3api create-bucket --bucket "${BUCKET}" && exit 0 || true
                sleep 2
              done
              echo "bucket init failed" >&2
              exit 1
EOF
)"
  printf '%s\n' "$yaml" | ctx apply -f -
  wait_jsonpath "bucket-init succeeded" "1" 180 \
    -n "$NAMESPACE" get job rustfs-bucket-init -o jsonpath='{.status.succeeded}'
}

# Run a one-shot labeled client Job; capture logs to a file. No secret argv.
# mount_admin_token: yes|no — only lawful authorized admin Jobs may mount the Secret.
run_client_job() {
  local name="$1" client_label="$2" image="$3" script="$4" logfile="$5"
  local mount_admin_token="${6:-no}"
  ctx -n "$NAMESPACE" delete job "$name" --ignore-not-found=true >/dev/null
  local sa="scripture-producer-client"
  [[ "$client_label" == "admin" ]] && sa="scripture-admin-client"
  local env_block=""
  if [[ "$mount_admin_token" == "yes" ]]; then
    env_block="$(cat <<'ENV'
          env:
            - name: SCRIPTURE_ADMIN_TOKEN
              valueFrom:
                secretKeyRef:
                  name: scripture-admin-token
                  key: token
ENV
)"
  else
    env_block="          env: []"
  fi
  local yaml
  yaml="$(cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
  annotations:
    scripture.dev/admin-token-mounted: "${mount_admin_token}"
spec:
  backoffLimit: 1
  template:
    metadata:
      labels:
        scripture.dev/client: ${client_label}
      annotations:
        scripture.dev/admin-token-mounted: "${mount_admin_token}"
    spec:
      serviceAccountName: ${sa}
      restartPolicy: Never
      containers:
        - name: client
          image: ${image}
          imagePullPolicy: IfNotPresent
${env_block}
          command: ["/bin/sh","-eu","-c"]
          args:
            - |
$(printf '%s\n' "$script" | sed 's/^/              /')
EOF
)"
  printf '%s\n' "$yaml" | ctx apply -f -
  # Persist rendered Job for evidence (no secret values).
  printf '%s\n' "$yaml" >"${logfile%.log}.job.yaml"
}

wait_client_job() {
  local name="$1" logfile="$2" allow_fail="${3:-no}"
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    local succeeded failed
    succeeded="$(ctx -n "$NAMESPACE" get job "$name" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
    failed="$(ctx -n "$NAMESPACE" get job "$name" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
    if [[ "$succeeded" == "1" ]]; then
      ctx -n "$NAMESPACE" logs "job/$name" >"$logfile" 2>&1 || true
      return 0
    fi
    if [[ -n "$failed" && "$failed" != "0" ]]; then
      ctx -n "$NAMESPACE" logs "job/$name" >"$logfile" 2>&1 || true
      if [[ "$allow_fail" == "yes" ]]; then
        return 1
      fi
      fail "client job $name failed; see $logfile"
    fi
    sleep 2
  done
  ctx -n "$NAMESPACE" logs "job/$name" >"$logfile" 2>&1 || true
  if [[ "$allow_fail" == "yes" ]]; then
    return 1
  fi
  fail "client job $name timed out; see $logfile"
}

producer_exchange() {
  local job_name="$1" payloads_csv="$2" logfile="$3" target="${4:-scripture-producer}" expect_ok="${5:-yes}"
  local script
  script="$(cat <<EOS
target="${target}"
port=9000
payloads='${payloads_csv}'
oldifs="\$IFS"
IFS=,
set -- \$payloads
IFS="\$oldifs"
n=0
for _ in \$payloads; do n=\$((n+1)); done
for p in "\$@"; do
  printf '%s\\n' "\$p"
done | nc -w 30 "\$target" "\$port" | tee /tmp/out || true
ok=\$(grep -c '^OK ' /tmp/out || true)
echo "ok_lines=\$ok want=\$n target=\$target"
if [ "${expect_ok}" = "yes" ]; then
  if [ "\$ok" -lt "\$n" ]; then
    echo "expected \$n OK lines, got \$ok" >&2
    cat /tmp/out >&2
    exit 1
  fi
else
  if [ "\$ok" -gt 0 ]; then
    echo "unexpected committed OK from fenced/stale target" >&2
    cat /tmp/out >&2
    exit 1
  fi
  echo "no committed OK (fenced) ok"
fi
cat /tmp/out
EOS
)"
  run_client_job "$job_name" "producer" "$BUSYBOX_IMAGE" "$script" "$logfile" "no"
  wait_client_job "$job_name" "$logfile"
}

admin_promote() {
  local job_name="$1" term="$2" expect_ok="$3" logfile="$4" token_mode="$5"
  local mount_token="no"
  local script
  case "$token_mode" in
    env)
      mount_token="yes"
      if [[ "$expect_ok" == "async" ]]; then
        script="$(cat <<EOS
set +e
code=\$(curl -sS -o /tmp/body -w "%{http_code}" -X POST \\
  "http://scripture-admin-b:9200/v1/promote" \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer \${SCRIPTURE_ADMIN_TOKEN}" \\
  -d '{"candidate_term":${term}}')
echo "http_code=\$code"
echo "body=\$(cat /tmp/body 2>/dev/null || true)"
exit 0
EOS
)"
      else
        script="$(cat <<EOS
set +e
code=\$(curl -sS -o /tmp/body -w "%{http_code}" -X POST \\
  "http://scripture-admin-b:9200/v1/promote" \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer \${SCRIPTURE_ADMIN_TOKEN}" \\
  -d '{"candidate_term":${term}}')
set -e
echo "http_code=\$code"
echo "body=\$(cat /tmp/body 2>/dev/null || true)"
if [ "${expect_ok}" = "yes" ]; then
  [ "\$code" = "200" ] || exit 1
else
  [ "\$code" = "200" ] && exit 1 || exit 0
fi
EOS
)"
      fi
      ;;
    wrong)
      # Must NOT mount the real Secret; send an inline wrong bearer only.
      mount_token="no"
      script="$(cat <<EOS
set +e
code=\$(curl -sS -o /tmp/body -w "%{http_code}" -X POST \\
  "http://scripture-admin-b:9200/v1/promote" \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer wrong-token" \\
  -d '{"candidate_term":${term}}')
set -e
echo "http_code=\$code"
echo "body=\$(cat /tmp/body 2>/dev/null || true)"
echo "admin_token_mounted=no"
[ "\$code" = "200" ] && exit 1 || exit 0
EOS
)"
      ;;
    missing)
      mount_token="no"
      script="$(cat <<EOS
set +e
code=\$(curl -sS -o /tmp/body -w "%{http_code}" -X POST \\
  "http://scripture-admin-b:9200/v1/promote" \\
  -H "Content-Type: application/json" \\
  -d '{"candidate_term":${term}}')
set -e
echo "http_code=\$code"
echo "body=\$(cat /tmp/body 2>/dev/null || true)"
echo "admin_token_mounted=no"
[ "\$code" = "200" ] && exit 1 || exit 0
EOS
)"
      ;;
    *) fail "unknown token_mode: $token_mode" ;;
  esac
  run_client_job "$job_name" "admin" "$CURL_IMAGE" "$script" "$logfile" "$mount_token"
  if [[ "$expect_ok" == "async" ]]; then
    return 0
  fi
  wait_client_job "$job_name" "$logfile"
}

# Launch two authorized term-2 promotes while B is standby; require exactly one 200.
concurrent_promote_term2() {
  local art="$1"
  admin_promote "promote-concurrent-a" 2 "async" "$art/promote-concurrent-a.log" "env"
  admin_promote "promote-concurrent-b" 2 "async" "$art/promote-concurrent-b.log" "env"
  wait_client_job "promote-concurrent-a" "$art/promote-concurrent-a.log"
  wait_client_job "promote-concurrent-b" "$art/promote-concurrent-b.log"
  local code_a code_b
  code_a="$(grep -E '^http_code=' "$art/promote-concurrent-a.log" | tail -1 | cut -d= -f2 || true)"
  code_b="$(grep -E '^http_code=' "$art/promote-concurrent-b.log" | tail -1 | cut -d= -f2 || true)"
  {
    echo "concurrent_a_http=${code_a}"
    echo "concurrent_b_http=${code_b}"
    echo "expected=exactly_one_200"
  } >"$art/promote-concurrent-summary.txt"
  local wins=0
  [[ "$code_a" == "200" ]] && wins=$((wins + 1))
  [[ "$code_b" == "200" ]] && wins=$((wins + 1))
  if [[ "$wins" -ne 1 ]]; then
    fail "concurrent promote expected exactly one HTTP 200, got a=${code_a} b=${code_b}"
  fi
  if [[ "$code_a" == "200" ]]; then
    echo "winner=promote-concurrent-a term=2" >>"$art/promote-concurrent-summary.txt"
  else
    echo "winner=promote-concurrent-b term=2" >>"$art/promote-concurrent-summary.txt"
  fi
  log "concurrent promote: exactly one winner (a=${code_a} b=${code_b})"
}

prove_stale_a_fenced() {
  local art="$1"
  log "waiting for restarted A to exist and remain unready/fenced"
  local deadline=$((SECONDS + 180))
  local a_ready="" a_phase=""
  while (( SECONDS < deadline )); do
    a_phase="$(ctx -n "$NAMESPACE" get pod -l scripture.dev/owner=a \
      -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)"
    a_ready="$(ctx -n "$NAMESPACE" get pod -l scripture.dev/owner=a \
      -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
    if [[ -n "$a_phase" && "$a_phase" != "Pending" && "$a_ready" == "false" ]]; then
      break
    fi
    sleep 3
  done
  {
    echo "a_phase=${a_phase}"
    echo "a_ready=${a_ready}"
  } >"$art/stale-a-status.txt"
  [[ "$a_ready" == "false" ]] || fail "restarted A unexpectedly ready (fencing not proven)"
  # Owner-route probe: must not yield a committed ACK.
  producer_exchange "producer-stale-a" "stale-a-probe" "$art/producer-stale-a.log" "scripture-actor-a" "no"
  echo "stale_a_owner_route_no_ack=true" >>"$art/stale-a-status.txt"
}

run_holylog_oracle() {
  local art="$1"
  local payloads_file="$art/payloads.txt"
  cat >"$payloads_file" <<EOF
drill-a-c0
drill-a-c1
drill-b-c0
drill-b-c1
EOF
  local local_port
  local_port="$(( (RANDOM % 10000) + 20000 ))"
  ctx -n "$NAMESPACE" port-forward svc/rustfs "${local_port}:9000" >/tmp/pf-rustfs.log 2>&1 &
  local pf_pid=$!
  local ready=0
  local _i
  for _i in $(seq 1 30); do
    if command -v nc >/dev/null && nc -z 127.0.0.1 "$local_port" 2>/dev/null; then
      ready=1
      break
    fi
    sleep 1
  done
  [[ "$ready" -eq 1 ]] || {
    kill "$pf_pid" 2>/dev/null || true
    fail "rustfs port-forward not ready; see /tmp/pf-rustfs.log"
  }
  set +e
  cargo run -q -p scripture-campaign --locked -- release-oracle \
    --endpoint "http://127.0.0.1:${local_port}" \
    --bucket "$BUCKET" \
    --prefix "scripture/stable/${RUN_ID}" \
    --payloads-file "$payloads_file" \
    --owner "scripture-own-b!" \
    --term 2 \
    --out "$art/holylog-oracle.json" \
    --timeout-secs 180
  local orc=$?
  set -e
  kill "$pf_pid" 2>/dev/null || true
  wait "$pf_pid" 2>/dev/null || true
  [[ "$orc" -eq 0 ]] || fail "Holylog durable oracle failed (exit $orc)"
}

live_state_machine() {
  local rendered="$1"
  local art="$2"
  local semantic="incomplete"
  local holylog="incomplete"

  [[ -n "${RUSTFS_ACCESS_KEY:-}" && -n "${RUSTFS_SECRET_KEY:-}" && -n "${SCRIPTURE_ADMIN_TOKEN:-}" ]] \
    || fail "live requires RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, SCRIPTURE_ADMIN_TOKEN in the environment"

  trap on_exit EXIT INT TERM

  log "LIVE step1: namespace ${NAMESPACE}"
  ctx apply -f "$rendered/namespace.yaml"
  live_namespace_created=1

  log "LIVE step2: secrets (env-file; no argv literals)"
  create_secrets_safe

  log "LIVE step3: rustfs Service + bucket"
  ctx apply -f "$rendered/rustfs.yaml"
  # Bucket initialization reaches RustFS through its in-cluster Service.  The
  # service must exist before the Job starts; the producer and owner-route
  # Services are harmless until their deployments are applied below.
  ctx apply -f "$rendered/services.yaml"
  wait_jsonpath "rustfs available" "1" 180 \
    -n "$NAMESPACE" get deploy rustfs -o jsonpath='{.status.availableReplicas}'
  run_bucket_init

  log "LIVE step4: config, policies, clients, deployments"
  ctx apply -f "$rendered/configmaps.yaml"
  ctx apply -f "$rendered/networkpolicies.yaml"
  ctx apply -f "$rendered/clients.yaml"
  ctx apply -f "$rendered/deployments.yaml"

  log "LIVE step5: wait A ready / B unready"
  local deadline=$((SECONDS + 240))
  local a_ready=false b_ready=true
  while (( SECONDS < deadline )); do
    a_ready="$(ctx -n "$NAMESPACE" get pod -l scripture.dev/owner=a \
      -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
    b_ready="$(ctx -n "$NAMESPACE" get pod -l scripture.dev/owner=b \
      -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
    if [[ "$a_ready" == "true" && "$b_ready" == "false" ]]; then
      break
    fi
    sleep 3
  done
  [[ "$a_ready" == "true" && "$b_ready" == "false" ]] \
    || fail "expected A ready and B unready (a=$a_ready b=$b_ready)"
  assert_producer_owners "a"
  {
    echo "a_ready=$a_ready"
    echo "b_ready=$b_ready"
    producer_endpoints_owners
  } >"$art/live-topology-phase-a.txt"

  log "LIVE step6: producer traffic (two client identities / payloads)"
  producer_exchange "producer-phase-a" "drill-a-c0,drill-a-c1" "$art/producer-phase-a.log"

  log "LIVE step7: unlabeled client must not reach producer (ingress NP)"
  ctx -n "$NAMESPACE" delete job "producer-unlabeled" --ignore-not-found=true >/dev/null || true
  cat <<EOF | ctx apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: producer-unlabeled
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        scripture.dev/probe: producer-reach
    spec:
      restartPolicy: Never
      containers:
        - name: client
          image: ${BUSYBOX_IMAGE}
          command: ["/bin/sh","-eu","-c"]
          args:
            - |
              # Has egress to producer port but lacks scripture.dev/client=producer.
              if nc -w 3 scripture-producer 9000 </dev/null; then
                echo "unlabeled client unexpectedly connected" >&2
                exit 1
              fi
              echo "unlabeled denied ok"
EOF
  wait_jsonpath "unlabeled deny job" "1" 90 \
    -n "$NAMESPACE" get job producer-unlabeled -o jsonpath='{.status.succeeded}'

  log "LIVE step8: kill A; producer endpoints empty of A"
  ctx -n "$NAMESPACE" delete pod -l scripture.dev/owner=a --grace-period=0 --force
  deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    local owners
    owners="$(producer_endpoints_owners | tr '\n' ' ')"
    if [[ "$owners" != *a* ]]; then
      break
    fi
    sleep 2
  done
  owners="$(producer_endpoints_owners | tr '\n' ' ')"
  [[ "$owners" != *a* ]] || fail "producer still lists A after kill: $owners"

  log "LIVE step9: fail-closed promotes while B still standby"
  # Unauthenticated / wrong-token Jobs must not mount the real admin Secret.
  admin_promote "promote-unauth" 2 "no" "$art/promote-unauth.log" "missing"
  grep -q 'admin-token-mounted: "no"' "$art/promote-unauth.job.yaml" \
    || fail "unauth job must not mount admin token"
  admin_promote "promote-wrong-token" 2 "no" "$art/promote-wrong-token.log" "wrong"
  grep -q 'admin-token-mounted: "no"' "$art/promote-wrong-token.job.yaml" \
    || fail "wrong-token job must not mount admin token"
  # Stale term 1 (A's term) — not a future lawful term like 99.
  admin_promote "promote-stale-term" 1 "no" "$art/promote-stale-term.log" "env"
  grep -q 'admin-token-mounted: "yes"' "$art/promote-stale-term.job.yaml" \
    || fail "stale-term job should mount token (auth ok, term rejected)"

  log "LIVE step10: concurrent lawful term-2 promotes while B standby"
  concurrent_promote_term2 "$art"

  log "LIVE step11: wait B ready sole producer endpoint"
  deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    b_ready="$(ctx -n "$NAMESPACE" get pod -l scripture.dev/owner=b \
      -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
    owners="$(producer_endpoints_owners | sort -u | tr '\n' ',' | sed 's/,$//')"
    if [[ "$b_ready" == "true" && "$owners" == "b" ]]; then
      break
    fi
    sleep 3
  done
  assert_producer_owners "b"

  log "LIVE step12: producer continuation on B"
  producer_exchange "producer-phase-b" "drill-b-c0,drill-b-c1" "$art/producer-phase-b.log"

  log "LIVE step13: prove restarted A is fenced (not only Endpoint absence)"
  prove_stale_a_fenced "$art"

  log "LIVE step14: Holylog durable oracle"
  run_holylog_oracle "$art"
  holylog="pass"
  semantic="pass"

  log "LIVE step15: delete namespace and verify gone"
  keep_failed=0
  live_namespace_created=1
  cleanup_namespace
  deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if ! ctx get namespace "$NAMESPACE" >/dev/null 2>&1; then
      echo "namespace_gone=true" >"$art/cleanup.txt"
      write_verdicts "$art" "clean:$(cd "$root" && git rev-parse HEAD)" 0 "present" true \
        "complete_pass" "$semantic" "$holylog" "pass"
      log "LIVE complete: all acceptance steps passed"
      # Disable trap cleanup (already done)
      trap - EXIT INT TERM
      return 0
    fi
    sleep 2
  done
  fail "namespace ${NAMESPACE} still present after delete"
}

# --- main ---
parse_overlay
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

rc_status="$(run_rc_provenance)"
log "rc provenance: ${rc_status}"

render_manifests "$rendered"
assert_no_forbidden_targets "$(cat "$rendered"/*.yaml)"

dry_run_ok=true
if ! kubectl_dry_run "$rendered"; then
  dry_run_ok=false
  fail "kubectl client-dry-run failed"
fi

write_approval_commands "$art"

if [[ "$execute" -eq 1 ]]; then
  if [[ "$joshua_approved" -ne 1 ]] || ! approval_ok; then
    write_verdicts "$art" "$clean_status" "$package_rc" "$rc_status" true "refused"
    log "LIVE REFUSED: need --joshua-approved and approval file line 'APPROVED ${RUN_ID}'"
    log "see $art/APPROVAL_REQUIRED_COMMANDS.txt"
    exit 2
  fi
  if [[ "$clean_status" != clean:* || "$package_rc" != "0" || "$rc_status" != "present" ]]; then
    write_verdicts "$art" "$clean_status" "$package_rc" "$rc_status" true "refused"
    log "LIVE REFUSED before any cluster mutation: provenance not fully attested"
    log "need clean tree + package preflight 0 + registry-only attestation + node import verify"
    exit 2
  fi
  if [[ "$skip_kubectl" -eq 1 ]]; then
    fail "live execute cannot use --skip-kubectl"
  fi
  set +e
  live_state_machine "$rendered" "$art"
  live_rc=$?
  set -e
  if [[ "$live_rc" -ne 0 ]]; then
    write_verdicts "$art" "$clean_status" "$package_rc" "$rc_status" true "cleaned_fail" \
      "fail" "fail" "fail"
    log "LIVE failed; namespace cleanup ran via trap (unless --keep-failed)"
    exit 1
  fi
  exit 0
fi

write_verdicts "$art" "$clean_status" "$package_rc" "$rc_status" true "not_requested"

if [[ "$package_rc" == "1" ]]; then
  exit 1
fi
if [[ "$clean_status" != clean:* || "$package_rc" != "0" || "$rc_status" != "present" ]]; then
  log "preflight render ok; provenance incomplete (exit 2)"
  exit 2
fi
log "preflight complete (provenance attested); live still requires Joshua approval"
exit 0
