#!/usr/bin/env bash
#
# Capture the portable, deterministic evidence bundle for a Scripture source
# revision. This deliberately does not start a cluster, spend cloud money, or
# claim a live deployment: it records the testable product spine a reviewer
# can run from a clean checkout.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

usage() {
  cat <<'EOF'
Usage: scripts/submission-check.sh [--evidence-dir PATH] [--skip-clippy]

Runs the portable Scripture submission gates and writes one redacted local
evidence bundle. No network, object-store credentials, container runtime, or
Kubernetes cluster is required.

The result proves deterministic implementation contracts. It does not replace
an approved live two-Scribe/RustFS attestation.
EOF
}

evidence_dir=""
skip_clippy=0
while (($#)); do
  case "$1" in
    --evidence-dir)
      (($# >= 2)) || { echo "--evidence-dir needs PATH" >&2; exit 2; }
      evidence_dir="$2"
      shift 2
      ;;
    --skip-clippy)
      skip_clippy=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$evidence_dir" ]]; then
  run_id="submission-$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short HEAD)"
  evidence_dir="$repo_root/.tmp/submission-evidence/$run_id"
fi

case "$evidence_dir" in
  "$repo_root"/.tmp/*|/tmp/*) ;;
  *)
    echo "refuse evidence outside ignored .tmp/ or /tmp/: $evidence_dir" >&2
    exit 2
    ;;
esac

mkdir -p "$evidence_dir"
if [[ -e "$evidence_dir/summary.txt" ]]; then
  echo "refuse to overwrite an existing evidence bundle: $evidence_dir" >&2
  exit 2
fi

run_gate() {
  local name="$1"
  shift
  echo "==> $name" | tee -a "$evidence_dir/summary.txt"
  if "$@" >"$evidence_dir/$name.log" 2>&1; then
    echo "PASS $name" | tee -a "$evidence_dir/summary.txt"
  else
    status=$?
    echo "FAIL $name (exit $status); see $name.log" | tee -a "$evidence_dir/summary.txt" >&2
    return "$status"
  fi
}

{
  echo "scripture submission check"
  echo "run_id=$(basename "$evidence_dir")"
  echo "utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "revision=$(git rev-parse HEAD)"
  echo "branch=$(git branch --show-current)"
  echo "lock_sha256=$(sha256sum Cargo.lock | awk '{print $1}')"
  echo "worktree_clean=$(git diff --quiet && git diff --cached --quiet && echo true || echo false)"
  echo "claim=portable deterministic gates only; no live deployment attestation"
} >"$evidence_dir/provenance.txt"

echo "evidence_dir=$evidence_dir" | tee "$evidence_dir/summary.txt"
run_gate fmt cargo fmt --all -- --check

if (( ! skip_clippy )); then
  run_gate clippy cargo clippy --workspace --all-targets --locked -- -D warnings
fi

# These suites together exercise the actual submission story: fenced
# Canon/Verse handoff, DataRef/blob recovery, bounded producer continuity,
# campaign oracles, and output/progress replay. Keep their verdicts separate;
# none is labeled a live fleet attestation.
run_gate scripture_core cargo test -p scripture -p scripture-runtime -p scripture-cli --locked
run_gate campaign cargo test -p scripture-campaign --locked
run_gate workload cargo test -p scripture-workload --locked
run_gate diff_check git diff --check

cat >>"$evidence_dir/summary.txt" <<'EOF'
PASS submission-check
Scope: deterministic source-level evidence. For a live claim, pair this bundle
with a separate approved two-Scribe object-store drill and its durable oracle.
EOF

echo "submission-check: PASS evidence=$evidence_dir"
