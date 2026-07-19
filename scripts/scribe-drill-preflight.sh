#!/usr/bin/env bash
# Safe preflight for multi-assignment Scribe SSH/ZeroTier drills.
#
# Does not print secrets. Does not start scribes. Refuses if credentials appear
# on argv. Materialize local templates under config/local/scribe-drills/ first.
#
# Usage:
#   scripts/scribe-drill-preflight.sh
#   SCRIPTURE_DRILL_ROOT=config/local/scribe-drills/<run> scripts/scribe-drill-preflight.sh

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
drill_root="${SCRIPTURE_DRILL_ROOT:-$repo_root/config/local/scribe-drills}"

if [[ "$*" == *"--access-key"* || "$*" == *"--secret-key"* ]]; then
  echo "preflight: refuse secrets on argv" >&2
  exit 2
fi

echo "preflight: repo=$repo_root"
echo "preflight: drill_root=$drill_root"

if [[ ! -d "$drill_root" ]]; then
  echo "preflight: missing drill root (create ignored templates under config/local/scribe-drills/)" >&2
  echo "preflight: start from crates/scripture-cli/examples/scripture-multi-assignment*.yaml" >&2
  exit 1
fi

required_envs=(RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY)
missing=0
for name in "${required_envs[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    # Accept AWS_* aliases used by some local profiles.
    if [[ "$name" == "RUSTFS_ACCESS_KEY" && -n "${AWS_ACCESS_KEY_ID:-}" ]]; then
      continue
    fi
    if [[ "$name" == "RUSTFS_SECRET_KEY" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
      continue
    fi
    echo "preflight: missing env $name (value not printed)" >&2
    missing=1
  else
    echo "preflight: env $name is set (value not printed)"
  fi
done

config_count=0
while IFS= read -r -d '' file; do
  config_count=$((config_count + 1))
  echo "preflight: found config $file"
  # Flag only assignment-like secret material, not comments naming env vars.
  if grep -Eiq \
    '^[[:space:]]*(secret_key|secret-key|password|aws_secret_access_key|r2_secret_access_key)[[:space:]]*:' \
    "$file" \
    || grep -Eiq 'BEGIN (RSA |OPENSSH )?PRIVATE KEY' "$file"; then
    echo "preflight: refuse config that looks like it embeds secrets: $file" >&2
    exit 2
  fi
done < <(find "$drill_root" -type f \( -name '*.yaml' -o -name '*.yml' \) -print0 2>/dev/null)

if [[ "$config_count" -eq 0 ]]; then
  echo "preflight: no yaml configs under $drill_root" >&2
  missing=1
fi

hosts_file=""
if [[ -f "$drill_root/hosts.env" ]]; then
  hosts_file="$drill_root/hosts.env"
else
  # Prefer the newest run-local hosts.env when SCRIBE drill root is the parent.
  hosts_file="$(find "$drill_root" -type f -name hosts.env 2>/dev/null | head -n 1 || true)"
fi
if [[ -n "$hosts_file" && -f "$hosts_file" ]]; then
  # shellcheck disable=SC1090
  source "$hosts_file"
  echo "preflight: loaded $hosts_file"
  for var in RUSTFS_HOST SCRIBE_A_HOST SCRIBE_B_HOST PRODUCER_HOST; do
    if [[ -z "${!var:-}" ]]; then
      echo "preflight: $hosts_file missing $var" >&2
      missing=1
    else
      echo "preflight: $var=${!var}"
    fi
  done
else
  echo "preflight: optional hosts.env absent (expected keys: RUSTFS_HOST SCRIBE_A_HOST SCRIBE_B_HOST PRODUCER_HOST)"
fi

if [[ "$missing" -ne 0 ]]; then
  echo "preflight: FAILED" >&2
  exit 1
fi

echo "preflight: OK (no secrets printed; SSH drill not started)"
