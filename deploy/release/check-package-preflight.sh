#!/usr/bin/env bash
# Package/preflight contract for fleet-only RC crates (WP09 review).
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
failed=0

for crate in scripture scripture-service scripture-runtime scripture-cli; do
  if ! grep -q 'publish = \["fleet"\]' "crates/$crate/Cargo.toml"; then
    echo "expected publish = [\"fleet\"] in crates/$crate/Cargo.toml" >&2
    failed=1
  fi
  if grep -q 'publish = true' "crates/$crate/Cargo.toml"; then
    echo "publish = true is forbidden (crates.io risk)" >&2
    failed=1
  fi
done

# Holylog deps must carry version + fleet registry (git is local resolve only).
if ! grep -q 'holylog = { version = "0.2.2", registry = "fleet"' Cargo.toml; then
  echo "holylog must declare version+registry=fleet in workspace.dependencies" >&2
  failed=1
fi
if ! grep -q 'holylog-correctness = { version = "0.1.0", registry = "fleet"' Cargo.toml; then
  echo "holylog-correctness must declare version+registry=fleet" >&2
  failed=1
fi

# Path deps must carry publishable version requirements.
if ! grep -q 'scripture = { path = "../scripture", version = "0.1.0-rc.1" }' crates/scripture-cli/Cargo.toml; then
  echo "scripture-cli path dep missing version" >&2
  failed=1
fi

if [[ ! -f .cargo/config.toml ]] || ! grep -q '\[registries.fleet\]' .cargo/config.toml; then
  echo "committed .cargo/config.toml must define registries.fleet index" >&2
  failed=1
fi

attempt_package() {
  local crate="$1"
  local log="/tmp/${crate}-package.log"
  if cargo package -p "$crate" --locked --no-verify --allow-dirty >"$log" 2>&1; then
    echo "cargo package -p $crate ok"
    return 0
  fi
  if grep -qi 'credential-provider\|authenticated registries\|401\|unauthorized' "$log"; then
    echo "cargo package -p $crate deferred: fleet registry auth required (contract ok; run with Kellnr token for clean-builder)"
    return 0
  fi
  echo "cargo package -p $crate failed:" >&2
  tail -40 "$log" >&2
  return 1
}

attempt_package scripture || failed=1
attempt_package scripture-cli || failed=1

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
echo "package preflight ok"
