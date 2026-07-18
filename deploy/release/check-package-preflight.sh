#!/usr/bin/env bash
# Package/preflight contract for fleet-only RC crates (WP09 review).
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
failed=0
deferred=0

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

registry_configured() {
  [[ -n "${CARGO_REGISTRIES_FLEET_INDEX:-}" ]] && return 0
  local cargo_home="${CARGO_HOME:-$HOME/.cargo}"
  [[ -f "$cargo_home/config.toml" ]] && grep -q '\[registries.fleet\]' "$cargo_home/config.toml"
}

attempt_package() {
  local crate="$1"
  local log="/tmp/${crate}-package.log"
  if cargo package -p "$crate" --locked --no-verify --allow-dirty >"$log" 2>&1; then
    echo "cargo package -p $crate ok"
    return 0
  fi
  if grep -qi 'credential-provider\|authenticated registries\|401\|unauthorized' "$log"; then
    echo "cargo package -p $crate not attested: fleet registry auth required" >&2
    deferred=1
    return 0
  fi
  echo "cargo package -p $crate failed:" >&2
  tail -40 "$log" >&2
  return 1
}

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi

if ! registry_configured; then
  echo "fleet registry configuration is operator-local and unavailable; package attestation not run" >&2
  exit 2
fi

for crate in scripture scripture-service scripture-runtime scripture-cli; do
  attempt_package "$crate" || failed=1
done

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
if [[ "$deferred" -ne 0 ]]; then
  echo "package contract valid, but authenticated fleet package attestation is incomplete" >&2
  exit 2
fi
echo "package preflight ok"
