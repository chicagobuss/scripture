#!/usr/bin/env bash
# Build fleet-lab-node and scripture-load for the local host architecture.
# Cross-compilation is optional and must be documented per-target when used.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"

TARGET_TRIPLE="${1:-}"
FEATURES=(--features fleet-lab)

echo "fleet-exercise: building release binaries (lab only; no HA claim)"
if [[ -n "$TARGET_TRIPLE" ]]; then
  echo "cross target: $TARGET_TRIPLE"
  echo "If this fails, fall back to a native build on each host:"
  echo "  ssh HOST 'cd ~/code/scripture && cargo build -p scriptured --release --features fleet-lab --bin fleet-lab-node && cargo build -p scripture-load --release'"
  cargo build -p scriptured --release "${FEATURES[@]}" --bin fleet-lab-node --target "$TARGET_TRIPLE"
  cargo build -p scripture-load --release --target "$TARGET_TRIPLE"
  OUT="target/${TARGET_TRIPLE}/release"
else
  cargo build -p scriptured --release "${FEATURES[@]}" --bin fleet-lab-node
  cargo build -p scripture-load --release
  OUT="target/release"
fi

mkdir -p deploy/fleet-exercise/bin
cp -f "${OUT}/fleet-lab-node" "${OUT}/scripture-load" deploy/fleet-exercise/bin/
echo "fleet-exercise: wrote deploy/fleet-exercise/bin/{fleet-lab-node,scripture-load}"
