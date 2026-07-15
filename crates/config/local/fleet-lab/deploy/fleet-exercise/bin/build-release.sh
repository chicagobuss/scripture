#!/usr/bin/env bash
# Build and stage the product `scripture` binary (and optional scripture-load).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPTURE_DIR="${SCRIPTURE_DIR:-$(cd "$ROOT/../../.." && pwd)}"
OUT="$ROOT/deploy/fleet-exercise/bin"
mkdir -p "$OUT"

echo "building scripture from $SCRIPTURE_DIR"
(cd "$SCRIPTURE_DIR" && cargo build --release -p scripture-cli --locked)
cp "$SCRIPTURE_DIR/target/release/scripture" "$OUT/scripture"
chmod +x "$OUT/scripture"

if [[ -d "$ROOT/crates/scripture-load" ]]; then
  echo "building scripture-load (bounded raw-lines client)"
  (cd "$ROOT" && cargo build --release -p scripture-load)
  cp "$ROOT/target/release/scripture-load" "$OUT/scripture-load"
  chmod +x "$OUT/scripture-load"
fi

echo "staged: $OUT/scripture${SCRIPTURE_LOAD:+ and scripture-load}"
ls -la "$OUT/scripture" "$OUT/scripture-load" 2>/dev/null || ls -la "$OUT/scripture"
