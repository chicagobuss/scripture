#!/usr/bin/env bash
# Scan tracked stable manifests for literal secret material.
set -euo pipefail
root="$(cd "$(dirname "$0")/../../.." && pwd)"
stable="$root/deploy/kubernetes/stable"
failed=0
while IFS= read -r file; do
  if grep -Eiq 'stringData:|AWS_SECRET|RUSTFS_SECRET_KEY:[[:space:]]*[^$]|password:[[:space:]]*[^$]|token:[[:space:]]*["'\'']?[A-Za-z0-9+/]{16,}' "$file"; then
    # Allow documented create commands in comments / README / placeholder.
    if [[ "$(basename "$file")" == "secrets.placeholder.yaml" || "$(basename "$file")" == "README.md" ]]; then
      continue
    fi
    echo "possible secret material in $file" >&2
    failed=1
  fi
done < <(find "$stable" -type f \( -name '*.yaml' -o -name '*.yml' -o -name '*.md' \))
if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
echo "stable secret scan ok"
