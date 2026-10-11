#!/usr/bin/env bash
set -euo pipefail

local_docs=false
if [[ $# -eq 1 && "$1" == --local ]]; then
  local_docs=true
elif [[ $# -ne 0 ]]; then
  echo 'usage: check-docs.sh [--local]' >&2
  exit 1
fi

required=(
  README.md
  CHANGELOG.md
  CODE_OF_CONDUCT.md
  CONTRIBUTING.md
  LICENSE
  SECURITY.md
  SUPPORT.md
  docs/api.md
  docs/architecture.md
  docs/compatibility.md
  docs/examples.md
  docs/guarantees.md
  docs/idempotency.md
  docs/migrations.md
  docs/operations.md
  docs/postgresql.md
  docs/quickstart.md
  docs/README.md
  docs/runbooks.md
  docs/telemetry.md
  docs/troubleshooting.md
)

for path in "${required[@]}"; do
  test -s "$path" || {
    echo "missing required documentation: $path" >&2
    exit 1
  }
done

if [[ "$local_docs" == false ]]; then
  while IFS= read -r package; do
    go doc "$package" >/dev/null
  done < <(go list ./...)
fi

if grep -RniE 'guarantees exactly.once|exactly.once delivery is guaranteed|provides exactly.once' \
  --include='*.md' --include='*.go' .; then
  echo "false exactly-once claim detected" >&2
  exit 1
fi

echo "required documentation and package docs are present"
