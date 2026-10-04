#!/usr/bin/env bash
set -euo pipefail
# Never generate or update lockfiles as part of a security check.
# The canonical workspace is required; explicit experiments are audited too.
cargo-audit audit --file Cargo.lock --no-yanked "$@"
while IFS= read -r -d '' lockfile; do
  cargo-audit audit --file "$lockfile" --no-yanked "$@"
done < <(python3 - <<'PY'
import os
import sys
for root, directories, files in os.walk('.'):
    directories[:] = sorted(d for d in directories if d not in {'target', '.git', '.direnv'})
    if root != '.' and 'Cargo.lock' in files:
        sys.stdout.buffer.write(os.fsencode(os.path.join(root, 'Cargo.lock')) + b'\0')
PY
)
