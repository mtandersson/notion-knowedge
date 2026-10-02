#!/usr/bin/env bash
set -euo pipefail
# Run from the repository root; an optional directory supports disposable tests.
repo="${1:-.}"
if [[ "$(git -C "$repo" rev-parse --is-shallow-repository)" != false ]]; then
  echo 'Secret scanning requires complete history: run git fetch --unshallow.' >&2
  exit 2
fi
# HEAD covers every ancestor; -m includes merge changes against each parent,
# including credentials introduced only while resolving a merge.
# Ignore inline allow comments; exceptions must use reviewed exact fingerprints.
exec gitleaks git --config .gitleaks.toml --gitleaks-ignore-path .gitleaksignore \
  --log-opts="HEAD -m" --redact=100 --no-banner --ignore-gitleaks-allow "$repo"
