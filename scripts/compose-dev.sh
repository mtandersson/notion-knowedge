#!/bin/sh
# Resolve the canonical workspace version before invoking Docker Compose.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
version=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' "$root/Cargo.toml")
if [ -z "$version" ]; then
  echo 'Cannot determine [workspace.package] version from Cargo.toml' >&2
  exit 2
fi
export NK_COMPOSE_VERSION="$version"
cd "$root"
exec docker compose -f compose.yaml "$@"
