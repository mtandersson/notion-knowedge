#!/usr/bin/env bash
set -euo pipefail
# Never generate or update the lockfile as part of a security check.
exec cargo audit --file Cargo.lock --no-yanked "$@"
