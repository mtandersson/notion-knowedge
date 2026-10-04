#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 1 ]]; then echo 'usage: download-assets.sh <assets-dir>' >&2; exit 2; fi
revision=97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3
mkdir -p "$1"
for name in config.json tokenizer.json model.safetensors; do
  curl --fail --location --retry 3 "https://huggingface.co/Qwen/Qwen3-Embedding-0.6B/resolve/$revision/$name" --output "$1/$name"
done
# The executable validates all SHA256 hashes before loading any weights.
