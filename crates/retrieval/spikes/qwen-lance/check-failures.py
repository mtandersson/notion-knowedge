#!/usr/bin/env python3
"""Verify real executable preflight failures without loading model weights."""
import json
import pathlib
import subprocess
import sys
import tempfile

exe = pathlib.Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix="nk-vector-failures-") as temp:
    root = pathlib.Path(temp)
    result = subprocess.run([str(exe), "create", str(root / "missing-assets"), str(root / "new-index")], capture_output=True, text=True)
    assert result.returncode != 0 and "missing model asset: config.json" in result.stderr, result.stderr
    assert not (root / "new-index").exists()
    print("missing assets: clear failure, no index created")

    assets = root / "invalid-assets"
    assets.mkdir()
    for name in ["config.json", "tokenizer.json", "model.safetensors"]:
        (assets / name).write_text("invalid synthetic asset")
    result = subprocess.run([str(exe), "create", str(assets), str(root / "new-index")], capture_output=True, text=True)
    assert result.returncode != 0 and "model asset hash mismatch: config.json" in result.stderr, result.stderr
    assert not (root / "new-index").exists()
    print("wrong asset revision: hash mismatch, no index created")

    index = root / "incompatible-index"
    index.mkdir()
    metadata = {"schema_version": "1", "provider_id": "different-provider", "model_id": "Qwen/Qwen3-Embedding-0.6B", "model_version": "different-version", "dimension": 1024}
    (index / "embedding.json").write_text(json.dumps(metadata))
    result = subprocess.run([str(exe), "query", str(root / "missing-assets"), str(index)], capture_output=True, text=True)
    assert result.returncode != 0 and "incompatible vector metadata" in result.stderr, result.stderr
    assert "missing model asset" not in result.stderr
    print("same-dimension incompatible metadata: rebuild failure before model loading")
