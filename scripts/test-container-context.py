#!/usr/bin/env python3
"""Probe Docker's actual context matching: required source, no private/runtime data."""
import io
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import uuid

ROOT = Path(__file__).resolve().parent.parent


def main():
    required = {
        "Cargo.toml", "Cargo.lock", "crates/server/Cargo.toml",
        "crates/server/src/main.rs", "crates/retrieval/migrations/0001_initial.sql",
        "vendor/candle-core/Cargo.toml", "vendor/candle-core/Cargo.toml.orig",
        "vendor/arrow-ipc/Cargo.toml", "vendor/arrow-ipc/src/lib.rs",
        "vendor/arrow-ipc/LICENSE.txt", "vendor/arrow-ipc/NOTICE.txt",
        "vendor/lance-encoding/Cargo.toml", "vendor/lance-encoding/build.rs",
        "vendor/lance-encoding/protos/encodings_v2_0.proto",
        "vendor/lance-encoding/protos/encodings_v2_1.proto",
        "vendor/candle-core/src/lib.rs", "vendor/candle-core/LICENSE", "vendor/README.md",
    }
    forbidden = {
        ".env", ".env.local", "credentials.json", "private.key", "runtime/state.sqlite",
        "crates/server/.env", "crates/server/src/private.key",
        "crates/retrieval/models/model.safetensors", "crates/retrieval/models/config.json",
        "crates/retrieval/models/tokenizer.json", "crates/retrieval/index/chunks.lance/data/file.lance",
        "crates/retrieval/state/state.sqlite", "crates/retrieval/private-notes.txt",
        "vendor/candle-core/models/model.safetensors", "vendor/candle-core/tests/test.pt",
        "vendor/candle-core/src/credentials.json",
    }
    name = f"nk-context-probe-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    image = name + ":local"
    with tempfile.TemporaryDirectory(prefix="nk-context-probe-") as temporary:
        directory = Path(temporary)
        shutil.copyfile(ROOT / ".dockerignore", directory / ".dockerignore")
        for file in required | forbidden:
            path = directory / file
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("public dummy context probe; never a credential or model\n")
        try:
            subprocess.run(["docker", "build", "--quiet", "--tag", image, "--file", "-", str(directory)],
                input="FROM scratch\nCOPY . /context\n", text=True, check=True, capture_output=True, timeout=60)
            subprocess.run(["docker", "create", "--name", name, image, "/never-executed"],
                text=True, check=True, capture_output=True, timeout=30)
            exported = subprocess.check_output(["docker", "export", name], timeout=30)
            with tarfile.open(fileobj=io.BytesIO(exported)) as archive:
                names = {member.name.removeprefix("context/") for member in archive if member.isfile()}
            assert required <= names, f"Required build inputs excluded: {sorted(required - names)}"
            assert not names & forbidden, f"Runtime/private data entered Docker context: {sorted(names & forbidden)}"
            print("Actual Docker context isolation passed: Rust/SQL/vendor inputs retained; models/index/state/keys excluded.")
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)
            subprocess.run(["docker", "image", "rm", image], capture_output=True, timeout=30)


if __name__ == "__main__":
    main()
