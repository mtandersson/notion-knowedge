#!/usr/bin/env python3
"""Opt-in real Notion read/index/search smoke using the shipping executable."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import stat
import subprocess
import tempfile
import time
import uuid
from urllib.parse import urlparse

LIMIT = 1024 * 1024


class SmokeFailure(ValueError):
    """A static, safe-to-retain diagnostic; never constructed from source data."""


def require(condition, message):
    if not condition:
        raise SmokeFailure(message)


def private(path, directory):
    require(path.is_absolute() and path.resolve() == path, "Paths must be absolute without symlinks")
    info = path.stat()
    require(info.st_uid == os.getuid(), "Paths must be operator owned")
    require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode), "Wrong path type")
    require(info.st_mode & 0o077 == 0, "Paths must be private (0700 directory / 0600 file)")


def configuration(args):
    # No inherited NK_* setting may widen this run, enable effects or start workers.
    root, workspace = str(uuid.UUID(args.root)), str(uuid.UUID(args.workspace))
    require(args.confirm_disposable_root == root, "Confirm the exact canonical disposable root UUID")
    require(8 <= len(args.marker) <= 256 and args.marker.isascii() and
            all(c.isalnum() or c in "-_ " for c in args.marker), "Use a distinctive synthetic ASCII marker")
    binary, assets, data, diagnostics = map(Path, (args.binary, args.assets, args.data, args.diagnostics))
    require(binary.is_absolute() and binary.is_file() and os.access(binary, os.X_OK), "Missing executable")
    require(assets.is_absolute() and assets.is_dir() and assets.resolve() == assets, "Missing model assets")
    private(data, True)
    private(data / "index", True)
    private(data / "state", True)
    private(data / "state/state.sqlite", False)
    private(diagnostics.parent, True)
    require(not diagnostics.exists() and diagnostics.is_absolute() and diagnostics.parent.resolve() == diagnostics.parent,
            "Diagnostics must be a new directory beneath a private parent")
    require(data != assets and not data.is_relative_to(assets) and not assets.is_relative_to(data),
            "Model assets and derived state must be separate")
    require(not diagnostics.is_relative_to(data) and not data.is_relative_to(diagnostics),
            "Diagnostics must be separate from derived state")
    require(not diagnostics.is_relative_to(assets) and not assets.is_relative_to(diagnostics),
            "Diagnostics must be separate from model assets")
    token = os.environ.get("NOTION_TOKEN", "")
    require(bool(token), "NOTION_TOKEN must be supplied locally")
    env = {k: v for k, v in os.environ.items() if not k.startswith("NK_") and k != "NOTION_TOKEN"}
    env.update({"NOTION_TOKEN": token, "NK_NOTION_AUTH": "integration", "NK_READ_ONLY": "true",
                "NK_GRAPH": "false", "NK_RETRIEVAL": "true", "NK_NOTION_SCOPE_ROOTS": root,
                "NK_NOTION_SCOPE_WORKSPACE_ID": workspace, "NK_NOTION_SCOPE_GENERATION": "1",
                "NK_WEBHOOK_INDEX_DIR": str(data / "index"),
                "NK_WEBHOOK_STATE_FILE": str(data / "state/state.sqlite"),
                "NK_WEBHOOK_QWEN_ASSETS": str(assets), "NK_WEBHOOK_TIMEOUT_SECONDS": "300"})
    return binary, env, diagnostics, root, workspace


def command(binary, args, env, timeout):
    # Raw output is transient and unlinked, never copied to retained diagnostics.
    with tempfile.TemporaryFile() as output:
        result = subprocess.run([str(binary), *args], env=env, stdout=output,
                                stderr=subprocess.DEVNULL, timeout=timeout, check=False)
        require(result.returncode == 0, "Shipping command failed; retain state and inspect privately")
        require(output.tell() <= LIMIT, "Shipping command output exceeded bound")
        output.seek(0)
        return json.loads(output.read())


class MCP:
    def __init__(self, binary, env, timeout):
        self.process = subprocess.Popen([str(binary)], env=env, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = b""
        self.timeout = timeout
        self.identity = 0

    def send(self, method, params=None, notify=False):
        self.identity += 1
        request = {"jsonrpc": "2.0", "method": method}
        if not notify:
            request["id"] = self.identity
        if params is not None:
            request["params"] = params
        self.process.stdin.write((json.dumps(request) + "\n").encode())
        self.process.stdin.flush()
        if notify:
            return None
        deadline = time.monotonic() + self.timeout
        received = 0
        while time.monotonic() < deadline:
            if b"\n" not in self.buffer:
                if not self.selector.select(timeout=min(1, max(0, deadline - time.monotonic()))):
                    continue
                chunk = os.read(self.process.stdout.fileno(), 65536)
                require(bool(chunk), "MCP exited before response")
                received += len(chunk)
                require(received <= LIMIT, "MCP response exceeded bound")
                self.buffer += chunk
                continue
            line, self.buffer = self.buffer.split(b"\n", 1)
            reply = json.loads(line)
            if reply.get("id") != self.identity:
                continue
            require("error" not in reply and isinstance(reply.get("result"), dict), "MCP protocol failure")
            result = reply["result"]
            require(result.get("isError") is not True, "MCP tool failed")
            return result
        raise SmokeFailure("MCP response deadline exceeded")

    def close(self):
        try:
            self.process.stdin.close()
            require(self.process.wait(timeout=10) == 0, "MCP shutdown failed")
        finally:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait()
            self.process.stdout.close()
            self.selector.close()


def source(source, root, provenance=False, physical_root=None):
    require(source.get("page_id") == root and (not provenance or source.get("root_page_id") == (physical_root or root)),
            "Source identity differs from the selected root page")
    url = source.get("url", "")
    require(isinstance(url, str), "Missing canonical Notion source link")
    require(not any(ord(c) < 33 or ord(c) == 127 for c in url), "Invalid Notion source link")
    parsed = urlparse(url)
    host = parsed.hostname or ""
    require(parsed.scheme == "https" and parsed.username is None and parsed.password is None and
            parsed.port is None and parsed.netloc.lower() == host and
            (host in ("app.notion.com", "notion.so", "notion.site") or
             host.endswith(".notion.so") or host.endswith(".notion.site")),
            "Missing canonical Notion source link")
    segment = parsed.path.rsplit("/", 1)[-1]
    # Match the production page adapter: a UUID or a slug ending in 32 hex digits.
    candidate = (segment if len(segment) == 36 and all(segment[i] == "-" for i in (8, 13, 18, 23))
                 else segment[-32:])
    require(str(uuid.UUID(candidate)) == root, "Notion source link identifies a different page")


def retrieve(client, root, marker, physical_root=None, initialize=True):
    if initialize:
        initialize_client(client)
    return retrieve_initialized(client, root, marker, physical_root)


def initialize_client(client):
    initialization = client.send("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                               "clientInfo": {"name": "notion-read-smoke", "version": "1"}})
    require(initialization.get("protocolVersion") == "2025-03-26", "Unexpected MCP version")
    client.send("notifications/initialized", notify=True)


def retrieve_initialized(client, root, marker, physical_root):
    result = client.send("tools/call", {"name": "knowledge_search", "arguments": {
        "query": marker, "mode": "semantic", "limit": 1, "filters": {"page_ids": [root]}}})
    hits = result.get("structuredContent", {}).get("results", [])
    require(len(hits) == 1, "Expected exactly one selected-page result")
    hit = hits[0]
    source(hit["source"], root)
    require(hit.get("matched_paths") == ["semantic"] and bool(hit["source"].get("chunk_id")),
            "Semantic path or stable indexed chunk identity missing")
    require(marker in hit.get("text", "") and isinstance(hit.get("score"), (int, float)) and
            math.isfinite(hit["score"]), "Indexed marker or finite semantic score missing")
    for freshness in ("indexed", "fresh"):
        result = client.send("tools/call", {"name": "knowledge_get", "arguments": {
            "refs": [{"kind": "page", "id": root}], "max_chars": 65536, "freshness": freshness}})
        sources = result.get("structuredContent", {}).get("sources", [])
        require(bool(sources), "Get returned no source")
        for item in sources:
            source(item["provenance"], root, provenance=True, physical_root=physical_root)
            require(item.get("content_scope") == ("page" if freshness == "fresh" else "indexed"),
                    "Get did not report the requested content scope")
        require(any(marker in item.get("text", "") for item in sources), "Get did not verify marker")
    return hit["source"]["chunk_id"]


def execute(args):
    binary, env, diagnostics, root, workspace = configuration(args)
    diagnostics.mkdir(mode=0o700)
    manifest = {"schema": 1, "root_page_id": root, "workspace_id": workspace,
                "run_id": "smoke-" + uuid.uuid4().hex,
                "phase": "assets", "outcome": "failed"}

    def save():
        path = diagnostics / "result.json"
        with path.open("w") as output:
            os.chmod(path, 0o600)
            json.dump(manifest, output, indent=2)

    try:
        save()
        with binary.open("rb") as executable:
            manifest["binary_sha256"] = hashlib.file_digest(executable, "sha256").hexdigest()
        # Hash exact externally provisioned assets; do not download or retain bytes.
        manifest["assets_sha256"] = {}
        for name in ("config.json", "tokenizer.json", "model.safetensors"):
            with (Path(args.assets) / name).open("rb") as model:
                manifest["assets_sha256"][name] = hashlib.file_digest(model, "sha256").hexdigest()
        manifest["phase"] = "reconcile"
        save()
        result = command(binary, ["--reconcile", "apply", manifest["run_id"]], env, args.timeout)
        require(result.get("operation") == "reconciliation" and result.get("phase") == "Completed" and
                result.get("errors") == 0 and result.get("pending") == 0,
                "Reconciliation did not complete cleanly")
        manifest["phase"] = "index"
        save()
        require(command(binary, ["--index", "ensure"], env, args.timeout) ==
                {"operation": "index", "status": "complete"}, "Index maintenance did not complete")
        manifest["phase"] = "retrieve"
        save()
        client = MCP(binary, env, args.timeout)
        try:
            manifest["chunk_id"] = retrieve(client, root, args.marker)
        finally:
            client.close()
        manifest.update(phase="complete", outcome="verified")
    except SmokeFailure as error:
        manifest["failure"] = str(error)
        raise
    except (ValueError, OSError, subprocess.SubprocessError, KeyError, TypeError):
        manifest["failure"] = "Process, storage or protocol failure; inspect privately"
        raise
    finally:
        save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "assets", "data", "diagnostics", "root", "workspace", "marker",
                 "confirm-disposable-root"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--timeout", type=int, default=600, choices=range(1, 1801), metavar="SECONDS")
    args = parser.parse_args()
    try:
        execute(args)
    except (ValueError, OSError, subprocess.SubprocessError, KeyError, TypeError):
        parser.exit(1, "Read smoke failed. Retain derived state and private result.json; no automatic retry or cleanup.\n")
    print("Read/index/search smoke verified; private result.json retained. Full live write/upload acceptance remains #110.")


if __name__ == "__main__":
    main()
