#!/usr/bin/env python3
"""Prove real production indexing, offline Qwen and retrieval in the final image.

Requires Docker, Python 3.9+, OpenSSL and external pinned public model assets.
The shipping executable uses its fixed HTTPS Notion endpoint, resolved only
inside an isolated Docker network to a disposable TLS fixture. No real secrets,
provider writes, production endpoint override or test embedding provider exist.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import selectors
import subprocess
import tempfile
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent.parent
FIXTURE_SCRIPT = ROOT / "scripts/container-notion-fixture.py"
PYTHON_IMAGE = "python:3.13-slim-bookworm@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641"
BINARY = "/usr/local/bin/notion-knowledge-server"
DATA = "/var/lib/notion-knowledge"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


common = load("container_smoke", ROOT / "scripts/smoke-container.py")
fixture = load("notion_fixture", FIXTURE_SCRIPT)


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 300)
    kwargs.setdefault("check", True)
    kwargs.setdefault("text", True)
    return subprocess.run(list(args), **kwargs)


def docker(*args, **kwargs):
    return run("docker", *args, **kwargs)


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def assets(directory, download):
    # The production provider owns the model revision and all asset checksums.
    source = (ROOT / "crates/retrieval/src/qwen.rs").read_text()
    revision = re.search(r'pub const REVISION: &str = "([0-9a-f]+)"', source).group(1)
    block = re.search(r"const ASSETS:.*?= \[(.*?)\n\];", source, re.S).group(1)
    entries = re.findall(r'"([^"/]+)",\s*"([0-9a-f]{64})"', block)
    common.require(len(entries) == 3, "Production model asset manifest could not be read")
    directory.mkdir(parents=True, exist_ok=True)
    for name, checksum in entries:
        path = directory / name
        if not path.is_file() or digest(path) != checksum:
            if not download:
                raise RuntimeError(f"Missing/incompatible public model asset: {name}; provision it externally")
            url = f"https://huggingface.co/Qwen/Qwen3-Embedding-0.6B/resolve/{revision}/{name}"
            temporary = path.with_suffix(path.suffix + ".download")
            try:
                with urllib.request.urlopen(url, timeout=60) as response, temporary.open("wb") as output:
                    size = 0
                    while chunk := response.read(1024 * 1024):
                        size += len(chunk)
                        common.require(size <= 2 * 1024 ** 3, "Public model asset exceeds expected size limit")
                        output.write(chunk)
                common.require(digest(temporary) == checksum, f"Pinned public asset checksum mismatch: {name}")
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
    print("External production Qwen assets verified against provider-owned SHA-256 manifest.")


def certificates(directory):
    quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=Disposable container fixture CA", "-keyout", str(directory / "ca.key"),
        "-out", str(directory / "ca.pem"), **quiet)
    run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=api.notion.com",
        "-keyout", str(directory / "server.key"), "-out", str(directory / "server.csr"), **quiet)
    (directory / "extensions").write_text(
        "subjectAltName=DNS:api.notion.com\nbasicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
    run("openssl", "x509", "-req", "-days", "1", "-in", str(directory / "server.csr"),
        "-CA", str(directory / "ca.pem"), "-CAkey", str(directory / "ca.key"), "-CAcreateserial",
        "-extfile", str(directory / "extensions"), "-out", str(directory / "server.pem"), **quiet)


class Stack:
    def __init__(self, image, model, ca):
        self.image, self.model, self.ca = image, model, ca
        self.prefix = f"nk-runtime-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.network = self.prefix + "-network"
        self.containers, self.volumes = [], []
        self.index = self.state = None
        self.network_created = False

    def helper(self, action, *args, network=False, check=True):
        return docker("run", "--rm", "--read-only", "--network", self.network if network else "none",
            "--user", "65532:65532", "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--mount", f"type=bind,src={FIXTURE_SCRIPT},dst=/fixture.py,readonly",
            "--mount", f"type=volume,src={self.index},dst=/index,readonly",
            "--mount", f"type=volume,src={self.state},dst=/state,readonly",
            PYTHON_IMAGE, "python", "/fixture.py", action, *args,
            capture_output=True, check=check, timeout=30)

    def options(self, name, model=None):
        self.containers.append(name)
        env = {
            "NK_NOTION_AUTH": "integration", "NOTION_TOKEN": "container-fixture-credential",
            "NK_NOTION_SCOPE_WORKSPACE_ID": fixture.WORKSPACE, "NK_NOTION_SCOPE_ROOTS": fixture.ROOT,
            "NK_WEBHOOK_INDEX_DIR": DATA + "/index", "NK_WEBHOOK_STATE_FILE": DATA + "/state/state.sqlite",
            "NK_WEBHOOK_QWEN_ASSETS": DATA + "/models", "NK_WEBHOOK_TIMEOUT_SECONDS": "300",
            "SSL_CERT_FILE": "/fixture-ca.pem",
        }
        options = ["--name", name, "--read-only", "--network", self.network,
            "--user", "65532:65532", "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=268435456,mode=0700,uid=65532,gid=65532",
            "--mount", f"type=volume,src={self.index},dst={DATA}/index",
            "--mount", f"type=volume,src={self.state},dst={DATA}/state",
            "--mount", f"type=bind,src={model or self.model},dst={DATA}/models,readonly",
            "--mount", f"type=bind,src={self.ca},dst=/fixture-ca.pem,readonly"]
        for key, value in env.items():
            options.extend(["--env", f"{key}={value}"])
        return options

    def new_volumes(self, generation):
        self.index, self.state = [self.prefix + f"-{kind}-{generation}" for kind in ("index", "state")]
        for volume in (self.index, self.state):
            docker("volume", "create", volume, stdout=subprocess.DEVNULL)
            self.volumes.append(volume)

    def worker(self, generation):
        name = self.prefix + f"-writer-{generation}"
        options = self.options(name)
        for key, value in {
            "NK_HTTP_HOST": "0.0.0.0", "NK_WEBHOOK_WORKER": "true", "NK_WEBHOOK_MODE": "verified",
            "NK_WEBHOOK_VERIFICATION_TOKEN": fixture.SIGNING_KEY,
            "NK_WEBHOOK_WORKSPACE_ID": fixture.WORKSPACE, "NK_WEBHOOK_INTEGRATION_ID": fixture.INTEGRATION,
            "NK_WEBHOOK_SUBSCRIPTION_ID": fixture.SUBSCRIPTION, "NK_WEBHOOK_POLL_MS": "20",
            "NK_WEBHOOK_DEBOUNCE_MS": "1", "NK_WEBHOOK_MAX_DELAY_MS": "1",
        }.items():
            options.extend(["--env", f"{key}={value}"])
        docker("run", "-d", "--network-alias", "indexer", *options, self.image, "--http",
            stdout=subprocess.DEVNULL)
        self.wait(lambda: self.helper("health", network=True, check=False).returncode == 0,
                  "Actual in-process indexing worker did not start", name)
        event = str(uuid.uuid4())
        self.helper("admit", "--event", event, network=True)
        self.wait(lambda: self.helper("page", check=False).returncode == 0,
                  "Signed hint did not produce an authoritative committed page", name)
        self.wait(lambda: self.helper("disposable", check=False).returncode == 0,
                  "Fixture work did not reach a completed no-write state", name)
        checkpoint = json.loads(self.helper("page").stdout)
        permissions = json.loads(self.helper("permissions").stdout)
        print(f"Actual runtime storage permissions: {permissions}", flush=True)
        common.require(permissions == {"index": {"uid": 65532, "gid": 65532, "mode": "0o700"},
            "state": {"uid": 65532, "gid": 65532, "mode": "0o600"}},
            "Actual indexing path did not create private operator-owned storage")
        docker("stop", "--time", "30", name, stdout=subprocess.DEVNULL)
        common.cleanup(name)
        return checkpoint

    def wait(self, predicate, message, container):
        deadline = time.monotonic() + 300
        while not predicate():
            running = docker("inspect", "--format", "{{.State.Running}}", container,
                             capture_output=True).stdout.strip()
            if running != "true":
                diagnostic = docker("logs", "--tail", "20", container, capture_output=True)
                raise RuntimeError(f"{message}: {diagnostic.stderr[-2000:]}")
            if time.monotonic() >= deadline:
                raise RuntimeError(message)
            time.sleep(1)

    def maintain(self, generation, action="ensure", model=None, expected=0):
        name = self.prefix + f"-index-{generation}-{action}"
        result = docker("run", *self.options(name, model), self.image, "--index", action,
                        check=False, capture_output=True)
        common.require(result.returncode == expected, "Shipping index command returned an unexpected exit status")
        output = json.loads(result.stdout)
        common.require(output == {"operation": "index", "status": "complete" if expected == 0 else "model_configuration"},
                       "Shipping index command did not report its expected bounded result")

    def retrieve(self, generation):
        name = self.prefix + f"-reader-{generation}"
        with tempfile.TemporaryFile(mode="w+t") as diagnostics:
            process = subprocess.Popen(["docker", "run", "-i", *self.options(name),
                "--env", "NK_RETRIEVAL=true", self.image], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=diagnostics, text=True)
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)

            def send(value, response=True):
                process.stdin.write(json.dumps(value) + "\n")
                process.stdin.flush()
                if not response:
                    return None
                deadline = time.monotonic() + 300
                while time.monotonic() < deadline:
                    if selector.select(timeout=1):
                        line = process.stdout.readline()
                        if not line:
                            diagnostics.seek(0)
                            raise RuntimeError("Feature-built MCP reader exited: " + diagnostics.read()[-2000:])
                        reply = json.loads(line)
                        if reply.get("id") == value["id"]:
                            result = common.response(reply, value["id"])
                            common.require(result.get("isError") is not True, "Installed retrieval tool failed")
                            return result
                raise RuntimeError("Feature-built MCP reader did not answer within its deadline")

            try:
                send(common.initialize())
                send({"jsonrpc": "2.0", "method": "notifications/initialized"}, False)
                canonical = None
                for request_id, mode in enumerate(("semantic", "lexical", "hybrid"), 2):
                    output = send({"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": {
                        "name": "knowledge_search", "arguments": {"query": "saffron ravioli", "mode": mode,
                            "limit": 1, "filters": {"page_ids": [fixture.PAGE]}}}})
                    hits = output["structuredContent"]["results"]
                    common.require(len(hits) == 1 and hits[0]["source"]["page_id"] == fixture.PAGE,
                                   "Real semantic/lexical/hybrid retrieval did not return indexed source")
                    common.require(math.isfinite(hits[0]["score"]) and "cinnamon" in hits[0]["text"],
                                   "Indexed source text/finite score missing")
                    identity = (hits[0]["source"]["chunk_id"], hits[0]["text"])
                    if canonical is None:
                        canonical = identity
                    common.require(identity == canonical, "Search paths disagree on the single canonical fixture")
                output = send({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {
                    "name": "knowledge_get", "arguments": {"refs": [{"kind": "page", "id": fixture.PAGE}],
                        "max_chars": 2000, "freshness": "fresh"}}})
                common.require("cinnamon" in json.dumps(output), "Fresh get did not retrieve the authoritative fixture")
                process.stdin.close()
                common.require(process.wait(timeout=30) == 0, "MCP reader did not shut down cleanly")
                return canonical
            finally:
                selector.close()
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)

    def cleanup(self):
        for name in reversed(self.containers):
            common.cleanup(name)
        for volume in self.volumes:
            docker("volume", "rm", volume, stdout=subprocess.DEVNULL)
        if self.network_created:
            docker("network", "rm", self.network, stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="notion-knowledge:smoke")
    parser.add_argument("--prebuilt", action="store_true")
    parser.add_argument("--assets", type=Path, required=True)
    parser.add_argument("--download-assets", action="store_true")
    args = parser.parse_args()
    assets(args.assets, args.download_assets)
    if not args.prebuilt:
        revision = run("git", "rev-parse", "HEAD", capture_output=True).stdout.strip()
        docker("build", "--build-arg", f"VERSION={common.workspace_version()}", "--build-arg",
            f"REVISION={revision}", "-t", args.image, str(ROOT), timeout=2400)
    docker("pull", PYTHON_IMAGE, stdout=subprocess.DEVNULL)
    with tempfile.TemporaryDirectory(prefix="nk-container-tls-") as temporary:
        directory = Path(temporary)
        certificates(directory)
        stack = Stack(args.image, args.assets.resolve(), directory / "ca.pem")
        try:
            docker("network", "create", "--internal", stack.network, stdout=subprocess.DEVNULL)
            stack.network_created = True
            stack.new_volumes("initial")
            source = stack.prefix + "-notion"
            stack.containers.append(source)
            docker("run", "-d", "--name", source, "--read-only", "--network", stack.network,
                "--user", f"{os.getuid()}:{os.getgid()}",
                "--network-alias", "api.notion.com", "--cap-drop=ALL", "--security-opt=no-new-privileges",
                "--sysctl", "net.ipv4.ip_unprivileged_port_start=0",
                "--mount", f"type=bind,src={directory},dst=/fixture,readonly",
                "--mount", f"type=bind,src={FIXTURE_SCRIPT},dst=/fixture.py,readonly",
                PYTHON_IMAGE, "python", "/fixture.py", "serve", stdout=subprocess.DEVNULL)
            for invalid, content in (("missing", None), ("tampered", "{}")):
                location = directory / invalid
                location.mkdir()
                location.chmod(0o755)
                if content is not None:
                    (location / "config.json").write_text(content)
                stack.maintain(invalid, model=location, expected=2)
            checkpoint = stack.worker("initial")
            stack.maintain("initial")
            before = json.loads(stack.helper("snapshot").stdout)
            first = stack.retrieve("initial")
            common.require(json.loads(stack.helper("snapshot").stdout) == before,
                           "Installed read-only retrieval mutated committed index/state bytes")
            # Replace the reader process/container while retaining the actual mounts.
            common.require(stack.retrieve("replacement") == first, "Container replacement lost retrieval state")
            common.require(json.loads(stack.helper("snapshot").stdout) == before,
                           "Replacement reader mutated the persistent mounts")
            # The supported production rebuild preserves ALL operational rows
            # (except the maintenance lease itself), including actual hint and
            # page-commit receipts. It only retrains derived ANN state.
            ledgers = json.loads(stack.helper("ledgers").stdout)
            common.require(ledgers["webhook_inbox"] and ledgers["index_commit_receipts"],
                           "Supported rebuild must preserve actual nonempty operational receipts")
            stack.maintain("retained-state", action="rebuild")
            common.require(json.loads(stack.helper("ledgers").stdout) == ledgers,
                           "ANN rebuild changed durable operational ledger/checkpoint rows")
            common.require(stack.retrieve("retained-state") == first,
                           "Supported retained-state ANN rebuild changed source-linked retrieval")
            # Remove derived index AND its bound operational state, with no live
            # writers/pending work. The independent authoritative fixture remains.
            stack.helper("disposable")
            for name in list(stack.containers):
                if name != source:
                    common.cleanup(name)
                    stack.containers.remove(name)
            for volume in (stack.index, stack.state):
                docker("volume", "rm", volume, stdout=subprocess.DEVNULL)
                stack.volumes.remove(volume)
            stack.new_volumes("rebuilt")
            common.require(stack.worker("rebuilt") == checkpoint, "Rebuild changed authoritative content identity")
            stack.maintain("rebuilt")
            common.require(stack.retrieve("rebuilt") == first, "Fresh derived storage did not reproduce source-linked retrieval")
            print("Production container acceptance passed: actual worker/Qwen/Lance/SQLite, search/get, replacement, rebuild and asset failures.")
        finally:
            stack.cleanup()


if __name__ == "__main__":
    main()
