#!/usr/bin/env python3
"""Build and exercise the final container without credentials or Python packages."""
import argparse
import http.client
import json
import os
from pathlib import Path
import selectors
import subprocess
import tempfile
import tarfile
import time

ROOT = Path(__file__).resolve().parent.parent
SOURCE = "https://github.com/mtandersson/notion-knowedge"


def docker(*args, **kwargs):
    kwargs.setdefault("timeout", 600)
    return subprocess.run(["docker", *args], check=True, text=True, **kwargs)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def search_catalog(value):
    tools = value.get("tools", [])
    names = [tool.get("name") for tool in tools]
    require(len(names) == len(set(names)), "Duplicate MCP tool names")
    search = next((tool for tool in tools if tool.get("name") == "knowledge_search"), None)
    get = next((tool for tool in tools if tool.get("name") == "knowledge_get"), None)
    require(set(names) == {"knowledge_search", "knowledge_get"},
            "Read-only catalog must hide all mutation and upload tools")
    require(search is not None and get is not None, "Expected read tools missing")
    require(search["inputSchema"]["properties"]["mode"]["enum"] == ["semantic", "lexical", "hybrid"], "Search modes missing")
    require("score" in search["outputSchema"]["properties"]["results"]["items"]["properties"], "Search scores missing")
    require(get["inputSchema"]["required"] == ["refs", "max_chars"], "knowledge_get inputs missing")
    require(get["annotations"]["readOnlyHint"] is True, "knowledge_get must be read-only")


def cleanup(name):
    # Some local ZFS Docker installations briefly hold a dataset after exit.
    for attempt in range(5):
        result = subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, timeout=20)
        if result.returncode == 0 or "No such container" in result.stderr:
            return
        time.sleep(0.2 * (attempt + 1))
    raise RuntimeError(f"Container cleanup failed: {result.stderr}")


def run_once(image, args, options=None):
    name = f"nk-smoke-once-{os.getpid()}"
    try:
        return subprocess.run(["docker", "run", "--name", name, "--network=none", "--read-only",
                               *(options or []), image, *args], capture_output=True, text=True, timeout=20)
    finally:
        cleanup(name)


def mounts_and_linkage(image, builder_image=None):
    name = f"nk-smoke-files-{os.getpid()}"
    compiler = f"nk-smoke-compiler-{os.getpid()}"
    build_builder = builder_image is None
    builder_image = builder_image or f"{image}-builder"
    volumes = [f"nk-smoke-{kind}-{os.getpid()}" for kind in ("index", "state", "models")]
    probe = r'''use std::fs;
unsafe extern "C" { fn getuid() -> u32; fn getgid() -> u32; }
fn main() {
    assert_eq!(unsafe { getuid() }, 65532);
    assert_eq!(unsafe { getgid() }, 65532);
    for kind in ["index", "state", "models"] {
        let path = format!("/var/lib/notion-knowledge/{kind}/smoke");
        fs::write(&path, b"writable").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"writable");
        fs::remove_file(&path).unwrap();
    }
}'''
    with tempfile.TemporaryDirectory() as temporary:
        try:
            docker("create", "--name", name, image, "--check", stdout=subprocess.DEVNULL)
            archive_path = Path(temporary) / "rootfs.tar"
            docker("export", "--output", str(archive_path), name)
            with tarfile.open(archive_path) as archive:
                for kind in ("index", "state", "models"):
                    member = archive.getmember(f"var/lib/notion-knowledge/{kind}")
                    require(member.isdir() and member.uid == 65532 and member.gid == 65532
                            and member.mode & 0o700 == 0o700, f"Wrong {kind} permissions")
                loaders = [m.name for m in archive.getmembers()
                           if Path(m.name).name.startswith("ld-linux") and (m.isfile() or m.issym())]
                require(loaders, "Runtime ELF loader missing")
            cleanup(name)
            linked = run_once(image, ["--list", "/usr/local/bin/notion-knowledge-server"],
                              ["--entrypoint", "/" + loaders[0]])
            require(linked.returncode == 0 and "libc.so" in linked.stdout
                    and "libgcc_s.so" in linked.stdout and "not found" not in linked.stdout,
                    f"Final executable linkage failed: {linked.stdout} {linked.stderr}")
            # Compile a disposable permission probe using the same pinned builder.
            # Inject it into a test container only; it never enters the runtime image.
            if build_builder:
                docker("build", "--target", "probe-builder", "--tag", builder_image, str(ROOT))
            docker("create", "-i", "--name", compiler, "--entrypoint", "rustc", builder_image,
                   "-", "-o", "/tmp/mount-probe", stdout=subprocess.DEVNULL)
            docker("start", "-ai", compiler, input=probe, capture_output=True)
            probe_path = str(Path(temporary) / "mount-probe")
            docker("cp", f"{compiler}:/tmp/mount-probe", probe_path)
            mounts = []
            for kind, volume in zip(("index", "state", "models"), volumes):
                docker("volume", "create", volume, stdout=subprocess.DEVNULL)
                mounts += ["--mount", f"type=volume,src={volume},dst=/var/lib/notion-knowledge/{kind}"]
            docker("create", "--name", name, "--network=none", "--read-only", *mounts,
                   "--mount", f"type=bind,src={probe_path},dst=/tmp/mount-probe,readonly",
                   "--entrypoint", "/tmp/mount-probe", image, stdout=subprocess.DEVNULL)
            docker("start", "-a", name, capture_output=True)
            state = json.loads(docker("inspect", name, capture_output=True).stdout)[0]["State"]
            require(state["ExitCode"] == 0, "Non-root writes to external volumes failed")
        finally:
            cleanup(name)
            cleanup(compiler)
            for volume in volumes:
                subprocess.run(["docker", "volume", "rm", volume], capture_output=True, timeout=20)


def initialize():
    return {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-03-26", "capabilities": {},
        "clientInfo": {"name": "container-smoke", "version": "1.0.0"}}}


def response(value, request_id):
    require(value.get("jsonrpc") == "2.0" and value.get("id") == request_id,
            f"Unexpected protocol response: {value}")
    require("error" not in value, f"MCP error: {value}")
    return value["result"]


def stdio(image):
    name = f"nk-smoke-stdio-{os.getpid()}"
    with tempfile.TemporaryFile(mode="w+t") as stderr:
        child = subprocess.Popen(["docker", "run", "-i", "--name", name,
                                  "--network=none", "--read-only", image],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=stderr, text=True, bufsize=1)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                def exchange(request):
                    child.stdin.write(json.dumps(request) + "\n")
                    child.stdin.flush()
                    require(selector.select(20), "Timed out waiting for stdio frame")
                    line = child.stdout.readline()
                    require(line, "Container closed stdout before replying")
                    return response(json.loads(line), request["id"])
                result = exchange(initialize())
                require(result["serverInfo"]["name"] == "notion-knowledge", "Wrong server")
                require(result["protocolVersion"] == "2025-03-26", "Wrong protocol")
                child.stdin.write(json.dumps({"jsonrpc": "2.0", "method":
                                              "notifications/initialized"}) + "\n")
                child.stdin.flush()
                tools = exchange({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
                search_catalog(tools)
                healthcheck = subprocess.run(
                    ["docker", "exec", name, "/usr/local/bin/notion-knowledge-server", "--healthcheck"],
                    capture_output=True, text=True, timeout=10)
                require(healthcheck.returncode == 0,
                        f"Stdio container process healthcheck failed: {healthcheck.stderr}")
                child.stdin.close()
                require(child.wait(timeout=20) == 0, "Stdio disconnect failed")
                require(child.stdout.read() == "", "Unexpected trailing protocol output")
            stderr.seek(0)
            require("Serving MCP over stdio" in stderr.read(), "Missing startup diagnostic")
        finally:
            cleanup(name)
            if child.poll() is None:
                child.kill()
            child.wait()


def http_smoke(image):
    name = f"nk-smoke-http-{os.getpid()}"
    docker("run", "-d", "--name", name, "--read-only",
           "-e", "NK_HTTP_HOST=0.0.0.0", "-p", "127.0.0.1::3000", image, "--http",
           stdout=subprocess.DEVNULL)
    try:
        port = int(docker("port", name, "3000/tcp", capture_output=True).stdout.strip().rsplit(":", 1)[1])
        deadline = time.monotonic() + 20
        while "Serving MCP over Streamable HTTP" not in docker("logs", name, capture_output=True).stderr:
            require(time.monotonic() < deadline, "HTTP listener did not start")
            time.sleep(0.1)

        healthcheck = subprocess.run(
            ["docker", "exec", name, "/usr/local/bin/notion-knowledge-server", "--healthcheck"],
            capture_output=True, text=True, timeout=10)
        require(healthcheck.returncode == 0,
                f"HTTP container liveness healthcheck failed: {healthcheck.stderr}")

        def post(request, session=None, extra=None):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
            if session:
                headers.update({"Mcp-Session-Id": session, "Mcp-Protocol-Version": "2025-03-26"})
            headers.update(extra or {})
            connection.request("POST", "/mcp", json.dumps(request), headers)
            reply = connection.getresponse()
            status, token = reply.status, reply.getheader("mcp-session-id")
            try:
                if status != 200:
                    return status, token, reply.read()
                if reply.getheader("content-type", "").startswith("text/event-stream"):
                    while True:
                        line = reply.readline()
                        require(line, "SSE ended without a response")
                        if line.startswith(b"data: ") and line[6:].strip():
                            value = json.loads(line[6:])
                            if "id" in value:
                                return status, token, value
                return status, token, json.loads(reply.read())
            finally:
                connection.close()

        for endpoint, expected_status, expected_body in [
            ("/livez", 200, {
                "server": {"name": "notion-knowledge", "version": workspace_version()},
                "transport": "http", "status": "alive"}),
            ("/readyz", 503, {
                "server": {"name": "notion-knowledge", "version": workspace_version()},
                "transport": "http", "status": "not_ready",
                "dependencies": {"index": "unavailable"}}),
            ("/health", 503, {
                "server": {"name": "notion-knowledge", "version": workspace_version()},
                "transport": "http", "status": "degraded",
                "access": {"read_only": True},
                "dependencies": {"notion": "unconfigured", "index": "unavailable"}}),
        ]:
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            try:
                connection.request("GET", endpoint)
                reply = connection.getresponse()
                require(reply.status == expected_status,
                        f"Unexpected {endpoint} status: {reply.status}")
                require(reply.getheader("cache-control") == "no-store",
                        f"{endpoint} must not be cached")
                require(json.loads(reply.read()) == expected_body,
                        f"Wrong {endpoint} report")
            finally:
                connection.close()

        status, session, value = post(initialize())
        require(status == 200 and session, "HTTP initialization failed")
        require(response(value, 1)["serverInfo"]["name"] == "notion-knowledge", "Wrong HTTP server")
        status, _, _ = post({"jsonrpc": "2.0", "method": "notifications/initialized"}, session)
        require(status == 202, "HTTP initialization notification failed")
        status, _, value = post({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, session)
        require(status == 200, "HTTP discovery failed")
        search_catalog(response(value, 2))
        for headers in [{"Host": "untrusted.example"}, {"Origin": "https://untrusted.example"}]:
            status, _, _ = post({"jsonrpc": "2.0", "id": 3, "method": "ping"}, session, headers)
            require(status == 403, "Untrusted Host/Origin was accepted")
        docker("stop", "--time", "10", name, stdout=subprocess.DEVNULL)
        state = json.loads(docker("inspect", name, capture_output=True).stdout)[0]["State"]
        require(state["ExitCode"] == 0, "HTTP graceful shutdown failed")
    finally:
        cleanup(name)


def workspace_version():
    # Read only the canonical workspace package version, without tomllib.
    section = ""
    for line in (ROOT / "Cargo.toml").read_text().splitlines():
        if line.startswith("["):
            section = line
        if section == "[workspace.package]" and line.startswith("version ="):
            return line.split('"')[1]
    raise RuntimeError("Missing Cargo workspace package version")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="notion-knowledge:smoke")
    parser.add_argument("--prebuilt", action="store_true",
                        help="Verify an already loaded final image instead of building it")
    parser.add_argument("--builder-image",
                        help="Already loaded pinned builder used for the disposable mount probe")
    args = parser.parse_args()
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    version = workspace_version()
    if not args.prebuilt:
        docker("build", "--build-arg", f"VERSION={version}", "--build-arg", f"REVISION={revision}",
               "--tag", args.image, str(ROOT))
    for invalid_version in [None, "not-the-Cargo-version"]:
        options = [] if invalid_version is None else ["--build-arg", f"VERSION={invalid_version}"]
        rejected = subprocess.run(["docker", "build", "--target", "builder", *options, str(ROOT)],
                                  capture_output=True, text=True)
        require(rejected.returncode != 0 and 'test "$VERSION"' in rejected.stderr,
                "Missing or mismatched OCI version must fail at version validation")
    metadata = json.loads(docker("image", "inspect", args.image, capture_output=True).stdout)[0]["Config"]
    require(metadata["User"] == "65532:65532", "Image must use numeric non-root UID/GID")
    require(metadata["Entrypoint"] == ["/usr/local/bin/notion-knowledge-server"], "Wrong entrypoint")
    require(metadata.get("Healthcheck", {}).get("Test") ==
            ["CMD", "/usr/local/bin/notion-knowledge-server", "--healthcheck"],
            "Container healthcheck must use the server liveness probe")
    for key, expected in {"version": version, "revision": revision, "source": SOURCE}.items():
        require(metadata["Labels"].get(f"org.opencontainers.image.{key}") == expected, f"Wrong {key} label")
    identity = run_once(args.image, ["--version"])
    require(identity.returncode == 0 and identity.stdout == f"notion-knowledge {version}\n",
            "Container identity must match Cargo and OCI label")
    sentinel = "https://private.example/file?X-Amz-Signature=secret-sentinel"
    diagnostic = run_once(args.image, ["--diagnostics"],
                          ["-e", "NK_NOTION_AUTH=integration", "-e", f"NOTION_TOKEN={sentinel}"])
    require(diagnostic.returncode == 0 and sentinel not in diagnostic.stdout + diagnostic.stderr,
            "Container diagnostics failed or leaked credentials")
    require(json.loads(diagnostic.stdout) == {
        "server": {"name": "notion-knowledge", "version": version}, "transport": "one-shot",
        "access": {"read_only": True},
        "status": "degraded", "dependencies": {"notion": "unavailable", "index": "unavailable"}},
        "Configured credentials must not imply healthy adapters")
    valid = run_once(args.image, ["--check"])
    require(valid.returncode == 0 and valid.stdout == "" and "bootstrap ready" in valid.stderr,
            "Composition check failed")
    for setting, value in [("NK_HTTP_PORT", "0"), ("NK_HTTP_HOST", "not-an-ip"),
                           ("NK_NOTION_AUTH", "unknown"), ("NOTION_TOKEN", "secret sentinel")]:
        env = ["-e", f"{setting}={value}"]
        if setting == "NOTION_TOKEN":
            env += ["-e", "NK_NOTION_AUTH=integration"]
        result = run_once(args.image, ["--check"], env)
        require(result.returncode == 2 and setting in result.stderr and not result.stdout,
                f"Invalid {setting} did not return configuration failure")
        require(value not in result.stderr if len(value) > 1 else True, "Configuration value leaked")
    mounts_and_linkage(args.image, args.builder_image)
    stdio(args.image)
    http_smoke(args.image)
    print("Final-image smoke passed: metadata/version guard, non-root startup, config errors, liveness/readiness/diagnostics, stdio and HTTP MCP.")


if __name__ == "__main__":
    main()
