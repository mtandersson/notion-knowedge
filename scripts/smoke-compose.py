#!/usr/bin/env python3
"""Exercise the real local Compose HTTP server in an isolated, disposable project.

No Notion token or real index is needed. Requires Docker Compose and access
to the container registry to build the pinned production image.
"""
import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "scripts" / "compose-dev.sh"


def command(env: dict[str, str], *args: str) -> None:
    subprocess.run([str(COMPOSE), *args], cwd=ROOT, env=env, check=True)


def http_json(url: str) -> tuple[int, dict]:
    req = urllib.request.Request(url, headers={"Host": "localhost:3000"})
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as exc:
        with exc:
            return exc.code, json.load(exc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prebuilt", action="store_true",
                        help="Use the already loaded notion-knowledge:local image")
    args = parser.parse_args()
    # Keep the live stack untouched even when a dev instance already exists.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = os.environ.copy()
    env.update(
        COMPOSE_PROJECT_NAME=f"nk-compose-smoke-{os.getpid()}",
        NK_COMPOSE_PORT=str(port),
        NK_NOTION_AUTH="none",
    )
    # Explicitly suppress any real credentials supplied by the parent shell.
    env["NOTION_TOKEN"] = ""
    try:
        command(env, "config", "--quiet")
        command(env, "up", "-d", "--no-build" if args.prebuilt else "--build")
        deadline = time.monotonic() + 45
        while True:
            try:
                live_status, live = http_json(f"http://127.0.0.1:{port}/livez")
                if live_status == 200 and live.get("status") == "alive":
                    break
            except (OSError, ValueError, json.JSONDecodeError):
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError("HTTP /livez did not become available")
            time.sleep(1)

        ready_status, ready = http_json(f"http://127.0.0.1:{port}/readyz")
        if ready_status not in (200, 503) or ready.get("status") not in ("ready", "not_ready"):
            raise RuntimeError(f"Unexpected /readyz result: {ready_status} {ready}")
        health_status, health = http_json(f"http://127.0.0.1:{port}/health")
        if health_status not in (200, 503) or "status" not in health:
            raise RuntimeError(f"Unexpected /health result: {health_status} {health}")
        print(f"Compose HTTP smoke passed (port {port}, readiness {ready_status}).")
        return 0
    finally:
        # Only the one-off smoke project is removed; never touch a real stack.
        # Run even after a partially completed 'up'.
        try:
            command(env, "down", "--volumes", "--remove-orphans")
        except subprocess.CalledProcessError as exc:
            print(f"Warning: could not clean up disposable smoke project: {exc}", file=sys.stderr)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"Compose smoke failed: {exc}", file=sys.stderr)
        sys.exit(1)
