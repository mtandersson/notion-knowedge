#!/usr/bin/env python3
"""Disposable local TLS Notion boundary and read-only volume probes for #145.

This file is mounted into a test-only Python container, never the runtime image.
No real provider credentials or configurable production endpoint are involved.
"""
import argparse
import hashlib
import hmac
import http.client
import http.server
import json
from pathlib import Path
import sqlite3
import ssl
import urllib.parse
from datetime import datetime, timezone

PAGE = "11111111-1111-1111-1111-111111111111"
ROOT = "22222222-2222-2222-2222-222222222222"
TEXT = "# Saffron\n\nSaffron ravioli with cinnamon is a public container fixture recipe.\n"
REVISION = "2026-10-17T12:00:00Z"
WORKSPACE = "44444444-4444-4444-4444-444444444444"
INTEGRATION = "66666666-6666-6666-6666-666666666666"
SUBSCRIPTION = "77777777-7777-7777-7777-777777777777"
SIGNING_KEY = "container-fixture-signing-key"


def metadata(page, root=False):
    return {
        "object": "page", "id": page, "url": f"https://www.notion.so/{page}",
        "last_edited_time": REVISION, "in_trash": False,
        "parent": {"type": "workspace", "workspace": True} if root else {
            "type": "page_id", "page_id": ROOT},
        "properties": {"Title": {"id": "title", "type": "title", "title": [
            {"plain_text": "Container root" if root else "Saffron recipe"}]}}
    }


class Notion(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        if self.headers.get("Authorization") != "Bearer container-fixture-credential":
            self.send_error(401)
            return
        if path == "/v1/users/me":
            body = {"object": "user", "type": "bot", "id": "66666666-6666-6666-6666-666666666666"}
        elif path in (f"/v1/pages/{PAGE}", f"/v1/pages/{ROOT}"):
            body = metadata(path.rsplit("/", 1)[1], path.endswith(ROOT))
        elif path == f"/v1/pages/{PAGE}/markdown":
            body = {"object": "page_markdown", "id": PAGE, "markdown": TEXT,
                    "truncated": False, "unknown_block_ids": []}
        elif path == f"/v1/pages/{ROOT}/markdown":
            body = {"object": "page_markdown", "id": ROOT, "markdown": "# Container root\n",
                    "truncated": False, "unknown_block_ids": []}
        elif path == f"/v1/blocks/{ROOT}/children":
            body = {"object": "list", "results": [{"object": "block", "id": PAGE,
                "type": "child_page", "archived": False, "has_children": False,
                "parent": {"type": "page_id", "page_id": ROOT},
                "child_page": {"title": "Saffron recipe"}}], "has_more": False, "next_cursor": None}
        elif path == f"/v1/blocks/{PAGE}/children":
            body = {"object": "list", "results": [], "has_more": False, "next_cursor": None}
        else:
            self.send_error(404)
            return
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["serve", "page", "snapshot", "health", "admit"])
    parser.add_argument("--event")
    args = parser.parse_args()
    if args.action == "serve":
        server = http.server.ThreadingHTTPServer(("0.0.0.0", 443), Notion)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain("/fixture/server.pem", "/fixture/server.key")
        server.socket = context.wrap_socket(server.socket, server_side=True)
        server.serve_forever()
    elif args.action in ("health", "admit"):
        connection = http.client.HTTPConnection("indexer", 3000, timeout=5)
        if args.action == "health":
            connection.request("GET", "/livez", headers={"Host": "localhost:3000"})
        else:
            if not args.event:
                parser.error("admit requires --event")
            body = json.dumps({"id": args.event, "workspace_id": WORKSPACE,
                "integration_id": INTEGRATION, "subscription_id": SUBSCRIPTION,
                "timestamp": datetime.now(timezone.utc).isoformat(), "attempt_number": 1,
                "type": "page.content_updated", "entity": {"id": PAGE, "type": "page"}}).encode()
            signature = hmac.new(SIGNING_KEY.encode(), body, hashlib.sha256).hexdigest()
            connection.request("POST", "/webhooks/notion", body, {
                "Content-Type": "application/json", "X-Notion-Signature": f"sha256={signature}"})
        response = connection.getresponse()
        if response.status != 200:
            raise SystemExit(f"fixture HTTP admission/liveness failed: {response.status}")
        response.read()
        connection.close()
    elif args.action == "page":
        with sqlite3.connect("file:/state/state.sqlite?mode=ro", uri=True) as state:
            row = state.execute("SELECT content_hash,tombstoned FROM page_sync_state WHERE page_id=?", (PAGE,)).fetchone()
            if not row or not row[0] or row[1] != 0:
                raise SystemExit(1)
            print(json.dumps({"content_hash": row[0], "tombstoned": False}))
    else:
        files = {}
        for directory in (Path("/index"), Path("/state")):
            for file in sorted(directory.rglob("*")):
                if file.is_file():
                    files[str(file)] = hashlib.sha256(file.read_bytes()).hexdigest()
        print(json.dumps(files, sort_keys=True))


if __name__ == "__main__":
    main()
