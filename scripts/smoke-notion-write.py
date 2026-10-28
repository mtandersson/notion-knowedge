#!/usr/bin/env python3
"""Explicit disposable-root write/upload smoke against installed OAuth HTTP MCP."""
import argparse
import base64
from contextlib import closing
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import uuid
from urllib.parse import urlparse

SPEC = importlib.util.spec_from_file_location("read_smoke", Path(__file__).with_name("smoke-notion-read.py"))
read = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(read)
require, SmokeFailure = read.require, read.SmokeFailure
# Publish only these synthetic bytes at operator-managed allowlisted HTTPS URLs.
IMAGE = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=")
DOCUMENT = b"Notion knowledge disposable smoke synthetic document.\n"


def canonical(value):
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value
    except ValueError:
        return False


class HTTP:
    """One request per call; no redirects, proxies or transport retries."""
    def __init__(self, settings, timeout):
        endpoint = urlparse(settings["endpoint"])
        require(endpoint.scheme == "http" and endpoint.hostname in ("127.0.0.1", "::1") and
                endpoint.path == "/mcp" and not endpoint.query and not endpoint.fragment and
                endpoint.username is None and endpoint.password is None,
                "Use the operator's existing loopback OAuth /mcp listener")
        self.host, self.port = endpoint.hostname, endpoint.port or 80
        self.token, self.timeout = settings["token"], timeout
        require(isinstance(self.token, str) and self.token and not any(c.isspace() for c in self.token),
                "Missing private OAuth access token")
        self.session, self.identity = None, 0

    def send(self, method, params=None, notify=False):
        self.identity += 1
        payload = {"jsonrpc": "2.0", "method": method}
        if not notify:
            payload["id"] = self.identity
        if params is not None:
            payload["params"] = params
        headers = {"Authorization": "Bearer " + self.token, "Content-Type": "application/json",
                   "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-03-26"}
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            connection.request("POST", "/mcp", json.dumps(payload), headers)
            response = connection.getresponse()
            require(200 <= response.status < 300, "OAuth MCP HTTP request failed")
            if method == "initialize":
                self.session = response.getheader("Mcp-Session-Id")
                require(bool(self.session), "Missing MCP session")
            if notify:
                return None
            if response.getheader("Content-Type", "").split(";", 1)[0] == "text/event-stream":
                total, data = 0, []
                deadline = time.monotonic() + self.timeout
                while time.monotonic() < deadline:
                    line = response.readline(read.LIMIT + 1)
                    total += len(line)
                    require(line and total <= read.LIMIT, "MCP SSE response ended or exceeded bound")
                    if line in (b"\n", b"\r\n") and data:
                        reply = json.loads(b"\n".join(data))
                        data = []
                        if reply.get("id") == self.identity:
                            break
                    elif line.startswith(b"data:"):
                        data.append(line[5:].strip())
                else:
                    raise SmokeFailure("MCP response deadline exceeded")
            else:
                body = response.read(read.LIMIT + 1)
                require(len(body) <= read.LIMIT, "MCP response exceeded bound")
                reply = json.loads(body)
            require(reply.get("id") == self.identity and "error" not in reply and
                    isinstance(reply.get("result"), dict), "MCP protocol failure")
            # Preserve structured effect receipts even when terminal audit failed.
            return reply["result"]
        finally:
            connection.close()


def call(client, name, arguments):
    return client.send("tools/call", {"name": name, "arguments": arguments})


def key_digest(workspace, key):
    digest = hashlib.sha256(b"nk-idempotency-key-v1")
    for value in (workspace, key):
        data = value.encode()
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def query(path, sql, parameters):
    # Read-only SQLite connections include live WAL evidence without copying secrets.
    with closing(sqlite3.connect(Path(path).as_uri() + "?mode=ro", uri=True)) as connection:
        return connection.execute(sql, parameters).fetchall()


def validate(args):
    require(args.allow_mutations, "Mutations require --allow-mutations")
    require(canonical(args.root) and args.confirm_disposable_root == args.root,
            "Confirm the exact operator-owned disposable root UUID")
    path, diagnostics = Path(args.settings), Path(args.diagnostics)
    read.private(path, False)
    require(path.stat().st_size <= 32768, "Private settings exceeded bound")
    settings = json.loads(path.read_text())
    require(settings["root_page_id"] == args.root and canonical(settings["workspace_id"]),
            "Private settings must select this exact physical root")
    read.private(diagnostics.parent, True)
    require(diagnostics.is_absolute() and not diagnostics.exists(), "Use a new private diagnostics directory")
    stores = [Path(settings[name]) for name in ("ledger", "audit", "file_audit")]
    for store in stores:
        read.private(store.parent, True)
        read.private(store, False)
        require(not diagnostics.is_relative_to(store.parent), "Diagnostics must be separate from durable stores")
    require(len({(p.stat().st_dev, p.stat().st_ino) for p in stores}) == 3,
            "Ledger and audits must be distinct")
    files = settings["files"]
    require(isinstance(files, list) and len(files) == 2, "Provide exactly one synthetic image and file")
    for item, mime, name in zip(files, ("image/png", "text/plain"), ("smoke.png", "smoke.txt")):
        require(set(item) == {"download_url", "file_id", "mime_type", "file_name"} and
                item["mime_type"] == mime and item["file_name"] == name and
                isinstance(item["file_id"], str) and 1 <= len(item["file_id"]) <= 256 and
                urlparse(item["download_url"]).scheme == "https", "Invalid private synthetic file reference")
    # Validate transport configuration before creating evidence or any mutation.
    return settings, diagnostics, HTTP(settings, args.timeout)


def execute(args):
    settings, diagnostics, client = validate(args)
    diagnostics.mkdir(mode=0o700)
    run = "smoke-" + uuid.uuid4().hex
    marker = "NK556 " + run
    report = {"schema": 1, "run_id": run, "root_page_id": args.root,
              "phase": "discovery", "outcome": "failed", "effects": {},
              "cleanup": "No automatic cleanup. Reconcile uncertain effects before any new run. "
                         "Manually archive only the verified run-owned child after inspection; never the root."}

    def save():
        with tempfile.NamedTemporaryFile(mode="w", dir=diagnostics, delete=False) as output:
            json.dump(report, output, indent=2)
            output.flush()
            os.fsync(output.fileno())
            temporary = output.name
        os.replace(temporary, diagnostics / "result.json")
        descriptor = os.open(diagnostics, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def write(name, arguments, expected_page=None):
        phase = report["phase"]
        effect = {"status": "uncertain", "idempotency_key": arguments["idempotency_key"]}
        report["effects"][phase] = effect
        save()  # Interrupted request is never silently considered unattempted.
        response = call(client, name, arguments)
        result = response.get("structuredContent", {})
        receipt = result.get("write", {})
        page = receipt.get("page_id")
        if isinstance(page, str) and canonical(page):
            effect["page_id"] = page
        effect["status"] = "verified" if receipt.get("status") == "verified" else "uncertain"
        index = result.get("index", {})
        if index.get("status") in ("queued", "running", "completed", "failed", "unavailable"):
            effect["index_status"] = index["status"]
        save()
        require(effect["status"] == "verified" and canonical(page) and page != args.root and
                (expected_page is None or page == expected_page) and receipt.get("replayed") is False,
                "Write receipt did not verify the new run-owned child")
        read.source(receipt, page)
        require(response.get("isError") is not True and result.get("audit", {}).get("status") == "recorded",
                "Write terminal audit not recorded")
        rows = query(settings["ledger"], "SELECT status,page_id FROM mutation_claims WHERE key_digest=?",
                     [key_digest(settings["workspace_id"], arguments["idempotency_key"])])
        require(rows == [("committed", page)], "Durable ledger does not match verified write receipt")
        effect["ledger_verified"] = True
        save()
        return page

    def retrieve(marker_to_check):
        deadline = time.monotonic() + args.wait_seconds
        while True:
            try:
                chunk = read.retrieve(client, report["child_page_id"], marker_to_check,
                                      physical_root=args.root, initialize=False)
                return chunk
            except SmokeFailure:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(min(2, max(0, deadline - time.monotonic())))

    try:
        save()
        read.initialize_client(client)
        tools = client.send("tools/list").get("tools", [])
        require({"knowledge_create_page", "knowledge_append", "knowledge_upload_file", "knowledge_get",
                 "knowledge_search", "knowledge_file_refresh"}.issubset({item["name"] for item in tools}), "Installed guarded tools unavailable")
        report["phase"] = "create"
        page = write("knowledge_create_page", {"root_page_id": args.root, "parent_page_id": args.root,
                     "title": run, "markdown": marker + "\n", "idempotency_key": run + "-create"})
        # Production verified create includes fresh physical direct-parent and exact content checks.
        report["child_page_id"] = page
        report["cleanup_child_page_id"] = page
        report["phase"] = "created_retrieval"
        save()
        report["created_chunk_id"] = retrieve(marker)
        fresh = call(client, "knowledge_get", {"refs": [{"kind": "page", "id": page}],
                      "max_chars": 65536, "freshness": "fresh"})
        sources = fresh.get("structuredContent", {}).get("sources", [])
        require(fresh.get("isError") is not True and len(sources) == 1 and not sources[0].get("truncated"),
                "Fresh source precondition unavailable")
        item = sources[0]
        read.source(item["provenance"], page, provenance=True, physical_root=args.root)
        revision = item["provenance"].get("refreshed_last_edited_time")
        require(item.get("content_scope") == "page" and marker in item.get("text", "") and
                isinstance(revision, str) and bool(revision), "Fresh source revision unavailable")
        report["phase"] = "append"
        appended_marker = marker + " appended"
        write("knowledge_append", {"root_page_id": args.root, "page_id": page,
              "markdown": "\n" + appended_marker + "\n", "expected_last_edited_time": revision,
              "idempotency_key": run + "-append"}, page)
        report["phase"] = "appended_retrieval"
        save()
        report["appended_chunk_id"] = retrieve(appended_marker)
        report["phase"] = "upload"
        report["effects"]["upload"] = {"status": "uncertain", "files": []}
        save()
        response = call(client, "knowledge_upload_file", {"target": page, "caption": run,
                                                         "files": settings["files"]})
        result = response.get("structuredContent", {})
        files = result.get("files", [])
        # Retain only typed identities/statuses, including earlier successful batch members.
        for item in files:
            safe = {"index": item.get("index") if type(item.get("index")) is int else None, "status": item.get("status") if item.get("status") in
                    ("verified", "verified_audit_failed", "failed", "denied", "indeterminate", "unattempted") else "uncertain"}
            for field in ("target", "upload_id", "block_id"):
                value = item.get(field)
                if isinstance(value, str) and canonical(value):
                    safe[field] = value
            refresh = item.get("refresh", {})
            digest = refresh.get("mutation_digest", "")
            if (refresh.get("page_id") == page and isinstance(digest, str) and len(digest) == 64 and
                    all(c in "0123456789abcdef" for c in digest)):
                safe["refresh"] = {"page_id": page, "mutation_digest": digest}
            if item.get("index_status") in ("held", "queued", "running", "completed", "failed", "unavailable", "not_needed"):
                safe["index_status"] = item["index_status"]
            report["effects"]["upload"]["files"].append(safe)
        save()
        require(response.get("isError") is not True and not result.get("failure") and
                len(files) == 2 and all(item.get("index") == index and item.get("status") == "verified" and
                item.get("terminal_audited") is True and item.get("target") == page and
                canonical(item.get("upload_id")) and canonical(item.get("block_id"))
                for index, item in enumerate(files)) and
                len({item["block_id"] for item in files}) == 2,
                "Upload requires two audited authoritative attachment readback receipts")
        # Verified here means the installed adapter checked attached native block readback,
        # not merely Notion's pending upload state. Do not resubmit this call for indexing.
        report["effects"]["upload"]["status"] = "verified"
        for path, tool, minimum in ((settings["audit"], "page_create", 1),
                                   (settings["audit"], "page_append", 1),
                                   (settings["file_audit"], "file_attach", 2)):
            rows = query(path, "SELECT count(*) FROM agent_audit_events WHERE target_page_id=? AND tool=? AND outcome='succeeded'",
                         [page, tool])
            require(rows[0][0] >= minimum, "Durable terminal audit evidence missing")
        for kind, size in (("image", len(IMAGE)), ("other", len(DOCUMENT))):
            rows = query(settings["file_audit"], "SELECT count(*) FROM agent_audit_events WHERE target_page_id=? AND tool='file_attach' AND outcome='succeeded' AND file_kind=? AND file_size_bytes=?",
                         [page, kind, size])
            require(rows[0][0] >= 1, "Synthetic attachment kind or observed tiny byte size differs")
        report["durable_audits_verified"] = True
        report["phase"] = "uploaded_retrieval"
        save()
        receipts = report["effects"]["upload"]["files"]
        require(all("refresh" in item for item in receipts) and receipts[0]["refresh"] == receipts[1]["refresh"],
                "Missing shared derived refresh receipt")
        deadline = time.monotonic() + args.wait_seconds
        while True:
            response = call(client, "knowledge_file_refresh", dict(receipts[0]["refresh"], retry=False))
            status = response.get("structuredContent", {}).get("index_status")
            if status in ("queued", "running", "completed", "failed", "unavailable", "held"):
                report["upload_index_status"] = status
                save()
            require(response.get("isError") is not True and status not in ("failed", "unavailable", "held"),
                    "Attachment verified but derived refresh requires operator reconciliation")
            if status == "completed":
                break
            require(time.monotonic() < deadline, "Attachment verified but derived refresh deadline exceeded")
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        report["uploaded_chunk_id"] = retrieve(appended_marker)
        report.update(phase="complete", outcome="verified")
    except SmokeFailure as error:
        report["failure"] = str(error)
        raise
    except (ValueError, OSError, KeyError, TypeError, sqlite3.Error, http.client.HTTPException):
        report["failure"] = "Private transport, storage or protocol failure; reconcile known effects without replay"
        raise
    finally:
        save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-sources", metavar="NEW_PRIVATE_DIRECTORY")
    for name in ("settings", "diagnostics", "root", "confirm-disposable-root"):
        parser.add_argument("--" + name)
    parser.add_argument("--allow-mutations", action="store_true")
    parser.add_argument("--timeout", type=int, default=120, choices=range(1, 601), metavar="SECONDS")
    parser.add_argument("--wait-seconds", type=int, default=120, choices=range(0, 601), metavar="SECONDS")
    args = parser.parse_args()
    try:
        if args.prepare_sources:
            require(not args.allow_mutations, "Source preparation cannot mutate Notion")
            directory = Path(args.prepare_sources)
            read.private(directory.parent, True)
            require(directory.is_absolute() and not directory.exists(), "Use a new private source directory")
            directory.mkdir(mode=0o700)
            for name, data in (("smoke.png", IMAGE), ("smoke.txt", DOCUMENT)):
                with os.fdopen(os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as output:
                    output.write(data)
            return
        require(all((args.settings, args.diagnostics, args.root, args.confirm_disposable_root)), "Missing required settings")
        execute(args)
    except (ValueError, OSError, KeyError, TypeError, sqlite3.Error, http.client.HTTPException):
        parser.exit(1, "Guarded smoke failed. Retain private result.json and durable stores; reconcile before any new run. No automatic retry or cleanup.\n")
    print("Guarded write/upload receipts and retrieval verified. Live acceptance/cleanup remains #110/#557.")


if __name__ == "__main__":
    main()
