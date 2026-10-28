#!/usr/bin/env python3
"""Credential-free HTTP tool contracts, not live Notion acceptance."""
import argparse
from contextlib import closing
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import zlib
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SPEC = importlib.util.spec_from_file_location("write_smoke", Path(__file__).with_name("smoke-notion-write.py"))
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)
ROOT = "11111111-1111-4111-8111-111111111111"
PAGE = "22222222-2222-4222-8222-222222222222"
WORKSPACE = "33333333-3333-4333-8333-333333333333"
IDS = ["44444444-4444-4444-8444-444444444444", "55555555-5555-4555-8555-555555555555"]


class Contracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        stores = self.base / "stores"
        stores.mkdir(mode=0o700)
        self.settings = {"token": "private-secret-token", "root_page_id": ROOT,
                         "workspace_id": WORKSPACE, "files": [
                             {"download_url": "https://private.invalid/?secret-image", "file_id": "image",
                              "file_name": "smoke.png", "mime_type": "image/png"},
                             {"download_url": "https://private.invalid/?secret-file", "file_id": "document",
                              "file_name": "smoke.txt", "mime_type": "text/plain"}]}
        for name in ("ledger", "audit", "file_audit"):
            path = stores / (name + ".sqlite")
            self.settings[name] = str(path)
            with closing(sqlite3.connect(path)) as db, db:
                if name == "ledger":
                    db.execute("CREATE TABLE mutation_claims(key_digest TEXT, status TEXT, page_id TEXT)")
                else:
                    db.execute("CREATE TABLE agent_audit_events(target_page_id TEXT, tool TEXT, outcome TEXT, file_kind TEXT, file_size_bytes INTEGER)")
            path.chmod(0o600)
        self.mode, self.calls, self.text = "", [], ""
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def do_POST(self):
                owner.assertEqual(self.path, "/mcp")
                owner.assertEqual(self.headers["Authorization"], "Bearer private-secret-token")
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if request["method"] != "initialize":
                    owner.assertEqual(self.headers["Mcp-Session-Id"], "private-session")
                if "id" not in request:
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                result = owner.respond(request)
                if result is None:
                    self.close_connection = True
                    return
                body = json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}).encode()
                if owner.mode == "sse":
                    body = b": heartbeat\n\nevent: message\ndata: " + body + b"\n\n"
                self.send_response(200)
                self.send_header("Mcp-Session-Id", "private-session")
                self.send_header("Content-Type", "text/event-stream" if owner.mode == "sse" else "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop)
        self.settings["endpoint"] = "http://127.0.0.1:%d/mcp" % self.server.server_port
        self.path = self.base / "settings.json"
        self.path.touch(mode=0o600)
        self.path.write_text(json.dumps(self.settings))
        self.args = argparse.Namespace(settings=str(self.path), diagnostics=str(self.base / "evidence"),
                                       root=ROOT, confirm_disposable_root=ROOT, allow_mutations=True,
                                       timeout=2, wait_seconds=0)

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def audit(self, name, tool, kind=None, size=None):
        if self.mode == "missing-durable-audit":
            return
        with closing(sqlite3.connect(self.settings[name])) as db, db:
            db.execute("INSERT INTO agent_audit_events VALUES(?,?,?,?,?)", [PAGE, tool, "succeeded", kind, size])

    def respond(self, request):
        if request["method"] == "initialize":
            return {"protocolVersion": "2025-03-26"}
        if request["method"] == "tools/list":
            return {"tools": [{"name": name} for name in ("knowledge_create_page", "knowledge_append",
                    "knowledge_upload_file", "knowledge_file_refresh", "knowledge_search", "knowledge_get")]}
        name, args = request["params"]["name"], request["params"]["arguments"]
        self.calls.append((name, args))
        if name in ("knowledge_create_page", "knowledge_append"):
            self.assertEqual(args["root_page_id"], ROOT)
            self.assertEqual(args.get("parent_page_id", args.get("page_id")), ROOT if name == "knowledge_create_page" else PAGE)
            if name == "knowledge_create_page":
                self.assertTrue(args["title"].startswith("smoke-"))
                self.assertEqual(args["idempotency_key"], args["title"] + "-create")
            else:
                self.assertEqual(args["expected_last_edited_time"], "2026-10-28T10:00:00Z")
                self.assertTrue(args["idempotency_key"].endswith("-append"))
            if self.mode == "lost-create" and name == "knowledge_create_page":
                return None
            if self.mode == "denied-append" and name == "knowledge_append":
                return {"isError": True, "structuredContent": {"write": {"status": "revision_conflict"}}}
            self.text += args["markdown"]
            with closing(sqlite3.connect(self.settings["ledger"])) as db, db:
                if self.mode != "missing-ledger":
                    db.execute("INSERT INTO mutation_claims VALUES(?,?,?)", [
                        smoke.key_digest(WORKSPACE, args["idempotency_key"]), "committed", PAGE])
            self.audit("audit", "page_create" if name == "knowledge_create_page" else "page_append")
            return {"isError": self.mode == "audit-failed", "structuredContent": {
                "write": {"status": "verified", "page_id": ROOT if self.mode == "wrong-child" else PAGE,
                          "url": "https://notion.so/" + PAGE, "replayed": False},
                "audit": {"status": "unavailable" if self.mode == "audit-failed" else "recorded"},
                "index": {"status": "queued"}}}
        if name == "knowledge_search":
            self.assertEqual(args["filters"], {"page_ids": [PAGE]})
            return {"structuredContent": {"results": [{"source": {"page_id": PAGE,
                    "url": "https://notion.so/" + PAGE, "chunk_id": "fixture-chunk"},
                    "text": self.text, "matched_paths": ["semantic"], "score": .8}]}}
        if name == "knowledge_get":
            self.assertEqual(args["refs"], [{"kind": "page", "id": PAGE}])
            return {"structuredContent": {"sources": [{"text": self.text, "truncated": False,
                    "content_scope": "page" if args["freshness"] == "fresh" else "indexed",
                    "provenance": {"page_id": PAGE, "url": "https://notion.so/" + PAGE,
                                   "root_page_id": WORKSPACE if self.mode == "wrong-scope" else ROOT,
                                   "refreshed_last_edited_time": "2026-10-28T10:00:00Z"}}]}}
        if name == "knowledge_upload_file":
            self.assertEqual(args["target"], PAGE)
            self.assertEqual(args["files"], self.settings["files"])
            if self.mode == "lost-upload":
                return None
            files = []
            for index, identity in enumerate(IDS):
                self.audit("file_audit", "file_attach", "image" if index == 0 else "other",
                           len(smoke.IMAGE) if index == 0 else len(smoke.DOCUMENT))
                files.append({"index": index, "target": PAGE, "upload_id": identity,
                              "block_id": identity, "status": "verified", "terminal_audited": True,
                              "index_status": "queued", "refresh": {"page_id": PAGE, "mutation_digest": "a" * 64}})
            if self.mode == "uncertain-second":
                files[1].update(status="indeterminate", block_id=None)
            if self.mode == "pending-upload":
                files[0].update(status="pending", block_id=None)
            if self.mode == "wrong-target":
                files[0]["target"] = ROOT
            return {"structuredContent": {"files": files}}
        self.assertEqual(name, "knowledge_file_refresh")
        self.assertEqual(args, {"page_id": PAGE, "mutation_digest": "a" * 64, "retry": False})
        return {"structuredContent": {"index_status": "failed" if self.mode == "index-failed" else "completed"}}

    def manifest(self):
        path = self.base / "evidence/result.json"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        text = path.read_text()
        for secret in ("private-secret-token", "private.invalid", "secret-image", "secret-file", "NK556"):
            self.assertNotIn(secret, text)
        return json.loads(text)

    def run_failure(self, mode):
        self.mode = mode
        with self.assertRaises((smoke.SmokeFailure, OSError, smoke.http.client.HTTPException)):
            smoke.execute(self.args)
        return self.manifest()

    def test_json_and_sse_real_http_confine_all_effects_to_created_child(self):
        for mode in ("", "sse"):
            with self.subTest(mode=mode):
                self.mode = mode
                smoke.execute(self.args)
                report = self.manifest()
                self.assertEqual(report["outcome"], "verified")
                self.assertTrue(report["durable_audits_verified"])
                self.assertEqual(report["cleanup_child_page_id"], PAGE)
                self.assertEqual(report["upload_index_status"], "completed")
                self.assertEqual([n for n, _ in self.calls if n in ("knowledge_create_page", "knowledge_append", "knowledge_upload_file")],
                                 ["knowledge_create_page", "knowledge_append", "knowledge_upload_file"])
                # Fresh run owns a new report and idempotency keys; never resume old evidence.
                self.args.diagnostics += "-next"
                self.calls = []

    def test_lost_create_response_never_retries_or_guesses_child_identity(self):
        report = self.run_failure("lost-create")
        self.assertEqual(report["phase"], "create")
        self.assertEqual(report["effects"]["create"]["status"], "uncertain")
        self.assertNotIn("child_page_id", report)
        self.assertEqual(len(self.calls), 1)
        with self.assertRaises(smoke.SmokeFailure):
            smoke.execute(self.args)
        self.assertEqual(len(self.calls), 1)

    def test_upload_disconnect_preserves_known_child_and_does_not_repeat(self):
        report = self.run_failure("lost-upload")
        self.assertEqual(report["cleanup_child_page_id"], PAGE)
        self.assertEqual(report["effects"]["upload"]["status"], "uncertain")
        self.assertEqual(sum(name == "knowledge_upload_file" for name, _ in self.calls), 1)

    def test_partial_upload_retains_prior_receipt_and_known_uncertain_upload(self):
        report = self.run_failure("uncertain-second")
        files = report["effects"]["upload"]["files"]
        self.assertEqual(files[0]["block_id"], IDS[0])
        self.assertEqual(files[1]["upload_id"], IDS[1])
        self.assertEqual(files[1]["status"], "indeterminate")
        self.assertNotIn("block_id", files[1])
        self.assertIn("refresh", files[1])

    def test_failed_index_preserves_verified_source_effects(self):
        report = self.run_failure("index-failed")
        self.assertEqual(report["effects"]["upload"]["status"], "verified")
        self.assertEqual(report["upload_index_status"], "failed")
        self.assertEqual(sum(name == "knowledge_upload_file" for name, _ in self.calls), 1)

    def test_invalid_receipt_scope_ledger_audit_and_pending_upload_fail_closed(self):
        for mode in ("wrong-child", "wrong-scope", "missing-ledger", "audit-failed", "denied-append",
                     "pending-upload", "wrong-target", "missing-durable-audit"):
            with self.subTest(mode=mode):
                self.args.diagnostics = str(self.base / ("evidence-" + mode))
                report_path = self.base / ("evidence-" + mode) / "result.json"
                self.mode = mode
                for name in ("audit", "file_audit"):
                    with closing(sqlite3.connect(self.settings[name])) as db, db:
                        db.execute("DELETE FROM agent_audit_events")
                with self.assertRaises(smoke.SmokeFailure):
                    smoke.execute(self.args)
                self.assertEqual(json.loads(report_path.read_text())["outcome"], "failed")

    def test_source_preparation_is_private_valid_tiny_png_and_has_no_network_effects(self):
        destination = self.base / "synthetic"
        process = subprocess.run([sys.executable, smoke.__file__, "--prepare-sources", str(destination)],
                                 capture_output=True, timeout=5)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(destination.stat().st_mode & 0o777, 0o700)
        for name, data in (("smoke.png", smoke.IMAGE), ("smoke.txt", smoke.DOCUMENT)):
            self.assertEqual((destination / name).read_bytes(), data)
            self.assertEqual((destination / name).stat().st_mode & 0o777, 0o600)
        image, position = smoke.IMAGE, 8
        self.assertEqual(image[:8], b"\x89PNG\r\n\x1a\n")
        while position < len(image):
            size = int.from_bytes(image[position:position + 4], "big")
            data = image[position + 4:position + 8 + size]
            expected = int.from_bytes(image[position + 8 + size:position + 12 + size], "big")
            self.assertEqual(zlib.crc32(data), expected)
            position += size + 12
        self.assertEqual(self.calls, [])

    def test_optin_root_and_remote_transport_reject_before_network_or_report(self):
        self.args.allow_mutations = False
        with self.assertRaises(smoke.SmokeFailure):
            smoke.execute(self.args)
        self.args.allow_mutations = True
        self.args.confirm_disposable_root = PAGE
        with self.assertRaises(smoke.SmokeFailure):
            smoke.execute(self.args)
        self.args.confirm_disposable_root = ROOT
        self.settings["endpoint"] = "http://attacker.invalid/mcp"
        self.path.write_text(json.dumps(self.settings))
        with self.assertRaises(smoke.SmokeFailure):
            smoke.execute(self.args)
        self.assertFalse((self.base / "evidence").exists())
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
