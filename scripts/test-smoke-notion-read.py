#!/usr/bin/env python3
"""Read-smoke process/protocol contracts; no Notion credentials or model needed."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("smoke", Path(__file__).with_name("smoke-notion-read.py"))
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)
ROOT = "11111111-1111-4111-8111-111111111111"
WORKSPACE = "22222222-2222-4222-8222-222222222222"
MARKER = "NK555 synthetic saffron ravioli"

# A process-level fixture for the shipping CLI + newline JSON-RPC contract.
# Production adapters remain covered by reconciliation_dispatch / container smoke.
FIXTURE = '''#!/usr/bin/env python3
import json, os, sys
assert os.environ['NK_NOTION_SCOPE_ROOTS'] == '{root}'
assert os.environ['NK_READ_ONLY'] == 'true'
assert os.environ['NK_GRAPH'] == 'false'
assert 'NK_OAUTH_WRITES' not in os.environ
assert 'NK_WEBHOOK_WORKER' not in os.environ
root = os.environ['NK_NOTION_SCOPE_ROOTS']
mode = os.environ.get('SMOKE_FIXTURE_MODE', '')
if sys.argv[1:] == ['--reconcile', 'apply', sys.argv[-1]]:
    if mode == 'command-failure':
        print('secret-do-not-disclose upstream raw text', file=sys.stderr)
        sys.exit(3)
    print(json.dumps(dict(operation='reconciliation', phase='Applying' if mode == 'incomplete' else 'Completed', errors=0, pending=0)))
    sys.exit(0)
if sys.argv[1:] == ['--index', 'ensure']:
    print(json.dumps(dict(operation='index', status='complete')))
    sys.exit(0)
for line in sys.stdin:
    request = json.loads(line)
    if 'id' not in request: continue
    if request['method'] == 'initialize':
        result = dict(protocolVersion='2025-03-26')
    else:
        args = request['params']['arguments']
        source = dict(page_id=root, url='https://www.notion.so/' + root.replace('-', ''))
        text = '{marker}'
        if mode == 'wrong-page': source['page_id'] = '{workspace}'
        if mode == 'wrong-url': source['url'] = 'https://attacker.invalid/' + root.replace('-', '')
        if mode.startswith('link='): source['url'] = mode.split('=', 1)[1]
        if mode == 'missing-marker': text = 'unrelated source'
        if request['params']['name'] == 'knowledge_search':
            assert args == dict(query='{marker}', mode='semantic', limit=1, filters=dict(page_ids=[root]))
            source['chunk_id'] = 'synthetic-stable-chunk'
            result = dict(structuredContent=dict(results=[dict(source=source, text=text, matched_paths=['semantic'], score=float('nan') if mode == 'nonfinite' else 0.75)]))
        else:
            assert request['params']['name'] == 'knowledge_get'
            assert args['refs'] == [dict(kind='page', id=root)]
            assert args['freshness'] in ('indexed', 'fresh')
            source['root_page_id'] = root
            if mode == 'wrong-root': source['root_page_id'] = '{workspace}'
            result = dict(structuredContent=dict(sources=[dict(provenance=source, text=text, content_scope='page' if args['freshness'] == 'fresh' else 'indexed')]))
    if mode == 'tool-error' and request['method'] == 'tools/call': result = dict(isError=True)
    print(json.dumps(dict(jsonrpc='2.0', id=request['id'], result=result)), flush=True)
'''.format(root=ROOT, marker=MARKER, workspace=WORKSPACE)


class SmokeContracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.data = self.base / "data"
        for path in (self.data, self.data / "index", self.data / "state", self.base / "assets"):
            path.mkdir(mode=0o700)
        self.state = self.data / "state/state.sqlite"
        self.state.touch(mode=0o600)
        for name in ("config.json", "tokenizer.json", "model.safetensors"):
            (self.base / "assets" / name).write_bytes(b"fixture-model")
        self.binary = self.base / "server"
        self.binary.write_text(FIXTURE)
        self.binary.chmod(0o700)
        self.args = [sys.executable, str(Path(smoke.__file__)), "--binary", str(self.binary),
                     "--assets", str(self.base / "assets"), "--data", str(self.data),
                     "--diagnostics", str(self.base / "diagnostics"), "--root", ROOT,
                     "--workspace", WORKSPACE, "--marker", MARKER,
                     "--confirm-disposable-root", ROOT, "--timeout", "5"]

    def run_smoke(self, mode="", extra=()):
        env = dict(os.environ, NOTION_TOKEN="secret-do-not-disclose", NK_OAUTH_WRITES="true",
                   NK_WEBHOOK_WORKER="true", NK_NOTION_SCOPE_ROOTS=WORKSPACE, SMOKE_FIXTURE_MODE=mode)
        return subprocess.run(self.args + list(extra), env=env, capture_output=True, text=True, timeout=20)

    def manifest(self):
        path = self.base / "diagnostics/result.json"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        content = path.read_text()
        self.assertNotIn("secret-do-not-disclose", content)
        self.assertNotIn(MARKER, content)
        self.assertNotIn("unrelated source", content)
        return json.loads(content)

    def test_shipping_commands_and_mcp_verify_exact_selected_source(self):
        result = self.run_smoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = self.manifest()
        self.assertEqual((report["outcome"], report["phase"]), ("verified", "complete"))
        self.assertEqual(report["root_page_id"], ROOT)
        self.assertEqual(len(report["assets_sha256"]), 3)
        self.assertEqual(sorted(p.name for p in self.data.iterdir()), ["index", "state"])

    def test_wrong_page_wrong_root_and_missing_marker_retain_failure_phase(self):
        for mode in ("wrong-page", "wrong-root", "missing-marker", "wrong-url", "nonfinite", "tool-error"):
            with self.subTest(mode=mode):
                result = self.run_smoke(mode)
                self.assertEqual(result.returncode, 1)
                self.assertNotIn("secret-do-not-disclose", result.stderr)
                report = self.manifest()
                self.assertEqual((report["outcome"], report["phase"]), ("failed", "retrieve"))
                (self.base / "diagnostics/result.json").unlink()
                (self.base / "diagnostics").rmdir()

    def test_failed_or_incomplete_reconciliation_stops_before_indexing(self):
        for mode in ("command-failure", "incomplete"):
            with self.subTest(mode=mode):
                result = self.run_smoke(mode)
                self.assertEqual(result.returncode, 1)
                self.assertNotIn("secret-do-not-disclose", result.stderr)
                report = self.manifest()
                self.assertEqual((report["outcome"], report["phase"]), ("failed", "reconcile"))
                self.assertIn("failure", report)
                (self.base / "diagnostics/result.json").unlink()
                (self.base / "diagnostics").rmdir()

    def test_production_notion_links_preserve_exact_page_identity_and_authority(self):
        raw = ROOT.replace("-", "")
        links = [
            ("https://app.notion.com/p/Selected-" + raw, True),
            ("https://app.notion.com/p/Foo-" + raw, True),
            ("https://team.notion.site/Selected-" + raw, True),
            ("https://team.notion.so/" + ROOT, True),
            ("https://app.notion.com.attacker.invalid/p/Selected-" + raw, False),
            ("https://notion.so.attacker.invalid/" + raw, False),
            ("https://user@app.notion.com/p/Selected-" + raw, False),
            ("https://app.notion.com:443/p/Selected-" + raw, False),
            ("https://app.notion.com/p/Selected-" + WORKSPACE.replace("-", ""), False),
        ]
        for link, accepted in links:
            with self.subTest(link=link):
                result = self.run_smoke("link=" + link)
                self.assertEqual(result.returncode, 0 if accepted else 1, result.stderr)
                self.assertEqual(self.manifest()["outcome"], "verified" if accepted else "failed")
                (self.base / "diagnostics/result.json").unlink()
                (self.base / "diagnostics").rmdir()

    def test_unconfirmed_root_and_shared_state_fail_before_process_execution(self):
        self.binary.write_text("#!/bin/sh\ntouch " + str(self.base / "effect") + "\n")
        result = self.run_smoke(extra=("--confirm-disposable-root", WORKSPACE))
        self.assertEqual(result.returncode, 1)
        self.state.chmod(0o644)
        self.assertEqual(self.run_smoke().returncode, 1)
        self.assertFalse((self.base / "effect").exists())
        self.assertFalse((self.base / "diagnostics").exists())

    def test_missing_assets_preserve_diagnostics_and_existing_diagnostics_are_not_reused(self):
        (self.base / "assets/config.json").unlink()
        self.assertEqual(self.run_smoke().returncode, 1)
        self.assertEqual(self.manifest()["phase"], "assets")
        original = (self.base / "diagnostics/result.json").read_bytes()
        self.assertEqual(self.run_smoke().returncode, 1)
        self.assertEqual((self.base / "diagnostics/result.json").read_bytes(), original)

    def test_symlinked_state_and_diagnostics_inside_derived_state_are_rejected(self):
        self.state.unlink()
        target = self.base / "other-state"
        target.touch(mode=0o600)
        self.state.symlink_to(target)
        self.assertEqual(self.run_smoke().returncode, 1)
        self.state.unlink()
        self.state.touch(mode=0o600)
        self.assertEqual(self.run_smoke(extra=("--diagnostics", str(self.data / "diagnostics"))).returncode, 1)


if __name__ == "__main__":
    unittest.main()
