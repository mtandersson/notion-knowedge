#!/usr/bin/env python3
"""Executable artifact delivery, source identity and failure propagation contracts."""
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('native', Path(__file__).with_name('native-test-artifacts.py'))
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)
REVISION = 'a' * 40


class Bundles(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve() / 'checkout'
        self.root.mkdir()
        self.metadata = {'workspace_root': str(self.root), 'packages': [
            {'id': name, 'name': name, 'manifest_path': str(self.root / 'crates' / name.removeprefix('notion-knowledge-') / 'Cargo.toml')}
            for name in native.PACKAGES]}
        for package in self.metadata['packages']:
            Path(package['manifest_path']).parent.mkdir(parents=True)
        self.records = []
        self.binary('notion-knowledge-server', 'notion-knowledge-server', False,
                    '#!/usr/bin/env python3\n')
        self.binary('notion-knowledge-server', 'deps/server-tests', True,
                    '#!/usr/bin/env python3\nfrom pathlib import Path\n'
                    f"assert Path({str(self.root / 'target/debug/notion-knowledge-server')!r}).is_file()\n"
                    f"assert Path.cwd() == Path({str(self.root / 'crates/server')!r})\n"
                    f"Path({str(self.root / 'server-ran')!r}).touch()\n")
        self.binary('notion-knowledge-retrieval', 'deps/retrieval-tests', True,
                    '#!/usr/bin/env python3\n')

    def binary(self, package, name, test, source):
        path = self.root / 'target/debug' / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
        path.chmod(0o755)
        self.records.append({'reason': 'compiler-artifact', 'package_id': package,
                             'executable': str(path), 'profile': {'test': test},
                             'target': {'kind': ['test'] if test else ['bin']}})

    def bundle(self):
        messages = [json.dumps(r) for r in self.records]
        native.pack(self.root, messages, self.metadata, REVISION, self.root / 'bundles')
        shutil.rmtree(self.root / 'target')
        return self.root / 'bundles/webhook-worker.tar.gz'

    def test_real_execution_restores_embedded_binary_paths_without_build_cache(self):
        scratch = self.root / 'target/debug/incremental/scratch'
        scratch.parent.mkdir(parents=True)
        scratch.write_text('not a runtime input')
        # Cargo may hardlink its ordinary binary into deps as well. Bundles
        # must preserve both paths as executable files, not tar hardlinks.
        sibling = self.root / 'target/debug/deps/server-ordinary'
        os.link(self.root / 'target/debug/notion-knowledge-server', sibling)
        record = dict(self.records[0], executable=str(sibling))
        self.records.append(record)
        archive = self.bundle()
        report = self.root / 'results.json'
        native.run(self.root, archive, REVISION, 'webhook-worker', report)
        self.assertTrue((self.root / 'server-ran').is_file())
        self.assertFalse(scratch.exists())
        results = json.loads(report.read_text())
        self.assertEqual([r['exit_code'] for r in results['tests']], [0])
        self.assertGreater(results['seconds'], 0)
        with tarfile.open(self.root / 'bundles/local-lancedb.tar.gz') as packed:
            manifest = json.load(packed.extractfile('manifest.json'))
        self.assertEqual(manifest['tests'], {'target/debug/deps/retrieval-tests': 'crates/retrieval'})
        self.assertNotIn('target/debug/notion-knowledge-server', manifest['files'])

    def test_wrong_revision_suite_or_workspace_is_rejected_before_extraction(self):
        archive = self.bundle()
        for root, revision, suite in [(self.root, 'b' * 40, 'webhook-worker'),
                                      (self.root, REVISION, 'local-lancedb'),
                                      (self.root / 'other', REVISION, 'webhook-worker')]:
            with self.subTest(root=root, revision=revision, suite=suite), self.assertRaises(ValueError):
                native.unpack(root, archive, revision, suite)
        self.assertFalse((self.root / 'target').exists())

    def altered(self, archive, name, payload):
        records = []
        with tarfile.open(archive) as old:
            for member in old:
                records.append((member.name, old.extractfile(member).read()))
        records = [(path, payload if path == name else data) for path, data in records]
        if name not in {path for path, _ in records}:
            records.append((name, payload))
        with tarfile.open(archive, 'w:gz') as target:
            for path, data in records:
                member = tarfile.TarInfo(path)
                member.size = len(data)
                target.addfile(member, io.BytesIO(data))

    def test_corrupted_executable_is_rejected(self):
        archive = self.bundle()
        self.altered(archive, 'target/debug/deps/server-tests', b'changed')
        with self.assertRaisesRegex(ValueError, 'digest'):
            native.unpack(self.root, archive, REVISION, 'webhook-worker')

    def test_traversal_and_unlisted_files_are_rejected(self):
        archive = self.bundle()
        original = archive.read_bytes()
        self.altered(archive, 'target/debug/unlisted', b'not listed')
        with self.assertRaisesRegex(ValueError, 'Unexpected archive'):
            native.unpack(self.root, archive, REVISION, 'webhook-worker')
        archive.write_bytes(original)
        with tarfile.open(archive, 'r:gz') as packed:
            manifest = json.load(packed.extractfile('manifest.json'))
        escaped = 'target/debug/../../../outside'
        manifest['files'][escaped] = '0' * 64
        self.altered(archive, 'manifest.json', json.dumps(manifest).encode())
        self.altered(archive, escaped, b'outside the checkout')
        with self.assertRaisesRegex(ValueError, 'Unsafe executable path'):
            native.unpack(self.root, archive, REVISION, 'webhook-worker')
        self.assertFalse((self.root.parent / 'outside').exists())

    def test_failing_binary_fails_execution_and_retains_timing_report(self):
        (self.root / 'target/debug/deps/server-tests').write_text('#!/usr/bin/env python3\nraise SystemExit(17)\n')
        archive = self.bundle()
        report = self.root / 'results.json'
        with self.assertRaisesRegex(ValueError, 'failed'):
            native.run(self.root, archive, REVISION, 'webhook-worker', report)
        self.assertEqual(json.loads(report.read_text())['tests'][0]['exit_code'], 17)

    def test_missing_normal_binary_cannot_produce_a_partial_bundle(self):
        self.records = self.records[1:]
        with self.assertRaisesRegex(ValueError, 'Ordinary server'):
            native.pack(self.root, map(json.dumps, self.records), self.metadata, REVISION,
                        self.root / 'bundles')


if __name__ == '__main__':
    unittest.main()
