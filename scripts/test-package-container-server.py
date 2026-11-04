#!/usr/bin/env python3
"""The deployment archive must come only from the selected tested image."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('package_server', Path(__file__).with_name('package-container-server.py'))
package_server = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package_server)
REVISION = 'a' * 40
IMAGE_ID = 'sha256:' + 'b' * 64
ELF = b'\x7fELF\x02\x01' + b'\0' * 12 + b'\x3e\x00' + b'fixture-runtime'
VERSION = tomllib.loads((package_server.ROOT / 'Cargo.toml').read_text())['workspace']['package']['version']


class LocalImage:
    def __init__(self, failure=None):
        self.calls = []
        self.failure = failure
        self.name = None
        self.metadata = {'Id': IMAGE_ID, 'Os': 'linux', 'Architecture': 'amd64', 'Config': {
            'User': '65532:65532', 'Entrypoint': ['/usr/local/bin/notion-knowledge-server'],
            'Labels': {'org.opencontainers.image.' + key: value for key, value in
                       {'source': package_server.SOURCE, 'revision': REVISION, 'version': VERSION}.items()}}}

    def __call__(self, *args, **kwargs):
        self.calls.append(args)
        if args[:2] == ('image', 'inspect'):
            return subprocess.CompletedProcess(args, 0, json.dumps([copy.deepcopy(self.metadata)]))
        if args[0] == 'create':
            if self.failure == 'create':
                raise subprocess.CalledProcessError(125, args)
            self.name = args[2]
            assert args[-1] == IMAGE_ID
            assert '--pull=never' in args and '--network=none' in args and '--read-only' in args
        elif args[0] == 'cp':
            assert args[1] == self.name + ':/usr/local/bin/notion-knowledge-server'
            if self.failure == 'copy':
                raise subprocess.CalledProcessError(1, args)
            destination = Path(args[2])
            if self.failure == 'symlink':
                destination.symlink_to(Path(__file__).resolve())
            else:
                destination.write_bytes(b'wrong executable' if self.failure == 'not_elf' else ELF)
                destination.chmod(0o755)
        elif args[0] == 'rm':
            assert args == ('rm', '-f', '--volumes', self.name)
        else:
            raise AssertionError('Unexpected image build, pull or application start: ' + repr(args))
        return subprocess.CompletedProcess(args, 0, '')


class RuntimeArtifact(unittest.TestCase):
    def test_archive_contains_only_runtime_and_verifiable_identity(self):
        image = LocalImage()
        with tempfile.TemporaryDirectory() as temporary:
            artifact = package_server.package('tested-image', REVISION, Path(temporary), image)
            with tarfile.open(artifact) as archive:
                self.assertEqual(archive.getnames(), ['manifest.json', 'notion-knowledge-server'])
                manifest = json.load(archive.extractfile('manifest.json'))
                binary = archive.extractfile('notion-knowledge-server').read()
                self.assertEqual(binary, ELF)
                self.assertEqual(manifest['binary'], {'file': 'notion-knowledge-server', 'bytes': len(ELF),
                                                     'sha256': hashlib.sha256(ELF).hexdigest()})
                self.assertEqual(manifest['revision'], REVISION)
                self.assertEqual(manifest['version'], VERSION)
                self.assertEqual(manifest['image_id'], IMAGE_ID)
                self.assertEqual(manifest['platform'], 'linux/amd64')
                self.assertEqual(manifest['cargo_features'], ['webhook-worker'])
                self.assertEqual(archive.getmember('notion-knowledge-server').mode, 0o755)
            self.assertEqual(image.calls[-1], ('rm', '-f', '--volumes', image.name))

    def test_wrong_image_identity_is_rejected_before_export(self):
        mutations = [('revision', 'previous'), ('source', 'https://wrong.example'), ('version', '0.0.0')]
        for key, value in mutations:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as temporary:
                image = LocalImage()
                image.metadata['Config']['Labels']['org.opencontainers.image.' + key] = value
                with self.assertRaisesRegex(ValueError, 'Wrong ' + key):
                    package_server.package('tested-image', REVISION, Path(temporary), image)
                self.assertEqual([call[0] for call in image.calls], ['image'])
        image = LocalImage()
        image.metadata['Architecture'] = 'arm64'
        with tempfile.TemporaryDirectory() as temporary, self.assertRaisesRegex(ValueError, 'amd64'):
            package_server.package('tested-image', REVISION, Path(temporary), image)
        self.assertEqual([call[0] for call in image.calls], ['image'])

    def test_failed_or_invalid_copy_removes_only_its_helper(self):
        for failure in ['copy', 'symlink', 'not_elf']:
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                image = LocalImage(failure)
                with self.assertRaises((subprocess.CalledProcessError, ValueError)):
                    package_server.package('tested-image', REVISION, Path(temporary), image)
                self.assertEqual(image.calls[-1], ('rm', '-f', '--volumes', image.name))
                self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_failed_creation_never_removes_an_unowned_container(self):
        image = LocalImage('create')
        with tempfile.TemporaryDirectory() as temporary, self.assertRaises(subprocess.CalledProcessError):
            package_server.package('tested-image', REVISION, Path(temporary), image)
        self.assertNotIn('rm', [call[0] for call in image.calls])


if __name__ == '__main__':
    unittest.main()
