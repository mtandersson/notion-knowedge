#!/usr/bin/env python3
"""Cached final images must match the current checkout before runtime checks."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('smoke', Path(__file__).with_name('smoke-container.py'))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class CachedImageIdentity(unittest.TestCase):
    def invoke(self, revision='current', binary_version=None):
        version = smoke.workspace_version()
        metadata = {'User': '65532:65532',
                    'Entrypoint': ['/usr/local/bin/notion-knowledge-server'],
                    'Healthcheck': {'Test': ['CMD', '/usr/local/bin/notion-knowledge-server',
                                             '--healthcheck']},
                    'Labels': {f'org.opencontainers.image.{k}': v for k, v in
                               {'version': version, 'revision': revision, 'source': smoke.SOURCE}.items()}}

        def docker(*args, **kwargs):
            if args[:2] == ('image', 'inspect'):
                return subprocess.CompletedProcess(args, 0, json.dumps([{'Config': metadata}]))
            raise AssertionError(f'Unexpected rebuild: {args}')

        with patch.object(sys, 'argv', ['smoke', '--prebuilt', '--builder-image', 'pinned-builder']), \
                patch.object(smoke.subprocess, 'check_output', return_value='current\n'), \
                patch.object(smoke.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', 'test "$VERSION"')), \
                patch.object(smoke, 'docker', side_effect=docker), \
                patch.object(smoke, 'run_once', return_value=subprocess.CompletedProcess([], 0, f'notion-knowledge {binary_version or version}\n', '')):
            smoke.main()

    def test_stale_cached_revision_is_rejected_before_runtime(self):
        with self.assertRaisesRegex(AssertionError, 'Wrong revision label'):
            self.invoke(revision='previous')

    def test_binary_must_match_cargo_even_with_correct_labels(self):
        with self.assertRaisesRegex(AssertionError, 'Container identity must match'):
            self.invoke(binary_version='0.0.0-stale')


if __name__ == '__main__':
    unittest.main()
