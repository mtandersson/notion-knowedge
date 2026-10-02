#!/usr/bin/env python3
"""Specify compatible fallback, source refresh and safe timestamp preparation."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

spec = importlib.util.spec_from_file_location('cache', Path(__file__).with_name('cargo-cache.py'))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)


class CargoSnapshots(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for name, contents in {'Cargo.toml': '[workspace]\n', 'Cargo.lock': '# locked\n',
                               'flake.nix': '{ pinned = true; }\n', 'flake.lock': '{}\n',
                               'crates/a/Cargo.toml': '[package]\n',
                               'crates/a/src/lib.rs': 'pub fn value() -> u8 { 1 }\n',
                               'eval/corpus.json': '{"fixture":1}\n'}.items():
            file = self.root / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(contents)
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True)
        subprocess.run(['git', 'add', '.'], cwd=self.root, check=True)

    def key(self, **kwargs):
        values = dict(job='check', profile='debug', platform='Linux-X64',
                      toolchain=['rustc pinned', 'cargo pinned'], environment={})
        values.update(kwargs)
        return cache.keys(self.root, **values)

    def test_source_refresh_keeps_compatible_fallback_and_creates_new_snapshot_key(self):
        before = self.key()
        file = self.root / 'crates/a/src/lib.rs'
        file.write_text('pub fn value() -> u8 { 2 }\n')
        after = self.key()
        self.assertEqual(before['prefix'], after['prefix'])
        self.assertNotEqual(before['key'], after['key'])
        self.assertEqual(after, self.key())

    def test_each_dependency_and_flake_input_is_a_fallback_boundary(self):
        for name in ['Cargo.toml', 'Cargo.lock', 'crates/a/Cargo.toml', 'flake.nix', 'flake.lock']:
            with self.subTest(name=name):
                before = self.key()
                file = self.root / name
                original = file.read_text()
                file.write_text(original + '\n')
                self.assertNotEqual(before['prefix'], self.key()['prefix'])
                file.write_text(original)

    def test_platform_profile_toolchain_and_flags_cannot_restore_each_other(self):
        before = self.key()['prefix']
        for variation in [dict(platform='Linux-ARM64'), dict(platform='macOS-X64'),
                          dict(job='build', profile='release'), dict(job='test'),
                          dict(toolchain=['rustc other', 'cargo pinned']),
                          dict(environment={'RUSTFLAGS': '-C opt-level=2'}),
                          dict(environment={'CARGO_BUILD_RUSTC': '/new/rustc'}),
                          dict(environment={'CARGO_BUILD_RUSTDOC': '/new/rustdoc'}),
                          dict(environment={'CARGO_BUILD_RUSTC_WRAPPER': '/new/wrapper'}),
                          dict(environment={'CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER': '/new/wrapper'}),
                          dict(environment={'CARGO_BUILD_INCREMENTAL': '0'}),
                          dict(environment={'CARGO_PROFILE_DEV_DEBUG': '0'}),
                          dict(environment={'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER': '/new/linker'})]:
            with self.subTest(variation=variation):
                self.assertNotEqual(before, self.key(**variation)['prefix'])

    def test_credentials_do_not_enter_keys_or_output(self):
        sentinel = 'never-print-this-value'
        result = self.key(environment={'NOTION_TOKEN': sentinel, 'CARGO_REGISTRIES_OTHER_TOKEN': sentinel})
        self.assertEqual(result, self.key())
        self.assertNotIn(sentinel, json.dumps(result))

    def test_verified_unchanged_mtime_survives_checkout_but_changed_content_is_fresh(self):
        file = self.root / 'crates/a/src/lib.rs'
        old = time.time_ns() - 10_000_000_000
        os.utime(file, ns=(old, old))
        cache.snapshot(self.root)
        os.utime(file, None)
        cache.prepare(self.root)
        self.assertEqual(file.stat().st_mtime_ns, old)
        file.write_text('pub fn value() -> u8 { 2 }\n')
        os.utime(file, ns=(old, old))
        start = time.time_ns()
        cache.prepare(self.root)
        self.assertGreaterEqual(file.stat().st_mtime_ns, start)
        cache.snapshot(self.root)
        updated = file.stat().st_mtime_ns
        os.utime(file, None)
        cache.prepare(self.root)
        self.assertEqual(file.stat().st_mtime_ns, updated)

    def test_corrupt_or_external_metadata_never_controls_external_paths(self):
        outside = self.root / 'not-a-build-input.txt'
        outside.write_text('private')
        before = outside.stat().st_mtime_ns
        state = self.root / cache.STATE
        state.parent.mkdir()
        for metadata in ['not json', '[]', json.dumps({'version': 1, 'files': {
                '../not-a-build-input.txt': {'sha256': 'irrelevant', 'mtime_ns': 1}}})]:
            state.write_text(metadata)
            cache.prepare(self.root)
            self.assertEqual(outside.stat().st_mtime_ns, before)

    def test_cli_writes_the_same_key_to_actions_output(self):
        tools = self.root / 'bin'
        tools.mkdir()
        for name in ['rustc', 'cargo']:
            tool = tools / name
            tool.write_text(f'#!/bin/sh\nprintf "{name} pinned\\n"\n')
            tool.chmod(0o755)
        output = self.root / 'actions-output'
        env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'], GITHUB_OUTPUT=str(output))
        env.pop('RUSTC', None)
        result = subprocess.run(['python3', str(Path(cache.__file__).resolve()), 'key',
                                 '--job', 'check', '--profile', 'debug', '--os', 'Linux', '--arch', 'X64'],
                                cwd=self.root, env=env, check=True, capture_output=True, text=True)
        emitted = dict(line.split('=', 1) for line in output.read_text().splitlines())
        self.assertEqual(emitted, json.loads(result.stdout))
        self.assertTrue(emitted['key'].startswith(emitted['prefix']))

    def test_snapshot_excludes_incremental_scratch_but_keeps_built_outputs(self):
        scratch = self.root / 'target/debug/incremental/crate/temporary'
        scratch.parent.mkdir(parents=True)
        scratch.write_text('scratch')
        binary = self.root / 'target/debug/server'
        binary.write_text('built output')
        cache.snapshot(self.root)
        self.assertFalse(scratch.exists())
        self.assertEqual(binary.read_text(), 'built output')


if __name__ == '__main__':
    unittest.main()
