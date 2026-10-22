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



class DependencyBuildLayers(unittest.TestCase):
    """Static cache/invalidation contract; hosted Docker smoke verifies the real solve."""

    def setUp(self):
        self.dockerfile = (Path(__file__).resolve().parent.parent / 'Dockerfile').read_text()

    def check_contract(self, content):
        self.assertIn('FROM rust:1.98.1-bookworm@sha256:', content)
        self.assertIn('FROM toolchain AS chef', content)
        self.assertIn('cargo install cargo-chef --version 0.1.78 --locked', content)
        chef = content.split('FROM toolchain AS chef\n', 1)[1].split('FROM chef AS planner\n', 1)[0]
        self.assertIn('protobuf-compiler=3.21.12-3+deb12u1', chef)
        self.assertIn('libprotobuf-dev=3.21.12-3+deb12u1', chef)
        planner = content.split('FROM chef AS planner\n', 1)[1].split('FROM chef AS builder\n', 1)[0]
        for item in ('COPY Cargo.toml Cargo.lock ./', 'COPY crates ./crates',
                     'COPY vendor ./vendor', 'cargo chef prepare --recipe-path recipe.json'):
            self.assertIn(item, planner)

        builder = content.split('FROM chef AS builder\n', 1)[1].split('FROM toolchain AS probe-builder\n', 1)[0]
        recipe = builder.index('COPY --from=planner /build/recipe.json recipe.json')
        vendor_cook = builder.index('COPY vendor ./vendor', recipe)
        cook = builder.index('RUN cargo chef cook --release --locked --package notion-knowledge-server')
        source = builder.index('COPY Cargo.toml Cargo.lock ./', cook)
        actual = builder.index('cargo build --release --locked -p notion-knowledge-server --features webhook-worker', source)
        self.assertLess(recipe, vendor_cook)
        self.assertLess(vendor_cook, cook)
        self.assertLess(cook, source)
        self.assertNotIn('COPY crates ./crates', builder[:cook])
        self.assertNotIn('COPY Cargo.toml Cargo.lock', builder[:cook])
        self.assertLess(source, actual)
        self.assertIn('COPY crates ./crates', builder[source:actual])
        self.assertIn('COPY vendor ./vendor', builder[source:actual])
        self.assertIn('--features webhook-worker --recipe-path recipe.json', builder[vendor_cook:source])
        self.assertEqual(builder.count('cargo chef cook '), 1)
        self.assertEqual(builder.count('cargo build --release --locked '), 1)
        self.assertIn('test "$VERSION" =', builder)
        self.assertIn('ldd target/release/notion-knowledge-server', builder)
        self.assertIn('FROM toolchain AS probe-builder', content)
        runtime = content.split('FROM gcr.io/distroless/cc-debian12:nonroot@sha256:', 1)[1]
        self.assertIn('COPY --from=builder /build/target/release/notion-knowledge-server', runtime)
        self.assertNotIn('cargo-chef', runtime)
        self.assertNotIn('continue-on-error', content)

    def test_cook_and_real_shipping_build_are_separate(self):
        self.check_contract(self.dockerfile)

    def test_no_accidental_feature_drift_or_early_source_copy(self):
        mutations = (
            self.dockerfile.replace('cargo chef cook --release --locked --package', 'cargo chef cook --release --package'),
            self.dockerfile.replace('--features webhook-worker --recipe-path recipe.json',
                                    '--recipe-path recipe.json', 1),
            self.dockerfile.replace('COPY --from=planner /build/recipe.json recipe.json',
                                    'COPY crates ./crates\nCOPY --from=planner /build/recipe.json recipe.json', 1),
            self.dockerfile.replace('RUN cargo chef cook --release --locked', 'RUN cargo chef cook --release', 1),
        )
        for mutated in mutations:
            with self.subTest(change=mutated != self.dockerfile):
                with self.assertRaises((AssertionError, ValueError)):
                    self.check_contract(mutated)



if __name__ == '__main__':
    unittest.main()
