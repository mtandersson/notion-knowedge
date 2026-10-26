#!/usr/bin/env python3
"""Exercise original/patched Arrow IPC and Lance compressors in both directions.

Run inside nix develop .#spike. Vulnerable original versions are used only in
separate disposable offline-fixture processes, never in the application graph.
Temporary manifests/locks stay outside the checkout and its security audit.
"""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def main():
    environment = dict(os.environ)
    environment.setdefault('CARGO_PROFILE_DEV_DEBUG', '0')
    environment.setdefault('CARGO_BUILD_JOBS', '4')
    with tempfile.TemporaryDirectory(prefix='nk-compression-compat-') as temporary:
        base = Path(temporary)
        for variant, zstd_version in [('original', '0.13.3'), ('patched', '0.14.3')]:
            project = base / variant
            project.mkdir()
            manifest = f'''[package]
name = "compression-compat-{variant}"
version = "0.1.0"
edition = "2024"
[workspace]
[dependencies]
arrow-array = "=58.4.0"
arrow-schema = "=58.4.0"
arrow-ipc = {{ version = "=58.4.0", features = ["zstd"] }}
lance-encoding = {{ version = "=12.0.0", features = ["zstd"] }}
zstd = "={zstd_version}"
'''
            if variant == 'patched':
                manifest += '\n[patch.crates-io]\n'
                for name in ['arrow-ipc', 'lance-encoding']:
                    manifest += f'{name} = {{ path = "{ROOT / "vendor" / name}" }}\n'
            (project / 'Cargo.toml').write_text(manifest)
            (project / 'src').mkdir()
            shutil.copyfile(ROOT / 'scripts/compression-compat.rs', project / 'src/main.rs')
            # Start from application pins so this probe tests the installed stack,
            # then let Cargo prune application-only packages in the disposable lock.
            shutil.copyfile(ROOT / 'Cargo.lock', project / 'Cargo.lock')
            for operation, fixture in [('write', variant), ('read', variant)]:
                subprocess.run(['cargo', 'run', '--manifest-path', str(project / 'Cargo.toml'),
                                '--', operation, str(base / 'fixtures' / fixture)],
                               check=True, env=environment)
        for variant, fixture in [('patched', 'original'), ('original', 'patched')]:
            subprocess.run(['cargo', 'run', '--locked', '--manifest-path',
                            str(base / variant / 'Cargo.toml'), '--', 'read',
                            str(base / 'fixtures' / fixture)], check=True, env=environment)
    print('Original ↔ patched Arrow IPC ZSTD, Lance length-prefixed blocks and legacy raw frames passed.')


if __name__ == '__main__':
    main()
