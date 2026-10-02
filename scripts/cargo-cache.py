#!/usr/bin/env python3
"""Versioned Cargo snapshots: compatible restores, source refresh, safe mtimes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

VERSION = 1
STATE = Path('target/.nk-cache-source-times.json')
PROFILES = {'clippy': 'debug', 'check': 'debug', 'test': 'debug', 'build': 'release'}
FLAG_NAMES = {'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTDOCFLAGS',
              'CARGO_ENCODED_RUSTDOCFLAGS', 'CARGO_BUILD_TARGET', 'CARGO_BUILD_RUSTFLAGS',
              'CARGO_INCREMENTAL', 'CARGO_TARGET_DIR', 'RUSTC_BOOTSTRAP',
              'CARGO_BUILD_RUSTC', 'CARGO_BUILD_RUSTDOC', 'CARGO_BUILD_RUSTC_WRAPPER',
              'CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER', 'CARGO_BUILD_INCREMENTAL',
              'RUSTC', 'RUSTDOC', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CC', 'CXX', 'AR',
              'CFLAGS', 'CXXFLAGS', 'LDFLAGS'}


def inputs(root):
    tracked = subprocess.check_output(['git', 'ls-files', '-z'], cwd=root)
    paths = [Path(os.fsdecode(p)) for p in tracked.split(b'\0') if p]
    compatibility = [p for p in paths if p.name == 'Cargo.toml' or
                     str(p) in {'Cargo.lock', 'flake.nix', 'flake.lock'} or
                     str(p).startswith(('.cargo/', '.github/actions/cargo-cache-'))]
    sources = [p for p in paths if str(p).startswith(('crates/', 'eval/', 'src/')) or
               str(p) == 'build.rs' or (len(p.parts) == 1 and p.suffix == '.rs')]
    selected = sorted(set(compatibility + sources))
    for path in selected:
        if (root / path).is_symlink() or not (root / path).resolve().is_relative_to(root.resolve()):
            raise ValueError(f'Cache input must be a repository file: {path}')
    return sorted(compatibility), sorted(set(sources) - set(compatibility)), selected


def digest_files(root, paths):
    digest = hashlib.sha256()
    for path in paths:
        digest.update(os.fsencode(path))
        digest.update(b'\0')
        digest.update(hashlib.sha256((root / path).read_bytes()).digest())
    return digest.hexdigest()


def keys(root, job, profile, platform, toolchain, environment):
    if PROFILES.get(job) != profile or not re.fullmatch(r'[A-Za-z0-9_-]+', platform):
        raise ValueError('Unsupported job/profile/platform')
    compatibility, sources, _ = inputs(root)
    flags = {name: value for name, value in environment.items() if name in FLAG_NAMES or
             name.startswith('CARGO_PROFILE_') or
             (name.startswith('CARGO_TARGET_') and name.endswith(('_RUSTFLAGS', '_LINKER', '_AR')))}
    payload = {'files': digest_files(root, compatibility), 'toolchain': toolchain,
               'flags': flags, 'policy': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    compatible = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    prefix = f'nk-cargo-v{VERSION}-{platform}-{job}-{profile}-{compatible}-'
    downloads_prefix = f'nk-cargo-downloads-v{VERSION}-{platform}-'
    return {'prefix': prefix, 'key': prefix + digest_files(root, sources),
            'downloads-prefix': downloads_prefix,
            'downloads-key': downloads_prefix + hashlib.sha256((root / 'Cargo.lock').read_bytes()).hexdigest()}


def prepare(root):
    # Only hashes/timestamps are restored, never source contents. Iterate current
    # tracked inputs, not paths supplied by the cached JSON.
    try:
        previous = json.loads((root / STATE).read_text())
        records = previous.get('files', {}) if previous.get('version') == VERSION else {}
        if not isinstance(records, dict):
            records = {}
    except (OSError, ValueError, AttributeError):
        records = {}
    now = time.time_ns()
    reused = changed = 0
    for path in inputs(root)[2]:
        file = root / path
        record = records.get(str(path), {})
        fingerprint = hashlib.sha256(file.read_bytes()).hexdigest()
        stamp = record.get('mtime_ns') if isinstance(record, dict) else None
        if isinstance(record, dict) and record.get('sha256') == fingerprint and \
                type(stamp) is int and 0 < stamp <= now:
            os.utime(file, ns=(file.stat().st_atime_ns, stamp))
            reused += 1
        else:
            # A changed file must be newer than prior build outputs even when
            # commits/checkouts have backdated mtimes.
            os.utime(file, ns=(file.stat().st_atime_ns, now))
            changed += 1
    print(f'Cargo cache inputs: {reused} unchanged timestamps restored, {changed} fresh inputs')


def snapshot(root):
    # Incremental compiler scratch state is large and less portable than the
    # dependency artifacts/fingerprints retained in the snapshot.
    for path in (root / 'target').glob('**/incremental'):
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
    records = {}
    for path in inputs(root)[2]:
        file = root / path
        records[str(path)] = {'sha256': hashlib.sha256(file.read_bytes()).hexdigest(),
                              'mtime_ns': file.stat().st_mtime_ns}
    (root / STATE).parent.mkdir(parents=True, exist_ok=True)
    (root / STATE).write_text(json.dumps({'version': VERSION, 'files': records}, sort_keys=True) + '\n')
    print(f'Cargo cache snapshot: {len(records)} input hashes/timestamps, no source contents')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    key = sub.add_parser('key')
    key.add_argument('--job', required=True, choices=PROFILES)
    key.add_argument('--profile', required=True, choices={'debug', 'release'})
    key.add_argument('--os', required=True)
    key.add_argument('--arch', required=True)
    sub.add_parser('prepare')
    sub.add_parser('snapshot')
    args = parser.parse_args()
    root = Path.cwd()
    if args.command == 'key':
        toolchain = [subprocess.check_output(command, text=True) for command in
                     ([os.environ.get('RUSTC') or os.environ.get('CARGO_BUILD_RUSTC') or 'rustc', '-vV'], ['cargo', '-V'])]
        result = keys(root, args.job, args.profile, f'{args.os}-{args.arch}', toolchain, os.environ)
        print(json.dumps(result, sort_keys=True))
        if os.environ.get('GITHUB_OUTPUT'):
            with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
                for name, value in result.items():
                    print(f'{name}={value}', file=output)
    elif args.command == 'prepare':
        prepare(root)
    else:
        snapshot(root)


if __name__ == '__main__':
    main()
