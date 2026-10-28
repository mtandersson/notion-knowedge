#!/usr/bin/env python3
"""Package Cargo test executables, then run them without Cargo or a compiler."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import subprocess
import tarfile
import time

PACKAGES = {'notion-knowledge-retrieval': 'local-lancedb',
            'notion-knowledge-server': 'webhook-worker'}


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def relative_file(root, path):
    path = Path(path).resolve()
    require(path.is_relative_to(root / 'target') and path.is_file(),
            'Executable must belong to this checkout target directory')
    return path.relative_to(root).as_posix()


def pack(root, messages, metadata, revision, output):
    root = root.resolve()
    require(metadata['workspace_root'] == str(root), 'Cargo workspace differs')
    require(re.fullmatch('[0-9a-f]{40}', revision), 'Exact source revision required')
    packages = {p['id']: (PACKAGES[p['name']],
                           Path(p['manifest_path']).parent.relative_to(root).as_posix())
                for p in metadata['packages'] if p['name'] in PACKAGES}
    suites = {suite: {} for suite in PACKAGES.values()}
    runtime = {suite: set() for suite in PACKAGES.values()}
    for line in messages:
        record = json.loads(line)
        if record.get('reason') != 'compiler-artifact' or not record.get('executable'):
            continue
        package = packages.get(record['package_id'])
        if package is None:
            continue
        suite, directory = package
        path = relative_file(root, record['executable'])
        if record['profile']['test']:
            suites[suite][path] = directory
        elif record['target']['kind'] == ['bin']:
            # Integration tests embed CARGO_BIN_EXE, not the test-harness binary.
            runtime[suite].add(path)
    require('target/debug/notion-knowledge-server' in runtime['webhook-worker'], 'Ordinary server binary missing')
    output.mkdir(parents=True, exist_ok=True)
    report = {}
    for suite, tests in suites.items():
        require(tests, f'No test executables for {suite}')
        files = sorted(set(tests) | runtime[suite])
        manifest = {'version': 1, 'revision': revision, 'workspace': str(root), 'suite': suite,
                    'tests': dict(sorted(tests.items())), 'files': {p: digest(root / p) for p in files}}
        archive_path = output / f'{suite}.tar.gz'
        with tarfile.open(archive_path, 'w:gz', compresslevel=1, dereference=True) as archive:
            data = json.dumps(manifest, sort_keys=True).encode()
            entry = tarfile.TarInfo('manifest.json')
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
            for name in files:
                archive.add(root / name, arcname=name, recursive=False)
        report[suite] = {'tests': len(tests), 'runtime_binaries': len(runtime[suite]),
                         'executable_bytes': sum((root / p).stat().st_size for p in files),
                         'archive_bytes': archive_path.stat().st_size}
    (output / 'footprint.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, sort_keys=True), flush=True)


def unpack(root, archive_path, revision, suite):
    root = root.resolve()
    with tarfile.open(archive_path, 'r:gz') as archive:
        members = archive.getmembers()
        require(len({m.name for m in members}) == len(members), 'Duplicate archive paths')
        header = archive.getmember('manifest.json')
        require(header.isfile() and header.size <= 1024 * 1024, 'Invalid bundle manifest')
        manifest = json.load(archive.extractfile(header))
        require(manifest['version'] == 1 and manifest['revision'] == revision
                and manifest['suite'] == suite, 'Bundle revision or suite differs')
        # CARGO_MANIFEST_DIR and CARGO_BIN_EXE contain the builder checkout path.
        require(manifest['workspace'] == str(root), 'Builder and runner workspace paths differ')
        require(manifest['tests'] and set(manifest['tests']) <= manifest['files'].keys(),
                'Missing test executables')
        for cwd in manifest['tests'].values():
            directory = PurePosixPath(cwd)
            require(not directory.is_absolute() and '..' not in directory.parts
                    and (root / cwd).resolve().is_relative_to(root)
                    and (root / cwd).is_dir(), 'Invalid package working directory')
        require({m.name for m in members} == {'manifest.json', *manifest['files']},
                'Unexpected archive contents')
        for member in members:
            if member.name == 'manifest.json':
                continue
            name = PurePosixPath(member.name)
            require(member.isfile() and not name.is_absolute() and '..' not in name.parts
                    and name.parts[:2] == ('target', 'debug'), 'Unsafe executable path')
            target = root / member.name
            require(target.resolve().is_relative_to(root / 'target'), 'Executable escapes target')
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open('xb') as destination:
                while block := source.read(1024 * 1024):
                    destination.write(block)
            require(digest(target) == manifest['files'][member.name], 'Executable digest differs')
            target.chmod(0o755)
    return manifest


def run(root, archive, revision, suite, report_path, timeout=180, budget=240):
    manifest = unpack(root, archive, revision, suite)
    results = []
    try:
        for executable, directory in manifest['tests'].items():
            require(sum(r['seconds'] for r in results) < budget, 'Suite execution budget exceeded')
            print(f'Running {suite}: {executable}', flush=True)
            started = time.monotonic()
            process = subprocess.Popen([str(root / executable)], cwd=root / directory, start_new_session=True)
            try:
                code = process.wait(timeout=min(timeout, budget - sum(r['seconds'] for r in results)))
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                results.append({'executable': executable, 'seconds': time.monotonic() - started,
                                'exit_code': process.returncode})
            require(code == 0, f'Test executable failed: {executable}')
    finally:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps({'suite': suite, 'revision': revision, 'tests': results,
                                          'seconds': sum(r['seconds'] for r in results)}, indent=2) + '\n')
    require(sum(r['seconds'] for r in results) <= budget, 'Suite execution budget exceeded')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    build = commands.add_parser('pack')
    build.add_argument('--messages', type=Path, required=True)
    build.add_argument('--metadata', type=Path, required=True)
    build.add_argument('--output', type=Path, required=True)
    execute = commands.add_parser('run')
    execute.add_argument('--archive', type=Path, required=True)
    execute.add_argument('--suite', choices=PACKAGES.values(), required=True)
    execute.add_argument('--report', type=Path, required=True)
    for command in (build, execute):
        command.add_argument('--revision', required=True)
    args = parser.parse_args()
    root = Path.cwd().resolve()
    if args.command == 'pack':
        with args.messages.open() as messages:
            pack(root, messages, json.loads(args.metadata.read_text()), args.revision, args.output)
    else:
        run(root, args.archive, args.revision, args.suite, args.report)


if __name__ == '__main__':
    main()
