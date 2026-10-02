#!/usr/bin/env python3
"""Select PR checks from committed inputs and validate the final Actions gate."""
import argparse
import json
import os
from pathlib import PurePosixPath
import subprocess
import sys

JOBS = ('agent-layout', 'fmt', 'clippy', 'check', 'test', 'build', 'container',
        'dependencies', 'secrets')
RUST = {'fmt', 'clippy', 'check', 'test', 'build'}
SECURITY = {'dependencies', 'secrets'}


def path_jobs(path):
    """Known narrow inputs are allowlisted; everything else requires all checks."""
    if path in {'AGENTS.md', 'CLAUDE.md', '.claude/skills', '.codex/skills'} or any(
        path.startswith(prefix) for prefix in ('.agents/', '.claude/', '.codex/')
    ) or path == 'scripts/check-agent-layout.sh':
        return {'agent-layout'}
    if path in {'README.md', 'CONTRIBUTING.md', 'LICENSE'} or (
        path.endswith('.md') and path.startswith(('docs/', 'eval/'))
    ):
        return set()
    if path.startswith('eval/'):
        return {'test'}
    if path.startswith('crates/') and '/tests/' in path and path.endswith('.rs'):
        # Docker's context allowlist includes every .rs file, even tests.
        return RUST | {'container'}
    if path in {'Dockerfile', '.dockerignore', 'scripts/smoke-container.py'}:
        return {'container'}
    if path.startswith('crates/') and (path.endswith('.rs') or
                                      PurePosixPath(path).name == 'Cargo.toml'):
        return RUST | {'container'}
    if path in {'Cargo.toml', 'Cargo.lock'}:
        return RUST | {'container'}
    return set(JOBS)


def changed_paths(base, head):
    # Rename detection supplies both paths: moving a build input into docs is
    # still a build change. -z keeps tabs/newlines in filenames unambiguous.
    data = subprocess.check_output([
        'git', 'diff', '--name-status', '-z', '--find-renames',
        f'{base}...{head}', '--',
    ]).split(b'\0')
    paths = []
    index = 0
    while index < len(data) and data[index]:
        status = data[index].decode('ascii')
        count = 2 if status[0] in 'RC' else 1
        if status[0] not in 'ACDMRTUXB' or index + count >= len(data):
            raise ValueError('Unrecognized git change record')
        paths.extend(p.decode('utf-8', errors='surrogateescape')
                     for p in data[index + 1:index + 1 + count])
        index += count + 1
    return paths


def select(event, base=None, head=None):
    if event != 'pull_request':
        selected = set(JOBS)
    else:
        if not base or not head:
            raise ValueError('PR selection requires base and head revisions')
        paths = changed_paths(base, head)
        # An empty diff is unusual; fail conservatively to full coverage.
        selected = SECURITY if paths else set(JOBS)
        for path in paths:
            selected |= path_jobs(path)
    return {job: 'true' if job in selected else 'false' for job in JOBS}


def validate(needs):
    errors = []
    if needs.get('changes', {}).get('result') != 'success':
        errors.append('Change selection did not succeed')
    outputs = needs.get('changes', {}).get('outputs', {})
    for job in JOBS:
        selected = outputs.get(job)
        result = needs.get(job, {}).get('result')
        if selected not in {'true', 'false'}:
            errors.append(f'{job}: invalid selection')
        elif job in SECURITY and selected != 'true':
            errors.append(f'{job}: security checks must always be selected')
        elif selected == 'true' and result != 'success':
            errors.append(f'{job}: required job finished {result!r}')
        elif selected == 'false' and result not in {'skipped', 'success'}:
            errors.append(f'{job}: unselected job finished {result!r}')
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    selection = commands.add_parser('select')
    selection.add_argument('--event', required=True)
    selection.add_argument('--base')
    selection.add_argument('--head')
    commands.add_parser('gate')
    args = parser.parse_args()
    if args.command == 'select':
        outputs = select(args.event, args.base, args.head)
        print(json.dumps(outputs, sort_keys=True))
        if os.environ.get('GITHUB_OUTPUT'):
            with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as output:
                for job, selected in outputs.items():
                    output.write(f'{job}={selected}\n')
    else:
        errors = validate(json.loads(os.environ['CI_NEEDS']))
        for error in errors:
            print(error, file=sys.stderr)
        return bool(errors)
    return 0


if __name__ == '__main__':
    sys.exit(main())
