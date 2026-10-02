#!/usr/bin/env python3
"""Hosted Nix probe: dry-run transfer estimate, actual setup, closure and tool identity."""
import json
import os
import pathlib
import subprocess
import time

shell = os.environ['NIX_BENCH_SHELL']
profile = '/tmp/nk-nix-benchmark-profile'
command = ['nix', 'develop', f'.#{shell}', '--profile', profile]
start = time.monotonic()
plan = subprocess.run(['nix', 'build', f'.#devShells.x86_64-linux.{shell}', '--dry-run'], text=True, capture_output=True)
print('DOWNLOAD_PLAN_BEGIN\n' + plan.stderr + '\nDOWNLOAD_PLAN_END', flush=True)
plan.check_returncode()
before = time.monotonic()
activation = subprocess.run(command + ['--command', 'true'], text=True, capture_output=True)
print(activation.stdout + activation.stderr, flush=True)
activation.check_returncode()
setup = time.monotonic() - before
closure = json.loads(subprocess.check_output(['nix', 'path-info', '--json', '--recursive', profile], text=True))
entries = closure.values() if isinstance(closure, dict) else closure
entries = list(entries)
report = {'shell': shell, 'setup_seconds': setup, 'plan_seconds': before-start,
          'closure_paths': len(entries), 'closure_nar_bytes': sum(x['narSize'] for x in entries),
          'copied_paths': activation.stderr.count("copying path '")}
print('NIX_BENCHMARK ' + json.dumps(report), flush=True)
pathlib.Path('nix-benchmark.json').write_text(json.dumps(report, indent=2) + '\n')
tools = {'default': [('cargo', '--version'), ('rustfmt', '--version'),
                     ('cargo-audit', '--version'), ('gitleaks', 'version')],
         'format': [('cargo', '--version'), ('rustfmt', '--version')],
         'security': [('cargo-audit', '--version'), ('gitleaks', 'version')]}
for tool, option in tools[shell]:
    subprocess.run(command + ['--command', tool, option], check=True)
