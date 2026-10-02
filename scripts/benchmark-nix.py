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
plan = subprocess.run(command + ['--dry-run'], text=True, capture_output=True, check=True)
print('DOWNLOAD_PLAN_BEGIN\n' + plan.stderr + '\nDOWNLOAD_PLAN_END', flush=True)
before = time.monotonic()
subprocess.run(command + ['--command', 'true'], check=True)
setup = time.monotonic() - before
closure = json.loads(subprocess.check_output(['nix', 'path-info', '--json', '--recursive', profile], text=True))
entries = closure.values() if isinstance(closure, dict) else closure
entries = list(entries)
report = {'shell': shell, 'setup_seconds': setup, 'plan_seconds': before-start,
          'closure_paths': len(entries), 'closure_nar_bytes': sum(x['narSize'] for x in entries)}
print('NIX_BENCHMARK ' + json.dumps(report), flush=True)
pathlib.Path('nix-benchmark.json').write_text(json.dumps(report, indent=2) + '\n')
subprocess.run(command + ['--command', 'bash', '-c',
    'for tool in cargo rustfmt cargo-audit gitleaks; do command -v "$tool" >/dev/null && "$tool" --version || true; done'], check=True)
