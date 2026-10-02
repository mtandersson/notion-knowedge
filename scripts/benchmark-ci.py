#!/usr/bin/env python3
"""Generate isolated matched CI harnesses; collect immutable hosted API/log evidence.

Requires PyYAML only for prepare. collect uses the standard library and gh.
Does not dispatch, create PRs, delete caches, or modify production files.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path
import subprocess

BASELINE = '4167a4726688fd79d9e5e831ad8e9a7fc4c321a6'
CANONICAL = ['Agent layout', 'Format', 'Clippy', 'Type check', 'Unit tests',
             'Release build', 'Container smoke', 'Dependency vulnerabilities', 'Secret scanning']


def git(*args):
    return subprocess.check_output(['git', *args])


def production_digest(ref):
    paths = git('ls-tree', '-r', '--name-only', ref).decode().splitlines()
    paths = [p for p in paths if p.startswith(('crates/', 'eval/', '.cargo/')) or
             p in {'Cargo.toml', 'Cargo.lock', 'Dockerfile', '.dockerignore', 'flake.nix', 'flake.lock', '.gitleaks.toml', '.gitleaksignore'} or
             p.startswith('scripts/check-') and p.endswith(('.sh', '.py')) or
             p in {'scripts/smoke-container.py', 'scripts/test-security-scanning.py'}]
    records = {p: hashlib.sha256(git('show', f'{ref}:{p}')).hexdigest() for p in paths}
    return {'files': [{'path':p, 'sha256':h} for p,h in records.items()], 'sha256': hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest()}


def prepare(args):
    import yaml
    if not re.fullmatch(r'[A-Za-z0-9_-]+', args.namespace):
        raise ValueError('Use a simple letters/digits/hyphens/underscores cache namespace.')
    subprocess.check_call(['git','check-ref-format','--branch',args.base_branch], stdout=subprocess.DEVNULL)
    # Quote GitHub's `on` before parsing with YAML 1.1; retain scalar types.
    text = git('show', f'{BASELINE if args.design == "baseline" else args.production}:.github/workflows/ci.yml').decode()
    workflow = yaml.safe_load(text.replace('\non:', "\n'on':"))
    workflow['on']['pull_request']['branches'] = ['main', args.base_branch]
    workflow['env']['BENCH_CACHE_NAMESPACE'] = args.namespace
    for job_id, job in workflow['jobs'].items():
        for step in job.get('steps', []):
            if 'uses' in step and step['uses'].startswith('actions/cache@'):
                config = step['with']
                config['key'] = args.namespace + '-' + config['key']
                config['restore-keys'] = '\n'.join(args.namespace + '-' + x for x in config.get('restore-keys', '').splitlines())
            if 'run' in step and 'cargo ' in step['run'] and 'nix develop' in step['run'] and 'cargo fmt' not in step['run']:
                step['run'] = step['run'].replace('--command cargo ', '--command env CARGO_LOG=cargo::core::compiler::fingerprint=info cargo ')
            if step.get('name') in {'Build and verify final runtime image', 'Verify final runtime image'}:
                step['run'] = step['run'].replace('scripts/smoke-container.py', 'scripts/benchmark-container.py')
        # Materialize each job's selected shell explicitly; separate Nix setup
        # from Cargo/security execution without changing selected tools.
        steps = job.get('steps', [])
        if any(s.get('name') == 'Install Nix' for s in steps):
            shell = 'default' if args.design == 'baseline' or job_id not in {'fmt','dependencies','secrets'} else ('format' if job_id == 'fmt' else 'security')
            position = next(i for i,s in enumerate(steps) if s.get('name') == 'Install Nix') + 1
            steps.insert(position, {'name': 'Materialize pinned Nix shell', 'run': f'nix develop .#{shell} --command true'})
        if job_id == 'container' and args.design == 'optimized':
            job['env']['DOCKER_CACHE_SCOPE'] = args.namespace + '-docker'
            for step in steps:
                if step.get('name') == 'Build and load final runtime image':
                    step['with']['cache-from'] = 'type=gha,scope=${{ env.DOCKER_CACHE_SCOPE }}'
    out = Path(args.output)
    (out / '.github/workflows').mkdir(parents=True, exist_ok=True)
    (out / '.github/workflows/ci.yml').write_text(yaml.safe_dump(workflow, sort_keys=False))
    (out / 'scripts').mkdir(exist_ok=True)
    (out / 'scripts/benchmark-container.py').write_text('''import importlib.util, json, sys, time
spec=importlib.util.spec_from_file_location("smoke", "scripts/smoke-container.py")
smoke=importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)
original=smoke.docker
def measured(*args, **kwargs):
    start=time.monotonic()
    try: return original(*args, **kwargs)
    finally: print("BENCH_DOCKER " + json.dumps({"operation": args[0], "arguments": list(args[:4]), "seconds": time.monotonic()-start}), flush=True)
smoke.docker=measured
start=time.monotonic()
try: smoke.main()
finally: print("BENCH_SMOKE " + json.dumps({"seconds": time.monotonic()-start}), flush=True)
''')
    if args.design == 'optimized':
        action = yaml.safe_load(git('show', f'{args.production}:.github/actions/cargo-cache-restore/action.yml').decode())
        step = next(s for s in action['runs']['steps'] if s.get('id') == 'keys')
        step['run'] = 'env GITHUB_OUTPUT=/tmp/nk-bench-keys ' + step['run'] + "\nsed 's/^\\([^=]*=\\)/\\1${{ env.BENCH_CACHE_NAMESPACE }}-/' /tmp/nk-bench-keys >> \"$GITHUB_OUTPUT\""
        for cache_step in action['runs']['steps']:
            if 'with' in cache_step and 'restore-keys' in cache_step['with']:
                cache_step['with']['restore-keys'] = cache_step['with']['restore-keys'].replace('cargo-${{ runner.os }}-', '${{ env.BENCH_CACHE_NAMESPACE }}-cargo-${{ runner.os }}-')
        path = out / '.github/actions/cargo-cache-restore/action.yml'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(action, sort_keys=False))
    Path(str(out) + '.inputs.json').write_text(json.dumps({'design':args.design, 'production':args.production,
        'historical_workflow':BASELINE, 'namespace':args.namespace, 'production_inputs':production_digest(args.production)}, indent=2)+'\n')


def collect(args):
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    for run_id in args.runs:
        prefix = f'repos/{args.repo}/actions/runs/{run_id}'
        run = json.loads(subprocess.check_output(['gh','api', prefix]))
        if run['status'] != 'completed':
            raise SystemExit(f'Run {run_id} is not complete; collect after it reaches a terminal state.')
        jobs = json.loads(subprocess.check_output(['gh','api',prefix+'/jobs?per_page=100']))
        (out / f'{run_id}-run.json').write_text(json.dumps(run, indent=2)+'\n')
        (out / f'{run_id}-jobs.json').write_text(json.dumps(jobs, indent=2)+'\n')
        log = subprocess.check_output(['gh','run','view',str(run_id),'--repo',args.repo,'--log']).decode()
        (out / f'{run_id}.log').write_text(log)
        print(json.dumps({'run':run_id,'head':run['head_sha'],'conclusion':run['conclusion'],
                          'log_sha256':hashlib.sha256(log.encode()).hexdigest()}))


def seconds(start, end):
    from datetime import datetime
    return (datetime.fromisoformat(end.replace('Z','+00:00')) - datetime.fromisoformat(start.replace('Z','+00:00'))).total_seconds()


def summarize(args):
    root = Path(args.input)
    ledger = []
    for run_id in args.runs:
        run = json.loads((root / f'{run_id}-run.json').read_text())
        jobs = json.loads((root / f'{run_id}-jobs.json').read_text())['jobs']
        raw = (root / f'{run_id}.log').read_text()
        text = re.sub(r'\x1b\[[0-9;]*m', '', raw)
        log_jobs = {}
        for line in text.splitlines():
            parts = line.split('\t', 2)
            if len(parts) == 3: log_jobs.setdefault(parts[0], []).append(parts[2])
        checkouts = sorted(set(re.findall(r'\[command\].*git log -1 --format=%H\n[^\n]*?\t[^\t]*?\t[^ ]+ ([0-9a-f]{40})(?:\n|$)', text)))
        for checkout in checkouts:
            found = subprocess.run(['git','cat-file','-e',checkout+'^{commit}'], capture_output=True)
            if found.returncode:
                subprocess.check_call(['git','fetch','origin',checkout], stdout=subprocess.DEVNULL)
        results = []
        for job in jobs:
            steps = [{**s, 'seconds':seconds(s['started_at'],s['completed_at']) if s.get('started_at') and s.get('completed_at') else 0} for s in job['steps']]
            joblog = log_jobs.get(job['name'], [])
            compiled = []
            finishes = []
            actions = {}
            nested = []
            evidence = []
            docker = []
            for line in joblog:
                if 'Compiling ' in line or 'Checking ' in line:
                    match = re.search(r'(?:Compiling|Checking) (\S+) v',line)
                    if match: compiled.append(match.group(1))
                if re.search(r'Finished.*profile.*target\(s\) in ',line): finishes.append(line)
                if '##[start-action' in line:
                    match=re.search(r'display=(.*);id=([^\]]+)\]',line)
                    if match: actions[match.group(2)]=match.group(1)
                if '##[end-action' in line:
                    match=re.search(r'id=([^;]+);.*duration_ms=(\d+)\]',line)
                    if match: nested.append({'name':actions.get(match.group(1),match.group(1)),'seconds':int(match.group(2))/1000})
                if any(term in line for term in ['Cache not found','Cache restored from key','Cache saved with key','Cache Size:','not saving cache','not saving','CACHED','importing cache manifest','exporting cache to','ChangedFile','StaleDepFingerprint','unchanged timestamps restored','Final-image smoke passed','commits scanned','no leaks found','Fetching advisory database','test result:']): evidence.append(line)
                if 'BENCH_DOCKER ' in line: docker.append(json.loads(line.split('BENCH_DOCKER ',1)[1]))
            results.append({'id':job['id'],'url':job['html_url'],'name':job['name'],'conclusion':job['conclusion'],
                'runner_labels':job['labels'], 'runner_image_evidence':[line for line in joblog if 'Image: ubuntu-' in line or 'Version: 20260927' in line or '24.04.5' in line],'queue_seconds':seconds(job['created_at'],job['started_at']) if job['conclusion'] != 'skipped' else 0,
                'job_envelope_seconds':seconds(job['started_at'],job['completed_at']) if job['conclusion'] != 'skipped' else 0,
                'step_execution_seconds':sum(s['seconds'] for s in steps),'steps':steps,
                'nested_action_spans':nested,'cargo_reported_spans':finishes,
                'compiled_or_checked_crates':compiled,'external_crate_events':sum(not x.startswith('notion-knowledge-') for x in compiled),
                'evidence':evidence,'docker_calls':docker})
        ledger.append({'run':run_id,'url':run['html_url'],'event':run['event'],'head':run['head_sha'],
            'conclusion':run['conclusion'],'attempt':run['run_attempt'],
            'workflow_wall_seconds':seconds(run['created_at'],max(j['completed_at'] for j in jobs)),
            'initial_queue_seconds':seconds(run['created_at'],min(j['started_at'] for j in jobs)),
            'summed_job_queue_seconds':sum(j['queue_seconds'] for j in results),
            'summed_step_execution_seconds':sum(j['step_execution_seconds'] for j in results),
            'summed_job_envelope_seconds':sum(j['job_envelope_seconds'] for j in results),
            'raw_log_sha256':hashlib.sha256(raw.encode()).hexdigest(),
            'actual_production_inputs':production_digest(run['head_sha']),
            'checkout_commits': [{'commit':c,'production_inputs':production_digest(c)} for c in checkouts], 'jobs':results})
    Path(args.output).write_text(json.dumps(ledger,indent=2)+'\n')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prep = commands.add_parser('prepare')
    prep.add_argument('--design', choices=['baseline','optimized'], required=True)
    prep.add_argument('--production', required=True)
    prep.add_argument('--namespace', required=True)
    prep.add_argument('--base-branch', required=True)
    prep.add_argument('--output', required=True)
    fetch = commands.add_parser('collect')
    fetch.add_argument('--repo', default='mtandersson/notion-knowedge')
    fetch.add_argument('--output', required=True)
    fetch.add_argument('runs', nargs='+', type=int)
    summary = commands.add_parser('summarize')
    summary.add_argument('--input', required=True)
    summary.add_argument('--output', required=True)
    summary.add_argument('runs', nargs='+', type=int)
    args = parser.parse_args()
    if args.command == 'prepare': prepare(args)
    elif args.command == 'collect': collect(args)
    else: summarize(args)


if __name__ == '__main__':
    main()
