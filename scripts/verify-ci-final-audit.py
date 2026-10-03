#!/usr/bin/env python3
"""Read-only, offline verification of retained CI audit calculations and inputs.

Requires the immutable checkout Git objects for input hashes; fetch the linked
heads/merge commits before verification if the local clone lacks them.
"""
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / 'docs'


def load(path):
    data = path.read_bytes()
    return json.loads(gzip.decompress(data) if path.suffix == '.gz' else data)


def seconds(a, b):
    return (datetime.fromisoformat(b.replace('Z', '+00:00')) -
            datetime.fromisoformat(a.replace('Z', '+00:00'))).total_seconds()


def module(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), ROOT / 'scripts' / (name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def verify_run(record, raw):
    run = load(raw / f"{record['run']}-run.json.gz")
    jobs = load(raw / f"{record['run']}-jobs.json.gz")['jobs']
    assert run['head_sha'] == record['head']
    assert run['conclusion'] == record['conclusion']
    assert seconds(run['created_at'], max(j['completed_at'] for j in jobs)) == record['workflow_wall_seconds']
    total = sum(seconds(s['started_at'], s['completed_at']) for j in jobs for s in j['steps']
                if s.get('started_at') and s.get('completed_at'))
    assert total == record['summed_step_execution_seconds']
    assert {j['name']: j['conclusion'] for j in jobs} == {j['name']: j['conclusion'] for j in record['jobs']}
    return jobs


def main():
    benchmark = module('benchmark-ci')
    for name in ('ci-performance', 'ci-dev-sharing', 'ci-final-audit'):
        path = DOCS / (name + ('-evidence.json' if name != 'ci-final-audit' else '.json'))
        data = load(path)
        for archive in data['raw_archives']:
            raw = DOCS / archive['path']
            assert raw.stat().st_size == archive['compressed_bytes'], raw
            assert hashlib.sha256(gzip.decompress(raw.read_bytes())).hexdigest() == archive['uncompressed_sha256'], raw
        for record in data['runs']:
            verify_run(record, DOCS / (name + '-raw'))
            if 'actual_production_inputs' in record:
                assert benchmark.production_digest(record['head']) == record['actual_production_inputs']
                for checkout in record['checkout_commits']:
                    assert benchmark.production_digest(checkout['commit']) == checkout['production_inputs']
        print(f"{name}: {len(data['raw_archives'])} archive hashes and {len(data['runs'])} complete run calculations verified")
    causal = module('calculate-docker-cache-evidence')
    for name, ledger_key in [('docker-cache-causal-raw', None), ('ci-final-audit-raw', 'docker_delivery')]:
        raw = DOCS / name
        generated = {'method': load(DOCS / 'docker-cache-causal-evidence.json')['method'],
                     'runs': [causal.calculate(raw, e) for e in load(raw / 'archive-manifest.json')]}
        expected = load(DOCS / 'docker-cache-causal-evidence.json') if ledger_key is None else load(DOCS / 'ci-final-audit.json')[ledger_key]
        assert generated == expected
        print(f"{name}: {len(generated['runs'])} complete Docker solve/index calculations verified")
    audit = load(DOCS / 'ci-final-audit.json')
    for number, graph in audit['native_graph'].items():
        raw = load(DOCS / 'ci-final-audit-raw' / f'issue-{number}.json.gz')
        assert hashlib.sha256(raw['issue']['body'].encode()).hexdigest() == graph['original_body_sha256']
        assert raw['issue']['state'] == graph['state']
        assert [{'number': i['number'], 'state': i['state']} for i in raw['children']] == graph['children']
        assert [{'number': i['number'], 'state': i['state']} for i in raw['blocked_by']] == graph['blocked_by']
    references = []
    for path in [ROOT / '.github/workflows/ci.yml', *sorted((ROOT / '.github/actions').glob('*/action.yml'))]:
        for ref in re.findall(r'uses:\s*([^\s#]+)', path.read_text()):
            if not ref.startswith('./'):
                assert re.fullmatch(r'[^@]+@[0-9a-f]{40}', ref), (path, ref)
                references.append(ref)
    assert len(references) == audit['verification']['external_action_references_checked']
    production = audit['production']
    paths = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', production], cwd=ROOT, text=True).splitlines()
    for name in paths:
        if name.startswith(('.github/', 'crates/')) or name.startswith('scripts/') or name in {'Dockerfile', '.dockerignore', 'flake.nix', 'flake.lock', 'Cargo.toml', 'Cargo.lock', '.cargo/audit.toml', '.gitleaks.toml', '.gitleaksignore'}:
            if name == 'scripts/verify-ci-final-audit.py':
                continue
            assert (ROOT / name).read_bytes() == subprocess.check_output(['git', 'show', f'{production}:{name}'], cwd=ROOT), name
    print(f"Native graph/body hashes, {len(references)} immutable action pins and unchanged production source verified")


if __name__ == '__main__':
    main()
