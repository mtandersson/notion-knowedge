#!/usr/bin/env python3
"""Archive authoritative run data, removing signed URL credentials from debug logs.

GitHub retains the originals at each immutable run/attempt. Both original and
redacted SHA-256 hashes are recorded; original debug logs stay out of Git.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess


def sanitize(log):
    count = 0
    def scrub(match):
        nonlocal count
        url, query = match.groups()
        if re.search(r'(?:^|[&;]|\\u0026)(?:sig|signature|token|access_token|X-Amz-Signature)=', query, re.I):
            count += 1
            return url + '?[SIGNED_QUERY_REDACTED]'
        return match.group(0)
    result = re.sub(r'(https?://[^\s"<>?]+)\?([^\s"<>]+)', scrub, log)
    if re.search(r'eyJ[A-Za-z0-9_-]{15,}\.[A-Za-z0-9_-]{15,}\.[A-Za-z0-9_-]{15,}', result):
        raise ValueError('Possible unredacted JWT; inspect privately before archiving')
    if re.search(r'Bearer (?!\*\*\*)[A-Za-z0-9_-]{15,}', result):
        raise ValueError('Possible unredacted bearer; inspect privately before archiving')
    return result, count


def sha(data):
    return hashlib.sha256(data).hexdigest()


def archive(path, data):
    with path.open('wb') as destination:
        with gzip.GzipFile(filename='', mode='wb', fileobj=destination, mtime=0) as output:
            output.write(data)
    return {'path': path.name, 'uncompressed_sha256': sha(data), 'archive_sha256': sha(path.read_bytes())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default='mtandersson/notion-knowedge')
    parser.add_argument('--output', required=True)
    parser.add_argument('--append', action='store_true', help='Keep already archived runs and append new immutable run IDs')
    parser.add_argument('--cache-ref', action='append', default=[], help='Capture visible entries for this exact ref')
    parser.add_argument('runs', nargs='+', type=int)
    args = parser.parse_args()
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    if args.cache_ref:
        caches = subprocess.check_output(['gh', 'api', f'repos/{args.repo}/actions/caches',
                                          '--paginate', '--jq', '.actions_caches'], text=True)
        decoder = json.JSONDecoder()
        entries = []
        while caches.strip():
            page, consumed = decoder.raw_decode(caches.lstrip())
            entries.extend(x for x in page if x['ref'] in args.cache_ref)
            caches = caches.lstrip()[consumed:]
        archive(root / 'cache-snapshot.json.gz', (json.dumps(entries, indent=2) + '\n').encode())
    manifest = root / 'archive-manifest.json'
    records = json.loads(manifest.read_text()) if args.append and manifest.exists() else []
    existing = {record['run'] for record in records}
    for run_id in args.runs:
        if run_id in existing:
            continue
        prefix = f'repos/{args.repo}/actions/runs/{run_id}'
        run = subprocess.check_output(['gh', 'api', prefix])
        identity = json.loads(run)
        if identity['status'] != 'completed':
            raise SystemExit(f'Run {run_id} has not completed')
        jobs = subprocess.check_output(['gh', 'api', prefix + '/jobs?per_page=100'])
        raw = subprocess.check_output(['gh', 'run', 'view', str(run_id), '--repo', args.repo,
                                       '--attempt', str(identity['run_attempt']), '--log'])
        clean, count = sanitize(raw.decode())
        records.append({'run': run_id, 'attempt': identity['run_attempt'], 'head': identity['head_sha'],
                        'conclusion': identity['conclusion'], 'original_log_sha256': sha(raw),
                        'signed_queries_redacted': count,
                        'files': [archive(root / f'{run_id}-run.json.gz', run),
                                  archive(root / f'{run_id}-jobs.json.gz', jobs),
                                  archive(root / f'{run_id}.log.gz', clean.encode())]})
    manifest.write_text(json.dumps(records, indent=2) + '\n')
    print(json.dumps([{'run': r['run'], 'redactions': r['signed_queries_redacted']} for r in records]))


if __name__ == '__main__':
    main()
