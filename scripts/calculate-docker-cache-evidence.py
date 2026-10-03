#!/usr/bin/env python3
"""Recalculate Docker causal observations from the sanitized immutable archives.

COPY input identity excludes mtime, which Docker does not use in COPY checksums.
Cache result counts describe graph coverage, never an inferred hit or cause.
"""
import argparse
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import re


def read(root, name):
    return gzip.decompress((root / name).read_bytes())


def seconds(start, end):
    return (datetime.fromisoformat(end.replace('Z', '+00:00')) -
            datetime.fromisoformat(start.replace('Z', '+00:00'))).total_seconds()


def calculate(root, entry):
    number = entry['run']
    run = json.loads(read(root, f'{number}-run.json.gz'))
    jobs = json.loads(read(root, f'{number}-jobs.json.gz'))['jobs']
    log = read(root, f'{number}.log.gz').decode()
    contexts, indices, preflight, artifacts, invalidations, solves = [], [], [], [], [], {}
    for line in log.splitlines():
        parts = line.split('\t', 2)
        if len(parts) != 3 or parts[0] != 'Container smoke':
            continue
        step = parts[1]
        body = re.sub(r'^\d{4}-\d\d-\d\dT\S+ ', '', parts[2])
        if body.startswith('EFFECTIVE_CONTEXT '):
            context = json.loads(body.split(' ', 1)[1])
            records = [{k: v for k, v in record.items() if k != 'mtime'}
                       for record in context['records']]
            context['copy_input_identity_sha256'] = hashlib.sha256(
                json.dumps(records, sort_keys=True).encode()).hexdigest()
            contexts.append({k: v for k, v in context.items() if k != 'records'})
        if body.startswith('CACHE_INDEX_LOOKUP '):
            index = json.loads(body.split(' ', 1)[1])
            index['step'] = step
            manifest = index.pop('manifest', None)
            if manifest:
                # BuildKit emits compact JSON. Reconstructing it from the probe
                # must match the producer's exact body hash and byte count.
                encoded = json.dumps(manifest, separators=(',', ':'), ensure_ascii=False).encode()
                if len(encoded) != index['bytes'] or hashlib.sha256(encoded).hexdigest() != index['sha256']:
                    raise ValueError(f"Run {number}: index body identity mismatch")
                index['records'] = len(manifest['records'])
                index['layers'] = len(manifest['layers'])
                index['result_layers_by_key'] = {
                    record['digest']: len(record.get('layers', []))
                    for record in manifest['records']}
                index['layer_descriptor_bytes'] = sum(
                    layer.get('annotations', {}).get('size', 0)
                    for layer in manifest['layers'])
            indices.append(index)
        if body.startswith('DOCKER_CACHE_ROOT '):
            preflight.append(json.loads(body.split(' ', 1)[1]))
        if body.startswith('INVALIDATION_RESULT '):
            invalidations.append(json.loads(body.split(' ', 1)[1]))
        if body.startswith('VERIFIED_EXECUTABLE '):
            artifacts.append(json.loads(body.split(' ', 1)[1]))
        parameter = body.lstrip()
        if parameter.startswith('cache-from:') or parameter.startswith('cache-to:'):
            solve = solves.setdefault(step, {'compiled_crates': [], 'vertices': {}})
            name, value = parameter.split(':', 1)
            solve.setdefault(name, []).extend([value.strip()] if value.strip() else [])
        elif parameter.startswith('type=gha,scope='):
            solves.setdefault(step, {'compiled_crates': [], 'vertices': {}}).setdefault('cache-from', []).append(parameter)
        if not body.startswith('#'):
            continue
        solve = solves.setdefault(step, {'compiled_crates': [], 'vertices': {}})
        crate = re.search(r'\bCompiling (\S+) v', body)
        if crate:
            solve['compiled_crates'].append(crate.group(1))
        vertex = re.match(r'(#\d+) \[([^]]+)\] (.*)', body)
        if vertex:
            solve['vertices'].setdefault(vertex[1], {}).update(
                {'stage': vertex[2], 'operation': vertex[3]})
        status = re.match(r'(#\d+) (CACHED|DONE)(?: ([\d.]+)s)?$', body)
        if status:
            item = solve['vertices'].setdefault(status[1], {})
            item.setdefault('progress_statuses', []).append(status[2])
            item.update(
                {'status': status[2], 'reported_seconds': float(status[3]) if status[3] else None})
    timed_jobs = []
    for job in jobs:
        steps = [{ 'name': step['name'], 'conclusion': step['conclusion'],
                   'seconds': seconds(step['started_at'], step['completed_at'])
                   if step.get('started_at') and step.get('completed_at') else 0}
                 for step in job['steps']]
        timed_jobs.append({'name': job['name'], 'conclusion': job['conclusion'],
                           'runner_labels': job['labels'],
                           'job_wall_seconds': seconds(job['started_at'], job['completed_at']),
                           'step_execution_seconds': sum(step['seconds'] for step in steps),
                           'steps': steps if job['name'] == 'Container smoke' else []})
    return {'run': number, 'url': run['html_url'], 'attempt': run['run_attempt'],
            'head': run['head_sha'], 'conclusion': run['conclusion'],
            'original_log_sha256': entry['original_log_sha256'],
            'sanitized_log_sha256': hashlib.sha256(log.encode()).hexdigest(),
            'workflow_wall_seconds': seconds(run['created_at'], max(job['completed_at'] for job in jobs)),
            'summed_step_execution_seconds': sum(job['step_execution_seconds'] for job in timed_jobs),
            'dispatch_to_first_job_seconds': seconds(run['created_at'], min(job['started_at'] for job in jobs)),
            'contexts': contexts, 'cache_index_lookups': indices, 'cache_preflight': preflight,
            'verified_executables': artifacts, 'invalidation_controls': invalidations, 'container_solves': solves, 'jobs': timed_jobs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    root = Path(args.input)
    entries = json.loads((root / 'archive-manifest.json').read_text())
    observations = [calculate(root, entry) for entry in entries]
    Path(args.output).write_text(json.dumps({
        'method': 'All archived runs retained, including cancelled and ineffective treatments. '
                  'Every decoded index body is checked against its exact original byte length and SHA-256. '
                  'Layer descriptor bytes are per-manifest transfer descriptions, not additive repository storage. '
                  'Cached vertices can reflect lazy downstream reuse; compiled events and actual record traces remain authoritative.',
        'runs': observations}, indent=2) + '\n')


if __name__ == '__main__':
    main()
