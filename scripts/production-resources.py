#!/usr/bin/env python3
"""Render explicit resource bounds and verify effective Docker containment.

No policy defaults are production sizing. No secrets are emitted in evidence.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

FIELDS = {
    'image', 'cpus', 'memory_bytes', 'pids', 'nofile', 'tmp_bytes', 'log_bytes',
    'log_files', 'proxy_connections', 'per_client_connections',
    'requests_per_second', 'request_burst', 'header_bytes', 'body_bytes',
    'header_timeout_seconds', 'body_timeout_seconds', 'write_timeout_seconds',
    'upstream_timeout_seconds',
}


def policy(path, fixture=False):
    data = json.loads(Path(path).read_text())
    if set(data) != FIELDS:
        raise ValueError('policy must contain exactly the documented fields')
    for name in FIELDS - {'image', 'cpus'}:
        if type(data[name]) is not int or not 1 <= data[name] <= 2**40:
            raise ValueError(f'{name} must be an explicit positive integer')
    if type(data['cpus']) not in (int, float) or not 0 < data['cpus'] <= 1024:
        raise ValueError('cpus must be positive and finite')
    if not isinstance(data['image'], str) or not re.fullmatch(r'[a-zA-Z0-9_./:@-]+', data['image']):
        raise ValueError('invalid image reference')
    if not fixture and not re.fullmatch(r'.+@sha256:[0-9a-f]{64}', data['image']):
        raise ValueError('production image must be digest pinned')
    if data['per_client_connections'] > data['proxy_connections']:
        raise ValueError('per-client connection ceiling exceeds total ceiling')
    return data


def render(data, environment, directory):
    environment = str(Path(environment).resolve(strict=True))
    directory = Path(directory)
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    service = {
        'image': data['image'], 'command': ['--http'], 'user': '65532:65532',
        'env_file': [environment],
        'environment': {'NK_HTTP_HOST': '0.0.0.0', 'NK_HTTP_PORT': '3000'},
        'ports': ['127.0.0.1:3000:3000'], 'read_only': True,
        'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges:true'],
        'cpus': data['cpus'], 'mem_limit': data['memory_bytes'],
        'memswap_limit': data['memory_bytes'], 'pids_limit': data['pids'],
        'ulimits': {'nofile': {'soft': data['nofile'], 'hard': data['nofile']}},
        'tmpfs': [f'/tmp:rw,noexec,nosuid,nodev,size={data["tmp_bytes"]},mode=1777'],
        'logging': {'driver': 'local', 'options': {'max-size': str(data['log_bytes']), 'max-file': str(data['log_files'])}},
        'stop_signal': 'SIGINT', 'stop_grace_period': '30s',
    }
    (directory / 'compose.json').write_text(json.dumps({'services': {'knowledge': service}}, indent=2) + '\n')
    # Reuse the reviewed OAuth authority/origin/routes contract; only add quotas.
    root = Path(__file__).resolve().parent.parent
    nginx = (root / 'docs/examples/oauth-nginx.conf').read_text()
    prefix = f'''# Policy-generated limits; use a dedicated nginx process/listener.
limit_conn_zone $binary_remote_addr zone=knowledge_clients:1m;
limit_req_zone $binary_remote_addr zone=knowledge_rate:1m rate={data['requests_per_second']}r/s;
'''
    bounds = f'''
    limit_conn knowledge_clients {data['per_client_connections']};
    limit_conn_status 429;
    limit_req zone=knowledge_rate burst={data['request_burst']} nodelay;
    limit_req_status 429;
    client_header_buffer_size {data['header_bytes']};
    large_client_header_buffers 1 {data['header_bytes']};
    client_header_timeout {data['header_timeout_seconds']}s;
    keepalive_timeout 5s;
    keepalive_requests 100;
'''
    nginx = nginx.replace('server {', 'server {' + bounds, 1)
    nginx = nginx.replace('client_max_body_size 1m;', f'client_max_body_size {data["body_bytes"]};')
    nginx = nginx.replace('client_body_timeout 10s;', f'client_body_timeout {data["body_timeout_seconds"]}s;')
    nginx = nginx.replace('send_timeout 30s;', f'send_timeout {data["write_timeout_seconds"]}s;')
    nginx = nginx.replace('proxy_read_timeout 360s;', f'''proxy_connect_timeout {data['upstream_timeout_seconds']}s;
        proxy_send_timeout {data['upstream_timeout_seconds']}s;
        proxy_read_timeout {data['upstream_timeout_seconds']}s;''')
    # Dedicated process has one worker and bounded worker connections; no upstream
    # retry or application queue is added. Client admission rejects excess work.
    nginx = f'worker_processes 1;\nevents {{ worker_connections {data["proxy_connections"]}; }}\nhttp {{\n' + prefix + nginx + '\n}\n'
    (directory / 'nginx.conf').write_text(nginx)


def inspect(data, container):
    result = subprocess.run(['docker', 'inspect', container], capture_output=True, text=True, check=True)
    current = json.loads(result.stdout)[0]
    host = current['HostConfig']
    expected = {'NanoCpus': int(data['cpus'] * 1_000_000_000),
                'Memory': data['memory_bytes'], 'MemorySwap': data['memory_bytes'],
                'PidsLimit': data['pids'], 'ReadonlyRootfs': True}
    if any(host.get(k) != v for k, v in expected.items()):
        raise ValueError('effective container resource bounds differ from policy')
    limits = {x['Name']: (x['Soft'], x['Hard']) for x in host.get('Ulimits', [])}
    if limits.get('nofile') != (data['nofile'], data['nofile']):
        raise ValueError('file descriptor ceiling differs from policy')
    tmp = host.get('Tmpfs', {}).get('/tmp', '')
    if f'size={data["tmp_bytes"]}' not in tmp.split(','):
        raise ValueError('temporary storage ceiling differs from policy')
    if not current['State']['Running']:
        raise ValueError('container is not running')
    return {'schema_version': 1, 'scope': 'effective Docker configuration only; not capacity acceptance',
            'image_id': current['Image'], 'limits': expected, 'nofile': data['nofile'],
            'tmp_bytes': data['tmp_bytes']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('policy')
    parser.add_argument('--fixture', action='store_true', help='allow local image tag; evidence is never production acceptance')
    sub = parser.add_subparsers(dest='operation', required=True)
    write = sub.add_parser('render')
    write.add_argument('--environment', required=True)
    write.add_argument('--output-directory', required=True)
    check = sub.add_parser('inspect')
    check.add_argument('--container', required=True)
    args = parser.parse_args()
    try:
        data = policy(args.policy, args.fixture)
        if args.operation == 'render':
            render(data, args.environment, args.output_directory)
        else:
            print(json.dumps(inspect(data, args.container), indent=2))
    except (ValueError, OSError, subprocess.CalledProcessError, KeyError) as error:
        # Never print docker inspect output or secret-bearing service environment.
        parser.exit(2, f'Resource acceptance failed: {type(error).__name__}\n')


if __name__ == '__main__':
    main()
