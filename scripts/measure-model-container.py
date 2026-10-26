#!/usr/bin/env python3
"""Opt-in bounded synthetic shipping-model measurements; never production sizing."""
import argparse
import contextlib
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parent.parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runtime = load('model_container_smoke', ROOT / 'scripts/smoke-retrieval-container.py')
resources = load('production_resources', ROOT / 'scripts/production-resources.py')


def require(value, reason):
    if not value:
        raise ValueError(reason)


def private(path):
    require(path.is_absolute() and path.resolve() == path and path.is_dir(), 'Private directory must exist without symlinks')
    info = path.stat()
    require(info.st_uid == os.getuid() and info.st_mode & 0o077 == 0, 'Directory must be private and operator owned')


def headroom(minimum, maximum, proc=Path('/proc')):
    memory = dict(line.split(':', 1) for line in (proc / 'meminfo').read_text().splitlines())
    available = int(memory['MemAvailable'].split()[0]) * 1024
    full = next(line for line in (proc / 'pressure/memory').read_text().splitlines() if line.startswith('full '))
    psi = float(dict(item.split('=') for item in full.split()[1:])['avg60'])
    require(available >= minimum and psi < maximum, 'Host memory headroom/pressure gate refused work')
    return {'available_bytes': available, 'memory_full_avg60': psi}


def caps(policy):
    return ['--cpus', str(policy['cpus']), '--memory', str(policy['memory_bytes']),
            '--memory-swap', str(policy['memory_bytes']), '--pids-limit', str(policy['pids']),
            '--ulimit', f"nofile={policy['nofile']}:{policy['nofile']}",
            '--log-driver', 'local', '--log-opt', f"max-size={policy['log_bytes']}",
            '--log-opt', f"max-file={policy['log_files']}"]


def probe_output(text):
    require(len(text) <= 65536, 'Sampler output bound exceeded')
    result = {}
    for block in text.split('\nEND\n'):
        if not block:
            continue
        key, value = block.split('\n', 1)
        result[key] = value.strip()
    expected = {'cpu.max', 'cpu.stat', 'memory.max', 'memory.current', 'memory.peak',
                'memory.events', 'memory.swap.max', 'pids.max', 'pids.current',
                'process.status', 'process.limits', 'process.fd_count',
                'tmp.allocated_bytes', 'index.allocated_bytes', 'state.allocated_bytes'}
    require(set(result) == expected, 'Incomplete kernel/process/filesystem sample')
    # Retain counters only, never arbitrary process names or diagnostic payloads.
    status = dict(line.split(':', 1) for line in result.pop('process.status').splitlines())
    result['process'] = {k: status[k].strip() for k in ('VmRSS', 'VmHWM', 'Threads')}
    limits = result.pop('process.limits')
    row = next(line for line in limits.splitlines() if line.startswith('Max open files'))
    result['process.nofile'] = list(map(int, row.split()[3:5]))
    for key in ('cpu.stat', 'memory.events'):
        result[key] = {k: int(v) for k, v in (line.split() for line in result[key].splitlines())}
    for key in expected - {'cpu.max', 'cpu.stat', 'memory.events', 'process.status', 'process.limits'}:
        result[key] = int(result[key])
    return result


def check_effective(info, policy, image_id):
    host = info['HostConfig']
    require(info['Image'] == image_id, 'Container image does not match inspected immutable image')
    require(host['NanoCpus'] == int(policy['cpus'] * 1e9) and
            host['Memory'] == host['MemorySwap'] == policy['memory_bytes'] and
            host['PidsLimit'] == policy['pids'], 'Effective container ceilings differ from policy')
    require(host['ReadonlyRootfs'] and not host.get('Privileged') and
            host['NetworkMode'] != 'host' and not host.get('PortBindings'), 'Container isolation differs from private baseline')
    require(info['Config']['User'] == '65532:65532' and 'ALL' in host.get('CapDrop', []) and
            'no-new-privileges' in ' '.join(host.get('SecurityOpt', [])), 'Container hardening differs')
    fd = next(v for v in host['Ulimits'] if v['Name'] == 'nofile')
    require(fd['Soft'] == fd['Hard'] == policy['nofile'], 'Effective FD ceiling differs')
    require(f"size={policy['tmp_bytes']}" in host['Tmpfs']['/tmp'], 'Effective temporary ceiling differs')
    log = host['LogConfig']
    require(log['Type'] == 'local' and log['Config']['max-size'] == str(policy['log_bytes']) and
            log['Config']['max-file'] == str(policy['log_files']), 'Effective log ceiling differs')


class Runner:
    def __init__(self, args, policy, evidence, backend=None):
        self.args, self.policy, self.evidence = args, policy, evidence
        self.backend = backend or runtime.docker
        self.deadline = time.monotonic() + args.duration
        self.phase = 'prepare'
        self.abort = threading.Event()
        self.done = threading.Event()
        self.models = set()
        self.lock = threading.Lock()
        self.failure = None
        self.samples = 0
        self.bytes = 0
        self.report = {'scope': 'tiny synthetic local container; not production capacity',
                       'outcome': 'failed', 'phases': [], 'retained_volumes': []}

    def call(self, *args, **kwargs):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0 and not self.abort.is_set(), 'Run deadline/headroom/evidence limit reached')
        kwargs['timeout'] = min(kwargs.get('timeout', 300), remaining)
        if args[0] in ('build', 'pull', 'buildx'):
            raise ValueError('Implicit builds and pulls are forbidden; provision bounded fixed images separately')
        if args[0] == 'run':
            headroom(self.args.stop_available_bytes, self.args.stop_psi)
            model = '--name' in args and args[args.index('--name') + 1] in self.models
            policy = self.policy if model else dict(self.policy, cpus=.25, memory_bytes=128*1024**2, pids=32, nofile=256)
            args = ('run', '--pull', 'never', *caps(policy), *args[1:])
        return self.backend(*args, **kwargs)

    def save(self):
        self.report['phase'] = self.phase
        self.report['samples'] = self.samples
        text = json.dumps(self.report, indent=2)
        require(len(text) <= 131072, 'Manifest bound exceeded')
        target = self.evidence / 'result.json'
        temporary = self.evidence / 'result.tmp'
        temporary.write_text(text)
        temporary.chmod(0o600)
        temporary.replace(target)

    def sample(self):
        try:
            with (self.evidence / 'samples.jsonl').open('x') as stream:
                os.chmod(stream.name, 0o600)
                while not self.done.is_set():
                    headroom(self.args.stop_available_bytes, self.args.stop_psi)
                    with self.lock:
                        names = list(self.models)
                    for name in names:
                        current = self.backend('inspect', name, capture_output=True, check=False, timeout=3)
                        if current.returncode:
                            continue
                        info = json.loads(current.stdout)[0]
                        if not info['State']['Running']:
                            continue
                        check_effective(info, self.policy, self.args.image_id)
                        measured = self.backend('exec', name, '/measurement-probe', capture_output=True, check=False, timeout=3)
                        if measured.returncode:
                            # Container may have exited between inspect and exec.
                            now = self.backend('inspect', name, capture_output=True, timeout=3)
                            require(not json.loads(now.stdout)[0]['State']['Running'], 'Read-only probe failed in running model')
                            continue
                        values = probe_output(measured.stdout)
                        quota, period = map(int, values['cpu.max'].split())
                        require(quota / period == self.policy['cpus'] and
                                values['memory.max'] == self.policy['memory_bytes'] and
                                values['memory.swap.max'] == 0 and values['pids.max'] == self.policy['pids'] and
                                values['process.nofile'] == [self.policy['nofile']]*2, 'Kernel ceilings differ from explicit policy')
                        line = json.dumps({'monotonic': time.monotonic(), 'phase': self.phase,
                                           'container': name, 'counters': values}) + '\n'
                        self.bytes += len(line)
                        require(self.bytes <= self.args.evidence_bytes, 'Private evidence byte bound reached')
                        stream.write(line)
                        stream.flush()
                        self.samples += 1
                    require(time.monotonic() < self.deadline, 'Overall measurement deadline exceeded')
                    self.done.wait(self.args.interval)
        except Exception:
            self.failure = 'Sampler, host headroom or evidence bound failed'
            self.abort.set()
            # Stop only explicitly owned model names; preserve mounts/receipts.
            with self.lock:
                names = list(self.models)
            for name in names:
                with contextlib.suppress(Exception):
                    self.backend('stop', '--time', '10', name, capture_output=True, timeout=15, check=False)

    def stage(self, name, operation):
        require(not self.abort.is_set(), 'Sampler refused subsequent admission')
        self.phase = name
        self.save()
        started, samples = time.monotonic(), self.samples
        value = operation()
        require(not self.abort.is_set(), 'Sampler failed during phase')
        self.report['phases'].append({'phase': name, 'seconds': time.monotonic()-started,
                                      'samples': self.samples-samples})
        self.save()
        return value


def execute(args):
    require(args.source_revision == subprocess.check_output(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip(), 'Checkout revision differs')
    require(re.fullmatch('[0-9a-f]{40}', args.fixed_merge) is not None, 'Explicit security-fixed merge required')
    subprocess.run(['git', '-C', str(ROOT), 'merge-base', '--is-ancestor', args.fixed_merge, args.source_revision], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    require(re.fullmatch('sha256:[0-9a-f]{64}', args.image_id) is not None, 'Immutable image ID required')
    private(args.evidence.parent)
    require(args.evidence.is_absolute() and not args.evidence.exists(), 'Evidence directory must be new')
    require(args.assets.is_absolute() and args.assets.resolve() == args.assets, 'Assets must be absolute without symlinks')
    require(not args.evidence.is_relative_to(args.assets) and not args.assets.is_relative_to(args.evidence), 'Assets and evidence must be separate')
    policy = resources.policy(args.policy, fixture=True)
    require(args.interval >= .5 and args.interval <= 30 and 1 <= args.duration <= 1800 and
            65536 <= args.evidence_bytes <= 64*1024**2, 'Sampling bounds invalid')
    require(all(math.isfinite(v) and v > 0 for v in (args.start_psi, args.stop_psi)) and
            args.start_available_bytes >= args.stop_available_bytes > 0 and args.start_psi <= args.stop_psi,
            'Explicit consistent host gates required')
    backend = runtime.docker
    current = json.loads(backend('image', 'inspect', policy['image'], capture_output=True).stdout)[0]
    require(current['Id'] == args.image_id and current['Config']['Labels'].get('org.opencontainers.image.revision') == args.source_revision, 'Image identity/source revision mismatch')
    # Existing helper image must already be present; do not let Docker pull on run.
    backend('image', 'inspect', runtime.PYTHON_IMAGE, capture_output=True)
    headroom(args.start_available_bytes, args.start_psi)
    args.evidence.mkdir(mode=0o700)
    runner = Runner(args, policy, args.evidence, backend)
    runner.report.update(source_revision=args.source_revision, security_fixed_merge=args.fixed_merge,
                         image_id=args.image_id, policy=policy, interval=args.interval,
                         probe_sha256=hashlib.sha256(args.probe.read_bytes()).hexdigest(),
                         fixture_root=runtime.fixture.ROOT, fixture_page=runtime.fixture.PAGE,
                         blind_spots='Exec adds a transient process; process FD/RSS and disk scans are sampled, short phases may have no sample; cgroup memory.peak covers each model container lifetime.')
    old_docker = runtime.docker
    # Every fixture Docker invocation routes through the finite, capped boundary.
    runtime.docker = runner.call
    original_env = os.environ.copy()
    clean_env = {k: v for k, v in original_env.items() if not k.startswith('NK_') and k not in ('NOTION_TOKEN', 'OPENAI_API_KEY')}
    os.environ.clear()
    os.environ.update(clean_env)
    stack = None
    thread = threading.Thread(target=runner.sample, daemon=True)
    try:
        runner.save()
        runtime.assets(args.assets, False)
        tls = args.evidence / 'tls'
        tls.mkdir(mode=0o755)
        runtime.certificates(tls)
        # Fixture must read its owned test key; no external secret is present.
        stack = runtime.Stack(args.image_id, args.assets, tls / 'ca.pem')
        original_options = stack.options
        def options(name, model=None):
            with runner.lock:
                runner.models.add(name)
            result = original_options(name, model)
            tmp = result.index('--tmpfs') + 1
            result[tmp] = f"/tmp:rw,nosuid,nodev,noexec,size={policy['tmp_bytes']},mode=0700,uid=65532,gid=65532"
            return [*result, '--mount',
                    f'type=bind,src={args.probe},dst=/measurement-probe,readonly', '--env', 'NK_GRAPH=false',
                    '--env', 'NK_READ_ONLY=true']
        stack.options = options
        runner.call('network', 'create', '--internal', stack.network, stdout=subprocess.DEVNULL)
        stack.network_created = True
        stack.new_volumes('measured')
        runner.report['retained_volumes'] = list(stack.volumes)
        source = stack.prefix + '-notion'
        stack.containers.append(source)
        runner.call('run', '-d', '--pull', 'never', '--name', source, '--read-only', '--network', stack.network,
                    '--user', f'{os.getuid()}:{os.getgid()}', '--network-alias', 'api.notion.com',
                    '--cap-drop=ALL', '--security-opt=no-new-privileges', '--sysctl', 'net.ipv4.ip_unprivileged_port_start=0',
                    '--mount', f'type=bind,src={tls},dst=/fixture,readonly', '--mount',
                    f'type=bind,src={runtime.FIXTURE_SCRIPT},dst=/fixture.py,readonly', runtime.PYTHON_IMAGE,
                    'python', '/fixture.py', 'serve', stdout=subprocess.DEVNULL)
        thread.start()
        checkpoint = runner.stage('worker_model_load_and_commit', lambda: stack.worker('initial'))
        runner.report['checkpoint'] = checkpoint
        runner.stage('index_ensure', lambda: stack.maintain('initial'))
        before = json.loads(stack.helper('snapshot').stdout)
        first = runner.stage('first_and_warm_query', lambda: stack.retrieve('initial'))
        require(json.loads(stack.helper('snapshot').stdout) == before, 'Read changed committed state')
        replacement = runner.stage('reader_replacement', lambda: stack.retrieve('replacement'))
        require(replacement == first, 'Replacement changed source identity')
        ledgers = json.loads(stack.helper('ledgers').stdout)
        runner.report['ledger_sha256'] = hashlib.sha256(json.dumps(ledgers, sort_keys=True).encode()).hexdigest()
        runner.report['ledger_row_counts'] = {k: len(v) for k, v in ledgers.items()}
        require(ledgers['webhook_inbox'] and ledgers['index_commit_receipts'], 'Missing real durable receipts')
        runner.stage('retained_receipt_rebuild', lambda: stack.maintain('retained', action='rebuild'))
        require(json.loads(stack.helper('ledgers').stdout) == ledgers, 'Rebuild changed operational receipts')
        require(runner.stage('receipt_recovery_query', lambda: stack.retrieve('retained')) == first, 'Recovery changed source identity')
        require(runner.samples > 0, 'No actual model samples captured')
        runner.report.update(outcome='measured_synthetic_fixture', chunk_id=first[0], receipt_recovery=True)
    finally:
        runner.done.set()
        if thread.ident is not None:
            thread.join(timeout=20)
        if runner.failure:
            runner.report['sampler_failure'] = runner.failure
            runner.report['outcome'] = 'failed'
        # Never delete volumes, including after failure. Only exact owned names.
        if stack:
            for name in reversed(stack.containers):
                with contextlib.suppress(Exception):
                    backend('rm', '-f', name, capture_output=True, timeout=20)
            if stack.network_created:
                with contextlib.suppress(Exception):
                    backend('network', 'rm', stack.network, capture_output=True, timeout=20)
        runner.save()
        runtime.docker = old_docker
        os.environ.clear()
        os.environ.update(original_env)
    require(runner.report['outcome'] != 'failed', 'Measurement failed; retain private evidence and volumes')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('policy', 'assets', 'evidence', 'probe'):
        parser.add_argument('--'+key, required=True, type=Path)
    for key in ('source-revision', 'fixed-merge', 'image-id'):
        parser.add_argument('--'+key, required=True)
    parser.add_argument('--interval', type=float, required=True)
    parser.add_argument('--duration', type=int, required=True)
    parser.add_argument('--evidence-bytes', type=int, required=True)
    for key in ('start-available-bytes', 'stop-available-bytes'):
        parser.add_argument('--'+key, type=int, required=True)
    for key in ('start-psi', 'stop-psi'):
        parser.add_argument('--'+key, type=float, required=True)
    args = parser.parse_args()
    try:
        require(args.probe.is_absolute() and args.probe.is_file() and args.probe.resolve() == args.probe, 'Precompiled read-only probe required')
        execute(args)
    except Exception:
        parser.exit(1, 'Synthetic measurement failed; retain private artifacts/volumes. No automatic retry.\n')
    print('Synthetic measurements retained; production acceptance remains #455/#416.')


if __name__ == '__main__':
    main()
