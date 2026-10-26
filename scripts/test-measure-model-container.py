#!/usr/bin/env python3
"""Credential-free contracts for bounded admission, evidence and kernel checks."""
import importlib.util
import json
import platform
import shutil
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('measurement', Path(__file__).with_name('measure-model-container.py'))
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)
POLICY = dict(image='fixture:local', cpus=2, memory_bytes=6442450944, pids=256,
              nofile=1024, tmp_bytes=268435456, log_bytes=8388608, log_files=2)


def sample():
    values = {'cpu.max': '200000 100000', 'cpu.stat': 'usage_usec 12\nnr_throttled 1',
              'memory.max': str(POLICY['memory_bytes']), 'memory.current': '40', 'memory.peak': '80',
              'memory.events': 'oom 0\noom_kill 0', 'memory.swap.max': '0', 'pids.max': '256',
              'pids.current': '4', 'process.status': 'Name:\tprivate-source-must-not-retain\nVmRSS:\t32 kB\nVmHWM:\t64 kB\nThreads:\t2',
              'process.limits': 'Max open files            1024                 1024                 files',
              'process.fd_count': '3', 'tmp.allocated_bytes': '0', 'index.allocated_bytes': '8192',
              'state.allocated_bytes': '4096'}
    return ''.join(k+'\n'+v+'\nEND\n' for k,v in values.items())


class MeasurementContracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.calls = []
        def backend(*args, **kwargs):
            self.calls.append((args, kwargs))
            return subprocess.CompletedProcess(args, 0, '', '')
        self.args = SimpleNamespace(duration=60, evidence_bytes=65536, interval=.5,
                                    stop_available_bytes=1, stop_psi=1, image_id='sha256:'+'a'*64)
        self.runner = m.Runner(self.args, POLICY, self.path, backend)

    def test_every_role_has_finite_limits_and_cannot_pull_or_publish(self):
        self.runner.models.add('owned-model')
        with patch.object(m, 'headroom', return_value={}):
            self.runner.call('run', '--name', 'owned-model', 'fixture:local')
            self.runner.call('run', '--rm', 'fixture-helper:local')
        for (args, kwargs), memory, cpus in zip(self.calls, [6442450944,134217728], ['2','0.25']):
            self.assertEqual(args[args.index('--memory')+1], str(memory))
            self.assertEqual(args[args.index('--memory-swap')+1], str(memory))
            self.assertEqual(args[args.index('--cpus')+1], cpus)
            self.assertEqual(args[args.index('--pull')+1], 'never')
            self.assertIn('--pids-limit', args)
            self.assertIn('--ulimit', args)
            self.assertIn('--log-opt', args)
            self.assertGreater(kwargs['timeout'], 0)

    def test_build_pull_pressure_abort_and_expired_budget_have_no_effect(self):
        for action in ('build', 'pull', 'buildx'):
            with self.assertRaises(ValueError): self.runner.call(action, 'image')
        with patch.object(m, 'headroom', side_effect=ValueError('pressure')):
            with self.assertRaises(ValueError): self.runner.call('run', 'image')
        self.runner.abort.set()
        with self.assertRaises(ValueError): self.runner.call('run', 'image')
        self.runner.abort.clear()
        self.runner.deadline = 0
        with self.assertRaises(ValueError): self.runner.call('inspect', 'image')
        self.assertEqual(self.calls, [])

    def test_probe_retains_counters_without_process_payload(self):
        result = m.probe_output(sample())
        self.assertEqual(result['memory.peak'], 80)
        self.assertEqual(result['process.nofile'], [1024,1024])
        self.assertEqual(result['cpu.stat']['nr_throttled'], 1)
        self.assertNotIn('private-source', json.dumps(result))
        for invalid in (sample().replace('memory.peak\n80\nEND\n',''), 'x'*65537):
            with self.assertRaises(ValueError): m.probe_output(invalid)

    def test_host_gate_requires_available_memory_and_low_full_pressure(self):
        (self.path/'pressure').mkdir()
        (self.path/'meminfo').write_text('MemAvailable: 4096 kB\n')
        (self.path/'pressure/memory').write_text('some avg60=1\nfull avg10=0 avg60=0.2 avg300=0 total=0\n')
        self.assertEqual(m.headroom(4*1024**2,.5,self.path)['available_bytes'],4*1024**2)
        with self.assertRaises(ValueError): m.headroom(5*1024**2,.5,self.path)
        with self.assertRaises(ValueError): m.headroom(1,.1,self.path)

    def test_private_manifest_survives_failed_phase_without_payload(self):
        def fail(): raise RuntimeError('source text must not be retained')
        with self.assertRaises(RuntimeError): self.runner.stage('query', fail)
        self.runner.save()
        target = self.path/'result.json'
        self.assertEqual(target.stat().st_mode&0o777,0o600)
        result = json.loads(target.read_text())
        self.assertEqual((result['phase'], result['outcome']), ('query','failed'))
        self.assertNotIn('source text', target.read_text())

    def test_sampler_failure_stops_only_owned_model_names(self):
        self.runner.models.add('owned-model')
        with patch.object(m, 'headroom', side_effect=ValueError('pressure')):
            self.runner.sample()
        self.assertTrue(self.runner.abort.is_set())
        self.assertEqual(self.calls[0][0], ('stop','--time','10','owned-model'))
        self.assertFalse(any('volume' in c[0] for c in self.calls))

    def test_actual_container_configuration_must_match_all_ceilings(self):
        host = dict(NanoCpus=2000000000, Memory=6442450944, MemorySwap=6442450944,
                    PidsLimit=256, ReadonlyRootfs=True, NetworkMode='owned-network',
                    CapDrop=['ALL'], SecurityOpt=['no-new-privileges:true'],
                    Ulimits=[dict(Name='nofile', Soft=1024, Hard=1024)],
                    Tmpfs={'/tmp':'size=268435456'}, LogConfig=dict(Type='local', Config={'max-size':'8388608','max-file':'2'}))
        info = dict(Image=self.args.image_id, HostConfig=host, Config={'User':'65532:65532'})
        m.check_effective(info,POLICY,self.args.image_id)
        for field,value in [('Memory',1),('MemorySwap',1),('PidsLimit',0),('NanoCpus',0),('Privileged',True),('PortBindings',{'3000':[]})]:
            changed=json.loads(json.dumps(info));changed['HostConfig'][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):m.check_effective(changed,POLICY,self.args.image_id)


    def test_sampler_records_actual_protocol_and_bounded_private_stream(self):
        host = dict(NanoCpus=2000000000, Memory=6442450944, MemorySwap=6442450944,
                    PidsLimit=256, ReadonlyRootfs=True, NetworkMode='owned-network',
                    CapDrop=['ALL'], SecurityOpt=['no-new-privileges:true'],
                    Ulimits=[dict(Name='nofile', Soft=1024, Hard=1024)],
                    Tmpfs={'/tmp':'size=268435456'}, LogConfig=dict(Type='local', Config={'max-size':'8388608','max-file':'2'}))
        info = dict(Image=self.args.image_id, HostConfig=host, Config={'User':'65532:65532'}, State={'Running':True})
        def backend(*args, **kwargs):
            self.calls.append((args,kwargs))
            if args[0]=='inspect': return subprocess.CompletedProcess(args,0,json.dumps([info]),'')
            self.runner.done.set()
            return subprocess.CompletedProcess(args,0,sample(),'')
        self.runner.backend=backend
        self.runner.models.add('owned-model')
        with patch.object(m,'headroom',return_value={}): self.runner.sample()
        self.assertIsNone(self.runner.failure)
        self.assertEqual(self.runner.samples,1)
        target=self.path/'samples.jsonl'
        self.assertEqual(target.stat().st_mode&0o777,0o600)
        row=json.loads(target.read_text())
        self.assertEqual(row['counters']['memory.peak'],80)
        self.assertNotIn('private-source',target.read_text())
        self.assertEqual(self.calls[1][0],('exec','owned-model','/measurement-probe'))
        self.assertEqual(self.calls[1][1]['timeout'],3)

    def test_shipping_fixture_orchestration_rejects_changed_receipts_and_no_samples(self):
        class Stack:
            def __init__(self, changed=False): self.calls=[];self.changed=changed;self.rebuilt=False
            def worker(self,g): self.calls.append('worker');return {'content_hash':'fixture-checkpoint'}
            def maintain(self,g,action='ensure'):
                self.calls.append(action)
                if action=='rebuild': self.rebuilt=True
            def retrieve(self,g): self.calls.append('retrieve');return ('fixture-chunk','private source text')
            def helper(self,action):
                if action=='snapshot': value={'fixture':'snapshot'}
                else: value={'webhook_inbox':[[1]],'index_commit_receipts':[[2 if self.changed and self.rebuilt else 1]]}
                return SimpleNamespace(stdout=json.dumps(value))
        self.runner.samples=1
        stack=Stack();m.workload(self.runner,stack)
        self.assertEqual(stack.calls,['worker','ensure','retrieve','retrieve','rebuild','retrieve'])
        self.assertEqual(self.runner.report['outcome'],'measured_synthetic_fixture')
        self.assertNotIn('private source',json.dumps(self.runner.report))
        self.assertTrue(self.runner.report['receipt_recovery'])
        with self.assertRaisesRegex(ValueError,'operational receipts'):m.workload(self.runner,Stack(changed=True))
        self.runner.samples=0
        with self.assertRaisesRegex(ValueError,'No actual model samples'):m.workload(self.runner,Stack())


    @unittest.skipUnless(platform.system()=='Linux' and platform.machine()=='x86_64' and shutil.which('cc'), 'Linux x86_64 compiler required')
    def test_static_probe_uses_read_only_bounded_filesystem_contract(self):
        root=self.path/'fixture'
        values={}
        for block in sample().split('\nEND\n'):
            if block:
                key,value=block.split('\n',1);values[key]=value
        for key in ['cpu.max','cpu.stat','memory.max','memory.current','memory.peak','memory.events','memory.swap.max','pids.max','pids.current']:
            target=root/'sys/fs/cgroup'/key;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(values[key])
        for name,key in [('status','process.status'),('limits','process.limits')]:
            target=root/'proc/1'/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(values[key])
        fd=root/'proc/1/fd';fd.mkdir();(fd/'0').touch();(fd/'1').touch()
        for name in ['tmp','var/lib/notion-knowledge/index','var/lib/notion-knowledge/state']:
            target=root/name;target.mkdir(parents=True);(target/'fixture-content').write_text('private payload not counters')
        (root/'tmp/outside-link').symlink_to(self.path,target_is_directory=True)
        binary=self.path/'probe'
        subprocess.run(['cc','-nostdlib','-static','-ffreestanding','-fno-builtin','-fno-stack-protector','-O2',
                        '-DPROBE_ROOT="'+str(root)+'"',str(m.ROOT/'deploy/production/measurement-probe.c'),'-o',str(binary)],check=True,capture_output=True,timeout=30)
        result=subprocess.run([str(binary)],capture_output=True,text=True,timeout=3,check=True)
        self.assertNotIn('private payload',result.stdout)
        decoded=m.probe_output(result.stdout)
        self.assertEqual(decoded['process.fd_count'],2)
        self.assertGreaterEqual(decoded['index.allocated_bytes'],0)
        nested=root/'tmp'
        for i in range(34): nested=nested/'d';nested.mkdir()
        failed=subprocess.run([str(binary)],capture_output=True,text=True,timeout=3)
        self.assertEqual(failed.returncode,1)
        self.assertLess(len(failed.stdout),65536)


if __name__ == '__main__': unittest.main()
