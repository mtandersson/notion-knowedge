#!/usr/bin/env python3
"""Credential-free contracts for bounded admission, evidence and kernel checks."""
import importlib.util
import json
import hashlib
import os
import sys
import time
import platform
import shutil
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
import uuid
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


    def test_real_stack_reader_launch_receives_caps_and_synchronous_admission(self):
        stack=m.runtime.Stack('fixture:local',self.path,self.path/'ca.pem')
        original_options=stack.options
        def options(name,model=None):
            self.runner.models.add(name)
            return original_options(name,model)
        stack.options=options
        stack.launch_reader=self.runner.launch_reader
        original_popen=subprocess.Popen
        launches=[]
        program = """import json,sys
for line in sys.stdin:
 r=json.loads(line)
 if 'id' not in r:continue
 p=r.get('params',{})
 if r['method']=='initialize':v={}
 elif p['name']=='knowledge_get':v={'text':'cinnamon'}
 else:
  mode=p['arguments']['mode']
  paths=['semantic','lexical'] if mode=='hybrid' else [mode]
  v={'structuredContent':{'results':[{'source':{'page_id':'11111111-1111-1111-1111-111111111111','chunk_id':'stable-fixture'},'score':0.8,'text':'cinnamon','matched_paths':paths}]}}
 print(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':v}),flush=True)
"""
        def popen(command,**kwargs):
            launches.append(command)
            return original_popen([sys.executable,'-u','-c',program],**kwargs)
        with patch.object(m.subprocess,'Popen',side_effect=popen),patch.object(m,'headroom',return_value={}):
            self.assertEqual(stack.retrieve('actual-method'),('stable-fixture','cinnamon'))
        self.runner.done.set()
        self.assertEqual(len(launches),1)
        command=launches[0]
        self.assertEqual(command[:2],['docker','run'])
        for flag,value in [('--pull','never'),('--cpus','2'),('--memory','6442450944'),('--memory-swap','6442450944'),('--pids-limit','256'),('--ulimit','nofile=1024:1024'),('--log-driver','local')]:
            self.assertEqual(command[command.index(flag)+1],value)
        self.runner.abort.set()
        with patch.object(m.subprocess,'Popen') as blocked:
            with self.assertRaises(ValueError):stack.retrieve('refused')
            blocked.assert_not_called()
        self.runner.abort.clear()
        with patch.object(m,'headroom',side_effect=ValueError('pressure')),patch.object(m.subprocess,'Popen') as blocked:
            with self.assertRaises(ValueError):stack.retrieve('pressure')
            blocked.assert_not_called()

    def test_dirty_trusted_checkout_is_rejected_before_docker(self):
        subprocess.run(['git','init','-q',str(self.path)],check=True)
        m.clean_checkout(self.path)
        source=self.path/'trusted.py';source.write_text('original')
        subprocess.run(['git','-C',str(self.path),'add','trusted.py'],check=True)
        subprocess.run(['git','-C',str(self.path),'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','fixture'],check=True)
        m.clean_checkout(self.path)
        for target in (source,self.path/'untracked.py'):
            target.write_text('dirty input')
            with patch.object(m,'ROOT',self.path),patch.object(m.runtime,'docker') as docker:
                with self.assertRaisesRegex(ValueError,'Trusted checkout'):m.execute(SimpleNamespace())
                docker.assert_not_called()
            if target==source:target.write_text('original')

    def test_probe_attestation_requires_protected_bytes_and_immutable_snapshot(self):
        probe=self.path/'operator-probe';probe.write_bytes(b'trusted fixture executable');probe.chmod(0o700)
        expected=hashlib.sha256(probe.read_bytes()).hexdigest()
        source=hashlib.sha256((m.ROOT/'deploy/production/measurement-probe.c').read_bytes()).hexdigest()
        evidence=self.path/'evidence';evidence.mkdir(mode=0o700)
        with self.assertRaises(ValueError):m.snapshot_probe(probe,evidence,'0'*64,source)
        probe.chmod(0o720)
        with self.assertRaises(ValueError):m.snapshot_probe(probe,evidence,expected,source)
        probe.chmod(0o700)
        snapshot=m.snapshot_probe(probe,evidence,expected,source)
        self.assertEqual(snapshot.stat().st_mode&0o777,0o555)
        self.runner.args.probe=snapshot;self.runner.args.probe_sha256=expected
        self.runner.verify_probe()
        probe.write_bytes(b'changed original')
        self.runner.verify_probe()
        evidence.chmod(0o755)
        with self.assertRaisesRegex(ValueError,'private'):self.runner.verify_probe()
        evidence.chmod(0o700)
        snapshot.chmod(0o755)
        with self.assertRaisesRegex(ValueError,'protection'):self.runner.verify_probe()
        snapshot.chmod(0o700);snapshot.write_bytes(b'changed snapshot')
        snapshot.chmod(0o555)
        with self.assertRaises(ValueError):self.runner.verify_probe()

    @unittest.skipUnless(os.environ.get('NK_MEASUREMENT_PROBE_DOCKER_TEST') == '1' and
                         platform.system() == 'Linux' and platform.machine() == 'x86_64' and shutil.which('cc'),
                         'requires explicit opt-in, preprovisioned helper image and Linux compiler')
    def test_nonroot_container_executes_the_protected_snapshot(self):
        binary=self.path/'compiled-probe'
        subprocess.run(['cc','-nostdlib','-static','-ffreestanding','-fno-builtin','-fno-stack-protector','-O2',
                        str(m.ROOT/'deploy/production/measurement-probe.c'),'-o',str(binary)],
                       check=True,capture_output=True,timeout=30)
        evidence=self.path/'evidence';evidence.mkdir(mode=0o700)
        expected=hashlib.sha256(binary.read_bytes()).hexdigest()
        source=hashlib.sha256((m.ROOT/'deploy/production/measurement-probe.c').read_bytes()).hexdigest()
        snapshot=m.snapshot_probe(binary,evidence,expected,source)
        small=dict(POLICY,cpus=.25,memory_bytes=128*1024**2,pids=32,nofile=256,tmp_bytes=16*1024**2)
        args=SimpleNamespace(duration=60,evidence_bytes=65536,interval=.5,
                             stop_available_bytes=256*1024**2,stop_psi=2,
                             probe=snapshot,probe_sha256=expected)
        runner=m.Runner(args,small,evidence,m.runtime.docker)
        name='nk-probe-permissions-'+uuid.uuid4().hex
        runner.models.add(name)
        self.addCleanup(lambda:m.runtime.docker('rm','--force',name,check=False,capture_output=True,timeout=10))
        mounts=['--mount',f'type=bind,src={snapshot},dst=/measurement-probe,readonly']
        for part in ('index','state'):
            directory=self.path/part;directory.mkdir(mode=0o755)
            mounts.extend(['--mount',f'type=bind,src={directory},dst=/var/lib/notion-knowledge/{part},readonly'])
        result=runner.call('run','--rm','--name',name,'--read-only','--network','none',
                          '--user','65532:65532','--cap-drop=ALL','--security-opt=no-new-privileges',
                          '--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=16777216,uid=65532,gid=65532',
                          *mounts,'--entrypoint','/measurement-probe',m.runtime.PYTHON_IMAGE,capture_output=True)
        self.assertRegex(result.stdout,r'\nUid:\s+65532\s+65532\s+65532\s+65532')
        counters=m.probe_output(result.stdout)
        self.assertEqual(counters['process.nofile'],[256,256])
        self.assertEqual(counters['memory.max'],128*1024**2)
        self.assertEqual(counters['pids.max'],32)
        self.assertEqual(evidence.stat().st_mode&0o777,0o700)
        runner.verify_probe()


    def test_reader_deadline_stops_owned_container_and_reaps_cli(self):
        original_popen=subprocess.Popen
        self.runner.models.add('owned-reader')
        self.runner.deadline=time.monotonic()+.03
        with patch.object(m.subprocess,'Popen',side_effect=lambda *a,**kw: original_popen([sys.executable,'-c','import time;time.sleep(60)'],**kw)),patch.object(m,'headroom',return_value={}):
            process=self.runner.launch_reader(['docker','run','--name','owned-reader','fixture:local'])
            self.assertNotEqual(process.wait(timeout=3),0)
        self.assertTrue(self.runner.abort.is_set())
        self.assertEqual(self.calls[0][0],('stop','--time','10','owned-reader'))


if __name__ == '__main__': unittest.main()
