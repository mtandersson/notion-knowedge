#!/usr/bin/env python3
"""Shared probe retains coverage, per-command failure authority and isolation."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import types
import unittest

import yaml

spec = importlib.util.spec_from_file_location('sharing', Path(__file__).with_name('benchmark-dev-sharing.py'))
sharing = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sharing)


class SharingProbe(unittest.TestCase):
    def generate(self, design, output):
        sharing.prepare(types.SimpleNamespace(design=design, production='ca26c51',
            namespace='test-sharing', base_branch='main', output=output))
        return yaml.safe_load((Path(output) / '.github/workflows/ci.yml').read_text())

    def test_same_canonical_commands_tools_security_and_cache_boundaries(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            parallel, shared = self.generate('parallel', a), self.generate('shared', b)
            pj, sj = parallel['jobs'], shared['jobs']
            self.assertEqual(parallel['on'], shared['on'])
            for job in ['dependencies','secrets','fmt','container','build','changes']:
                self.assertEqual(pj[job], sj[job])
            self.assertTrue(set(sharing.benchmark.CANONICAL) <= {j['name'] for j in sj.values()})
            for job in ['check','clippy','test']:
                original = next(s['run'] for s in pj[job]['steps'] if 'fingerprint' in s.get('run',''))
                command = next(s for s in sj['dev-shared']['steps'] if s.get('id') == job)
                self.assertEqual(original, command['run'])
                self.assertTrue(command['continue-on-error'])
            restore = next(s for s in sj['dev-shared']['steps'] if s.get('id') == 'cargo-cache')
            cache_spec = importlib.util.spec_from_file_location('cache', 'scripts/cargo-cache.py')
            cache = importlib.util.module_from_spec(cache_spec); cache_spec.loader.exec_module(cache)
            keys = cache.keys(Path('.'), restore['with']['job'], restore['with']['profile'], 'Linux-X64', 'pinned-toolchain', {})
            self.assertIn('-check-debug-', keys['key'])
            self.assertEqual((Path(a)/'.github/actions/cargo-cache-restore/action.yml').read_bytes(),
                             (Path(b)/'.github/actions/cargo-cache-restore/action.yml').read_bytes())
            action = (Path(b)/'.github/actions/cargo-cache-restore/action.yml').read_text()
            self.assertNotIn('\n      cargo-${{ runner.os }}-', action)
            self.assertIn('test-sharing', shared['env']['BENCH_CACHE_NAMESPACE'])

    def test_each_report_rejects_failure_cancel_skip_missing_and_gate_fails(self):
        with tempfile.TemporaryDirectory() as out:
            jobs = self.generate('shared', out)['jobs']
            for job in ['check','clippy','test']:
                report = jobs[job]['steps'][0]
                self.assertIn('needs.dev-shared.outputs.' + job, report['env']['OUTCOME'])
                self.assertEqual(jobs[job]['if'], "always() && needs.changes.outputs." + job + " == 'true'")
                for outcome in ['success','failure','cancelled','skipped','']:
                    result = subprocess.run(['bash','-c',report['run']],env={'OUTCOME':outcome,'SHARED_RESULT':'success'},capture_output=True)
                    self.assertEqual(result.returncode == 0, outcome == 'success')
                    needs = {'changes':{'result':'success','outputs':{k:'true' for k in ['agent-layout','fmt','clippy','check','test','build','container','dependencies','secrets']}}}
                    needs.update({k:{'result':'success'} for k in needs['changes']['outputs']})
                    needs[job]['result'] = 'success' if result.returncode == 0 else 'failure'
                    gate = subprocess.run(['python3','scripts/ci-jobs.py','gate'],env={'CI_NEEDS':json.dumps(needs),'PATH':__import__('os').environ['PATH']},capture_output=True)
                    self.assertEqual(gate.returncode == 0, outcome == 'success')
            for shared_result in ['failure','cancelled','skipped','']:
                    result = subprocess.run(['bash','-c',report['run']],env={'OUTCOME':'success','SHARED_RESULT':shared_result},capture_output=True)
                    self.assertNotEqual(result.returncode, 0)
            save = next(s for s in jobs['dev-shared']['steps'] if s.get('uses','').endswith('/cargo-cache-save'))
            for job in ['check','clippy','test']:
                self.assertIn('steps.' + job + ".outcome != 'failure'", save['if'])

if __name__ == '__main__':
    unittest.main()
