#!/usr/bin/env python3
"""Matched harnesses preserve coverage and isolate all cache restores."""
import importlib.util
from pathlib import Path
import tempfile
import json
import types
import unittest

import yaml

spec = importlib.util.spec_from_file_location('benchmark', Path(__file__).with_name('benchmark-ci.py'))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class MatchedHarness(unittest.TestCase):
    def test_variants_preserve_checks_and_isolate_cache_namespaces(self):
        for design in ['baseline', 'optimized']:
            with self.subTest(design=design), tempfile.TemporaryDirectory() as output:
                args = types.SimpleNamespace(design=design, production='850c341',
                    namespace='test-matched', base_branch='test-base', output=output)
                benchmark.prepare(args)
                workflow = yaml.safe_load((Path(output)/'.github/workflows/ci.yml').read_text())
                self.assertIsNone(workflow['on']['workflow_dispatch'])
                self.assertIs(workflow['concurrency']['cancel-in-progress'], True)
                jobs = workflow['jobs']
                self.assertTrue(set(benchmark.CANONICAL) <= {j['name'] for j in jobs.values()})
                for job in jobs.values():
                    self.assertEqual(job['runs-on'], 'ubuntu-latest')
                    self.assertIsInstance(job['timeout-minutes'], int)
                    for step in job.get('steps', []):
                        if step.get('uses', '').startswith('actions/cache@'):
                            self.assertTrue(step['with']['key'].startswith('test-matched-'))
                            self.assertTrue(all(k.startswith('test-matched-') for k in step['with']['restore-keys'].splitlines()))
                        if 'cache-from' in step.get('with', {}):
                            self.assertEqual(step['with']['cache-from'], 'type=gha,scope=${{ env.DOCKER_CACHE_SCOPE }}')
                if design == 'optimized':
                    action=yaml.safe_load((Path(output)/'.github/actions/cargo-cache-restore/action.yml').read_text())
                    command=next(s['run'] for s in action['runs']['steps'] if s.get('id') == 'keys')
                    self.assertIn('GITHUB_OUTPUT=/tmp/nk-bench-keys',command)
                    self.assertIn('>> "$GITHUB_OUTPUT"',command)
                    self.assertIn('${{ env.BENCH_CACHE_NAMESPACE }}-',command)

    def test_execution_summary_separates_queue_and_step_spans(self):
        with tempfile.TemporaryDirectory() as output:
            root=Path(output)
            run={'head_sha':'850c341','html_url':'run','event':'workflow_dispatch',
                 'conclusion':'success','run_attempt':1,'created_at':'2026-10-02T00:00:00Z'}
            step={'name':'Run unit tests','started_at':'2026-10-02T00:00:15Z',
                  'completed_at':'2026-10-02T00:00:20Z','conclusion':'success'}
            job={'id':1,'name':'Unit tests','html_url':'job','labels':['ubuntu-latest'],
                 'conclusion':'success','created_at':'2026-10-02T00:00:02Z',
                 'started_at':'2026-10-02T00:00:10Z','completed_at':'2026-10-02T00:00:22Z','steps':[step]}
            (root/'1-run.json').write_text(json.dumps(run))
            (root/'1-jobs.json').write_text(json.dumps({'jobs':[job]}))
            (root/'1.log').write_text('Unit tests\tRun unit tests\t2026-10-02T00:00:16Z Compiling example v1.0.0\n')
            benchmark.summarize(types.SimpleNamespace(input=output,output=str(root/'ledger.json'),runs=[1]))
            measured=json.loads((root/'ledger.json').read_text())[0]
            self.assertEqual(measured['workflow_wall_seconds'],22)
            self.assertEqual(measured['initial_queue_seconds'],10)
            self.assertEqual(measured['summed_step_execution_seconds'],5)
            self.assertEqual(measured['summed_job_envelope_seconds'],12)
            self.assertEqual(measured['jobs'][0]['queue_seconds'],8)
            self.assertEqual(measured['jobs'][0]['external_crate_events'],1)

    def test_current_production_differs_from_historical_inputs(self):
        self.assertNotEqual(benchmark.production_digest(benchmark.BASELINE)['sha256'],
                            benchmark.production_digest('850c341')['sha256'])


if __name__ == '__main__':
    unittest.main()
