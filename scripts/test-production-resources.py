#!/usr/bin/env python3
"""Resource policy contracts, without a daemon or production credentials."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('resources', Path(__file__).with_name('production-resources.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PolicyTests(unittest.TestCase):
    def test_unmeasured_and_infinite_limits_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'policy.json'
            data = json.loads(Path('deploy/production/policy.example.json').read_text())
            path.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                module.policy(path)
            data = {key: 100 for key in module.FIELDS}
            data['image'] = 'fixture:local'
            data['cpus'] = float('nan')
            path.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                module.policy(path, True)

    def test_explicit_bounds_render_without_disclosing_service_secrets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            environment = root / 'service.env'
            environment.write_text('PRIVATE=do-not-copy\n')
            data = {key: 100 for key in module.FIELDS}
            data['image'] = 'fixture:local'
            data['cpus'] = 1
            module.render(data, environment, root / 'output')
            compose = json.loads((root / 'output/compose.json').read_text())
            service = compose['services']['knowledge']
            self.assertEqual(service['mem_limit'], service['memswap_limit'])
            self.assertEqual(service['pids_limit'], 100)
            self.assertEqual(service['ports'], ['127.0.0.1:3000:3000'])
            self.assertNotIn('do-not-copy', json.dumps(compose))
            proxy = (root / 'output/nginx.conf').read_text()
            self.assertIn('worker_connections 100', proxy)
            self.assertIn('limit_req_status 429', proxy)
            self.assertIn('client_header_timeout 100s', proxy)
            self.assertIn('proxy_buffering off', proxy)
            self.assertIn('if ($host != knowledge.example.com)', proxy)
            with self.assertRaises(FileExistsError):
                module.render(data, environment, root / 'output')


if __name__ == '__main__':
    unittest.main()
