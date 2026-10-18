#!/usr/bin/env python3
"""Resource policy contracts, without a daemon or production credentials."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

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


    def test_effective_inspection_rejects_missing_limits_and_public_backend(self):
        data = {key: 100 for key in module.FIELDS}
        data.update(image='fixture:local', cpus=1)
        current = {'Image': 'sha256:fixture', 'Config': {'Image': 'fixture:local', 'User': '65532:65532'},
                   'State': {'Running': True}, 'HostConfig': {
                       'NanoCpus': 1000000000, 'Memory': 100, 'MemorySwap': 100,
                       'PidsLimit': 100, 'ReadonlyRootfs': True,
                       'PortBindings': {'3000/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '3000'}]},
                       'CapDrop': ['ALL'], 'SecurityOpt': ['no-new-privileges:true'],
                       'LogConfig': {'Type': 'local', 'Config': {'max-size': '100', 'max-file': '100'}},
                       'Ulimits': [{'Name': 'nofile', 'Soft': 100, 'Hard': 100}],
                       'Tmpfs': {'/tmp': 'rw,noexec,nosuid,nodev,size=100,mode=1777'}}}
        def check(record):
            with patch.object(module.subprocess, 'run', return_value=SimpleNamespace(stdout=json.dumps([record]))):
                return module.inspect(data, 'fixture')
        self.assertEqual(check(current)['limits']['Memory'], 100)
        for field, value in [('Memory', 0), ('PidsLimit', 0), ('LogConfig', {}),
                             ('PortBindings', {'3000/tcp': [{'HostIp': '0.0.0.0', 'HostPort': '3000'}]}),
                             ('NetworkMode', 'host'), ('Privileged', True), ('CapDrop', []), ('CapAdd', ['SYS_ADMIN'])]:
            changed = json.loads(json.dumps(current))
            changed['HostConfig'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                check(changed)
        current['Config']['User'] = '0:0'
        with self.assertRaises(ValueError):
            check(current)


if __name__ == '__main__':
    unittest.main()
