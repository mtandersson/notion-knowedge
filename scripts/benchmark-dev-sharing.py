#!/usr/bin/env python3
"""Generate isolated parallel/shared-development probes; never edit production CI."""
import argparse
import copy
import importlib.util
from pathlib import Path
import types

spec = importlib.util.spec_from_file_location('benchmark', Path(__file__).with_name('benchmark-ci.py'))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def prepare(args):
    import yaml
    benchmark.prepare(types.SimpleNamespace(design='optimized', production=args.production,
        namespace=args.namespace, base_branch=args.base_branch, output=args.output))
    path = Path(args.output) / '.github/workflows/ci.yml'
    workflow = yaml.safe_load(path.read_text())
    if args.design == 'shared':
        jobs = workflow['jobs']
        shared = copy.deepcopy(jobs['check'])
        shared['name'] = 'Shared development probe'
        shared['timeout-minutes'] = 30
        shared['if'] = "needs.changes.outputs.check == 'true' || needs.changes.outputs.clippy == 'true' || needs.changes.outputs.test == 'true'"
        shared['outputs'] = {job: '${{ steps.' + job + '.outcome }}' for job in ['check','clippy','test']}
        restore = next(s for s in shared['steps'] if s.get('id') == 'cargo-cache')
        # Keep the cache policy's supported check/debug identity; the probe's
        # isolated namespace and PR ref prevent any parallel/production reuse.
        restore['with']['job'] = 'check'
        steps = shared['steps']
        command_index = next(i for i,s in enumerate(steps) if s.get('name') == 'Run cargo check')
        commands = []
        for job in ['check','clippy','test']:
            source = next(s for s in jobs[job]['steps'] if 'cargo ' in s.get('run','') and 'fingerprint' in s['run'])
            command = copy.deepcopy(source)
            command.update(id=job, **{'continue-on-error': True, 'if': "needs.changes.outputs." + job + " == 'true'"})
            commands.append(command)
        steps[command_index:command_index+1] = commands
        save = next(s for s in steps if s.get('uses','').endswith('/cargo-cache-save'))
        save['if'] = "success() && steps.check.outcome != 'failure' && steps.clippy.outcome != 'failure' && steps.test.outcome != 'failure' && (github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository)"
        jobs['dev-shared'] = shared
        for job in ['check','clippy','test']:
            name = jobs[job]['name']
            jobs[job] = {'name': name, 'needs': ['changes','dev-shared'],
                'if': "always() && needs.changes.outputs." + job + " == 'true'",
                'runs-on': 'ubuntu-latest', 'timeout-minutes': 5,
                'steps': [{'name': 'Require canonical command success',
                    'env': {'OUTCOME': '${{ needs.dev-shared.outputs.' + job + ' }}',
                            'SHARED_RESULT': '${{ needs.dev-shared.result }}'},
                    'run': 'test "$OUTCOME" = success && test "$SHARED_RESULT" = success'}]}
        jobs['gate']['needs'].append('dev-shared')
    path.write_text(yaml.safe_dump(workflow, sort_keys=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--design', choices=['parallel','shared'], required=True)
    parser.add_argument('--production', required=True)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--base-branch', required=True)
    parser.add_argument('--output', required=True)
    prepare(parser.parse_args())

if __name__ == '__main__':
    main()
