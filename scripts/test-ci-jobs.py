#!/usr/bin/env python3
"""Exercise the production CI command with committed inputs and Actions results."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('ci-jobs.py').resolve()
ALL = {'agent-layout', 'fmt', 'clippy', 'check', 'test', 'build', 'container',
       'dependencies', 'secrets'}
SECURITY = {'dependencies', 'secrets'}
WORKSPACE = {'fmt', 'clippy', 'check', 'test', 'build', 'container'}


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()


class Selection(unittest.TestCase):
    def selection(self, paths, event='pull_request', rename=None, delete=None):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            git(repo, 'init', '-q')
            git(repo, 'config', 'user.name', 'CI contract test')
            git(repo, 'config', 'user.email', 'ci-test@example.invalid')
            initial = set(paths) | ({rename[0]} if rename else set())
            for path in initial:
                target = repo / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text('initial content\n')
            git(repo, 'add', '.')
            git(repo, 'commit', '--allow-empty', '-qm', 'Base')
            base = git(repo, 'rev-parse', 'HEAD')
            for path in paths:
                (repo / path).write_text('changed content\n')
            if rename:
                target = repo / rename[1]
                target.parent.mkdir(parents=True, exist_ok=True)
                (repo / rename[0]).rename(target)
            if delete:
                (repo / delete).unlink()
            git(repo, 'add', '-A')
            git(repo, 'commit', '--allow-empty', '-qm', 'Changed inputs')
            result = subprocess.run([
                'python3', str(SCRIPT), 'select', '--event', event,
                '--base', base, '--head', 'HEAD',
            ], cwd=repo, text=True, capture_output=True, check=True)
            outputs = json.loads(result.stdout)
            self.assertEqual(set(outputs), ALL)
            return {job for job, value in outputs.items() if value == 'true'}

    def test_documentation_keeps_fresh_security_without_builds(self):
        self.assertEqual(self.selection(['docs/diagnostics.md', 'README.md']), SECURITY)

    def test_input_classes_select_their_transitive_consumers(self):
        cases = {
            'AGENTS.md': {'agent-layout'},
            '.agents/skills/writing-tests/SKILL.md': {'agent-layout'},
            'scripts/check-agent-layout.sh': {'agent-layout'},
            'crates/core/src/lib.rs': WORKSPACE,
            'crates/server/tests/http.rs': WORKSPACE,
            'eval/retrieval/personal-knowledge-v1.json': {'test'},
            'Cargo.lock': WORKSPACE,
            'Cargo.toml': WORKSPACE,
            'crates/core/Cargo.toml': WORKSPACE,
            'crates/server/build.rs': WORKSPACE,
            'Dockerfile': {'container'},
            '.dockerignore': {'container'},
            'scripts/smoke-container.py': {'container'},
            '.github/workflows/ci.yml': ALL,
            'flake.nix': ALL,
            'flake.lock': ALL,
            'rust-toolchain.toml': ALL,
            'new-input.txt': ALL,
            'docs/input.json': ALL,
            'scripts/ci-jobs.py': ALL,
        }
        for path, jobs in cases.items():
            with self.subTest(path=path):
                self.assertEqual(self.selection([path]), SECURITY | jobs)

    def test_deleted_and_renamed_inputs_keep_both_sides_covered(self):
        self.assertEqual(self.selection(['Cargo.lock'], delete='Cargo.lock'), SECURITY | WORKSPACE)
        self.assertEqual(self.selection([], rename=('crates/core/src/lib.rs', 'docs/moved.md')),
                         SECURITY | WORKSPACE)
        self.assertEqual(self.selection([], rename=('README.md', 'new-input.txt')), ALL)

    def test_mixed_inputs_and_unusual_filenames_are_conservative(self):
        self.assertEqual(self.selection(['README.md', 'AGENTS.md', 'Dockerfile']),
                         SECURITY | {'agent-layout', 'container'})
        self.assertEqual(self.selection(['docs/read\tme\n.md']), SECURITY)
        self.assertEqual(self.selection(['README.md', 'unknown\npath']), ALL)

    def test_main_manual_and_empty_pr_diffs_run_every_check(self):
        for event in ('push', 'workflow_dispatch', 'pull_request'):
            self.assertEqual(self.selection(['README.md'] if event != 'pull_request' else [], event), ALL)

    def test_missing_pr_revision_cannot_succeed(self):
        result = subprocess.run(['python3', str(SCRIPT), 'select', '--event', 'pull_request'],
                                capture_output=True)
        self.assertNotEqual(result.returncode, 0)


class Gate(unittest.TestCase):
    def needs(self, selected):
        needs = {'changes': {'result': 'success', 'outputs': {
            job: 'true' if job in selected else 'false' for job in ALL}}}
        needs.update({job: {'result': 'success' if job in selected else 'skipped'} for job in ALL})
        return needs

    def gate(self, needs):
        return subprocess.run(['python3', str(SCRIPT), 'gate'], text=True,
                              capture_output=True,
                              env={**os.environ, 'CI_NEEDS': json.dumps(needs)})

    def test_full_success_and_intentional_skips_pass(self):
        for selected in (ALL, SECURITY, SECURITY | {'test'}, SECURITY | {'container'}):
            self.assertEqual(self.gate(self.needs(selected)).returncode, 0)

    def test_every_selected_failure_cancellation_skip_and_missing_result_blocks(self):
        for job in ALL:
            for result in ('failure', 'cancelled', 'skipped', None):
                needs = self.needs(ALL)
                needs[job]['result'] = result
                with self.subTest(job=job, result=result):
                    self.assertNotEqual(self.gate(needs).returncode, 0)

    def test_selection_failure_and_malformed_outputs_block(self):
        for result in ('failure', 'cancelled', 'skipped'):
            needs = self.needs(SECURITY)
            needs['changes']['result'] = result
            self.assertNotEqual(self.gate(needs).returncode, 0)
        for output in (None, '', 'TRUE', True):
            needs = self.needs(SECURITY)
            needs['changes']['outputs']['build'] = output
            self.assertNotEqual(self.gate(needs).returncode, 0)

    def test_security_cannot_be_deselected(self):
        needs = self.needs(set())
        self.assertNotEqual(self.gate(needs).returncode, 0)

    def test_unselected_failed_or_cancelled_jobs_still_block(self):
        for result in ('failure', 'cancelled', None):
            needs = self.needs(SECURITY)
            needs['build']['result'] = result
            self.assertNotEqual(self.gate(needs).returncode, 0)


if __name__ == '__main__':
    unittest.main()
