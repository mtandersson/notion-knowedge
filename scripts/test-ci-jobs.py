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
            'compose.yaml': {'container'},
            'scripts/compose-dev.sh': {'container'},
            'scripts/smoke-compose.py': {'container'},
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
        needs['container-main'] = {'result': 'skipped'}
        needs['runtime-choice-test'] = {'result': 'success' if 'test' in selected else 'skipped'}
        return needs

    def gate(self, needs):
        return subprocess.run(['python3', str(SCRIPT), 'gate'], text=True,
                              capture_output=True,
                              env={**os.environ, 'CI_NEEDS': json.dumps(needs)})

    def test_alternate_main_registry_job_satisfies_gate_and_both_are_required(self):
        for selected in (ALL, SECURITY | {'container'}):
            pr = self.needs(selected)
            pr['container-main'] = {'result': 'skipped'}
            self.assertEqual(self.gate(pr).returncode, 0)
            main = self.needs(selected)
            main['container-main'] = {'result': 'success'}
            main['container']['result'] = 'skipped'
            self.assertEqual(self.gate(main).returncode, 0)
            for r1, r2 in [('skipped', 'skipped'), ('success', 'success'),
                           ('skipped', 'failure'), ('failure', 'skipped')]:
                invalid = self.needs(selected)
                invalid['container']['result'] = r1
                invalid['container-main'] = {'result': r2}
                with self.subTest(container=r1, main=r2):
                    self.assertNotEqual(self.gate(invalid).returncode, 0)
        disabled = self.needs(SECURITY)
        disabled['container-main'] = {'result': 'skipped'}
        self.assertEqual(self.gate(disabled).returncode, 0)
        disabled['container-main'] = {'result': 'success'}
        self.assertNotEqual(self.gate(disabled).returncode, 0)

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

    def test_runtime_choice_failures_block_when_selected(self):
        for result in ('failure', 'cancelled', 'skipped', None):
            needs = self.needs(ALL)
            needs['runtime-choice-test']['result'] = result
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


class ContainerRegistryCachePublication(unittest.TestCase):
    """Registry manifests are published only by trusted main, after smoke."""

    def setUp(self):
        self.workflow = (SCRIPT.parent.parent / '.github/workflows/ci.yml').read_text()

    def check_contract(self, workflow):
        pr = workflow.split('\n  container:\n', 1)[1].split('\n  container-main:\n', 1)[0]
        main = workflow.split('\n  container-main:\n', 1)[1].split('\n  dependencies:\n', 1)[0]
        self.assertIn('packages: read', pr)
        self.assertNotIn('packages: write', pr)
        self.assertIn('packages: write', main)
        self.assertNotIn('packages: read', main)
        self.assertIn('steps: &container_smoke_steps', pr)
        self.assertIn('steps: *container_smoke_steps', main)
        self.assertIn("github.event_name == 'pull_request' || github.ref != 'refs/heads/main'", pr)
        self.assertIn("github.event_name != 'pull_request' && github.ref == 'refs/heads/main'", main)
        self.assertIn('docker/login-action@81c6f93d4bc991e510b342a00b419b3bb4efc5c6', pr)
        self.assertIn('registry: ghcr.io', pr)
        self.assertIn('username: ${{ github.actor }}', pr)
        self.assertIn('password: ${{ secrets.GITHUB_TOKEN }}', pr)
        self.assertNotIn('DOCKER_CACHE_ROOT', workflow)
        self.assertNotIn('docker-cache-source', pr)
        self.assertNotIn('type=gha', pr)
        tag = 'ghcr.io/${{ github.repository }}:buildcache-main-amd64'
        builder_tag = tag + '-builder'
        build = pr.split('      - name: Build and load final runtime image\n', 1)[1].split('      - name: ', 1)[0]
        self.assertIn('cache-from: type=registry,ref=' + tag, build)
        builder = pr.split('      - name: Export verified builder root\n', 1)[1].split('      - name: Export verified final image graph\n', 1)[0]
        final = pr.split('      - name: Export verified final image graph\n', 1)[1].split('      - name: Report optional cache publication\n', 1)[0]
        for block, export_tag, import_tag in (
            (builder, builder_tag, builder_tag), (final, tag, tag)):
            self.assertIn('uses: docker/build-push-action@', block)
            self.assertIn('cache-from: type=registry,ref=' + import_tag, block)
            self.assertIn('cache-to: type=registry,ref=' + export_tag +
                          ',mode=max,oci-mediatypes=true,image-manifest=true,ignore-error=true', block)
            self.assertIn("if: github.event_name != 'pull_request' && github.ref == 'refs/heads/main'", block)
            self.assertNotIn('continue-on-error:', block)
        required = ('Build and load final runtime image',
                    'Load pinned builder for permission probe',
                    'Verify final runtime image',
                    'Verify production indexing and offline retrieval in final image',
                    'Smoke test local Docker Compose stack')
        for name in required:
            self.assertLess(pr.index('      - name: ' + name + '\n'),
                            pr.index('      - name: Export verified builder root\n'))
            required_block = pr.split('      - name: ' + name + '\n', 1)[1].split('      - name: ', 1)[0]
            self.assertNotIn('continue-on-error:', required_block)
            self.assertNotIn('ignore-error=true', required_block)
        self.assertNotIn('continue-on-error:', pr)
        self.assertIn('GITHUB_STEP_SUMMARY', pr)
        self.assertIn('CI gate', workflow)

    def test_registry_export_is_main_only_and_required_smoke_is_not_optional(self):
        self.check_contract(self.workflow)
        self.assertEqual(self.workflow.count(
            "if: success() && github.event_name != 'pull_request' && github.ref == 'refs/heads/main'"), 5)
        pr = self.workflow.split('\n  container:\n', 1)[1].split('\n  container-main:\n', 1)[0]
        self.assertEqual(pr.count(
            "if: github.event_name != 'pull_request' && github.ref == 'refs/heads/main'"), 3)
        self.assertEqual(pr.count(',ignore-error=true'), 2)

    def test_broken_permissions_or_exports_or_checks_are_rejected(self):
        self.check_contract(self.workflow)
        for mutation in (
            self.workflow.replace('      packages: read', '      packages: write', 1),
            self.workflow.replace('      packages: write', '      packages: read', 1),
            self.workflow.replace('mode=max,oci-mediatypes=true,image-manifest=true,ignore-error=true',
                                  'mode=max,oci-mediatypes=true,image-manifest=true', 1),
            self.workflow.replace("if: github.event_name != 'pull_request' && github.ref == 'refs/heads/main'",
                                  'if: success()', 1),
            self.workflow.replace('      - name: Verify final runtime image\n',
                                  '      - name: Verify final runtime image\n        continue-on-error: true\n', 1),
        ):
            with self.subTest(mutation=mutation != self.workflow), self.assertRaises(AssertionError):
                self.check_contract(mutation)


if __name__ == '__main__':
    unittest.main()
