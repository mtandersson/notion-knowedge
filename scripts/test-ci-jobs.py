#!/usr/bin/env python3
"""Exercise the production CI command with committed inputs and Actions results."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('ci-jobs.py').resolve()
WORKFLOWS = SCRIPT.parents[1] / '.github/workflows'
REQUIRED = {'agent-layout', 'fmt', 'clippy', 'test', 'dependencies', 'secrets'}
OPTIONAL = {'clippy-features', 'test-local-qwen', 'native-build', 'native-test', 'build'}
ALL = REQUIRED | OPTIONAL
SECURITY = {'dependencies', 'secrets'}
WORKSPACE = {'fmt', 'clippy', 'clippy-features', 'test', 'test-local-qwen', 'native-build',
             'native-test', 'build'}
# Container inputs select nothing here: container.yml runs the smoke.
CONTAINER_ONLY = set()


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
            'crates/server/tests/it/http.rs': WORKSPACE,
            'eval/retrieval/personal-knowledge-v1.json': {'test', 'test-local-qwen', 'native-build',
                                                          'native-test'},
            'Cargo.lock': WORKSPACE,
            'Cargo.toml': WORKSPACE,
            'crates/core/Cargo.toml': WORKSPACE,
            'crates/server/build.rs': WORKSPACE,
            'Dockerfile': CONTAINER_ONLY,
            '.dockerignore': CONTAINER_ONLY,
            'scripts/smoke-container.py': CONTAINER_ONLY,
            'compose.yaml': CONTAINER_ONLY,
            'scripts/compose-dev.sh': CONTAINER_ONLY,
            'scripts/smoke-compose.py': CONTAINER_ONLY,
            '.github/workflows/ci.yml': ALL,
            '.github/workflows/container.yml': ALL,
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
                         SECURITY | {'agent-layout'})
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
        # Actions passes only the gate's own needs; optional jobs are outputs only.
        needs = {'changes': {'result': 'success', 'outputs': {
            job: 'true' if job in selected else 'false' for job in ALL}}}
        needs.update({job: {'result': 'success' if job in selected else 'skipped'} for job in REQUIRED})
        return needs

    def gate(self, needs):
        return subprocess.run(['python3', str(SCRIPT), 'gate'], text=True,
                              capture_output=True,
                              env={**os.environ, 'CI_NEEDS': json.dumps(needs)})

    def test_full_success_and_intentional_skips_pass(self):
        for selected in (ALL, SECURITY, SECURITY | {'test'}, SECURITY | {'build'}):
            self.assertEqual(self.gate(self.needs(selected)).returncode, 0)

    def test_every_selected_failure_cancellation_skip_and_missing_result_blocks(self):
        for job in REQUIRED:
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
            needs['fmt']['result'] = result
            self.assertNotEqual(self.gate(needs).returncode, 0)

    def test_required_gate_excludes_optional_feature_and_container_jobs(self):
        workflow = (WORKFLOWS / 'ci.yml').read_text()
        gate = workflow.split('\n  gate:\n', 1)[1]
        needs = re.search(r'^    needs: \[([^\]]*)\]$', gate, re.M).group(1)
        self.assertEqual({job.strip() for job in needs.split(',')}, {'changes'} | REQUIRED)
        jobs = set(re.findall(r'^  ([a-z-]+):$', workflow.split('\njobs:\n', 1)[1], re.M))
        self.assertEqual(jobs, {'changes', 'gate'} | ALL)
        for job in ALL - SECURITY:
            with self.subTest(job=job):
                self.assertIn(f'      {job}: ${{{{ steps.select.outputs.{job} }}}}', workflow)
                self.assertIn(f"if: needs.changes.outputs.{job} == 'true'", workflow)

    def test_check_job_removed_and_clippy_covers_all_targets(self):
        workflow = (WORKFLOWS / 'ci.yml').read_text()
        self.assertNotIn('\n  check:\n', workflow)
        self.assertNotIn('cargo check', workflow)
        clippy = workflow.split('\n  clippy:\n', 1)[1].split('\n  clippy-features:\n', 1)[0]
        self.assertIn('nix develop --command cargo clippy --workspace --all-targets --locked -- -D warnings',
                      clippy)
        self.assertNotIn('.#spike', clippy)
        features = workflow.split('\n  clippy-features:\n', 1)[1].split('\n  test:\n', 1)[0]
        self.assertEqual(features.count('nix develop .#spike --command cargo clippy'), 4)
        self.assertIn('job: clippy-features', features)

    def test_debug_info_and_incremental_settings_apply_to_every_cargo_job(self):
        workflow = (WORKFLOWS / 'ci.yml').read_text()
        env = workflow.split('\nenv:\n', 1)[1].split('\njobs:\n', 1)[0]
        for setting in ('CARGO_INCREMENTAL: 0', 'CARGO_PROFILE_DEV_DEBUG: line-tables-only',
                        'CARGO_PROFILE_TEST_DEBUG: line-tables-only'):
            self.assertIn(setting, env)
            self.assertEqual(workflow.count(setting.split(':')[0] + ':'), 1)


class ContainerWorkflow(unittest.TestCase):
    def test_container_workflow_runs_on_main_schedule_and_container_paths(self):
        workflow = (WORKFLOWS / 'container.yml').read_text()
        triggers = workflow.split('\non:\n', 1)[1].split('\npermissions:\n', 1)[0]
        pull = triggers.split('  pull_request:\n', 1)[1].split('\n  push:\n', 1)[0]
        paths = set(re.findall(r'^      - (\S+)$', pull, re.M))
        for path in ('Dockerfile', '.dockerignore', 'compose.yaml', 'scripts/*container*',
                     'scripts/smoke-*.py', 'nix/**', 'flake.lock', 'Cargo.lock', 'crates/*/Cargo.toml',
                     '.github/workflows/container.yml'):
            self.assertIn(path, paths)
        self.assertIn('  push:\n    branches:\n      - main\n', triggers)
        self.assertIn('  schedule:\n    - cron: "0 3 * * *"\n', triggers)
        self.assertIn('  workflow_dispatch:\n', triggers)
        self.assertNotIn('container_smoke_steps', (WORKFLOWS / 'ci.yml').read_text())


class NativeTestDistribution(unittest.TestCase):
    def test_native_execution_consumes_binaries_while_other_suites_start_independently(self):
        workflow = (WORKFLOWS / 'ci.yml').read_text()
        direct = workflow.split('  test:\n', 1)[1].split('  native-build:\n', 1)[0]
        qwen = direct.split('  test-local-qwen:\n', 1)[1]
        direct = direct.split('  test-local-qwen:\n', 1)[0]
        build = workflow.split('  native-build:\n', 1)[1].split('  native-test:\n', 1)[0]
        run = workflow.split('  native-test:\n', 1)[1].split('  build:\n', 1)[0]
        self.assertIn('needs: changes', direct)
        self.assertNotIn('needs: [changes, native-build]', direct)
        self.assertIn('cargo test --workspace --locked', direct)
        self.assertIn('needs: changes', qwen)
        self.assertIn('.#spike --command cargo test -p notion-knowledge-retrieval --features local-qwen', qwen)
        self.assertIn('--lib --tests $NATIVE_EXAMPLES --no-run', build)
        # The production evaluation example links on main only, not on PRs.
        self.assertIn("NATIVE_EXAMPLES: ${{ github.event_name != 'pull_request' && '--example production-eval' || '' }}",
                      build)
        self.assertIn('--doc --locked', build)
        self.assertIn('scripts/test-compression-compat.py', build)
        self.assertIn('needs: [changes, native-build]', run)
        self.assertIn('.#test-runtime --command python3', run)
        self.assertIn('actions/download-artifact@', run)
        self.assertIn('ref: ${{ github.sha }}', build)
        self.assertIn('ref: ${{ github.sha }}', run)
        self.assertNotIn('cargo-cache', run)
        self.assertNotIn('--command cargo', run)


class ContainerRegistryCachePublication(unittest.TestCase):
    """Registry manifests are published only by trusted main, after smoke."""

    def setUp(self):
        self.workflow = (WORKFLOWS / 'container.yml').read_text()

    def check_contract(self, workflow):
        pr = workflow.split('\n  container:\n', 1)[1].split('\n  container-main:\n', 1)[0]
        main = workflow.split('\n  container-main:\n', 1)[1]
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

    def test_registry_export_is_main_only_and_required_smoke_is_not_optional(self):
        self.check_contract(self.workflow)
        ci = (WORKFLOWS / 'ci.yml').read_text()
        self.assertEqual(ci.count(
            "if: success() && github.event_name != 'pull_request' && github.ref == 'refs/heads/main'"),
            ci.count("uses: ./.github/actions/cargo-cache-save"))
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
