#!/usr/bin/env python3
"""Exercise actual security gates using offline, disposable synthetic fixtures."""
import pathlib
import shutil
import secrets
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(*args, cwd):
    return subprocess.run(args, cwd=cwd, text=True, capture_output=True, check=False)


def git(repo, *args):
    result = run('git', *args, cwd=repo)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def initialize(repo):
    repo.mkdir()
    git(repo, 'init', '-q')
    git(repo, 'config', 'user.email', 'security-test@example.invalid')
    git(repo, 'config', 'user.name', 'Security gate test')


def commit(repo):
    git(repo, 'add', '.')
    git(repo, 'commit', '-qm', 'Synthetic security fixture')


class SecurityGates(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        (self.root / 'scripts').mkdir()
        for name in ('check-dependencies.sh', 'check-secrets.sh'):
            shutil.copy2(ROOT / 'scripts' / name, self.root / 'scripts' / name)
        shutil.copy2(ROOT / '.gitleaks.toml', self.root)
        (self.root / '.gitleaksignore').write_text('')

    def scan_secrets(self, repo):
        return run('bash', 'scripts/check-secrets.sh', str(repo), cwd=self.root)

    def test_history_finds_deleted_tokens_and_exact_exception_does_not_hide_new_tokens(self):
        repo = self.root / 'repo'
        initialize(repo)
        # Construct a never-issued token at runtime; do not commit a token-shaped
        # literal into this repository or print the synthetic value in CI logs.
        fake = 'ghp_' + secrets.token_hex(18)
        (repo / 'fixture.txt').write_text('credential=' + fake + '\n')
        commit(repo)
        leaked_commit = git(repo, 'rev-parse', 'HEAD')
        (repo / 'fixture.txt').unlink()
        commit(repo)
        result = self.scan_secrets(repo)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertNotIn(fake, result.stdout + result.stderr)
        # A fingerprint suppresses only this precise historical synthetic finding.
        (self.root / '.gitleaksignore').write_text(
            leaked_commit + ':fixture.txt:github-pat:1\n'
        )
        result = self.scan_secrets(repo)
        self.assertEqual(result.returncode, 0, result.stderr)
        (repo / 'another.txt').write_text('credential=' + fake + '\n')
        commit(repo)
        self.assertEqual(self.scan_secrets(repo).returncode, 1)

    def test_merge_only_token_is_detected_even_after_deletion(self):
        repo = self.root / 'repo'
        initialize(repo)
        (repo / 'base.txt').write_text('Clean base.\n')
        commit(repo)
        base_branch = git(repo, 'branch', '--show-current')
        git(repo, 'checkout', '-qb', 'feature')
        (repo / 'feature.txt').write_text('Clean feature.\n')
        commit(repo)
        git(repo, 'checkout', '-q', base_branch)
        (repo / 'main.txt').write_text('Clean main.\n')
        commit(repo)
        git(repo, 'merge', '--no-ff', '--no-commit', 'feature')
        fake = 'ghp_' + secrets.token_hex(18)
        (repo / 'merge.txt').write_text('credential=' + fake + '\n')
        commit(repo)
        self.assertEqual(len(git(repo, 'rev-list', '--parents', '-1', 'HEAD').split()), 3)
        result = self.scan_secrets(repo)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertNotIn(fake, result.stdout + result.stderr)
        (repo / 'merge.txt').unlink()
        commit(repo)
        result = self.scan_secrets(repo)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertNotIn(fake, result.stdout + result.stderr)

    def test_clean_history_passes_and_shallow_history_is_rejected(self):
        repo = self.root / 'repo'
        initialize(repo)
        (repo / 'fixture.txt').write_text('No credentials here.\n')
        commit(repo)
        self.assertEqual(self.scan_secrets(repo).returncode, 0)
        shallow = self.root / 'shallow'
        git(self.root, 'clone', '-q', '--depth=1', repo.as_uri(), str(shallow))
        self.assertEqual(self.scan_secrets(shallow).returncode, 2)

    def test_critical_dependency_fails_and_exact_advisory_exception_does_not_hide_another(self):
        db = self.root / 'advisory-db'
        initialize(db)
        advisories = db / 'crates' / 'security-fixture'
        advisories.mkdir(parents=True)
        for number in ('0001', '0002'):
            (advisories / ('RUSTSEC-2025-' + number + '.md')).write_text('''```toml
[advisory]
id = "RUSTSEC-2025-''' + number + '''"
package = "security-fixture"
date = "2025-01-01"
url = "https://example.invalid/synthetic-advisory"
cvss = "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"

[versions]
patched = [">= 1.0.1"]
```

# Synthetic critical vulnerability

A disposable advisory for verifying the CI gate.
''')
        commit(db)
        (self.root / 'Cargo.lock').write_text('''version = 4
[[package]]
name = "security-fixture"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
''')
        args = ('bash', 'scripts/check-dependencies.sh', '--db', str(db),
                '--no-fetch', '--stale')
        result = run(*args, cwd=self.root)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('RUSTSEC-2025-0001', result.stdout + result.stderr)
        cargo_config = self.root / '.cargo'
        cargo_config.mkdir()
        (cargo_config / 'audit.toml').write_text(
            '[advisories]\nignore = ["RUSTSEC-2025-0001"]\n'
        )
        result = run(*args, cwd=self.root)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('RUSTSEC-2025-0002', result.stdout + result.stderr)
        (self.root / 'Cargo.lock').write_text(
            (self.root / 'Cargo.lock').read_text().replace('1.0.0', '1.0.1')
        )
        result = run(*args, cwd=self.root)
        self.assertEqual(result.returncode, 0, result.stderr)


    def test_nested_workspace_advisory_fails_even_when_root_workspace_is_clean(self):
        db = self.root / 'advisory-db'
        initialize(db)
        advisories = db / 'crates' / 'security-fixture'
        advisories.mkdir(parents=True)
        (advisories / 'RUSTSEC-2025-0001.md').write_text('```toml\n[advisory]\nid = "RUSTSEC-2025-0001"\npackage = "security-fixture"\ndate = "2025-01-01"\nurl = "https://example.invalid/synthetic-advisory"\n[versions]\npatched = [">= 1.0.1"]\n```\n# Synthetic advisory\nA disposable nested workspace fixture.\n')
        commit(db)
        clean = 'version = 4\n[[package]]\nname = "security-fixture"\nversion = "1.0.1"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\n'
        (self.root / 'Cargo.lock').write_text(clean)
        nested = self.root / 'crates' / 'experiment'
        nested.mkdir(parents=True)
        (nested / 'Cargo.lock').write_text(clean.replace('1.0.1', '1.0.0'))
        result = run('bash', 'scripts/check-dependencies.sh', '--db', str(db), '--no-fetch', '--stale', cwd=self.root)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('RUSTSEC-2025-0001', result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
