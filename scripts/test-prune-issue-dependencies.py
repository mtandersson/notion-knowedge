#!/usr/bin/env python3
"""Credential-free unit tests for the #266 GitHub dependency cleanup."""
import contextlib
import importlib.util
import io
import pathlib
import unittest
from unittest.mock import patch

SCRIPT = pathlib.Path(__file__).with_name("prune-issue-dependencies.py")
SPEC = importlib.util.spec_from_file_location("prune_issue_dependencies", SCRIPT)
assert SPEC and SPEC.loader
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


class DependencyCleanupTests(unittest.TestCase):
    def test_batches_are_exact_and_do_not_touch_other_bots_work(self):
        self.assertEqual(len(mod.A_EDGES), 99)
        self.assertEqual(len(mod.B_EDGES), 11)
        self.assertEqual(len(set(mod.A_EDGES)), 99)
        self.assertEqual(len(set(mod.B_EDGES)), 11)
        self.assertFalse(set(mod.A_EDGES) & set(mod.B_EDGES))
        self.assertNotIn(257, {n for edge in (*mod.A_EDGES, *mod.B_EDGES) for n in edge})
        self.assertNotIn(254, {n for edge in (*mod.A_EDGES, *mod.B_EDGES) for n in edge})
        self.assertEqual(set(mod.group_edges(mod.A_EDGES)[124]), {9})
        self.assertEqual(set(mod.group_edges(mod.B_EDGES)[124]) if 124 in mod.group_edges(mod.B_EDGES) else set(), set())
        self.assertEqual(mod.group_edges(mod.B_EDGES)[117], {9})

    def test_live_plan_skips_already_removed_relations(self):
        target = {66: {7}, 67: {7}, 124: {9}}
        snapshot = {66: {7: 111}, 67: {}, 124: {9: 222, 123: 333}}
        self.assertEqual(mod.plan_removals(target, snapshot), [(66, 7, 111), (124, 9, 222)])

    def test_dry_run_performs_no_mutation(self):
        with patch.object(mod, "blockers", return_value={}), \
             patch.object(mod, "assert_protected"), \
             patch.object(mod, "gh") as api, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            mod.run("A", False, 0.5)
        api.assert_not_called()
        self.assertIn("DRY RUN", output.getvalue())
        self.assertIn("live: 0", output.getvalue())

    def test_apply_only_preverified_ids_and_rechecks_after_delete(self):
        state = {66: {7: 12345}}
        calls = []

        def fake_blockers(issue):
            return dict(state.get(issue, {}))

        def fake_api(method, path):
            self.assertEqual(method, "DELETE")
            self.assertEqual(path, f"{mod.API_ROOT}/66/dependencies/blocked_by/12345")
            calls.append(path)
            state[66] = {}

        with patch.object(mod, "blockers", side_effect=fake_blockers), \
             patch.object(mod, "assert_protected"), \
             patch.object(mod, "gh", side_effect=fake_api), \
             patch.object(mod.time, "sleep") as sleep, \
             contextlib.redirect_stdout(io.StringIO()):
            mod.run("A", True, 0.5)
        self.assertEqual(len(calls), 1)
        sleep.assert_called_once_with(0.5)

    def test_apply_aborts_on_concurrent_id_change(self):
        state = {66: {7: 111}}
        calls = 0

        def fake_blockers(issue):
            nonlocal calls
            if issue == 66:
                calls += 1
                return {7: 111 if calls == 1 else 222}
            return {}

        with patch.object(mod, "blockers", side_effect=fake_blockers), \
             patch.object(mod, "assert_protected"), \
             patch.object(mod, "gh") as api, \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(mod.ApiError, "changed during migration"):
                mod.run("A", True, 0.5)
        api.assert_not_called()

    def test_batch_b_requires_a_to_be_finished(self):
        with patch.object(mod, "blockers", return_value={7: 1}), \
             patch.object(mod, "assert_protected"), \
             patch.object(mod, "gh") as api, \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(mod.ApiError, "Batch A still"):
                mod.run("B", True, 0.5)
        api.assert_not_called()

    def test_protected_edges_are_verified(self):
        with patch.object(mod, "blockers", side_effect=lambda issue: {
            predecessor: predecessor * 100 for predecessor in mod.PROTECTED[issue]
        }):
            mod.assert_protected()
        with patch.object(mod, "blockers", side_effect=lambda issue: {}):
            with self.assertRaisesRegex(mod.ApiError, "Critical dependency"):
                mod.assert_protected()

    def test_rejects_non_github_and_mutation_methods(self):
        for method, path in (
            ("POST", f"{mod.API_ROOT}/124/dependencies/blocked_by"),
            ("DELETE", "/repos/another/repository/issues/124"),
            ("PATCH", f"{mod.API_ROOT}/124"),
        ):
            with self.assertRaisesRegex(mod.ApiError, "Refusing"):
                mod.gh(method, path)


if __name__ == "__main__":
    unittest.main()
