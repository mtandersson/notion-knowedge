#!/usr/bin/env python3
"""Safety tests for one-shot BuildKit GitHub Actions cache cleanup."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("prune-buildkit-gha-cache.py")
spec = importlib.util.spec_from_file_location("purge_buildkit_cache", SCRIPT)
purge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(purge)


def entry(i, key, size=1024, ref="refs/heads/main"):
    return {"id": i, "key": key, "size_in_bytes": size, "ref": ref}


INDEX = "index-nk-container-v1-Linux-X64-main-1-94398b49"
PR_INDEX = "index-nk-container-v1-Linux-X64-189-builder-1-1234abcd"
BLOB = "buildkit-blob-1-sha256:" + "a" * 64
CARGO = "cargo-target-pinned-rust-release-2026"


class SelectionTests(unittest.TestCase):
    def test_matches_main_and_pr_builder_index_and_buildkit_blob(self):
        current = [entry(1, INDEX, 33), entry(2, PR_INDEX, 44),
                   entry(3, BLOB, 55), entry(4, CARGO, 66)]
        chosen, others = purge.select_obsolete(current)
        self.assertEqual([x["id"] for x in chosen], [1, 2, 3])
        self.assertFalse(others)
        self.assertEqual(sum(x["size_in_bytes"] for x in chosen), 132)

    def test_older_cache_cannot_delete_cargo_or_other_users(self):
        others = ["cargo-downloads-rust", "rust-clippy-cache", "other-service-cache",
                  "index-unknown-project-1-12345678", "buildkit-blob-2-sha256:" + "b"*64,
                  "index-nk-container-v1-Linux-X64-X-2-1234abcd",
                  "buildkit-blob-1-sha256:" + "not-a-real-digest"]
        caches = [entry(i+5, key) for i, key in enumerate(others)] + [
            entry(1, INDEX), entry(2, BLOB)]
        selected, foreign = purge.select_obsolete(caches)
        self.assertEqual([x["id"] for x in selected], [1])
        self.assertEqual(len(foreign), 2, "Unknown indexes must protect shared blobs")

    def test_no_foreign_index_means_all_old_blobs_are_selectable(self):
        caches = [entry(1, CARGO), entry(2, BLOB, ref="refs/pull/189/merge")]
        selected, foreign = purge.select_obsolete(caches)
        self.assertEqual([x["id"] for x in selected], [2])
        self.assertFalse(foreign)

    def test_prefix_tricks_and_substrings_are_rejected(self):
        for key in (INDEX + "/tail", "prefix-" + INDEX, BLOB + "-oops",
                    BLOB[:-1], "buildkit-blob-1-SHA256:" + "a"*64):
            self.assertFalse(purge.INDEX.fullmatch(key))
            self.assertFalse(purge.BLOB.fullmatch(key))


class InventoryTests(unittest.TestCase):
    def test_pages_are_snapshot_before_any_deletion(self):
        caches = [entry(i+1, CARGO + str(i)) for i in range(100)] + [entry(101, INDEX)]
        calls = []
        def api(path):
            calls.append(path)
            page = int(path.split("page=")[-1])
            return {"total_count": 101, "actions_caches": caches[(page-1)*100:page*100]}
        found = purge.collect_caches(api)
        self.assertEqual([e["id"] for e in found], list(range(1, 102)))
        self.assertEqual(len(calls), 2)
        self.assertIn("page=1", calls[0])
        self.assertIn("page=2", calls[1])

    def test_limits_and_duplicate_ids_fail_closed(self):
        with self.assertRaises(RuntimeError):
            purge.collect_caches(lambda _: {"total_count": 6000, "actions_caches": []})
        with self.assertRaises(RuntimeError):
            purge.collect_caches(lambda _: {"total_count": 2,
                           "actions_caches": [entry(3, INDEX), entry(3, BLOB)]})

    def test_foreign_caches_never_deleted_even_under_apply(self):
        data = [entry(1, INDEX), entry(2, BLOB), entry(3, CARGO)]
        calls = []
        def api(path, *, delete=False):
            calls.append((path, delete))
            if delete:
                return None
            return {"total_count": len(data),
                    "actions_caches": [e for e in data if e["id"] != 1 and e["id"] != 2]
                    if any(is_delete for _, is_delete in calls) else data}
        with patch.object(purge, "gh_api", side_effect=api), \
             patch.object(purge, "authorize_one_shot"):
            with patch("sys.argv", [str(SCRIPT), "--apply"]):
                purge.main()
        deleted = [path for path, yes in calls if yes]
        self.assertEqual(deleted, [
            f"/repos/{purge.REPOSITORY}/actions/caches/1",
            f"/repos/{purge.REPOSITORY}/actions/caches/2"])
        self.assertNotIn("/caches/3", "\n".join(deleted))

    def test_unauthorized_ref_and_non_exact_commit_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "event.json"
            path.write_text(json.dumps({"head_commit": {"message": "unrelated change"}}))
            valid = {"GITHUB_REPOSITORY": purge.REPOSITORY, "GITHUB_REF": "refs/heads/main",
                     "GITHUB_EVENT_NAME": "push", "GITHUB_EVENT_PATH": str(path)}
            with patch.dict(os.environ, valid):
                with self.assertRaises(RuntimeError):
                    purge.authorize_one_shot()
                path.write_text(json.dumps({"head_commit": {"message": purge.ONE_SHOT_TITLE + " (#511)"}}))
                purge.authorize_one_shot()
            with patch.dict(os.environ, {**valid, "GITHUB_REF": "refs/pull/511/merge"}):
                with self.assertRaises(RuntimeError):
                    purge.authorize_one_shot()


if __name__ == "__main__":
    unittest.main()
