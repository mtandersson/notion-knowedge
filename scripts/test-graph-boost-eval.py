#!/usr/bin/env python3
"""Credential-free protocol and safety tests for graph boost evaluation (#102).

The tiny adapter below is a contract stub, NOT a production ranker.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("graph-boost-eval.py")
DATASET = SCRIPT.parent.parent / "eval/retrieval/personal-knowledge-v1.json"
CASES = SCRIPT.parent.parent / "eval/retrieval/graph-boost-v1.json"
ADAPTER = r'''
import json, os, sys
payload = json.load(sys.stdin)
fail = os.environ.get("GRAPH_TEST_FAILURE")
if fail == "crash":
    print("PRIVATE CONTENT", file=sys.stderr)
    sys.exit(4)
config = payload["graph_boost"]
if fail == "missing_echo":
    config = None
if fail == "wrong_echo":
    config = {**config, "max_hops": 2}
target = {
    "link-energy": "fixture:chunk:energy-decision:decision",
    "link-garden": "fixture:chunk:odling-review:notes",
    "link-study": "fixture:chunk:reading:queue",
}
guard = {
    "link-energy": "fixture:chunk:project-lighthouse-old:overview",
    "link-garden": "fixture:chunk:garden-old:plan",
    "link-study": "fixture:chunk:study:routine",
    "semantic-energy": "fixture:chunk:project-lighthouse-old:overview",
    "exact-bike": "fixture:chunk:bike-old:service",
    "date-review": "fixture:chunk:energy-decision:decision",
}
control_answer = {
    "semantic-energy": "fixture:chunk:project-lighthouse:overview",
    "exact-bike": "fixture:chunk:bike:service",
    "date-review": "fixture:chunk:lighthouse-review:notes",
}
unrelated = "fixture:chunk:travel:list"
other = "fixture:chunk:bike-old:service"
control_other = "fixture:chunk:reading:queue"
control_unrelated = "fixture:chunk:odling:plan"
results = []
for query in payload["queries"]:
    stem = query["id"].rsplit("-", 1)[0]
    if stem in target:
        baseline = [unrelated, guard[stem], target[stem], other]
        boosted = [target[stem], unrelated, guard[stem], other]
        if fail == "no_lift": boosted = baseline
        if fail == "lost_target": boosted = [unrelated, guard[stem], other, target[stem]]
        if fail == "guard_promoted": boosted = [guard[stem], unrelated, target[stem], other]
    else:
        baseline = [control_answer[stem], unrelated, control_unrelated,
                    guard[stem], control_other]
        boosted = baseline[:]
        if fail == "drift":
            boosted = [guard[stem], control_answer[stem], unrelated,
                       control_unrelated, control_other]
        if fail == "unjudged_drift":
            boosted = [control_other, control_answer[stem], unrelated,
                       control_unrelated, guard[stem]]
        if fail == "graph_only":
            boosted = [guard[stem], control_answer[stem], unrelated,
                       control_unrelated, "fixture:chunk:project-lighthouse:decision"]
        if fail == "lost_candidate": boosted = baseline[:-1]
    results.append({"query_id": query["id"],
                    "ranked_source_ids": boosted if payload["graph_boost"]["enabled"] else baseline})
if fail == "duplicate":
    results.append(results[0])
if fail == "unknown":
    results[0]["ranked_source_ids"][0] = "private-unknown"
if fail == "wrong_mode":
    payload["mode"] = "fts"
if fail == "duplicate_rank":
    results[0]["ranked_source_ids"][0] = results[0]["ranked_source_ids"][1]
response = {
    "schema_version": 1,
    "dataset_id": payload["dataset_id"],
    "mode": payload["mode"],
    "results": results
}
if config is not None:
    response["graph_boost"] = config
print(json.dumps(response))
'''


class GraphBoostEvaluationTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.folder = Path(tmp.name)
        self.adapter = self.folder / "fixture.py"
        self.adapter.write_text(ADAPTER, encoding="utf-8")
        self.command = f"{sys.executable} {self.adapter}"

    def invoke(self, *args, fail=None, fixture=CASES):
        env = {**os.environ}
        env.pop("GRAPH_TEST_FAILURE", None)
        if fail:
            env["GRAPH_TEST_FAILURE"] = fail
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--dataset", str(DATASET),
             "--cases", str(fixture), *args],
            text=True, capture_output=True, encoding="utf-8", env=env
        )

    def test_manifest_integrity_and_bilingual_coverage(self):
        result = self.invoke("--validate-only")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("12", result.stdout)
        manifest = json.loads(CASES.read_text(encoding="utf-8"))
        self.assertEqual(manifest["config"]["max_hops"], 1)
        self.assertEqual(manifest["config"]["weight"], 0.15)
        self.assertEqual(
            {item["kind"] for item in manifest["cases"]},
            {"linked", "negative_control"},
        )
        self.assertEqual(len({c["query_id"] for c in manifest["cases"]}), 12)

    def test_measured_lift_and_deterministic_report(self):
        first = self.invoke("--adapter", self.command, "--top-k", "3")
        self.assertEqual(first.returncode, 0, first.stderr)
        report = json.loads(first.stdout)
        self.assertEqual(report["evidence"], "adapter_measured")
        self.assertEqual(report["config"]["candidate_limit"], 20)
        self.assertEqual(report["decision"], "candidate_for_review")
        self.assertEqual(report["summary"]["linked_queries"], 6)
        self.assertEqual(report["summary"]["control_queries"], 6)
        self.assertEqual(report["summary"]["target_rank_improvements"], 6)
        self.assertFalse(report["summary"]["new_guard_regressions"])
        self.assertGreater(report["summary"]["linked"]["boosted"]["mrr"],
                           report["summary"]["linked"]["baseline"]["mrr"])
        self.assertEqual(first.stdout,
                         self.invoke("--adapter", self.command, "--top-k", "3").stdout)
        self.assertEqual(
            len({row["query_id"] for row in report["results"]}), 12
        )

    def test_zero_lift_and_false_positive_drift_leave_boost_off(self):
        for mode in ("no_lift", "drift", "unjudged_drift", "lost_target",
                     "guard_promoted"):
            with self.subTest(mode=mode):
                run = self.invoke("--adapter", self.command, fail=mode)
                self.assertEqual(run.returncode, 0, run.stderr)
                report = json.loads(run.stdout)
                self.assertEqual(report["decision"], "keep_disabled")
                if mode == "drift":
                    self.assertEqual(len(report["summary"]["new_guard_regressions"]), 6)
                if mode == "unjudged_drift":
                    self.assertTrue(report["summary"]["new_unjudged_regressions"])
                if mode == "lost_target":
                    self.assertTrue(report["summary"]["target_top3_regressions"])
                if mode == "guard_promoted":
                    self.assertEqual(len(report["summary"]["guard_rank_regressions"]), 6)

    def test_invalid_adapter_protocol_is_not_a_zero_quality_score(self):
        for mode in ("missing_echo", "wrong_echo", "duplicate", "unknown",
                     "wrong_mode", "crash", "graph_only", "lost_candidate",
                     "duplicate_rank"):
            with self.subTest(mode=mode):
                run = self.invoke("--adapter", self.command, fail=mode)
                self.assertEqual(run.returncode, 2)
                self.assertFalse(run.stdout)
                self.assertNotIn("PRIVATE CONTENT", run.stderr)

    def test_rejects_bad_fixtures_before_network(self):
        manifest = json.loads(CASES.read_text(encoding="utf-8"))
        def assert_bad(modifier):
            changed = json.loads(json.dumps(manifest))
            modifier(changed)
            path = self.folder / "cases.json"
            path.write_text(json.dumps(changed), encoding="utf-8")
            run = self.invoke("--validate-only", fixture=path)
            self.assertEqual(run.returncode, 2, run.stderr)

        assert_bad(lambda m: m["config"].update({"max_hops": 4}))
        assert_bad(lambda m: m["config"].update({"weight": -0.1}))
        assert_bad(lambda m: m["config"].update({"weight": 0.6}))
        assert_bad(lambda m: m["cases"][0].update({"guard_source_ids": [{}]}))
        assert_bad(lambda m: m["cases"][0].update({"target_source_id": "unknown"}))
        assert_bad(lambda m: m["cases"][0].update({
            "target_source_id": "fixture:chunk:reading:queue"}))
        assert_bad(lambda m: m["cases"][0].update({
            "guard_source_ids": ["fixture:chunk:energy-decision:decision"]}))
        assert_bad(lambda m: m["cases"].append(dict(m["cases"][0])))
        assert_bad(lambda m: m["cases"].pop())
        assert_bad(lambda m: m.update({"dataset_id": "wrong"}))

    def test_missing_adapter_and_unsafe_output_paths(self):
        self.assertEqual(self.invoke().returncode, 2)
        self.assertEqual(self.invoke("--top-k", "2", "--validate-only").returncode, 2)
        self.assertEqual(self.invoke("--top-k", "21", "--validate-only").returncode, 2)
        self.assertEqual(self.invoke("--adapter", self.command, "--output", str(DATASET)).returncode, 2)
        self.assertEqual(self.invoke("--adapter", self.command, "--output", str(CASES)).returncode, 2)

    def test_report_can_be_written_and_remains_small(self):
        path = self.folder / "graph-report.json"
        response = self.invoke("--adapter", self.command, "--output", str(path))
        self.assertEqual(response.returncode, 0, response.stderr)
        self.assertFalse(response.stdout)
        report = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(report["summary"]["linked_queries"], 6)
        self.assertNotIn("PRIVATE CONTENT", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
