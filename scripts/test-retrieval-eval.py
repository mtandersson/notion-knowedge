#!/usr/bin/env python3
"""Credential-free subprocess contract tests for the offline retrieval harness."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("retrieval-eval.py")
DATASET = Path(__file__).resolve().parents[1] / "eval/retrieval/personal-knowledge-v1.json"

# Contract probe only: purposely *not* a ranking model or ground-truth oracle.
STUB = '''
import json, os, sys, time
payload = json.load(sys.stdin)
mode = payload["mode"]
if os.environ.get("EVAL_STUB_FAIL") == "crash":
    print("secret content", file=sys.stderr)
    sys.exit(7)
if os.environ.get("EVAL_STUB_FAIL") == "hang":
    time.sleep(3)
ids = [chunk["id"] for page in payload["pages"] for chunk in page["chunks"]]
ranked = (
    ids[:payload["top_k"]] if mode == "vector"
    else list(reversed(ids))[:payload["top_k"]] if mode == "fts"
    else sorted(ids)[:payload["top_k"]]
)
entries = [
    {"query_id": q["id"], "ranked_source_ids": ranked}
    for q in payload["queries"]
]
problem = os.environ.get("EVAL_STUB_FAIL")
if problem == "duplicate": entries.append(entries[0])
if problem == "missing": entries.pop()
if problem == "unknown": entries[0]["ranked_source_ids"][0] = "not-a-chunk"
if problem == "overlong": entries[0]["ranked_source_ids"] = ids
if problem == "wrong_mode": mode = "wrong"
if problem == "malformed":
    print("not json")
    sys.exit(0)
print(json.dumps({
    "schema_version": 1, "dataset_id": payload["dataset_id"],
    "mode": mode, "results": entries,
}))
'''


class EvaluationHarnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.adapter = Path(self.temp.name) / "adapter.py"
        self.adapter.write_text(STUB, encoding="utf-8")
        self.adapter_command = f"{sys.executable} {self.adapter}"

    def run_cli(self, *opts, modes="vector,fts,hybrid", environment=None):
        return subprocess.run(
            [
                sys.executable, str(SCRIPT),
                "--dataset", str(DATASET),
                "--adapter", self.adapter_command,
                "--modes", modes, *opts,
            ],
            capture_output=True, text=True, encoding="utf-8", env=environment,
        )

    def test_three_modes_and_deterministic_per_query_ranked_results(self):
        first = self.run_cli("--top-k", "3")
        self.assertEqual(first.returncode, 0, first.stderr)
        report = json.loads(first.stdout)
        self.assertEqual(report["modes"], ["vector", "fts", "hybrid"])
        self.assertEqual(len(report["results"]), 72)
        self.assertEqual(set(report["summary"]), {"vector", "fts", "hybrid"})
        self.assertEqual(len(report["results"][0]["ranked_source_ids"]), 3)
        expected = {"recall_at_1", "recall_at_3", "ndcg_at_1", "ndcg_at_3", "mrr"}
        self.assertEqual(set(report["results"][0]["metrics"]), expected)
        self.assertEqual(set(report["summary"]["vector"]["metrics"]), expected)
        self.assertEqual(report["summary"]["vector"]["queries"], 24)
        self.assertEqual(
            report["summary"]["vector"]["metrics"]["mrr"],
            sum(row["metrics"]["mrr"] for row in report["results"][:24]) / 24,
        )
        self.assertTrue(all(
            0 <= value <= 1
            for row in report["results"]
            for value in row["metrics"].values()
        ))
        self.assertNotEqual(
            report["results"][0]["ranked_source_ids"],
            report["results"][24]["ranked_source_ids"],
        )
        self.assertEqual(first.stdout, self.run_cli("--top-k", "3").stdout)
        self.assertTrue(all(len(row["ranked_source_ids"]) == len(row["grades"])
                            for row in report["results"]))

    def test_json_output_file_and_dataset_safety(self):
        output = Path(self.temp.name) / "report.json"
        run = self.run_cli("--output", str(output), modes="fts")
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertFalse(run.stdout)
        self.assertEqual(json.loads(output.read_text())["modes"], ["fts"])
        self.assertEqual(self.run_cli("--output", str(DATASET)).returncode, 2)

    def test_bad_adapter_outputs_are_fatal_and_not_exposed(self):
        for problem in ("crash", "duplicate", "missing", "unknown",
                        "wrong_mode", "malformed", "overlong"):
            with self.subTest(problem=problem):
                environment = {**os.environ, "EVAL_STUB_FAIL": problem}
                result = self.run_cli("--top-k", "3", environment=environment)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertNotIn("secret content", result.stderr)
                self.assertFalse(result.stdout)
        environment = {**os.environ, "EVAL_STUB_FAIL": "hang"}
        self.assertEqual(
            self.run_cli("--timeout", "0.05", environment=environment).returncode, 2
        )

    def test_bad_arguments_and_invalid_dataset_are_fatal(self):
        for opts in (
            ("--modes", "vector,vector"), ("--modes", "unknown"),
            ("--top-k", "0"), ("--timeout", "0"),
        ):
            with self.subTest(options=opts):
                self.assertEqual(self.run_cli(*opts).returncode, 2)
        path = Path(self.temp.name) / "corrupt.json"
        path.write_text("invalid json")
        self.assertEqual(self.run_cli("--dataset", str(path)).returncode, 2)
        data = json.loads(DATASET.read_text(encoding="utf-8"))
        data["queries"][0]["relevant"][0]["source_id"] = "not-in-corpus"
        path.write_text(json.dumps(data), encoding="utf-8")
        self.assertEqual(self.run_cli("--dataset", str(path)).returncode, 2)


if __name__ == "__main__":
    unittest.main()
