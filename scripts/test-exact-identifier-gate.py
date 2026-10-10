#!/usr/bin/env python3
"""Credential-free tests of exact-identifier judgments and regression gate.

Synthetic rankings here exercise only the checker, not a search backend.
"""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "eval/retrieval/exact-identifiers-v1.json"
GATE = ROOT / "scripts/exact-identifier-gate.py"


def golden_report(dataset):
    rows = []
    for mode in ("fts", "hybrid"):
        for query in dataset["queries"]:
            correct = query["relevant"][0]["source_id"]
            distractor = query["excluded_source_ids"][0]
            rows.append({
                "mode": mode, "query_id": query["id"],
                "language": query["language"], "category": query["category"],
                "ranked_source_ids": [correct, distractor], "grades": [3, 0],
            })
    return {
        "schema_version": 1, "dataset_id": dataset["dataset_id"],
        "as_of": dataset["as_of"], "top_k": 3,
        "modes": ["fts", "hybrid"], "summary": {"fts": {}, "hybrid": {}},
        "results": rows,
    }


class ExactIdentifierGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = json.loads(DATASET.read_text(encoding="utf-8"))

    def run_gate(self, report, *args):
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "measurement.json"
            report_path.write_text(json.dumps(report), encoding="utf-8")
            return subprocess.run(
                [sys.executable, str(GATE), "--dataset", str(DATASET),
                 "--report", str(report_path), *args],
                capture_output=True, text=True, encoding="utf-8",
            )

    def test_fixture_has_distinct_case_and_tokenization_traps(self):
        pages = self.dataset["pages"]
        queries = self.dataset["queries"]
        self.assertEqual(len(pages), 8)
        self.assertEqual(len(queries), 8)
        self.assertEqual({q["id"].split("-")[0] for q in queries},
                         {"order", "issue", "name", "mixed"})
        all_chunks = {chunk["id"] for page in pages for chunk in page["chunks"]}
        self.assertEqual(len(all_chunks), 8)
        for family in ("order", "issue", "name", "mixed"):
            pair = {q["language"]: q for q in queries if q["id"].startswith(family + "-")}
            self.assertEqual(set(pair), {"sv", "en"})
            self.assertEqual(pair["sv"]["relevant"], pair["en"]["relevant"])
            self.assertEqual(pair["sv"]["excluded_source_ids"],
                             pair["en"]["excluded_source_ids"])
            for query in pair.values():
                self.assertIn(query["relevant"][0]["source_id"], all_chunks)
                self.assertIn(query["excluded_source_ids"][0], all_chunks)
                self.assertNotEqual(query["relevant"][0]["source_id"],
                                    query["excluded_source_ids"][0])
        self.assertIn("abc731X", next(q["text"] for q in queries if q["id"] == "mixed-sv"))
        self.assertIn("Åberg", next(q["text"] for q in queries if q["id"] == "name-en"))

    def test_good_both_modes_pass_and_output_is_aggregate_only(self):
        result = self.run_gate(golden_report(self.dataset))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("fts: exact top-1 8/8", result.stdout)
        self.assertIn("hybrid: exact top-1 8/8", result.stdout)
        self.assertNotIn("ORD7241B", result.stdout)

    def test_regression_fails_on_low_top1_or_top3(self):
        for mode in ("fts", "hybrid"):
            report = golden_report(self.dataset)
            rows = [row for row in report["results"] if row["mode"] == mode]
            for row in rows[:3]:
                row["ranked_source_ids"].reverse()
                row["grades"].reverse()
            result = self.run_gate(report)
            self.assertEqual(result.returncode, 1, (mode, result.stderr))
            self.assertIn("FAIL", result.stdout)
            report = golden_report(self.dataset)
            rows = [row for row in report["results"] if row["mode"] == mode]
            for row in rows[:3]:
                row["ranked_source_ids"] = [row["ranked_source_ids"][1]]
                row["grades"] = [0]
            self.assertEqual(self.run_gate(report).returncode, 1)

    def test_each_pattern_and_both_languages_are_covered(self):
        report = golden_report(self.dataset)
        for row in report["results"]:
            if row["mode"] == "fts" and row["query_id"].startswith("mixed-"):
                row["ranked_source_ids"] = row["ranked_source_ids"][1:]
                row["grades"] = [0]
        self.assertEqual(self.run_gate(report, "--min-top1", "0",
                                      "--min-top3", "0").returncode, 1)
        report = golden_report(self.dataset)
        for row in report["results"]:
            if row["mode"] == "hybrid" and row["language"] == "sv":
                row["ranked_source_ids"] = row["ranked_source_ids"][1:]
                row["grades"] = [0]
        self.assertEqual(self.run_gate(report, "--min-top1", "0",
                                      "--min-top3", "0").returncode, 1)

    def test_missing_or_forged_evidence_is_rejected(self):
        mutations = [
            lambda report: report["results"].pop(),
            lambda report: report["results"].append(copy.deepcopy(report["results"][0])),
            lambda report: report.update(modes=["fts"]),
            lambda report: report.update(dataset_id="another-dataset"),
            lambda report: report.update(top_k=2),
            lambda report: report["results"][0].update(grades=[0, 3]),
            lambda report: report["results"][0].update(ranked_source_ids=["fake:chunk"]),
            lambda report: report["summary"].pop("hybrid"),
            lambda report: report["results"].__setitem__(0, []),
            lambda report: report.update(results="not a list"),
        ]
        for mutation in mutations:
            with self.subTest(mutation=str(mutation)):
                report = golden_report(self.dataset)
                mutation(report)
                result = self.run_gate(report)
                self.assertEqual(result.returncode, 2, result.stderr)

    def test_invalid_threshold_rejected(self):
        self.assertEqual(self.run_gate(golden_report(self.dataset),
                                      "--min-top1", "1.01").returncode, 2)


if __name__ == "__main__":
    unittest.main()
