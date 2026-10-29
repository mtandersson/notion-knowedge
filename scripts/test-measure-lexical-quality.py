#!/usr/bin/env python3
"""Specifications for offline policy accounting; no model or quality simulation."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("measurement", Path(__file__).with_name("measure-lexical-quality.py"))
measurement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measurement)


def passing_evidence():
    dataset = {"queries": [
        {"id": "a", "relevant": [{"source_id": "one", "grade": 3}]},
        {"id": "b", "relevant": [{"source_id": "two", "grade": 3}]},
    ]}
    metrics = {"recall_at_5": 1., "mrr": 1., "ndcg_at_5": 1.}
    report = {"modes": ["vector", "fts", "hybrid"],
              "summary": {mode: {"metrics": dict(metrics)} for mode in ("vector", "fts", "hybrid")},
              "results": [
                  {"mode": mode, "query_id": q, "language": language, "category": category,
                   "ranked_source_ids": [source], "metrics": dict(metrics)}
                  for mode in ("vector", "fts", "hybrid")
                  for q, language, category, source in (("a", "sv", "exact_name", "one"), ("b", "en", "semantic", "two"))
              ]}
    return dataset, report


class EvidenceContracts(unittest.TestCase):
    def test_original_numeric_floors_do_not_round_failures_up(self):
        dataset, report = passing_evidence()
        self.assertTrue(measurement.policies(dataset, report)["cutover_offline"]["passed"])
        report["summary"]["hybrid"]["metrics"]["mrr"] = .89999
        verdict = measurement.policies(dataset, report)
        self.assertTrue(verdict["g1"]["passed"])
        self.assertFalse(verdict["cutover_offline"]["passed"])
        report["summary"]["hybrid"]["metrics"]["mrr"] = .79999
        self.assertFalse(measurement.policies(dataset, report)["g1"]["passed"])

    def test_missing_exact_top3_or_a_cohort_failure_blocks_cutover(self):
        dataset, original = passing_evidence()
        report = copy.deepcopy(original)
        next(row for row in report["results"] if row["mode"] == "fts" and row["query_id"] == "a")["ranked_source_ids"] = []
        self.assertFalse(measurement.policies(dataset, report)["cutover_offline"]["passed"])
        report = copy.deepcopy(original)
        next(row for row in report["results"] if row["mode"] == "hybrid" and row["query_id"] == "b")["metrics"]["recall_at_5"] = .89999
        self.assertFalse(measurement.policies(dataset, report)["cutover_offline"]["passed"])

    def test_cohorts_count_zero_hit_queries(self):
        _, report = passing_evidence()
        for row in report["results"]:
            row["language"] = "sv"
            if row["query_id"] == "b":
                row["metrics"] = {key: 0. for key in row["metrics"]}
        self.assertEqual(measurement.cohorts(report)["fts"]["language"]["sv"]["recall_at_5"], .5)

    def test_historical_failing_baseline_remains_a_failure(self):
        root = Path(__file__).resolve().parent.parent / "eval/retrieval"
        dataset = json.loads((root / "personal-knowledge-v1.json").read_text())
        report = json.loads((root / "results/production-qwen-2026-10-18-personal-ranking.json").read_text())
        verdict = measurement.policies(dataset, report)
        self.assertFalse(verdict["g1"]["passed"])
        self.assertFalse(verdict["cutover_offline"]["passed"])

    def test_preserved_adapter_evidence_includes_zero_hits_and_identities(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / "state"
            state.mkdir()
            evidence = {"results": [{"query_id": "q", "ranked_source_ids": []}], "lexical_identity": {"plan": "fixed"}}
            measurement.write(state / "adapter-fts.json", evidence)
            measurement.write(state / "input-fts.json", {"pages": [], "queries": []})
            measurement.write(state / "manifest.json", {"fixed": True})
            measurement.write(state / "snapshot.json", {"index_sha256": "fixed"})
            destination = root / "preserved"
            measurement.preserve_adapter(state, destination)
            self.assertEqual((state / "adapter-fts.json").read_bytes(), (destination / "adapter-fts.json").read_bytes())
            self.assertEqual(len(list(destination.iterdir())), 4)


if __name__ == "__main__":
    unittest.main()
