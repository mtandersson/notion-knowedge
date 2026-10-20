#!/usr/bin/env python3
"""Specifications for complete versus bounded lexical coverage evidence."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("coverage", Path(__file__).with_name("lexical-coverage.py"))
coverage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(coverage)


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.dataset = {"schema_version": 1, "dataset_id": "fictional", "as_of": "2026-01-01",
                        "pages": [{"chunks": [{"id": str(n)} for n in range(6)]}],
                        "queries": [{"id": "query", "language": "sv", "category": "semantic",
                                     "relevant": [{"source_id": "1", "grade": 3}, {"source_id": "2", "grade": 1}]}]}
        self.row = {"query_id": "query", "language": "sv", "category": "semantic", "mode": "fts",
                    "ranked_source_ids": ["0", "1"], "grades": [0, 3],
                    "metrics": coverage.harness.query_metrics({"1": 3, "2": 1}, ["0", "1"], 10)}
        self.report = {"schema_version": 1, "dataset_id": "fictional", "as_of": "2026-01-01",
                       "modes": ["fts"], "top_k": 10, "results": [self.row]}

    def test_complete_candidates_bound_oracle_and_retain_missing_sources(self):
        result = coverage.analyze(self.dataset, self.report)
        self.assertTrue(result["complete_candidate_coverage"])
        self.assertEqual(result["interpretation"], "oracle_ceiling")
        self.assertEqual(result["overall"]["mean_best_recall_at_5_with_observed_candidates"], 0.5)
        self.assertEqual(result["results"][0]["missing_relevant_ids"], ["2"])
        self.assertEqual(result["cohorts"]["language"]["sv"], result["overall"])

    def test_truncated_corpus_is_explicitly_not_a_ceiling(self):
        self.report["top_k"] = 5
        self.row["metrics"] = coverage.harness.query_metrics({"1": 3, "2": 1}, ["0", "1"], 5)
        result = coverage.analyze(self.dataset, self.report)
        self.assertFalse(result["complete_candidate_coverage"])
        self.assertEqual(result["interpretation"], "bounded_observation_not_a_ceiling")

    def test_unjudged_zero_hit_queries_remain_in_macro_mean(self):
        self.row.update(ranked_source_ids=[], grades=[], metrics=coverage.harness.query_metrics({"1": 3, "2": 1}, [], 10))
        result = coverage.analyze(self.dataset, self.report)
        self.assertEqual(result["overall"]["queries"], 1)
        self.assertEqual(result["overall"]["mean_best_recall_at_5_with_observed_candidates"], 0)

    def test_oracle_respects_five_result_capacity_even_with_all_candidates(self):
        judged = {str(n): 1 for n in range(6)}
        self.dataset["queries"][0]["relevant"] = [{"source_id": key, "grade": value} for key, value in judged.items()]
        self.row.update(ranked_source_ids=list(judged), grades=[1] * 6,
                        metrics=coverage.harness.query_metrics(judged, list(judged), 10))
        result = coverage.analyze(self.dataset, self.report)
        self.assertEqual(result["overall"]["mean_best_recall_at_5_with_observed_candidates"], 5 / 6)
        self.assertEqual(result["overall"]["queries_missing_relevant_candidates"], 0)

    def test_invalid_report_identities_and_candidates_fail_closed(self):
        cases = []
        for field, value in [("dataset_id", "other"), ("modes", ["hybrid"]), ("results", []), ("results", [0]), ("results", [self.row, self.row])]:
            report = copy.deepcopy(self.report); report[field] = value; cases.append(report)
        for field, value in [("ranked_source_ids", ["unknown"]), ("ranked_source_ids", ["1", "1"]), ("grades", [3, 3]), ("metrics", {}), ("language", "en")]:
            report = copy.deepcopy(self.report); report["results"][0][field] = value; cases.append(report)
        for report in cases:
            with self.subTest(report=report), self.assertRaises(ValueError):
                coverage.analyze(self.dataset, report)


if __name__ == "__main__":
    unittest.main()
