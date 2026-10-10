#!/usr/bin/env python3
"""Independent, exact-valued unit tests for retrieval ranking metrics."""
import math
import unittest

from retrieval_metrics import macro_metrics, query_metrics


class RankingMetricsTests(unittest.TestCase):
    def test_exact_recall_mrr_and_exponential_gain(self):
        relevant = {"a": 3, "b": 2, "c": 1}
        # A distractor first, partial relevance second, direct answer third.
        metrics = query_metrics(relevant, ["x", "b", "a"], 10)
        self.assertEqual(metrics["recall_at_1"], 0)
        self.assertAlmostEqual(metrics["recall_at_3"], 2 / 3)
        self.assertAlmostEqual(metrics["recall_at_5"], 2 / 3)
        self.assertAlmostEqual(metrics["recall_at_10"], 2 / 3)
        self.assertAlmostEqual(metrics["mrr"], 0.5)
        dcg = 0 / math.log2(2) + 3 / math.log2(3) + 7 / math.log2(4)
        ideal = 7 / math.log2(2) + 3 / math.log2(3) + 1 / math.log2(4)
        self.assertAlmostEqual(metrics["ndcg_at_3"], dcg / ideal)
        self.assertAlmostEqual(metrics["ndcg_at_10"], dcg / ideal)

    def test_ideal_ranking_with_all_judgments_is_one(self):
        metrics = query_metrics({"a": 3, "b": 2, "c": 1}, ["a", "b", "c"], 10)
        self.assertEqual(metrics["mrr"], 1.0)
        self.assertEqual(metrics["recall_at_1"], 1 / 3)
        self.assertEqual(metrics["recall_at_3"], 1.0)
        self.assertEqual(metrics["ndcg_at_1"], 1.0)
        self.assertAlmostEqual(metrics["ndcg_at_3"], 1.0)
        self.assertAlmostEqual(metrics["ndcg_at_10"], 1.0)

    def test_graded_gain_distinguishes_direct_from_background(self):
        relevant = {"a": 3, "b": 1}
        direct = query_metrics(relevant, ["a", "b"], 10)
        background = query_metrics(relevant, ["b", "a"], 10)
        self.assertEqual(direct["recall_at_1"], background["recall_at_1"])
        self.assertEqual(direct["mrr"], background["mrr"])
        self.assertGreater(direct["ndcg_at_1"], background["ndcg_at_1"])
        self.assertGreater(direct["ndcg_at_3"], background["ndcg_at_3"])

    def test_empty_or_irrelevant_result_is_zero_not_failure(self):
        for ranked in ([], ["unjudged"]):
            with self.subTest(ranked=ranked):
                metrics = query_metrics({"a": 3}, ranked, 10)
                self.assertTrue(all(value == 0 for value in metrics.values()))

    def test_cutoffs_beyond_requested_depth_are_not_misrepresented(self):
        metrics = query_metrics({"a": 3, "b": 1}, ["a"], 1)
        self.assertEqual(set(metrics), {"recall_at_1", "ndcg_at_1", "mrr"})
        self.assertEqual(metrics["recall_at_1"], 0.5)
        metrics = query_metrics({"a": 3, "b": 1}, ["a"], 3)
        self.assertNotIn("recall_at_5", metrics)
        self.assertEqual(metrics["recall_at_3"], 0.5)
        self.assertLess(metrics["ndcg_at_3"], 1.0)

    def test_empty_macros_rejected_and_zeros_count_toward_mean(self):
        one = query_metrics({"a": 3}, ["a"], 10)
        zero = query_metrics({"a": 3}, [], 10)
        aggregate = macro_metrics([one, zero])
        self.assertEqual(set(aggregate), set(one))
        self.assertEqual(aggregate["recall_at_1"], 0.5)
        self.assertEqual(aggregate["mrr"], 0.5)
        self.assertAlmostEqual(aggregate["ndcg_at_10"], 0.5)
        with self.assertRaises(ValueError):
            macro_metrics([])
        with self.assertRaises(ValueError):
            macro_metrics([one, {"mrr": 0.0}])


if __name__ == "__main__":
    unittest.main()
