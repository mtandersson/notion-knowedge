#!/usr/bin/env python3
"""Analyze actual FTS candidates after retrieval; never generate ranking inputs.

A complete ceiling requires top_k to cover the entire corpus. Larger corpora
are explicitly bounded observations and cannot establish a candidate ceiling.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import sys

spec = importlib.util.spec_from_file_location("retrieval_eval", Path(__file__).with_name("retrieval-eval.py"))
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


def analyze(dataset, report):
    queries = {query["id"]: query for query in dataset["queries"]}
    chunks = {chunk["id"] for page in dataset["pages"] for chunk in page["chunks"]}
    if not isinstance(report, dict):
        raise ValueError("candidate report must be an object")
    if (report.get("schema_version") != 1
            or report.get("dataset_id") != dataset["dataset_id"]
            or report.get("as_of") != dataset["as_of"]
            or report.get("modes") != ["fts"]
            or type(report.get("top_k")) is not int
            or not 5 <= report["top_k"] <= 100
            or not isinstance(report.get("results"), list)):
        raise ValueError("incompatible FTS candidate report")
    rows = report["results"]
    if (any(not isinstance(row, dict) for row in rows)
            or len(rows) != len(queries) or {r.get("query_id") for r in rows} != set(queries)):
        raise ValueError("missing or duplicate query measurements")
    complete = report["top_k"] >= len(chunks)
    results = []
    for row in rows:
        query = queries[row["query_id"]]
        ranked = row.get("ranked_source_ids")
        if (row.get("mode") != "fts" or row.get("language") != query["language"]
                or row.get("category") != query["category"]
                or not isinstance(ranked, list) or len(ranked) > report["top_k"]
                or any(not isinstance(cid, str) or cid not in chunks for cid in ranked)
                or len(ranked) != len(set(ranked))):
            raise ValueError("invalid lexical candidate identities or labels")
        judged = {item["source_id"]: item["grade"] for item in query["relevant"]}
        if (row.get("grades") != [judged.get(cid, 0) for cid in ranked]
                or row.get("metrics") != harness.query_metrics(judged, ranked, report["top_k"])):
            raise ValueError("report grades or metrics disagree with unchanged judgments")
        found = set(ranked) & set(judged)
        results.append({
            "query_id": query["id"], "language": query["language"], "category": query["category"],
            "candidate_count": len(ranked), "relevant_count": len(judged),
            "matched_relevant_ids": sorted(found), "missing_relevant_ids": sorted(set(judged) - found),
            "best_recall_at_5_with_observed_candidates": min(5, len(found)) / len(judged),
        })
    def aggregate(group):
        return {"queries": len(group),
                "queries_missing_relevant_candidates": sum(bool(r["missing_relevant_ids"]) for r in group),
                "mean_best_recall_at_5_with_observed_candidates": sum(r["best_recall_at_5_with_observed_candidates"] for r in group) / len(group)}
    return {"schema_version": 1, "dataset_id": dataset["dataset_id"], "corpus_chunks": len(chunks),
            "requested_top_k": report["top_k"], "complete_candidate_coverage": complete,
            "interpretation": "oracle_ceiling" if complete else "bounded_observation_not_a_ceiling",
            "overall": aggregate(results),
            "cohorts": {field: {key: aggregate([r for r in results if r[field] == key])
                                for key in sorted({r[field] for r in results})}
                        for field in ("language", "category")}, "results": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        dataset, _ = harness.load_dataset(args.dataset)
        result = analyze(dataset, json.loads(args.report.read_text(encoding="utf-8")))
        args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    except (OSError, ValueError, KeyError, TypeError, harness.EvaluationError):
        print("lexical-coverage: invalid dataset/report or unavailable output", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
