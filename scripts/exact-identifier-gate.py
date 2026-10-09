#!/usr/bin/env python3
"""Fail an actual FTS/hybrid retrieval report when exact-identifier recall regresses.

Reports must come from scripts/retrieval-eval.py with a real adapter. This checker
does not perform retrieval or invent rankings. Contract-only tests are separate.
"""
import argparse
import json
from pathlib import Path
import sys


class GateError(ValueError):
    pass


def load_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise GateError("cannot read JSON input") from error


def evaluate_gate(dataset, report, min_top1=0.75, min_top3=0.75):
    """Return safe aggregate evidence or reject malformed/inadequate reports."""
    if dataset.get("schema_version") != 1 or not isinstance(dataset.get("queries"), list):
        raise GateError("unsupported dataset")
    queries = {q["id"]: q for q in dataset["queries"]}
    if len(queries) != len(dataset["queries"]) or len(queries) < 4:
        raise GateError("invalid query identities")
    for query in queries.values():
        if query.get("category") != "exact_name" or query.get("language") not in ("en", "sv"):
            raise GateError("only bilingual exact-name judgments are supported")
        if len(query.get("relevant", [])) != 1 or query["relevant"][0]["grade"] != 3:
            raise GateError("one direct ground truth required per query")
        if not query.get("excluded_source_ids"):
            raise GateError("every case needs a near-miss exclusion")
    if (report.get("schema_version") != 1
        or report.get("dataset_id") != dataset.get("dataset_id")
        or report.get("as_of") != dataset.get("as_of")
        or report.get("top_k", 0) < 3
        or not isinstance(report.get("results"), list)
        or not {"fts", "hybrid"}.issubset(set(report.get("modes", [])))):
        raise GateError("report is not an FTS/hybrid top-3 measurement of this dataset")
    if not isinstance(report.get("summary"), dict):
        raise GateError("missing harness summary")
    known_chunks = {c["id"] for p in dataset["pages"] for c in p["chunks"]}
    evidence = {}
    for mode in ("fts", "hybrid"):
        rows = [row for row in report["results"] if row.get("mode") == mode]
        if len(rows) != len(queries) or len({r.get("query_id") for r in rows}) != len(queries):
            raise GateError("missing or duplicate query measurements")
        by_id = {row["query_id"]: row for row in rows}
        if set(by_id) != set(queries) or mode not in report["summary"]:
            raise GateError("unexpected query identity or missing mode summary")
        at1 = at3 = 0
        by_language = {"en": 0, "sv": 0}
        by_family = {}
        for qid, query in queries.items():
            row = by_id[qid]
            ranked = row.get("ranked_source_ids")
            if (not isinstance(ranked, list) or len(ranked) > report["top_k"]
                or len(ranked) != len(set(ranked))
                or any(not isinstance(cid, str) or cid not in known_chunks for cid in ranked)):
                raise GateError("invalid ranked source identities")
            correct = query["relevant"][0]["source_id"]
            grades = [3 if cid == correct else 0 for cid in ranked]
            if row.get("grades") != grades or row.get("language") != query["language"] or row.get("category") != "exact_name":
                raise GateError("report grades or labels do not match ground truth")
            first = bool(ranked and ranked[0] == correct)
            top3 = correct in ranked[:3]
            at1 += first
            at3 += top3
            by_language[query["language"]] += top3
            family = qid.rsplit("-", 1)[0]
            by_family[family] = by_family.get(family, 0) + top3
        total = len(queries)
        language_totals = {lang: sum(q["language"] == lang for q in queries.values())
                           for lang in ("en", "sv")}
        passed = (at1 / total >= min_top1 and at3 / total >= min_top3
                  and all(by_language[lang] >= language_totals[lang] / 2
                          for lang in language_totals)
                  and all(count >= 1 for count in by_family.values()))
        evidence[mode] = {"top1": at1, "top3": at3, "total": total,
                          "by_language": by_language, "passed": passed}
    return evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="eval/retrieval/exact-identifiers-v1.json")
    parser.add_argument("--report", required=True, help="Output of retrieval-eval.py")
    parser.add_argument("--min-top1", type=float, default=0.75)
    parser.add_argument("--min-top3", type=float, default=0.75)
    args = parser.parse_args(argv)
    if not all(0 <= value <= 1 for value in (args.min_top1, args.min_top3)):
        print("exact-identifier-gate: thresholds must be in [0, 1]", file=sys.stderr)
        return 2
    try:
        evidence = evaluate_gate(load_json(args.dataset), load_json(args.report),
                                 args.min_top1, args.min_top3)
    except (GateError, KeyError, TypeError, AttributeError) as error:
        print(f"exact-identifier-gate: invalid measurement ({error})", file=sys.stderr)
        return 2
    for mode, result in evidence.items():
        print(f"{mode}: exact top-1 {result['top1']}/{result['total']}; "
              f"top-3 {result['top3']}/{result['total']}; "
              f"EN {result['by_language']['en']}, SV {result['by_language']['sv']}; "
              f"{'PASS' if result['passed'] else 'FAIL'}")
    return 0 if all(result["passed"] for result in evidence.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
