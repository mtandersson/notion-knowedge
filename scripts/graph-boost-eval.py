#!/usr/bin/env python3
"""Compare a real hybrid search adapter with optional graph boosting (#102).

This is a *measurement harness*, not a graph ranker or a semantic model.
The adapter must implement the documented graph_boost request/echo contract.
Synthetic test adapters verify protocol behavior, not actual retrieval quality.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import shlex
import sys

from retrieval_metrics import query_metrics, macro_metrics

BASE = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = BASE / "eval/retrieval/personal-knowledge-v1.json"
DEFAULT_CASES = BASE / "eval/retrieval/graph-boost-v1.json"
K = 3


def retrieval_module():
    spec = importlib.util.spec_from_file_location(
        "retrieval_eval", Path(__file__).with_name("retrieval-eval.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load_cases(path, data):
    cases = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(cases, dict) and cases.get("schema_version") == 1,
            "invalid graph case schema")
    require(cases.get("dataset_id") == data["dataset_id"],
            "graph case dataset mismatch")
    config = cases.get("config")
    require(isinstance(config, dict) and set(config) ==
            {"max_hops", "weight", "candidate_limit"}, "invalid graph config fields")
    require(type(config["max_hops"]) is int and 1 <= config["max_hops"] <= 2,
            "invalid hop limit")
    require(type(config["weight"]) in (float, int)
            and 0 < config["weight"] <= 1, "invalid boost weight")
    require(type(config["candidate_limit"]) is int
            and 1 <= config["candidate_limit"] <= 100, "invalid candidate limit")
    require(isinstance(cases.get("cases"), list) and cases["cases"],
            "empty graph cases")
    pages = {p["id"]: p for p in data["pages"]}
    chunks = {c["id"]: p["id"] for p in data["pages"] for c in p["chunks"]}
    queries = {q["id"]: q for q in data["queries"]}
    seen = set()
    links = set()
    controls = set()
    language_groups = {}
    for case in cases["cases"]:
        require(isinstance(case, dict) and set(case) ==
                {"query_id", "kind", "anchor_page_id", "target_source_id",
                 "guard_source_ids"}, "invalid graph case fields")
        qid = case["query_id"]
        require(isinstance(qid, str) and qid in queries and qid not in seen,
                "duplicate/unknown graph query")
        seen.add(qid)
        kind = case["kind"]
        require(kind in ("linked", "negative_control"), "invalid graph case kind")
        anchor = case["anchor_page_id"]
        require(isinstance(anchor, str) and anchor in pages, "unknown anchor page")
        guard = case["guard_source_ids"]
        require(isinstance(guard, list) and bool(guard)
                and len(set(guard)) == len(guard)
                and all(isinstance(cid, str) and cid in chunks for cid in guard)
                and set(guard) <= set(queries[qid]["excluded_source_ids"]),
                "invalid or non-excluded graph guard")
        target = case["target_source_id"]
        if kind == "linked":
            require(queries[qid]["category"] == "cross_link",
                    "linked case must be cross-link query")
            require(isinstance(target, str) and target in chunks,
                    "linked case target missing")
            require(chunks[target] in pages[anchor]["links"],
                    "linked target must be a resolved direct edge")
            require(any(j["source_id"] == target and j["grade"] == 3
                        for j in queries[qid]["relevant"]),
                    "linked target must be directly relevant")
            require(target not in guard, "target cannot be excluded")
            links.add(qid)
        else:
            require(target is None, "control must not target a graph source")
            controls.add(qid)
        stem = qid.rsplit("-", 1)[0]
        language_groups.setdefault(stem, set()).add(queries[qid]["language"])
    require(len(links) >= 6 and len(controls) >= 6,
            "require bilingual positive and negative coverage")
    require(all(langs == {"sv", "en"} for langs in language_groups.values()),
            "paired bilingual cases required")
    return cases, queries, chunks


def run_pair(data, cases, queries, chunks, adapter, timeout, top_k):
    harness = retrieval_module()
    selected = [queries[c["query_id"]] for c in cases["cases"]]
    focused = {**data, "queries": selected}
    base = {
        "schema_version": 1, "dataset_id": data["dataset_id"],
        "as_of": data["as_of"], "mode": "hybrid",
        "top_k": top_k, "pages": data["pages"],
        "queries": [{field: q[field] for field in ("id", "text", "language", "category")}
                    for q in selected],
    }
    rankings = {}
    for enabled, name in ((False, "baseline"), (True, "boosted")):
        requested_config = {**cases["config"], "enabled": enabled}
        response = harness.run_adapter(
            adapter, {**base, "graph_boost": requested_config}, timeout
        )
        harness.required_object(response, {"graph_boost"}, "graph response")
        # Exact config echo makes unsupported or silently ignored graph modes
        # ineligible as valid evidence. Types must match (bool != int).
        echoed = response["graph_boost"]
        require(isinstance(echoed, dict)
                and json.dumps(echoed, sort_keys=True) ==
                json.dumps(requested_config, sort_keys=True),
                "adapter did not acknowledge exact graph configuration")
        rankings[name] = harness.validate_response(
            response, "hybrid", focused, set(chunks), top_k
        )
    return rankings


def compare(data, cases, queries, rankings, top_k):
    require(type(top_k) is int and K <= top_k <= 100, "top-k must be 3..100")
    rows = []
    linked_rows, control_rows = [], []
    guard_regressions = []
    target_regressions = []
    target_improvements = 0
    for case in cases["cases"]:
        qid = case["query_id"]
        q = queries[qid]
        judged = {j["source_id"]: j["grade"] for j in q["relevant"]}
        before = rankings["baseline"][qid]
        after = rankings["boosted"][qid]
        metric_before = query_metrics(judged, before, top_k)
        metric_after = query_metrics(judged, after, top_k)
        target = case["target_source_id"]
        rank_before = before.index(target) + 1 if target in before else None
        rank_after = after.index(target) + 1 if target in after else None
        new_guards = sorted((set(after[:K]) - set(before[:K]))
                            & set(case["guard_source_ids"]))
        if new_guards:
            guard_regressions.append(qid)
        if target is not None and rank_before is not None and rank_before <= K:
            if rank_after is None or rank_after > K:
                target_regressions.append(qid)
        if target is not None and rank_after is not None:
            if rank_before is None or rank_after < rank_before:
                target_improvements += 1
        row = {
            "query_id": qid, "kind": case["kind"], "language": q["language"],
            "baseline": {"ranked_source_ids": before, "metrics": metric_before},
            "boosted": {"ranked_source_ids": after, "metrics": metric_after},
            "target_rank_before": rank_before, "target_rank_after": rank_after,
            "new_guard_sources_top3": new_guards,
        }
        rows.append(row)
        (linked_rows if case["kind"] == "linked" else control_rows).append(row)

    def means(group, variant):
        return macro_metrics([r[variant]["metrics"] for r in group])

    lb, la = means(linked_rows, "baseline"), means(linked_rows, "boosted")
    cb, ca = means(control_rows, "baseline"), means(control_rows, "boosted")
    eligible = (target_improvements > 0
                and la["ndcg_at_3"] > lb["ndcg_at_3"] + 0.001
                and ca["ndcg_at_3"] + 0.001 >= cb["ndcg_at_3"]
                and ca["mrr"] + 0.001 >= cb["mrr"]
                and not guard_regressions and not target_regressions)
    return {
        "schema_version": 1, "dataset_id": data["dataset_id"],
        "as_of": data["as_of"], "config": cases["config"], "top_k": top_k,
        "evidence": "adapter_measured",
        "decision": "candidate_for_review" if eligible else "keep_disabled",
        "reason": ("Measured relevant lift without specified regressions; manual "
                   "review still required" if eligible else
                   "Insufficient graph lift or safety/control regression"),
        "summary": {
            "linked_queries": len(linked_rows), "control_queries": len(control_rows),
            "linked": {"baseline": lb, "boosted": la},
            "negative_controls": {"baseline": cb, "boosted": ca},
            "target_rank_improvements": target_improvements,
            "new_guard_regressions": guard_regressions,
            "target_top3_regressions": target_regressions,
        }, "results": rows,
    }


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    p.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    p.add_argument("--adapter", help="Trusted local adapter command (no shell)")
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--timeout", type=float, default=60)
    p.add_argument("--output", default="-")
    p.add_argument("--validate-only", action="store_true")
    args = p.parse_args(argv)
    try:
        require(K <= args.top_k <= 100 and 0 < args.timeout <= 300,
                "invalid top-k or timeout")
        harness = retrieval_module()
        data, _ = harness.load_dataset(args.dataset)
        cases, queries, chunks = load_cases(args.cases, data)
        if args.validate_only:
            require(not args.adapter, "validate-only cannot run adapter")
            print(f"graph cases valid: {len(cases['cases'])}")
            return 0
        require(bool(args.adapter), "--adapter is required for measured evaluation")
        command = shlex.split(args.adapter)
        require(bool(command), "empty adapter command")
        if args.output != "-":
            require(Path(args.output).resolve() not in (
                args.cases.resolve(), args.dataset.resolve()),
                "refusing to overwrite evaluation input")
        rankings = run_pair(data, cases, queries, chunks, command, args.timeout, args.top_k)
        report = compare(data, cases, queries, rankings, args.top_k)
        result = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
        if args.output == "-":
            sys.stdout.write(result)
        else:
            Path(args.output).write_text(result, encoding="utf-8")
        print(f"graph boost: {report['decision']}; "
              f"{report['summary']['target_rank_improvements']} improved target ranks; "
              f"{len(report['summary']['new_guard_regressions'])} new guard violations",
              file=sys.stderr)
        return 0
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        # Never echo adapter stdout/stderr or any query/document content.
        print(f"graph-boost-eval: {type(exc).__name__}: invalid data/config/adapter",
              file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
