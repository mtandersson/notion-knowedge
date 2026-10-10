#!/usr/bin/env python3
"""Run fixed retrieval queries through a mode-aware JSON subprocess adapter.

The adapter must query a fixed model/index and return ranked chunk identities.
This tool does not build an index or claim production retrieval quality.
"""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys

from retrieval_metrics import macro_metrics, query_metrics

MODES = ("vector", "fts", "hybrid")
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class EvaluationError(ValueError):
    """Bad corpus, adapter or protocol; never a low retrieval score."""


def required_object(value, fields, location):
    if not isinstance(value, dict) or not fields.issubset(value):
        raise EvaluationError(f"{location}: expected {', '.join(sorted(fields))}")
    return value


def nonempty(value, location):
    if not isinstance(value, str) or not value.strip():
        raise EvaluationError(f"{location}: expected nonempty text")
    return value


def load_dataset(path):
    try:
        with path.open(encoding="utf-8") as stream:
            data = json.load(stream)
    except (OSError, ValueError) as exc:
        raise EvaluationError("cannot read dataset as UTF-8 JSON") from exc
    required_object(data, {"schema_version", "dataset_id", "as_of", "pages", "queries"}, "dataset")
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise EvaluationError("unsupported dataset schema")
    nonempty(data["dataset_id"], "dataset_id")
    nonempty(data["as_of"], "as_of")
    if not isinstance(data["pages"], list) or not data["pages"]:
        raise EvaluationError("dataset.pages must be nonempty")
    if not isinstance(data["queries"], list) or not data["queries"]:
        raise EvaluationError("dataset.queries must be nonempty")
    pages, chunks, queries = set(), set(), set()
    for page in data["pages"]:
        required_object(page, {"id", "title", "status", "updated_at", "links", "chunks"}, "page")
        page_id = nonempty(page["id"], "page.id")
        if page_id in pages:
            raise EvaluationError("duplicate page ID")
        pages.add(page_id)
        if not isinstance(page["chunks"], list) or not page["chunks"]:
            raise EvaluationError("page.chunks must be nonempty")
        for chunk in page["chunks"]:
            required_object(chunk, {"id", "text"}, "chunk")
            cid = nonempty(chunk["id"], "chunk.id")
            if cid in chunks:
                raise EvaluationError("duplicate chunk ID")
            chunks.add(cid)
    for query in data["queries"]:
        required_object(query, {"id", "text", "language", "category", "relevant", "excluded_source_ids"}, "query")
        qid = nonempty(query["id"], "query.id")
        if qid in queries:
            raise EvaluationError("duplicate query ID")
        queries.add(qid)
        nonempty(query["text"], "query.text")
        if not isinstance(query["relevant"], list) or not query["relevant"]:
            raise EvaluationError("query.relevant must be nonempty")
        judgments = set()
        for item in query["relevant"]:
            required_object(item, {"source_id", "grade"}, "judgment")
            cid = nonempty(item["source_id"], "judgment.source_id")
            if cid not in chunks or cid in judgments:
                raise EvaluationError("unknown or duplicated judgment source")
            judgments.add(cid)
            if type(item["grade"]) is not int or item["grade"] not in (1, 2, 3):
                raise EvaluationError("invalid judgment grade")
        excluded = query["excluded_source_ids"]
        if not isinstance(excluded, list) or any(
            not isinstance(cid, str) or cid not in chunks or cid in judgments
            for cid in excluded
        ):
            raise EvaluationError("invalid excluded source")
    return data, chunks


def run_adapter(command, request, timeout):
    try:
        process = subprocess.run(
            command, input=json.dumps(request, ensure_ascii=False),
            text=True, encoding="utf-8", capture_output=True,
            timeout=timeout, check=False,
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeError) as exc:
        raise EvaluationError("adapter unavailable, timed out or returned invalid UTF-8") from exc
    # Never echo adapter stdout/stderr: it may contain secrets, signed URLs or
    # private Notion page bodies, including in error messages.
    if process.returncode:
        raise EvaluationError(f"adapter failed (exit status {process.returncode})")
    if len(process.stdout.encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise EvaluationError("adapter response exceeds size limit")
    try:
        return json.loads(process.stdout)
    except ValueError as exc:
        raise EvaluationError("adapter returned invalid JSON") from exc


def validate_response(response, mode, data, chunks, top_k):
    required_object(response, {"schema_version", "dataset_id", "mode", "results"}, "response")
    if type(response["schema_version"]) is not int or response["schema_version"] != 1:
        raise EvaluationError("adapter schema mismatch")
    if response["dataset_id"] != data["dataset_id"] or response["mode"] != mode:
        raise EvaluationError("adapter dataset or mode mismatch")
    if not isinstance(response["results"], list):
        raise EvaluationError("adapter results must be a list")
    expected = {query["id"] for query in data["queries"]}
    seen = {}
    for entry in response["results"]:
        required_object(entry, {"query_id", "ranked_source_ids"}, "result")
        qid = entry["query_id"]
        if not isinstance(qid, str) or qid not in expected or qid in seen:
            raise EvaluationError("unknown or duplicate query ID from adapter")
        ranked = entry["ranked_source_ids"]
        if not isinstance(ranked, list) or len(ranked) > top_k:
            raise EvaluationError("ranked_source_ids must be a list within top_k")
        if any(not isinstance(cid, str) or cid not in chunks for cid in ranked):
            raise EvaluationError("adapter returned unknown chunk ID")
        if len(ranked) != len(set(ranked)):
            raise EvaluationError("adapter repeated a chunk ID")
        seen[qid] = ranked
    if seen.keys() != expected:
        raise EvaluationError("adapter must return exactly one entry per query")
    return seen


def evaluate(data, command, modes, top_k, timeout):
    chunks = {chunk["id"] for page in data["pages"] for chunk in page["chunks"]}
    request = {
        "schema_version": 1, "dataset_id": data["dataset_id"],
        "as_of": data["as_of"], "top_k": top_k,
        "pages": data["pages"],
        "queries": [
            {name: q[name] for name in ("id", "text", "language", "category")}
            for q in data["queries"]
        ],
    }
    results, summaries = [], {}
    for mode in modes:
        rankings = validate_response(
            run_adapter(command, {**request, "mode": mode}, timeout),
            mode, data, chunks, top_k,
        )
        relevant_hits = direct_hits = 0
        query_metric_rows = []
        for query in data["queries"]:
            judged = {j["source_id"]: j["grade"] for j in query["relevant"]}
            ranked = rankings[query["id"]]
            grades = [judged.get(cid, 0) for cid in ranked]
            metrics = query_metrics(judged, ranked, top_k)
            query_metric_rows.append(metrics)
            relevant_hits += any(grade > 0 for grade in grades)
            direct_hits += 3 in grades
            results.append({
                "mode": mode, "query_id": query["id"],
                "language": query["language"], "category": query["category"],
                "ranked_source_ids": ranked, "grades": grades,
                "metrics": metrics,
            })
        summaries[mode] = {
            "queries": len(data["queries"]),
            "queries_with_any_relevant_at_k": relevant_hits,
            "queries_with_direct_answer_at_k": direct_hits,
            "metrics": macro_metrics(query_metric_rows),
        }
    return {
        "schema_version": 1, "dataset_id": data["dataset_id"],
        "as_of": data["as_of"], "top_k": top_k, "modes": list(modes),
        "summary": summaries, "results": results,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=Path("eval/retrieval/personal-knowledge-v1.json"))
    parser.add_argument("--adapter", required=True, help="Executable and arguments, no shell")
    parser.add_argument("--modes", default=",".join(MODES))
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--output", default="-", help="JSON path or '-' for stdout")
    args = parser.parse_args(argv)
    try:
        modes = args.modes.split(",")
        if not modes or any(mode not in MODES for mode in modes) or len(modes) != len(set(modes)):
            raise EvaluationError("modes must be distinct vector,fts,hybrid names")
        if not 1 <= args.top_k <= 100 or not 0 < args.timeout <= 300:
            raise EvaluationError("top-k must be 1..100 and timeout must be (0,300] seconds")
        command = shlex.split(args.adapter)
        if not command:
            raise EvaluationError("adapter command must not be empty")
        data, _ = load_dataset(args.dataset)
        if args.output != "-" and Path(args.output).resolve() == args.dataset.resolve():
            raise EvaluationError("output may not overwrite dataset")
        report = evaluate(data, command, modes, args.top_k, args.timeout)
        serialized = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
        if args.output == "-":
            sys.stdout.write(serialized)
        else:
            Path(args.output).write_text(serialized, encoding="utf-8")
        for mode in modes:
            total = report["summary"][mode]
            print(
                f"{mode}: {total['queries_with_any_relevant_at_k']}/{total['queries']} "
                f"with relevant top-{args.top_k} hit; "
                f"{total['queries_with_direct_answer_at_k']} with grade-3 hit; "
                f"MRR={total['metrics']['mrr']:.4f}; "
                + ", ".join(
                    f"{key}={value:.4f}" for key, value in total["metrics"].items()
                    if key.startswith("recall_at_") or key.startswith("ndcg_at_")
                ),
                file=sys.stderr,
            )
        return 0
    except EvaluationError as exc:
        print(f"retrieval-eval: {exc}", file=sys.stderr)
        return 2
    except OSError:
        print("retrieval-eval: cannot write output", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
