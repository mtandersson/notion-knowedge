#!/usr/bin/env python3
"""Collect complete fictional-corpus evidence; judgments stay in this harness."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent.parent


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


harness = module("quality_harness", "retrieval-eval.py")
coverage = module("quality_coverage", "lexical-coverage.py")
exact_gate = module("quality_exact", "exact-identifier-gate.py")


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def cohorts(report):
    return {mode: {
        field: {key: harness.macro_metrics([
            row["metrics"] for row in report["results"]
            if row["mode"] == mode and row[field] == key
        ]) for key in sorted({row[field] for row in report["results"] if row["mode"] == mode})}
        for field in ("language", "category")
    } for mode in report["modes"]}


def policies(dataset, report):
    """Original offline floors; passing these cannot establish live cutover."""
    grouped = cohorts(report)
    checks = {}
    for mode in ("fts", "hybrid"):
        metrics = report["summary"][mode]["metrics"]
        checks[mode] = {
            "recall_at_5": metrics["recall_at_5"] >= .95,
            "mrr": metrics["mrr"] >= .90,
            "ndcg_at_5": metrics["ndcg_at_5"] >= .90,
            "language_recall_at_5": all(v["recall_at_5"] >= .90 for v in grouped[mode]["language"].values()),
            "category_recall_at_5": all(v["recall_at_5"] >= .90 for v in grouped[mode]["category"].values()),
        }
    by_id = {q["id"]: q for q in dataset["queries"]}
    exact = {mode: all(
        any(j["grade"] == 3 and j["source_id"] in row["ranked_source_ids"][:3]
            for j in by_id[row["query_id"]]["relevant"])
        for row in report["results"]
        if row["mode"] == mode and row["category"] == "exact_name"
    ) for mode in ("fts", "hybrid")}
    hybrid = report["summary"]["hybrid"]["metrics"]
    g1 = {name: hybrid[name] >= .80 for name in ("recall_at_5", "mrr", "ndcg_at_5")}
    g1["exact_name_top3"] = exact["hybrid"]
    return {
        "g1": {"checks": g1, "passed": all(g1.values())},
        "cutover_offline": {"checks": checks, "exact_name_top3": exact,
                            "passed": all(all(c.values()) for c in checks.values()) and all(exact.values())},
        "live_pilot_and_cutover": "unmeasured; offline results cannot establish acceptance",
    }


def preserve_adapter(state, destination):
    destination.mkdir()
    for pattern in ("manifest.json", "snapshot.json", "input-*.json", "adapter-*.json"):
        for path in state.glob(pattern):
            shutil.copyfile(path, destination / path.name)


def measure(binary, assets, bundle_info, output):
    if os.environ.get("NK_LEXICAL_EXPANSION") != "true":
        raise ValueError("explicit lexical enablement required")
    bundle = Path(os.environ["NK_LEXICAL_ASSET_BUNDLE"])
    if digest(bundle) != os.environ["NK_LEXICAL_ASSET_SHA256"]:
        raise ValueError("configured lexical digest differs")
    output.mkdir(parents=True, exist_ok=False)
    write(output / "run-state.json", {"complete": False})
    provenance = {
        "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "compiler": subprocess.check_output(["rustc", "--version", "--verbose"], text=True).strip(),
        "build_profile": "release", "graph": "OFF", "quality_top_k": 10,
        "binary_sha256": digest(binary), "lock_sha256": digest(ROOT / "Cargo.lock"),
        "bundle": json.loads(bundle_info.read_text(encoding="utf-8")),
        "model_asset_sha256": {name: digest(assets / name) for name in ("config.json", "tokenizer.json", "model.safetensors")},
        "datasets": {}, "repeats": {},
    }
    verdicts = {}
    for corpus in ("personal", "exact"):
        filename = "personal-knowledge-v1.json" if corpus == "personal" else "exact-identifiers-v1.json"
        source = ROOT / "eval/retrieval" / filename
        dataset, _ = harness.load_dataset(source)
        shutil.copyfile(source, output / filename)
        provenance["datasets"][corpus] = digest(source)
        state = output / f"{corpus}-state"
        command = [str(binary), str(assets), str(state)]
        reports = []
        for repeat in (1, 2):
            report = harness.evaluate(dataset, command, ["vector", "fts", "hybrid"], 10, 3600)
            path = output / f"{corpus}-ranking-{repeat}.json"
            write(path, report)
            preserve_adapter(state, output / f"{corpus}-adapter-{repeat}")
            reports.append(report)
        first, second = [output / f"{corpus}-ranking-{repeat}.json" for repeat in (1, 2)]
        if first.read_bytes() != second.read_bytes():
            raise ValueError("complete ranking reports changed on repeat")
        identities = {}
        for mode in ("vector", "fts", "hybrid"):
            name = f"adapter-{mode}.json"
            a, b = [output / f"{corpus}-adapter-{repeat}" / name for repeat in (1, 2)]
            if a.read_bytes() != b.read_bytes():
                raise ValueError("complete adapter evidence changed on repeat")
            identities[mode] = digest(a)
        provenance["repeats"][corpus] = {"ranking_sha256": digest(first), "byte_identical": True,
                                           "adapter_sha256": identities}
        write(output / f"{corpus}-cohorts.json", cohorts(reports[0]))
        verdicts[corpus] = policies(dataset, reports[0])
        if corpus == "exact":
            write(output / "exact-regression-gate.json", exact_gate.evaluate_gate(dataset, reports[0]))
        candidates = harness.evaluate(dataset, command, ["fts"], 100, 3600)
        write(output / f"{corpus}-candidates.json", candidates)
        diagnostic = coverage.analyze(dataset, candidates)
        diagnostic["execution_scope"] = "Complete tiny-corpus candidate list for the bounded installed plan; not complete linguistic coverage."
        write(output / f"{corpus}-coverage.json", diagnostic)
        baseline = ROOT / "eval/retrieval/results" / f"lexical-coverage-2026-10-20-{corpus}-analysis.json"
        shutil.copyfile(baseline, output / f"{corpus}-baseline-coverage.json")
        write(output / f"{corpus}-coverage-comparison.json", {
            "baseline_sha256": digest(baseline),
            "baseline": json.loads(baseline.read_text(encoding="utf-8"))["overall"],
            "enabled": diagnostic["overall"],
            "interpretation": "Candidate availability diagnostic, not relevance or policy acceptance.",
        })
    write(output / "quality-policies.json", verdicts)
    write(output / "provenance.json", provenance)
    write(output / "run-state.json", {"complete": True, "quality_passed": all(
        v["g1"]["passed"] and v["cutover_offline"]["passed"] for v in verdicts.values())})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "assets", "bundle-info", "output"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args()
    measure(args.binary.resolve(), args.assets.resolve(), args.bundle_info.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
