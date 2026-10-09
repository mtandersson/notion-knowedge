"""Pure, dependency-free graded ranking metrics for the fixed retrieval dataset.

All metrics are evaluated per query at the *chunk* level. Positive relevance
is grade 1..3; unjudged chunks have grade zero. Aggregates are macro averages
across queries, not pooled judgments or only successful results.
"""
from math import log2

RECALL_CUTOFFS = (1, 3, 5, 10)


def _discounted_gain(grades):
    return sum((2 ** grade - 1) / log2(rank + 1)
               for rank, grade in enumerate(grades, start=1) if grade)


def query_metrics(judged, ranked, top_k):
    """Compute finite cutoff metrics from complete judgments and a ranked prefix.

    `judged` maps source ID to relevance grade 1..3, `ranked` holds the
    adapter's returned IDs (which are already validated by the harness).
    Only cutoffs <= top_k are reported: a truncated retrieval must never be
    mislabeled as Recall@10. MRR uses the first positive result within top_k.
    """
    cutoffs = [k for k in RECALL_CUTOFFS if k <= top_k]
    ideal = sorted(judged.values(), reverse=True)
    relevant_total = len(judged)
    observed = [judged.get(source, 0) for source in ranked[:top_k]]
    result = {}
    for k in cutoffs:
        at_k = observed[:k]
        result[f"recall_at_{k}"] = sum(grade > 0 for grade in at_k) / relevant_total
        ideal_gain = _discounted_gain(ideal[:k])
        result[f"ndcg_at_{k}"] = (
            _discounted_gain(at_k) / ideal_gain if ideal_gain else 0.0
        )
    result["mrr"] = next(
        (1.0 / rank for rank, grade in enumerate(observed, start=1) if grade),
        0.0,
    )
    return result


def macro_metrics(rows):
    """Equal-weight per-query means, including zero-hit queries."""
    if not rows:
        raise ValueError("cannot aggregate an empty query set")
    keys = rows[0].keys()
    if any(row.keys() != keys for row in rows):
        raise ValueError("mismatched metric fields between queries")
    return {key: sum(row[key] for row in rows) / len(rows) for key in keys}
