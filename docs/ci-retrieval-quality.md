# Offline retrieval quality gate (#103)

The permanent CI unit-test job runs the retrieval crate with its optional
local-lancedb feature. The new integration test at
crates/retrieval/tests/ci_retrieval_quality.rs is part of that job; it fails
CI when native search ranking drops below fixed thresholds.

## Corpus and measurements

The four independent fictional chunks cover English vehicle telemetry,
English sourdough, Swedish hiking and Swedish package pickup. There is no
private Notion content, credentials, external model download or network dependency.

Each of four known-answer queries is run against the real local LanceDB
BM25 FTS and IVF-Flat vector search (L2 distance). The expected top-1
chunk identity is compared to the actual result. The gate calculates
Recall@1 over all four queries separately for both retrieval paths.

| Path | Minimum recall@1 | Why |
| --- | --- | --- |
| Native FTS | 0.75 | Three of four exact bilingual queries should retrieve the expected chunk first |
| Native vector | 0.75 | Three of four synthetic nearest-neighbor queries should retrieve the expected chunk first |

The error message identifies the specific query IDs that regressed, not just
an aggregate score. The threshold allows one ranking shift but catches a
systematic regression. The vector inputs are hand-selected, deterministic
synthetic vectors, **not** Qwen embeddings: this tests the *real native storage
and ranking path*, not semantic model relevance or production search quality.

## Run and baseline maintenance

    nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-lancedb --test ci_retrieval_quality --locked

The existing CI Unit tests job runs the same integration test as part of
its local-lancedb feature suite, and CI gate requires the selected tests to pass.

For intentional tokenizer, model-space, or retrieval behavior changes:

1. Review the fictional fixture, expected IDs, and regression failures.
2. Explain the change and any proposed threshold adjustment in the PR.
3. Update tests only with explicit evidence, not solely to make CI green.
4. Run the dedicated test and feature suite and verify the GitHub CI gate.

The complete offline evaluation harness remains independently runnable as
described in [retrieval evaluation](../eval/retrieval/README.md). It measures
MRR/Recall/nDCG with a configured pinned *real* adapter; the current CI test
does not replace that benchmark. Actual model relevance, hybrid/graph
comparisons, authorized Notion sync, or production latency are not evaluated
by these synthetic probes.
