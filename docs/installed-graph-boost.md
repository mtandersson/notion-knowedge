# Installed relationship-aware retrieval

Graph boost is an optional reranker over the installed scoped hybrid adapter.
It adds no graph-only hits and does not resolve caller-supplied authority.
The ordinary server defaults to boost **off**, even when navigation is enabled.
Build with Unix `webhook-worker` and populate compatible committed state using
[authoritative reconciliation](manual-reconciliation.md). Supply the trusted
scope, protected index/SQLite paths and pinned offline model settings in
[installed retrieval](installed-retrieval.md).

| Startup setting | Default | Constraint |
| --- | --- | --- |
| `NK_GRAPH` | `false` | Explicit navigation activation; graph-only operation needs no model. |
| `NK_RETRIEVAL` | `false` | Explicit semantic/lexical/hybrid activation. |
| `NK_GRAPH_BOOST` | `false` | `true` requires both settings above. |
| `NK_GRAPH_BOOST_MAX_HOPS` | `1` | Integer 1–2. |
| `NK_GRAPH_BOOST_WEIGHT` | `0.15` | Finite value in (0, 0.5]. |
| `NK_GRAPH_BOOST_CANDIDATE_LIMIT` | `20` | Integer 1–100; search limit must fit. |

Settings are read and validated once. Invalid settings fail startup, including
invalid numeric settings supplied while boost is off. MCP JSON cannot enable or
retune boost. Semantic and lexical modes retain their behavior. Hybrid boost
retrieves the bounded real vector/FTS candidate pool, freshly authorizes every
candidate, and traverses only resolved edges between those candidates. Missing
or incompatible graph/index state, unfinished commits and graph errors fail
closed. One shared read fence spans hybrid snapshot opening, embedding, ancestry
checks and graph traversal; it excludes producer writes while allowing nested
readers. No read activation initializes, migrates, repairs or rebuilds state.

The existing seed, weight and hop rules in [graph boost](graph-boost.md) apply.
Returned `score` remains the baseline RRF score and `matched_paths` remains the
baseline provenance; enabled ordering may therefore differ from score order.
Only trusted stdio and authenticated loopback bearer HTTP install this private
runtime. HTTP still authenticates every session request; a physical root filter
is not caller authentication.

For an existing compatible index, check configuration before launching:

```sh
NK_GRAPH=true NK_RETRIEVAL=true NK_GRAPH_BOOST=false \
  target/debug/notion-knowledge-server --check
```

Keep boost off until the paired production-model quality gates and false-positive
review pass. An operator may explicitly opt into an experiment with
`NK_GRAPH_BOOST=true`; no measurement automatically activates serving.
For old/missing graph or alias projections, back up operational state and use the
explicit owned `--reconcile rebuild-relations RUN` and
`--reconcile rebuild-aliases RUN` workflows. These invalidate derived inventory
and repopulate through fresh authorized source reads; never delete SQLite or
hand-edit edges to repair production. See [index commits](index-commits.md),
[internal links](internal-page-graph-links.md) and [aliases](canonical-aliases.md).

## Joined credential-free smoke

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server \
  --features webhook-worker --locked --test webhook_worker joined_graph
python3 scripts/test-graph-boost-eval.py
```

The joined tests use real Notion loopback HTTP plus typed deterministic embedding
seams, then actual authoritative commits, persisted SQLite/Lance vector/FTS reopen,
and the common installed MCP handler over stdio and authenticated HTTP. They prove
relation and body-link families, canonical and ambiguous configured aliases,
Swedish/English source text, bounded navigation, off/on candidate and score
identity with a changed rank, caller-configuration rejection, edits, removal,
fresh denied-root suppression, unfinished effects and writer exclusion across an
awaited query. No sleeps emulate production admission, no candidates are invented,
and no credentials or model downloads are required. This is behavioral evidence,
not a production multilingual relevance benchmark.

## Trusted paired production-model evaluation

`scripts/installed-graph-eval-adapter.py` starts the **ordinary compiled server**
over stdio using the normal trusted environment. It never indexes request pages,
selects a model from caller JSON or creates a scripted rank list. Configure an
unchanging evaluation-only index containing the complete chosen dataset under
trusted physical roots, using the real production Qwen provider and normal
reconciliation/commit pipeline. Build its vector/FTS indexes as in installed
retrieval. Map the actual canonical IDs to the dataset's chunk IDs in a local
operator-reviewed manifest:

```json
{
  "schema_version": 1,
  "dataset_id": "personal-knowledge-fixtures-v1",
  "sources": {
    "fixture:chunk:energy-decision:decision": {
      "page_id": "<canonical Notion page UUID>",
      "chunk_id": "<actual committed stable chunk ID>",
      "text": "<exact matching dataset chunk text>"
    }
  }
}
```

Include **every** dataset chunk exactly once. This identity mapping is not a
relevance judgment or ranking. Text must equal the dataset and the returned
snippet (maximum 2,000 characters, with no URL-bearing content), so stale/foreign
mappings fail instead of fabricating scores. If production chunk boundaries do
not match the evaluation dataset, prepare a versioned dataset/judgments matching
those real chunks; do not guess IDs or silently drop sources. Preserve the
original dataset when comparing an existing baseline.

With the normal integration token and runtime settings supplied privately:

```sh
python3 scripts/graph-boost-eval.py \
  --adapter 'python3 scripts/installed-graph-eval-adapter.py --manifest /private/evaluation-sources.json --server target/debug/notion-knowledge-server' \
  --timeout 1800 --top-k 10 --output /private/graph-evaluation.json
```

The explicit evaluation-only timeout permits production Notion pacing across
twelve complete candidate pools (maximum 3,600 seconds per adapter run); the
default harness timeout and unit/CI timeouts are unchanged.
Both processes open the same installed index/model/scope. Only enabled changes;
full hop/weight/candidate settings are echoed. The adapter verifies exact outgoing graph targets against dataset links and source text,
returns actual baseline scores/path provenance, and fingerprints index/state
identity and modification metadata before/after. The paired harness rejects
revision changes and any difference in the complete candidate set. Pause writers
and use an unchanged evaluation index across the pair; an active producer is a
failed evaluation, not a zero-quality result. Output contains source identities
and metrics, so keep real-corpus reports private and review before sharing.

The #102 linked/control/guard gates still decide `keep_disabled` or
`candidate_for_review`; passing requires manual relevance and root-scope review.
No production-model report is claimed by the credential-free smoke. Record model
identity, corpus version, full config, report, safety/control findings and approval
separately before production activation.
