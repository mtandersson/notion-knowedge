# Cited semantic MCP experiment (#214)

The explicit isolated `qwen-lance-spike` executable now composes the existing
`KnowledgeServer` with a provider-independent `core::search::SemanticSearch`
port and a local experimental adapter. `serve-stdio` uses the normal rmcp stdio
transport; `serve-http` calls the existing server's `http::serve_with_handler`
and retains its session, JSON-RPC, Host/Origin and listener policies. Both modes
construct the same adapter and application handler. The normal production
binary remains an honest unavailable bootstrap, with no model dependencies.
This experiment is not the completed production runtime or ChatGPT proof.

## Reproduction

Use the model assets and explicit selected-page index from
[Step 2](notion-vector-spike.md). No Notion token is needed to search an existing
local index; searches make no Notion requests. Keep the index and raw responses
outside Git because they contain source content.

```sh
nix develop .#spike
cargo build --locked --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -j 2
exe=crates/retrieval/spikes/qwen-lance/target/debug/qwen-lance-spike
"$exe" serve-stdio /tmp/qwen-assets /tmp/notion-index
# Or use the same application over Streamable HTTP:
NK_HTTP_PORT=3000 "$exe" serve-http /tmp/qwen-assets /tmp/notion-index
```

A client initializes MCP, sends `notifications/initialized`, discovers
`knowledge_search`, then calls it with the existing contract, for example:

```json
{"name":"knowledge_search","arguments":{"query":"Which organization did H. Norman Schwarzkopf Jr. command from 1988 to 1991?","limit":2,"mode":"semantic"}}
```

HTTP uses `POST /mcp`, Accept `application/json, text/event-stream` and the
returned `Mcp-Session-Id` after initialization. Stdio stdout contains only
protocol frames; model timing and sanitized diagnostics use stderr. The
configured HTTP Host/Origin policy still applies. No OAuth or public deployment
claim is made.

## Adapter behavior and limitations

Startup checks schema-v1 sidecar and table embedding identities, model asset
SHA256, Float32/1024 vector schema and the tiny 32-row bound. Unavailable assets,
index or incompatible identity do not fabricate results: the explicit server
stays available and semantic calls return a structured `retrieval_unavailable`
tool error. Lexical and hybrid return `mode_unavailable`. Unknown fields/options
and invalid query, limit or filter arguments retain JSON-RPC invalid-params.

Actual query embeddings use the same pinned Qwen revision, retrieval instruction,
last-token pooling, left-padding and L2 normalization as Step 2. One owned CPU
model is guarded by a mutex and inference runs on a blocking worker. Execution
cannot be cancelled once inference starts; production resource/queue bounds are
still required. The spike performs an exact cosine scan with ANN bypassed over
at most 32 rows. More rows fail explicitly, including growth detected while
reading results. Canonical record IDs/text must agree with their stored columns.

Filters use OR within each page/root list and AND across both fields. They
operate on persisted canonical metadata before top-limit truncation; IDs are
compared as strings and never inserted into SQL. Filtering only narrows this
already-derived authorized index, with no source discovery or access expansion.
This is not dynamic source authorization or freshness checking.

Results sort by descending `1 - cosine distance`, with stable chunk-ID ordering
for ties. Every score must be finite. Excerpts take at most 2000 Unicode
characters from each canonical chunk and retain its page/chunk ID, title,
Notion URL, heading path and optional block ID. This prefix truncation is not
query-aware snippet selection. The structured output matches the existing
schema; accompanying content and tool discovery mark excerpts as untrusted
source data. No minimum relevance threshold is invented: an unrelated query
can return irrelevant nearest neighbors. Empty results here reflect a completed
search whose scope filters match no rows, not a claimed quality classifier.

## Actual process evidence (2026-10-04)

Two separate executable processes reopened the retained 11-chunk selected-page
index, using the SHA256-verified local Qwen assets and matched `.#spike` compiler.
The program was built in the unoptimized dev profile with debug information
removed, as in Steps 1/2. No new source read, extra page or index rebuild occurred.
The actual stdio JSON-RPC and Streamable HTTP session responses were inspected;
raw response text remained in local `/tmp` artifacts. The ranked outputs below
contain only citation identities, finite scores and relevance observations.

Both transports returned the same two chunk identities and scores for all three
questions. The source was **H. Norman Schwarzkopf Jr.**, page ID
`3ef26b4f-5990-8161-b656-e6a30784d8ca`, with this
[verified selected-page link](https://app.notion.com/p/H-Norman-Schwarzkopf-Jr-3ef26b4f59908161b656e6a30784d8ca).
Rank 1 was page-level introductory content (`heading_path: []`, 313 characters),
chunk `nk-chunk-v1:984faf7a6c920014b6d05fa5964a15aba1e86726fc8171e0f551f0d99a2a6470`.
Rank 2 had heading path `["Källor"]` (774 characters), chunk
`nk-chunk-v1:d0b71b096a2a7182e76d47b7378966cf6f30c448dcf4b32e4de54878b62d7825`.
Each result's returned title, URL and stable page ID identified that same page.

| Question | Rank 1 score | Rank 2 score | Observed relevance |
| --- | ---: | ---: | --- |
| Vilken organisation var H. Norman Schwarzkopf Jr. chef för mellan 1988 och 1991? | 0.8410222 | 0.6436416 | Introductory passage naming U.S. Central Command returned first. |
| Which organization did H. Norman Schwarzkopf Jr. command from 1988 to 1991? | 0.8254355 | 0.6381480 | Same expected passage returned first. |
| How long should sourdough bread bake in a home oven? | 0.1113075 | 0.1074886 | Neither passage answers the question; irrelevant nearest neighbors remain visible. |

The earlier Step 2 question “Vilket år blev H. Norman Schwarzkopf Jr. fyrstjärnig
general?” remains a **relevance failure**: the expected promotion section was
not in its first two results. Repeating the successful question here does not
turn that failed outcome into a success. These four distinct real-page questions
plus Step 1's backup fixture question consume the epic's five-question budget.
Steps 4/5 must reuse these questions unless the scope is explicitly revised.
Repeated transport/filter checks used the same successful Swedish question.

Actual requests verified OR page/root lists containing an outside ID plus the
selected ID, AND across both lists, and zero results when either list excluded
the selected ID. Lexical/hybrid calls returned `mode_unavailable`; limit zero
returned JSON-RPC `-32602` without supplied query text. Both transports discovered
the same tool and successful responses marked source excerpts untrusted.
Separate actual processes with missing assets/index initialized successfully
and returned `isError: true`, `retrieval_unavailable`, no structured results and
no supplied query text. These are actual process responses, not mocked retrieval
or an SDK-only emulation.

Model verification/loading took 35,427 ms in the stdio process and 35,180 ms in
the HTTP process. Query embedding plus response handling was approximately
8–12 seconds per request (Swedish: 11,588 ms stdio / 11,593 ms HTTP; English:
10,904 / 10,818 ms; unmatched: 7,971 / 7,929 ms). These are single-run debug-profile
wall observations, including stream/response handling, on the existing host;
they do not establish production latency, cold OS-cache cost or peak memory.

## Verification and production follow-up

Model-free workspace tests exercise the injected handler through actual MCP
JSON-RPC, preserving citations/filter inputs and error behavior. Existing
process stdio and Streamable HTTP suites still cover the normal server.
Isolated adapter specifications cover AND/OR filters and Unicode excerpt bounds.
Ordinary CI needs neither model assets nor live Notion credentials/content.

#45 can reuse the domain search port, application injection and transport wiring.
It still owns production vector search, resource control, lifecycle and complete
metadata/filter/authorization requirements. #49 can reuse stable canonical
citation mapping and the bounded success schema, but still owns query-aware
snippets, deduplication, ranking and its complete evidence requirements. This
single-page experiment is neither a production ranking baseline nor their
completion. Manual refresh/restart/rebuild is documented in the
[Step 4 experiment](manual-refresh-spike.md); an actual ChatGPT
answer and source link belongs to #216.
