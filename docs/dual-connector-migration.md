# Dual-connector migration playbook (#114)

This playbook defines **client-side routing and operator rollback**, not a new
MCP API, automatic failover proxy, production launch, or permission grant. During
rollout, the official Notion connector remains the authoritative fallback for
supported Notion operations. The local `notion-knowledge` MCP is an optional
*read/search accelerator* over derived local state; Notion remains the source
of truth. The production server currently advertises `knowledge_search` and
`knowledge_get`, but its default composition returns
`retrieval_unavailable`. A separate experimental Qwen/LanceDB spike can
demonstrate retrieval; it is not a private-production ChatGPT integration
([experiment](semantic-mcp-spike.md), [integrated retrieval smoke](retrieval-smoke.md)).

**Current security gate:** do not expose private Notion data through the
development/no-auth HTTP endpoint or interpret OAuth discovery metadata as
working login. The discovery-only mode deliberately rejects MCP requests.
A production private-data cutover requires OAuth, single-identity authorization,
scope enforcement and end-to-end verification (#117, #109, #129, #130).
Until then, use the official connector for private ChatGPT work and limit custom
MCP trials to a disposable, non-sensitive test root with approved access.

## Which connector handles an operation?

The routing policy applies to *user intent*, not the mere presence of similarly
named tools. Never call both connectors for the same **mutation**.

| Intent | Initial/unsupported custom MCP | After custom read capability is verified for that scope | Required behavior |
| --- | --- | --- | --- |
| Natural-language or exact Notion **search** | Official Notion search/read, where available | Prefer custom `knowledge_search` for permitted, indexed scope; fall back to official search if the custom capability is unavailable or fails | Preserve source page IDs/links, distinguish no matching results from unavailable/stale results; do not invent citations |
| Expand a **known search hit** or section | Official Notion page read | Prefer custom `knowledge_get` for authorized indexed references; request `freshness: "fresh"` where configured/required | For a guaranteed current read, use authoritative Notion; `fresh` may be unavailable and is not a cross-page snapshot |
| Unindexed/unknown page ID, tree navigation, database properties, exact current page text | Official Notion connector | Official Notion connector | Do not use custom `knowledge_get` as an arbitrary page-ID reader or bypass its indexed-root authorization |
| Upload, attach, inspect or download **images/files** | Official Notion connector's supported file workflow | Official Notion connector until dedicated upload tools, security controls and smoke tests (#9) are delivered | Do not route to a nonexistent custom upload tool or assume ChatGPT file parameters are supported |
| Create, append, update, delete, move, or change **Notion content/properties** | Official Notion connector, with user-approved scope | Official Notion connector until authenticated, scoped, verified custom write tools (#10/#11/#117) are delivered | Exactly one write path per operation; reconcile ambiguous results by rereading; never retry blindly or let a search fallback perform a write |
| Cross-source work, unrelated to Notion | Appropriate non-Notion connector | Same | Neither connector has implied access outside its authorized sources |

For searches that must be **provably current**, route directly to the official
Notion read/search capability; an indexed answer and a fresh authoritative
answer are not interchangeable. If custom retrieval returns a genuine empty
result from an in-scope complete index, present that outcome as an indexed
result. Do not automatically call the official connector *just to manufacture*
a hit, but offer or perform an explicit authoritative lookup when freshness or
index coverage is uncertain. If custom returns `retrieval_unavailable`,
`notion_unavailable`, a permission/scope error, or incomplete content,
do not quietly display stale/partial data as complete: switch to the official
route **only when it is independently authorized** and say which source was
used. A denial by either connector never authorizes trying another identity
or expanding the allowed roots.

## Minimize duplicate tools and conflicting results

- Identify the connectors unambiguously in the ChatGPT workspace:
  **Notion (official)** for authoritative source CRUD/files and
  **notion-knowledge (custom)** for scoped retrieval. Tool discovery and
  availability depend on the actual connected account; do not assume either
  catalog is present.
- Give custom tools the distinct `knowledge_*` names from their MCP contract
  ([search](knowledge-search.md), [fresh expansion](fresh-source.md)).
  Do not alias them to generic `search`, `get_page`, or `update_page` tools.
- Keep the official connector connected during the pilot. Prefer one route
  per user request; run shadow comparisons on **read-only** synthetic test data
  only when explicitly evaluating migration, not on each user request.
- Include the chosen connector, source ID/URL, indexed versus authoritative
  timestamp (when returned), and the failure class in operator notes.
  Never log page contents, signed URLs, raw OAuth tokens or uploaded files.
  Notion page links are evidence, not authentication.
- When two reads conflict, authoritative Notion wins. Flag any stale/missing
  custom result for reconciliation; do not merge text from conflicting versions
  or promote derived/index data back into Notion.

## Rollout and measurable reduction criteria

1. **Baseline (current):** official Notion serves real/private reads, file
   operations and writes. Record a small synthetic test set covering Swedish
   and English paraphrases, exact titles/IDs, modified and deleted pages,
   and authorized/out-of-scope targets. Record the current successful route.
2. **Isolated trial:** test custom `knowledge_search` and `knowledge_get`
   on a disposable non-sensitive root, then verify citations against Notion.
   The experimental spike is not evidence that the production connection,
   OAuth or file/write routing works.
3. **Supervised read pilot:** only after actual supported ChatGPT connection,
   authorized scoped production transport and index freshness checks exist,
   use custom retrieval for selected authorized **read-only** tasks. Keep
   official reads available as explicit fallback; compare failures and
   drift against the same source pages.
4. **Prefer custom search:** consider reducing *official search calls only*
   when the last **30 observed pilot days**, with at least **50 mixed
   source-grounded test queries**, show: at least **95%** of expected
   in-scope pages within top 5, **100%** correct/stable source attribution
   for answers using a result, **zero** successful out-of-scope retrievals,
   and **zero** silently masked stale/error results. Log query class,
   expected/not-found status and failures without storing private query text.
   These are **proposed migration thresholds**, not measured results.
5. **Write/file and primary cutover:** stay on the official connector until
   relevant upload/write/security features and smoke tests ship. [the primary cutover gates](cutover-gates.md) (#115) define
   the final numerical quality, freshness, recovery, file and write
   acceptance requirements before the official connector may be disabled.
   Passing the read-only pilot does **not** authorize that final step.

If a metric cannot be measured, coverage is too sparse, a critical authorization
finding is open, or the authoritative connector is not available as fallback,
the stage is **not** passed. Avoid false confidence from counting tool
invocations rather than completed supported workflows. Do not disable a
working official connection merely to remove duplicate tools.

## Operational smoke checklist (dry-run first)

Use a disposable **non-sensitive** Notion root and a test ChatGPT connection
only if supported. Record date, custom binary revision, index revision and
authorized workspace, but no secrets or raw private data. Inspect the tool
catalog before testing; treat missing custom tools as a failed prerequisite,
not an invitation to substitute another tool silently.

| Test | Action and expected result |
| --- | --- |
| A. Exact and paraphrase search | Query a known test page in each language, inspect actual source ID/link and indexed freshness; fallback remains available |
| B. Missing page/unknown answer | Query absent synthetic fact; no invented page citation, and genuine zero hits are not called an authorization failure |
| C. Index stale or adapter down | Modify a disposable page or stop the test adapter; identify stale/unavailable response, select official read for current content without presenting indexed text as fresh |
| D. Scope rejection | Query another, unauthorized root; no custom data disclosed; do not route through a different identity to bypass denial |
| E. Files and writes | Check the catalog and **route** an image upload or content update to official Notion only, with explicit target/user permission; in a routing-only dry run, do **not** execute a mutation |
| F. Duplicate-tool discipline | Confirm no request issued duplicate create/append/update, and shadow comparisons remain read-only |
| G. Rollback | Disable the custom connector (or its preference) in a disposable test session; repeat the read/search via official Notion and verify links/current text |

Record each result as `pass`, `fail` or `not runnable` with the tool
actually used. A documented checklist is **not** a claim these live tests
already passed. Retain sanitized evidence before advancing a rollout stage.

## Rollback and incident routing

**Trigger rollback immediately** on unexpected private-data access, unsafe
tool authorization, missed writes, wrong citations, silent stale results or
custom MCP outage outside pilot tolerance. Stop selecting custom tools for new
requests and disable/disconnect its client-side integration or revert the
routing preference; leave the official Notion connector enabled. Re-run the
read and citation smoke using authoritative Notion, verify its scope and
credentials independently, then investigate custom health and index
freshness offline. Never delete Notion pages or the durable SQLite sync
journal as a rollback measure. Derived LanceDB content can be rebuilt only
through its documented reconciliation/version policy after retaining needed
journal state. Do not replay an uncertain write.

Re-enable only after the failing path has a reproducible test, its root cause
is fixed and the applicable rollout smoke is green. This is a manual
operator/client policy; no automatic failover or toggle is implemented by
this documentation. See [diagnostics](diagnostics.md),
[release rollback](releasing.md), [threat model](threat-model.md)
and the [primary-cutover acceptance gates](chatgpt-cutover-acceptance.md) (#115).
