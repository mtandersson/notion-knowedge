# Primary ChatGPT MCP cutover acceptance gates (#115)

**Status: NOT READY — policy and executable-by-operator smoke plan only.** This
document defines the evidence required to make the custom `notion-knowledge`
MCP the **primary** ChatGPT surface and disconnect the official Notion
connector. It does not authorize that change today. Notion remains the
authoritative system for pages, properties and files; LanceDB is derived.
Read-only routing and rollback during the interim are defined in
[dual-connector migration](dual-connector-migration.md).

As of this issue, the ordinary server still has an unavailable retrieval
bootstrap, and authenticated private HTTP access, production full sync,
file ingestion and verified write tools are not all integrated. A local spike,
mock test or OAuth *discovery* response does not meet a production gate.
Any `not runnable`, unknown denominator, unavailable feature, unreviewed
security finding or missing evidence below means **no cutover**.

## Evidence and decision protocol

The operator must use one pinned production candidate (server Git SHA, model
revision/dimension, vector-space identity, index generation, Notion workspace
and allowed-root configuration) throughout a trial. Use a separately authorized
**disposable, non-sensitive test root** for mutations and negative security
tests. Conduct real read-only observations on approved scoped content only.
Keep a dated, access-controlled evidence record with test ID, expected and
observed result, mode/tool actually used, backend revision, source page ID,
timestamps, outcome and a *redacted* transcript or test report. Do **not**
commit or log private page bodies, tokens, signed URLs, uploaded files, or full
user queries. An error or a fallback to the official connector is not a
successful custom-MCP result.

Apply each gate below as pass/fail/not runnable. All gates must pass on the
**same release candidate**. Safety gates have zero tolerance; a later passing
attempt cannot erase a critical earlier failure without an investigated fix
and new full run. Retain evidence and sign-off date; do not claim results from
unit tests alone. Run the checks after deployment, not solely on localhost.

## Quantitative and functional release gates

| ID | Required result | Minimum evidence |
| --- | --- | --- |
| G0 — Security and availability | 100% of the authorization negatives deny access *before* data retrieval or mutation; **zero** successful cross-workspace, wrong-user, excluded-root, stale-grant or unauthenticated access. Authenticated ChatGPT discovers only real supported tools. No unresolved critical/high security defects. | Production-style OAuth/PKCE + grant-binding end-to-end suite (#117, #127–#129), hosted/private ChatGPT connection smoke (#109), and operator review of logs and routes. A tunnel/no-auth test is not sufficient. |
| G1 — Retrieval quality | Fixed evaluation dataset: **hybrid macro Recall@5 ≥ 0.80, nDCG@5 ≥ 0.80 and MRR ≥ 0.80**; **every exact_name query** has a grade-3 hit in top 3 for **both FTS and hybrid**. The real pinned adapter must return valid results for every query. No ungrounded or out-of-scope source citation. | Run the actual evaluation adapter on [personal-knowledge-v1](../eval/retrieval/personal-knowledge-v1.json) through [retrieval-eval.py](../scripts/retrieval-eval.py), `--modes vector,fts,hybrid --top-k 10`. Capture the JSON, inspect each exact-name query's graded top 3, and record model/index identity. The synthetic contract stub does **not** count. |
| G1b — Observed search quality | In at least **30 consecutive pilot days** and **50** diverse, independently source-judged Swedish/English production-intent questions, ≥ **95%** of expected authorized pages appear in top 5; **100%** of accepted answers have correct page attribution; **zero** silent stale/error substitutions. | A sampling log by category (paraphrase, exact title/ID, dates, linked pages, absent answer), with denominators and independent source read-backs. This preserves the staged acceptance policy of #114. |
| G2 — Direct files | At least **5 image and 5 non-image** supported file cases, including small and near-configured-limit sizes, succeed directly from ChatGPT to the **intended Notion page** with native block, content/type and read-back verification; **100%** pass. Unsupported/oversize cases fail safely and do not leave an attached partial file. **Zero** Drive staging or Agent Handover invocations. | Real ChatGPT file-parameter tests (#66–#75), durable per-case target/block references and safe metadata; do not use an upload stub or an official-connector fallback to count a pass. |
| G3 — Scoped read/write workflows | **100%** of the read and write smoke cases below pass, including create, append, targeted edit, property update, read-back, stale-write conflict, duplicate retry and denied out-of-scope mutation. Each user intent produces **at most one** authoritative write. No false success on timeout, source mismatch or unknown result. | Invoke production semantic tools through the actual ChatGPT connector with test-root permissions (#76–#83, #87, #117); independently re-fetch the authoritative Notion page. No destructive test on real pages. |
| G4 — Freshness and retry safety | In ≥ **100** timestamped synthetic create/update/delete/move events across a minimum **7-day** pilot, ≥ **95%** reach *correct* searchable state in ≤ **5 minutes** and ≥ **99%** in ≤ **15 minutes** measured from accepted source event to verified lexical **and** vector results. A complete authorized reconciliation runs at least every **24 hours**. **Zero** silently lost events, stale-owner commits or unauthorized tombstones; failures remain inspectable/retryable. | Worker inbox and reconciliation evidence (#55–#58, #242–#255), source reads plus actual index queries, and failure/replay tests. Separate API outage time from normal latency; an unavailable source must be reported, never silently excluded from the denominator. |
| G5 — Recovery | On an isolated **100-page disposable** corpus, a fresh authorized rebuild to both retrieval modes, safe checkpoint/replay reconciliation and source/provenance verification complete within **60 minutes** of operator start. The Notion source has **zero expected data loss**; locally unacknowledged work is either replayed or explicitly flagged, never silently discarded. | Follow [local-state recovery](local-state-recovery.md), back up/restore operational SQLite to staging, rebuild a distinct derived index from Notion, exercise interrupted apply-before-ack, and validate scope and vector identity. Retain old generation for rollback. This is an acceptance target, **not** a currently supported production restore command. |
| G6 — Standalone ChatGPT | With the **official Notion connector disconnected in a test ChatGPT workspace**, all applicable S1–S4, F1–F4, W1–W5 and Y1–Y4 workflows succeed solely via the custom MCP; **zero** invisible fallback to official Notion, Handover or Drive. The C3 rollback drill is separately evaluated with official routing restored. One full negative/security pass follows connector removal. | Human-observed ChatGPT invocation/response traces, tool discovery and authoritative Notion read-back. Any missing capability blocks primary cutover. |

**These numerical targets are provisional acceptance policy, not measured
quality or availability claims.** Pin this policy for the trial *before*
measuring it; a failing metric should lead to a fix or a separately reviewed
threshold change, not an unrecorded relaxation. Use the existing dataset's
graded *chunk*-level metric definitions. The exact-name rule means at least
one grade-3 judged chunk among the first three, **not** necessarily that all
chunks from a page must appear. Inspect actual adapter output: the harness'
exit status 0 means valid execution, not passing relevance.

Suggested reproducible evaluation (replace the adapter with the actual pinned
production-backed invocation, not the contract stub):

    python3 scripts/retrieval-eval.py \
      --adapter "/absolute/path/to/real-adapter" \
      --dataset eval/retrieval/personal-knowledge-v1.json \
      --modes vector,fts,hybrid --top-k 10 \
      --output /private/evidence/retrieval.json
    python3 scripts/test-retrieval-eval.py
    python3 scripts/test-retrieval-metrics.py

Run the same dataset on the same built index for every mode. The JSON
`summary.hybrid.metrics` carries the three macro metrics; inspect
`results` where `category == "exact_name"` for both modes and match
`grades` with the ranked chunk list. Record any failure by query ID, never
copy private query text into evidence. Future exact-identifier cases (#100)
and a true CI quality gate (#103) may strengthen this policy; they cannot be
replaced by the existing harness' deterministic stub.

## Operational smoke matrix

Run these **against the candidate connector and disposable root**, not against
the official connector. Confirm both resulting Notion source state and local
retrieval where applicable. If the custom tool is absent, mark **not runnable**.

| Case | Action | Pass criterion |
| --- | --- | --- |
| S1 | English and Swedish exact title/ID plus paraphrased search | Correct allowed source, stable citation and relevant chunk; no invented source |
| S2 | `knowledge_get` indexed vs fresh after an edit | Fresh authoritative content/timestamp; indexed view clearly marked stale or refuses; no silent mutation |
| S3 | Unknown page, unauthorized root and removed permission | Fail closed without leaking indexed text, identifiers beyond allowed diagnostics or redirecting to another identity |
| S4 | Valid Notion OAuth vs wrong user/workspace, expired/revoked grant and restart | Only bound identity can use the intended scope, every denial logged with safe correlation |
| F1 | ChatGPT-supplied image to allowed page | Native image block visible in read-back, correct bytes/type, no staging service |
| F2 | PDF/other non-image file | Native file block and downloadable matching test attachment verified |
| F3 | Retry/interrupted upload | No duplicate native blocks or false success; safe temporary cleanup |
| F4 | Oversize, invalid type, excluded parent | Consistent rejection; no external fetch/SSRF, no partial page mutation |
| W1 | Create knowledge page under selected root | New canonical Notion page ID/URL returned; authoritative read-back matches |
| W2 | Append and targeted section/property update | Unrelated blocks unchanged; correct final version and index-refresh indication |
| W3 | Retry a previously acknowledged create/append | One resulting effect, not two |
| W4 | Competing/stale update or changed scope | Clear conflict/denial, no overwrite and no fallback writer |
| W5 | Operator-disabled mutation/destructive request | Tool not advertised or rejected before backend changes; user confirmation required when enabled |
| Y1 | Create/update content and metadata-only change | Fresh full-page search, correct citations, stable unchanged chunk IDs and no needless re-embedding |
| Y2 | Delete, move out/into scope, restore | Confirmed authorized transitions reflected in lexical/vector search with no speculative delete |
| Y3 | Interrupted index apply before durable acknowledgment | New fence replays convergently, never loses a newer page or reports premature success |
| Y4 | Model/index restart and full reconciliation | No lost pending event, incompatible vector identity rejected, correct scoped inventory |
| C1 | Disconnect official Notion in isolated ChatGPT test | S1–W5 and relevant Y-cases still run entirely on custom tools |
| C2 | Negative query and missing feature after disconnect | No invented answer or invisible Drive/Handover/official fallback |
| C3 | Disable custom MCP during rollout dry-run | Re-enable official connector through manual rollback, then verify authoritative reads and source links |
| C4 | Repeat after recovery/restart | Same routing, scope and writes; no unsafe automatic reauthorization |

The operator should collect each case's pass/fail/not-runnable outcome and
sanitized source evidence, and verify the actual ChatGPT tool call. Never
count a direct standalone component test as a ChatGPT end-to-end smoke.
For destructive semantics use a **disposable test page only** and verify the
documented approval policy; no production deletion is authorized by this
runbook.

## Cutover decision and rollback

An operator and independent reviewer must check all G0–G6 gate groups and the
full smoke matrix, sign the evidence record, and explicitly authorize the
client-side change. First preserve a working official connection as the
fallback during the dual-connector pilot (#114); **only** after G0–G6 pass
should the operator disable it for the controlled primary cutover. Observe
the rollout, including missed changes, citation accuracy and error/fallback
classification; restore official routing on any privacy, authorization,
write-integrity, source-attribution or silent-staleness failure.

Rollback is a **manual client routing action**: stop custom requests, reconnect
the independently authorized official Notion connector, verify current source
read/write permissions and rerun the read/citation smoke. Never erase SQLite
journal receipts or overwrite Notion from LanceDB. An unsafe or unauthenticated
custom MCP must remain disabled until repaired and retested. See
[dual-connector migration](dual-connector-migration.md) and
[recovery](local-state-recovery.md).

### Evidence sign-off template (private operator record)

| Field | Value |
| --- | --- |
| Date/time, reviewer and decision | *unfilled* |
| Candidate Git SHA, deployment, model and index revision | *unfilled* |
| Approved Notion workspace/root fingerprint | *unfilled* |
| G0–G6 results with links to private evidence | *not run* |
| S1–C4 case status and failure investigations | *not run* |
| 30-day search sample and 7-day freshness samples | *not run* |
| Rollback drill result and explicit approval | *not run* |

**Decision for this issue:** the policy and operator test plan can be merged
independently. That is **not** the primary cutover itself. The official
Notion connector remains in place until separate, real end-to-end evidence
satisfies every release gate.
