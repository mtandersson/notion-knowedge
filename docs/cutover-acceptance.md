# Cutover acceptance: custom MCP as the primary ChatGPT Notion surface (#115)

**Status: specification only — NOT APPROVED for production cutover.**
This document defines quantitative, auditable **go/no-go criteria**. It neither
makes the current experimental custom MCP production-safe nor claims any
threshold has passed. Notion stays authoritative. Follow the
[dual-connector routing and rollback playbook](dual-connector-migration.md)
throughout; the official Notion connector remains enabled until an explicit
operator cutover decision based on the evidence below.

## Decision scope and stages

There are **two separate decisions**:

1. **Read preference**: route selected authorized search/expansion requests to
   `knowledge_search` / `knowledge_get`; the official connector continues
   serving source-of-truth reads, uploads and writes. Its provisional 30-day
   pilot criteria are already defined in [#114](dual-connector-migration.md).
2. **Primary cutover**: all **required, supported** daily Notion operations can
   be completed with the custom MCP alone, and disconnecting the official
   connector does not remove any required workflow. This is **not** authorized
   by a successful read-only pilot. Every gate A–F below must be independently
   green, and the operator must explicitly approve the cutover.

There is no implicit automatic failover, automatic switch in ChatGPT, or
permission to move sensitive data to a development/no-auth tunnel. OAuth
discovery metadata or one successful spike conversation does not prove a
production private-data connection. Before *any* production-data pilot, the
production OAuth/ChatGPT connection, allowed identity/workspace, scoped tool
authorization and end-to-end rejection tests must be complete (notably
#117/#109/#129/#130 and the relevant #11 security controls). Record actual
client/tool capabilities: never count a missing tool as an exercised workflow.

## Evidence ledger and accounting

Keep a sanitized, durable acceptance ledger outside private data sources,
with one row per gate and per reproducible test run. Suggested fields:

| Field | Required evidence |
| --- | --- |
| Identity | Gate/case ID, run time (UTC), operator/reviewer, verdict `pass`/`fail`/`not-runnable` |
| Provenance | Git commit, packaged image digest, index generation, model/provider revision, fixture dataset ID, configuration fingerprint, environment and ChatGPT connector version/name |
| Execution | Exact command/test or documented manual procedure, selected tool, expected result, observed result, sample count and time window |
| Result | Counts/numerators/denominators, distribution/percentiles where specified, sanitized source identifiers and citations, replayable artifact/log location |
| Review | Root-cause ticket for each failure, remedial PR and retest evidence, independent reviewer, final operator decision |

Never store OAuth tokens, private page bodies, signed URLs, uploaded-file contents
or raw private queries in the ledger. Track queries by class and opaque case ID;
source IDs/URLs may be retained only in a suitably private evidence store.
`not-runnable`, missing proof and inconclusive/stale results are **not passes**.
Tests must exercise real adapters and the intended ChatGPT custom connection;
fixture-only/unit tests supplement, but do not replace, production integration
evidence. Record *why* a check was skipped instead of silently reducing the
denominator. A red critical safety case cannot be averaged away.

## Gate A — grounded retrieval quality

**Prerequisites:** real adapter bridge to the versioned
[retrieval eval](../eval/retrieval/README.md); pinned and reproducible
model/index configuration; authorized scope; stable citations. Compare the
same corpus and configuration in `fts`, `vector` and `hybrid`. Run:

```sh
python3 scripts/retrieval-eval.py \
  --adapter "./path/to/trusted-production-retrieval-adapter" \
  --dataset eval/retrieval/personal-knowledge-v1.json \
  --modes vector,fts,hybrid --top-k 10 \
  --output /private/evidence/retrieval-eval.json
```

### Offline fixture thresholds (proposed policy, not observed results)

- **Hybrid** macro `recall_at_5 >= 0.85`, `mrr >= 0.75` and
  `ndcg_at_10 >= 0.80` on the pinned 24-query Swedish/English fixture,
  evaluated over **all** queries including zero-hit queries.
- **FTS exact-name** cases must have a grade-3 (direct-answer) chunk within
  the first five results for **every** query in the fixture's `exact_name`
  category; use query-level grades, not the general binary Recall@K, for this
  additional guardrail. Run the separately versioned exact-identifier dataset
  when #100 ships, with its own explicit lexical/hybrid floor.
- No material regression compared to the last approved adapter/model/index
  baseline: decrease in any above macro metric **no more than 0.03 absolute**;
  record both full reports, not only a rounded percentage.
- Both languages must be included, with no silently omitted queries or modes.
  The fixture measures internal consistency, **not** real-world relevance.

### Live source-grounded pilot (required independently)

Over **30 observed pilot days**, at least **50** independently verified,
mixed-class authorized queries are required, with **at least 20 Swedish and
20 English**, at least **10 exact title/ID** and **10 paraphrase/semantic**
cases, and both current and changed/archived-page cases. Tests may satisfy
multiple categories but count once in the overall denominator. Hold a fixed
evaluation set without tuning answers against the expected results.

- At least **95%** of known relevant in-scope pages appear by **rank five**
  (for 50 cases, at least 48 passes). Record per-case failures and inspect
  them; a broad page with many chunks does not multiply successes.
- **100%** of used citations resolve to the correct, authorized Notion page,
  with accurate indexed/fresh source state.
- **Zero** out-of-scope results, permission bypasses, fabricated attributions
  or silent stale/partial results. These are hard blockers even if the mean
  quality score passes.
- Compare expected source truth using the authoritative Notion connector or a
  controlled authoritative test read; do **not** label an absent/stale index
  result correct solely because search returned HTTP 200.

These observed pilot thresholds intentionally preserve the criteria in
[the #114 playbook](dual-connector-migration.md). A real adapter and
source-grounded reports must exist before this gate can turn green.

## Gate B — direct images and files without Drive/Handover

Use the approved ChatGPT file parameter transport, server-side validation,
Notion's native file-upload/attachment API, and authoritative read-back.
Exercise a disposable, authorized non-sensitive test root. The custom MCP
**itself**, not a staging Drive, external agent handover, or official Notion
connector, must complete each required operation.

Minimum case matrix, with **two successful repetitions of each** after a
fresh server/session start:

| Case | Required success evidence |
| --- | --- |
| PNG and JPEG image, with and without caption | Native Notion image block, correct target and caption, read-back of uploaded object |
| PDF or ordinary non-image document | Native Notion file block and correct target/read-back, no image-only assumption |
| Two attachments in one request | Correct per-file attachment and ordering, no dropped/duplicated uploads |
| Lost acknowledgment or retried request | No duplicate attachment, or explicit ambiguity/recovery requiring operator intervention before retry |
| Invalid MIME, oversized payload, bad target, out-of-scope target | Structured refusal, zero committed unauthorized blocks, no SSRF or secret echo |
| Expired URL, Notion upload failure, failed read-back | No success claim; safe cleanup/recovery and explicit retryable outcome |

**Pass:** 100% of the positive cases have independently verified target
block IDs and uploaded-file identities; 100% of negative cases fail closed;
zero requirement to call Drive, Handover or the official Notion connector.
Store only metadata such as fixture checksum/size and stable block IDs, never
file contents or temporary signed URLs. The #9 direct-file issues and relevant
#11 controls must actually be integrated and tested before this gate passes.

## Gate C — scoped, safe read and write smoke

Use the actual authenticated ChatGPT-to-MCP connection and the allowed Notion
workspace/root. Exercise every **required** normal workflow, not only a
server-local API call. Maintain a signed-off catalog mapping user intents to
the custom tool, current capability and verified result. At minimum:

| Operation | Positive evidence | Negative/recovery evidence |
| --- | --- | --- |
| Search and section expansion | Current source citation; indexed vs fresh stated | Unknown or forbidden source cannot widen root |
| Create and append | Correct authorized parent and exact content on authoritative read-back | Retry/idempotency does not create duplicates |
| Targeted update | Only intended section/property changes | Concurrent Notion edit produces conflict, not a blind overwrite |
| Archive/delete/replace (if enabled) | Explicit user authorization and configured confirmation | Unconfirmed and out-of-scope requests cause no mutation |
| OAuth grant changes/revocation | Only configured owner+workspace continue to work | Expired/revoked or different identity fails closed |
| Source/Notion unavailable | Classified error and truthful user-facing status | No success placeholder, stale substitution or cross-identity fallback |

**Pass:** every supported required positive and negative case succeeds on
**two independent clean runs** and authoritative read-back confirms exactly
one intended mutation. No unexpected or unauthorized write, leaked private
content, silently accepted overwrite or untracked uncertain retry is allowed.
Out-of-scope read and mutation attempts must both be rejected. These are
hard-stop criteria, never percentage allowances. Unimplemented required
tools or unsafe identity/permission behavior keep the gate red.

## Gate D — freshness, restart, journal and recovery

The sync freshness SLO applies only to pages demonstrably **in the authorized
selected scope** and under a supported complete indexing lifecycle. Notion is
the source of truth; local SQLite journal and LanceDB are derived/operational
state, not an independent authority. Instrument real end-to-end latency from
confirmed source change to changed result visible through ChatGPT
`knowledge_search`, including index maintenance.

**Proposed production cutover targets:**

- Across **at least 30** controlled create/update/delete/move/restore cases
  over **14 consecutive days**, **p95 <= 5 minutes** and
  **p99 <= 15 minutes** from authoritative source change to correct search
  visibility/tombstone. Record the full sample, method and timestamps;
  p99 on small samples is a guardrail, not evidence of general tail latency.
- Repeat duplicate and reordered webhook delivery, source API outage and
  two process restarts; zero acknowledged-but-missing effects, zero
  resurrection of out-of-scope/deleted pages, and zero stale writes.
- Run an authoritative reconciliation at least **daily** throughout the
  14-day window, with no unresolved difference lasting over **24 hours**.
  Show its actual dry-run, apply, retry and resume evidence rather than
  assuming a scheduled timer works.
- Demonstrate **two independent disaster-recovery drills**: loss of
  derived LanceDB, SQLite state restore from a documented backup, safe
  replay of uncertain apply-before-checkpoint work, then fresh authoritative
  Notion rescan. Target **RTO <= 60 minutes** to restored searchable coverage
  for the controlled fixture, and **zero loss of confirmed Notion source
  mutations** (this is *not* a zero-loss guarantee for any unsupported
  operational events). Never depend on the previous generation's vector IDs.
- The operator can tell **fresh**, **stale**, **unknown** and **unavailable**
  apart; missing ancestry/scope proof does not authorize deletion.

Measure from authoritative events and durable checkpoints; an in-memory queue
or initial one-off successful rebuild does not pass this gate. Dependencies
include actual #55/#56/#57/#58 production lifecycle and #111 backup/recovery
evidence. Until these ship and their timing is observed, D is **not runnable**.

## Gate E — supportability and security

Before a primary connection is preferred, run deployment/operational checks
against the **exact candidate image/revision**:

- CI gate green for that revision; production image smoke, HTTP transport
  health/readiness and an actual ChatGPT reconnect are successful.
- OAuth flow, refresh, revocation, allowlisted user/workspace and root/operation
  policies tested end-to-end. No private production data crosses an anonymous
  development transport or a different authorized identity.
- Operator can inspect sync age, retry/dead-letter counts, rebuild status and
  redacted correlation identifiers; no raw secrets/private document text in
  operational logs. A documented alert/failure route exists.
- Backup location/restore permissions and the on-call rollback procedure are
  reviewed by an independent reviewer; no unreviewed critical security issue.

**Pass:** all items verified, zero open critical authorization/privacy flaws;
a passing implementation unit test alone is insufficient.

## Gate F — disable-the-official-connector rehearsal

This is the final **workflow parity** decision, after A–E are green. Preserve
the official connector configured for rapid rollback until the trial ends,
but disconnect/disable it **only in an explicitly approved, separate disposable
test session** for the rehearsal; do not silently change the user's production
ChatGPT connector settings.

During a **14-day supervised rehearsal**, run at least **20 end-to-end**
workflows representative of real intended use: minimum five reads/searches,
five safe creates/appends/updates, five image/file cases and five freshness or
failure/recovery cases. Confirm the custom tool actually handled each request.
One workflow is counted successful only after authoritative Notion read-back,
verified citations (if relevant), and absence of fallback tools.

**Pass:** 20/20 complete through custom MCP; zero dependence on the official
connector or Drive/Handover, zero unsafe effects, and explicitly demonstrated
client-side rollback: restore official route, repeat authoritative read/search,
and preserve any partial/uncertain-write investigation. A missing required
workflow is a failed cutover, not an invitation to quietly lower the checklist.
More extensive private-domain workflows must be represented if the user's
actual required catalog exceeds this minimum.

## Final go/no-go and rollback decision

Fill in the following template **only after evidence exists**:

| Gate | Verdict | Evidence link/revision | Reviewer |
| --- | --- | --- | --- |
| A. Retrieval | Not measured | — | — |
| B. Files | Not measured | — | — |
| C. Safe reads/writes | Not measured | — | — |
| D. Freshness/recovery | Not measured | — | — |
| E. Security/operations | Not measured | — | — |
| F. Connector-off rehearsal | Not measured | — | — |

**Default decision: NO-GO.** Only an explicit operator decision after **six
passing gates** may authorize changing the production connector preference.
Document scope, timestamp, evidence revisions, reviewer and the exact rollback
action. Do not describe the connector as replaceable solely because a test
Notion root or experimental Qwen/LanceDB spike worked.

Rollback immediately on authorization bypass, wrong attribution, missed or
duplicated writes, silently stale results, inaccessible authoritative fallback
or a production outage breaching the agreed SLO. Stop new custom operations;
restore the official Notion route and verify current source truth; preserve
journal/evidence for incident triage. Do not blindly repeat an ambiguous write
or delete Notion content to restore a local index. Return to primary only after
a defect fix, targeted regression, affected gate retest and explicit approval.

**Related issues:** #94–#103 retrieval eval; #9 files; #10 safe writes;
#11/#117 auth/security; #55–#58 lifecycle; #104/#109 image/ChatGPT
deployment; #111 backup; #114 dual-connector routing.
