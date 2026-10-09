# Primary ChatGPT cutover acceptance gates (#115)

**Decision:** Do not disable the official Notion connector until every gate below
is **PASS** on the same release candidate and authorized production-like
deployment. A published design, unit test, fake adapter, or isolated experiment
is not evidence of successful cutover. Until then, the official connector is
the authoritative path for real private writes, files, and current reads.

This is an **operator acceptance policy**, not a feature-completeness claim or
an automated migration. Targets below are proposed acceptance thresholds, not
observed performance. Record any threshold revision before collecting its
decision evidence. Notion remains the authoritative store; LanceDB and local
embeddings remain derived, disposable state.

## Gate sequence and required evidence

| Gate | Pass criterion | Evidence to retain (sanitized) | Failure disposition |
| --- | --- | --- | --- |
| 0. Authorized connection | ChatGPT completes real OAuth/PKCE and Notion grant, permits **only** the configured user/workspace and roots; invalid state, expired/replayed tokens and out-of-scope reads/writes expose zero data. Production HTTP must not fall back to unauthenticated development mode. | Release SHA, configuration fingerprint without secrets, positive and negative end-to-end test IDs and outcomes | BLOCK; keep official connector |
| 1. Retrieval quality | On a fixed, versioned bilingual evaluation corpus, **FTS and hybrid** each achieve Recall@5 ≥ **0.95**, MRR ≥ **0.90**, nDCG@5 ≥ **0.90**; the separately versioned exact-identifier regression has no critical ID lookup misses at top 3. Independently, a 30-day supervised pilot with ≥ **50** representative, judged, in-scope queries yields ≥ **95%** expected pages in top 5, **100%** correct source attribution and zero invented answers. | Corpus IDs/digests, model/tokenizer/index revisions, real adapter command, full metric JSON, query-class counts and sanitized source-attribution adjudications | BLOCK on any failed mode, missing report or unjudged cases; re-baseline only by review |
| 2. Authoritative reads and safe writes | On a disposable authorized Notion root, fresh read matches exact Notion content and timestamp; 10 distinct authorized create/append/update operations succeed with authoritative read-back, expected scoped index refresh and no duplicate effects on induced retries; rejected root and user operations cause **zero** writes. Report partial/index failure explicitly, never claim that an unverified write failed. | Per-operation test IDs, source-side before/after checks, sanitized outcome, index state and retry checks; no page bodies/tokens | BLOCK; do not substitute index state for authoritative proof |
| 3. Native image/file workflow | Through ChatGPT → **custom MCP → native Notion file/image blocks**, at least 10 image and 10 other-file/PDF trials succeed without Google Drive, Agent Handover, manual upload or the official connector. Check file bytes/size/hash where supported, MIME, attached block, independent authoritative read-back, and scoped authorization; reject bad source URL, wrong root, oversize and expired credentials without internal fetch or partial disclosure. | Test artifact hashes/sizes (not private bytes), Notion block IDs in restricted evidence, fail/deny results and proof of direct route | BLOCK if any required supported type needs a workaround |
| 4. Sync freshness and correctness | Over ≥ 30 calendar days and ≥ 100 controlled create/update/delete/move/restore events across the approved root, ≥ 95% become correctly searchable or absent (as appropriate) within **5 minutes**, **100% within 15 minutes**; no out-of-scope content survives a confirmed removal. Authoritative-read failures must *not* be misread as evidence for deletion. | Event receipt time, authoritative commit time, settled index time, P50/P95/max delays, lifecycle audit and error counts | BLOCK; stale/unsafe scope is not a successful sync |
| 5. Recovery, reconciliation and rollback | Deliberately interrupt an in-flight worker and drop a webhook, then demonstrate durable resumption, bounded retries and reconciliation with **zero lost acknowledged events** and no duplicate mutations. Recover from documented SQLite state backup and from a freshly rebuilt **derived LanceDB** index, independently verifying source citations; each drill completes within **2 hours** on the documented host. Show that restoring official connector routing takes ≤ **15 minutes** without altering Notion content. | Backup timestamp/checksum, restore log, reconciliation diff, authoritative sample comparison, timings and rollback operator log | BLOCK if only a dry-run, theoretical runbook or unverified rebuild exists |
| 6. Connector independence | For ≥ **7 consecutive days** of supervised, representative authorized workflows, **100%** of the declared required search/read/write/upload tasks work using only custom MCP; no call or hidden dependency on the official connector, Drive or Handover. Disconnect the official connector in a controlled test session and repeat every required task including error/denial paths; restore official routing if any gate regresses. | Supported task inventory, coverage matrix, connector/tool invocation trace with private payloads omitted, dates and explicit sign-off | BLOCK; keep official connector connected |

Metrics are measured against complete judged ground truth. For the offline
ranking suite, evaluate chunks and use its documented Recall@K, MRR and nDCG
definitions, rather than confusing a page match with a correctly cited chunk.
Report Swedish and English cohorts separately; **neither language may fall
below 0.90 Recall@5** even when the overall average passes. Do not round a
failing score upward. Define the sample and query classes before running the
pilot; do not cherry-pick successful requests. Use a stable top-k of at least 5
and the exact same corpus across modes. Unavailable production adapters and
offline contract stubs have **no usable quality score**.

The freshness denominator is all controlled events with an affirmative
authoritative source outcome; missing webhook hints still count. Measure from
the authoritative change to the correct retrieval state, not merely to queue
receipt or successful worker acknowledgment. Record and investigate timeouts and
unknown-state events separately; they cannot be silently excluded to improve
the percentile. For scope removals, only affirmative authoritative evidence
permits deletion. A read failure is not a successful removal.

Recovery times apply to the documented test host, fixed corpus and backup
size; capture these in the evidence rather than assuming the target holds
for a different deployment. Back up operational SQLite state as required;
never assume the state journal is disposable just because the vector index is.

## Required workflow inventory and checks

The operator must list every real workflow that would be lost by disabling
the official connector: natural-language and exact-ID search, source-linked
answers, fresh read of a known page, unknown/unindexed page lookup,
properties/databases/navigation, scoped create/append/update, image and
non-image file upload/read, failures/retries, and safe handling of a page move,
archive or deletion. Mark each **SUPPORTED**, **UNSUPPORTED**, or **NOT TESTED**
for the custom MCP at the exact tested revision. Every workflow used by the
owner must be SUPPORTED and pass the relevant gate; omitting a workflow from
the pilot does **not** make it unnecessary. If a workflow is deliberately
retired, document the owner's explicit decision before the cutover decision.

Authentication and safety are non-negotiable: zero unauthorized successful
operations, zero private data exfiltration, no secrets in the evidence, and
no unacknowledged destructive write. Treat any such failure as immediate
rollback, regardless of favorable aggregate scores.

## Operator evidence record (copy for each candidate)

```text
Date / operator:
Git SHA / image digest / custom MCP version:
Configured authorized workspace/root (redacted fingerprint):
Notion integration and OAuth setup version:
Indexer / embedding model / tokenizer / FTS schema revisions:
Host CPU/RAM/disk, dataset size, backup size:
Offline dataset IDs + checksums; FTS and hybrid metric report paths:
Pilot period, query count by class/language, evaluated failures:
Lifecycle event count and freshness P50/P95/max:
Write and file smoke IDs + pass/fail, denied-scope attempts:
Crash / missed-webhook / backup-restore / index-rebuild evidence:
Official-only dependency audit and 7-day no-fallback smoke:
Gate 0: PASS / FAIL / NOT RUN
Gate 1: PASS / FAIL / NOT RUN
Gate 2: PASS / FAIL / NOT RUN
Gate 3: PASS / FAIL / NOT RUN
Gate 4: PASS / FAIL / NOT RUN
Gate 5: PASS / FAIL / NOT RUN
Gate 6: PASS / FAIL / NOT RUN
Decision: GO only if all seven PASS; otherwise NO-GO
Operator sign-off and date:
Rollback route revalidated:
```

Never write raw private page content, tokens, signed URLs or uploaded file
bytes into the evidence record. A NOT RUN gate is a NO-GO, not a pass.
Re-evaluate the full matrix after auth/transport changes, model/index/schema
changes, or a materially changed source scope.

## Current state and follow-up ownership

As of this document's creation, **none of the gates above has been certified
for a private production cutover**. The default production server's adapters
are not fully composed; the local retrieval spike and contract fixtures are
not a full ChatGPT OAuth + search/write/file deployment. The ongoing OAuth,
webhook/indexing, reconciliation, native file and write tickets must ship
independently and pass their own tests before the operator can exercise
these acceptance gates. Do not mark #115 as implementation of those features.

See [migration routing and rollback](dual-connector-migration.md),
[evaluation methodology](../eval/retrieval/README.md),
[security model](threat-model.md), [webhook delivery](notion-webhooks.md),
[reconciliation journal](reconciliation-journal.md),
[local state persistence](sync-state.md), and
[container runbook](container.md).
