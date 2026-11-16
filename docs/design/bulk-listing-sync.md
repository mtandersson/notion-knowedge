# Bulk-listing sync: page inventory as a resource

Status: proposed (2026-11-16). Owner-approved approach: **A — rework the
existing pipeline in place**.

## Intent

A full initial sync of the configured root (~3,000 pages) takes many hours today
and was OOM-killed before finishing. The owner wants full sync fast enough to be
an ordinary operation, and incremental sync (later driven by Notion callbacks)
to be near-instant. Full resync remains the recovery mechanism — this design
deliberately adds **no** recovery ledger or repair procedure; the earlier "no
replacement sync framework" rule existed to prevent exactly that kind of
complexity, not to forbid restructuring.

### Why it is slow today (measured)

- ~48–57 Notion API operations per page (root page 184). `resolve_lifecycle`
  (`crates/notion/src/lifecycle.rs`) walks every ancestor to the workspace and
  then re-reads the chain in reverse, i.e. ~2×depth reads, and it runs at
  preparation **and** again at commit revalidation.
- The shared limiter spaces requests 334 ms (~3 req/s,
  `crates/notion/src/transport.rs`). At ~50 calls/page that caps throughput at
  ~200 pages/hour regardless of CPU.
- Each page commit runs merge + FTS optimize + vector optimize (2.7–8.7 s), see #663.
- Local Qwen embedding on 1 CPU is a second wall; the owner intends to move to
  a paid external embedder later.

## Goals

1. **Full sync of ~3,000 pages in ~20 minutes** (floor: ~1 content read per page
   at ~3 req/s ≈ 17 min, plus listing). Target about 1.1–1.3 Notion calls per page.
2. **Incremental sync in seconds to a couple of minutes**: only pages whose
   `last_edited_time` (or membership) changed are fetched.
3. **Page inventory is a first-class local resource**: a persisted list of every
   page under the root with id, parent, title-free metadata, `last_edited_time`,
   in-trash/archived state, and local sync state (indexed revision, last
   fetched, outcome). Exposed for diagnostics (counts, out-of-sync list) without
   page contents.
4. **Callback-ready**: a single-page change event (later, via the Cloudflare
   tunnel) updates the inventory entry and refreshes only that page, resolving
   membership from the inventory's parent map, not a workspace walk.
5. **Out-of-sync detection**: a cheap periodic reconciliation compares a fresh
   bulk listing with the inventory and reports/queues drift (new, changed,
   moved, removed, trashed, permission-lost pages). Drift is visible as counts
   and the existing progress log.
6. **Pluggable embedder**: embedding goes through an interface with the local
   Qwen backend as default; an external batched/parallel backend can be added
   later without touching sync. (Backend implementation itself is out of scope.)

7. **Simpler, not just faster.** The owner moved this work to a fresh agent
   because the current design feels over-complicated with a dead end. Each
   slice must **delete** the machinery it supersedes: the ancestor walk and
   reverse bracket, per-page lifecycle revalidation, and redundant planner and
   stream paths. Net code should shrink. Prefer one sync path (full = listing
   of everything; incremental = listing diff; callback = one-page diff) over
   parallel special cases.
8. **Shorter CI.** Measure current CI wall time per job and set a target, for
   example under 10 minutes for the required gate. Remove tests made redundant
   by removed code, keep behavior tests at the HTTP/storage boundary, and avoid
   adding native-feature test binaries. Report CI time before and after each
   slice. Baseline (main run 43786090925): 35 min total. The longest jobs are
   container smoke on main (34 min), compiling native test executables
   (16 min) and Clippy (10 min); unit-test jobs take 2–4 min.

## Non-goals

- Implementing the external embedding provider or webhook receiver now.
- Changing retrieval ranking, chunking, or the vector identity (no re-embed).
- Multi-root scope, graph mode, write-back to Notion.

## Anti-goals (would make this a failure even if fast)

- Indexing a page that is not under the configured root or not readable by the
  integration, or keeping a page after it left the root / was trashed / lost
  permission beyond the next listing.
- Silent partial syncs: every page must end as indexed, honestly skipped
  (typed reason), or failed, and totals must add up to the inventory size.
- A recovery ledger, multi-step repair state machine, or retained cache
  generations. If state is doubtful: full resync.
- Higher RAM/CPU limits.

## Constraints

- 1 CPU, 4 GiB RAM, swap 0; one local model while Qwen is the backend.
- Read-only Notion integration auth, single root.
- Keep: per-page atomic commits with receipts, leases/ownership fences, #649
  page deadlines and settlement, #654/#661 bounded prefetch, #657/#660 stale
  retry, #659 arena cap.

## Approaches (decided)

| | Approach | Trade-off |
|---|---|---|
| **A (chosen)** | Rework existing pipeline: replace per-page ancestor walks with a bulk listing + local membership resolution; one content read per page; batched index maintenance; embedder interface. | Keeps the tested commit/receipt/deadline machinery; ~95% of B's speed because both hit the same Notion rate limit. |
| B | New two-stage snapshot → index sync. | Cleaner re-indexing, but rewrites most sync and safety guarantees. |
| C | Tune only (#651, #662–#665). | Safe but keeps per-page walks; still hours for 3,000 pages. |

## Design

### 1. Membership: walk the tree from the root (owner decision)

Notion has no "list every page" API, and search is documented as incomplete
and lagging. So **scope is the physical tree below the configured root**,
discovered by walking it:

- Read the root's content and collect its child pages; repeat for each child.
  The content we must fetch anyway lists child pages, so membership costs no
  extra calls. Child references hidden in `unknown_block_ids` (truncated
  blocks) are fetched through the same endpoint; inaccessible ones (404) are
  typed skips.
- Databases and data sources under the root: list their rows with data-source
  queries (100 rows per call). Rows are pages and are walked the same way.
- Links and mentions to pages outside the root are not children and are not
  followed. That is correct: those pages are out of scope.
- A page is in scope iff the walk reached it in this run. This replaces
  `resolve_lifecycle`'s per-page ancestor walk and reverse bracket entirely;
  ancestry evidence becomes "reached from root via parent P in run R".
- Exclusion rules (`descendants_of`, source types, page ids) are applied
  during the walk by pruning subtrees.

### 2. Content fetch: one read per page

- One Markdown read per page (`GET /v1/pages/{id}/markdown`), plus follow-up
  reads only for truncated blocks. Metadata (`last_edited_time`, parent,
  in-trash, properties) comes from the listing/query result or the parent's
  child entry. Use `fetch_page` only when that metadata is partial or a
  property is truncated (relations/people over 25 items).
- Drop the before/after double content read in `prepare_scoped_page`.
- Reuse the #654/#661 prefetch workers, the 64 MiB budget and the shared limiter.
- Budget: about 1.1–1.3 calls per page, roughly 20–25 min for ~3,000 pages
  at ~3 req/s.

### 3. No commit-time recheck (owner decision: option b)

- There is no separate page lookup before saving. The owner accepts this risk:
  a page whose ancestor moved out of the root or was trashed between being read
  and being saved can be indexed briefly. The next callback, reconciliation or
  resync removes it.
- Staleness is bounded by the reconciliation interval. Ownership, lease and
  receipt fences stay, as do per-page atomic commits and the #649 deadlines.
- Relations with the page graph off stay unresolved references and cost no calls.

### 1b. Search as a change accelerator (incremental only)

- Incremental runs use `POST /v1/search` sorted ascending by `last_edited_time`
  since the last sync to find changed pages fast. Pages it returns that aren't
  in the inventory get placed through their parent chain against the inventory's
  parent map.
- Search misses are covered by the periodic full walk (§4). Search never
  decides scope alone.

### 4. Inventory resource and drift detection

- Persist the inventory in the existing operational SQLite (new table; derived
  and disposable — dropping it simply forces a full listing).
- **Reconciliation pass** (scheduled, and on startup): a fresh walk from the root, or a search diff for a cheap check →
  diff with inventory → classify new / changed / moved / removed-or-unreadable /
  trashed → enqueue page refreshes or removals. A page missing from search is
  confirmed with one page retrieve before removal (search lag guard).
- Report drift counts in progress logs and a diagnostics endpoint/tool; no page
  titles or contents in logs.

### 5. Callback path (later, ready now)

- Event → page id → one page retrieve → update inventory entry → membership via
  parent map → refresh through the same consumer. Unknown parent → treat as
  drift and let the next reconciliation (or an immediate partial listing)
  settle it.

### 6. Index maintenance batching

- Do merge per page commit (atomicity unchanged) but run FTS/vector optimize
  per N pages or T seconds and at the end of a sync (#663), with compaction/
  pruning (#664). Must prove not-yet-optimized rows remain searchable.

### 7. Embedder interface

- A trait over "embed batch of texts → vectors + model identity", local Qwen
  as the implementation. Vector identity/versioning stays as today so switching
  backends forces an explicit rebuild, not silent mixing.

## Measurements / acceptance

- Calls/page and pages/hour by phase (#662 logging) on the release build.
- Full sync of the production root completes; totals = inventory size; peak RSS
  under limit; retrieval acceptance (#455 probe) passes.
- Incremental run with no changes: only listing calls, finishes in < 1 min.
- Drift tests: move a page out of root, trash a page, revoke a subtree (fake
  Notion), add a page — each detected by reconciliation and reflected after one
  pass.

## Delivery slices (proposed issues, under #455)

1. Inventory table + bulk listing + local membership resolver (behind a flag;
   compare against existing lifecycle results in tests).
2. Switch planner/discovery to the inventory; one-read content fetch; cheap
   commit check (§3).
3. Reconciliation/drift pass + diagnostics.
4. Batched index maintenance (#663) + compaction (#664).
5. Embedder interface (no new backend).
6. Measurement on release (#653/#662) and full-root acceptance (#455).

#651 (in progress) should be re-scoped to fit slice 2 rather than optimize the
walk that this design removes.

## Left to the builder

Exact SQLite schema, listing pagination/retry details, batching thresholds,
flag name, and test fixtures.

## Calls made by the designer (marked for review)

- Changing the lifecycle evidence or digest format is allowed: the cache is
  disposable, and existing caches need one full resync.
- Moves, permission changes and minute-granular timestamps are caught by the
  periodic walk.
- Search pagination sorts ascending and re-reads the tail.
- Comparison with the old lifecycle results runs in tests only, with no
  production shadow run.
- A crash between commit and index optimize must leave rows searchable,
  proved by a test.
- To verify on the real workspace in slice 1: how database rows and data
  sources appear, and how deep block nesting goes.
