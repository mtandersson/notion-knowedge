# Per-page sync phase timings (#662)

Every page the startup, periodic or manual sync consumer settles emits **one**
structured stderr/journald line, so a single `jq` pass shows where a page's time
went. The fields are durations (whole milliseconds) and counts only. No page
title, ID, URL or text can reach the line: the serializer accepts only a fixed
status word, a fixed reason class and numbers, and a test asserts that none of a
fixture's titles, IDs or content appear in it.

```json
{"operation":"cache_sync","phase":"indexing","status":"page_committed","wall_ms":251,
 "consumer_wait_ms":0,"embed_ms":0,"lance_merge_ms":43,"fts_optimize_ms":0,
 "vector_optimize_ms":0,"receipt_ms":112,"other_ms":96,"queue_wait_ms":363,
 "walk_api_ops":1,"walk_api_ms":11,"prepare_api_ops":0,"prepare_api_ms":0,
 "commit_api_ops":0,"commit_api_ms":0,"limiter_wait_ms":0,
 "text_chunks":1,"skipped_chunks":0,"sync_progress":{"...":"..."}}
```

`status` is `page_committed`, `page_unchanged` (already applied; nothing was
written) or `page_skipped` with a fixed `reason`: `page_timeout`,
`out_of_scope`, `permanent_source` or `source`. Every settled page logs at
info level.
Pages the walk itself skips (archived, excluded, unreadable) never reach the
consumer and have no line; a failure that stops the whole sync (authentication,
index or authority) has none either.

## The wall-time identity

`wall_ms` is the consumer's wall time for the page: from asking for the next
prepared page until the page settles (the previous page's line and progress
report fall in the next page's `consumer_wait_ms`). These fields are disjoint
parts of it and **sum to `wall_ms` exactly**:

| Field | Meaning |
| --- | --- |
| `consumer_wait_ms` | The consumer waited for a prefetched page (the walk or preparation is the bottleneck). With `NK_PREFETCH_WORKERS=0` the page is prepared only after the consumer asks, so this includes its whole preparation. |
| `embed_ms` | Zero in the source/text phase. The semantic worker runs separately. |
| `lance_merge_ms` | The Lance `merge_insert` (or delete) for the page. |
| `fts_optimize_ms` | FTS index ensure, compaction and `optimize_fts_index`. |
| `vector_optimize_ms` | Vector-index layout check and optimize. |
| `receipt_ms` | SQLite receipt begin and finish. |
| `other_ms` | The remainder: reading and diffing the page's current chunks, relation projection, lease renewal, thread hand-off, storage compaction and pruning, and everything not listed above. |

A page that hits its deadline is the one exception to completeness: its owned
commit thread is not drained before the line is written, so time the thread
spends after the deadline is not in that line (the page's `wall_ms` still adds
up, the unfinished work being counted in `other_ms`).

The index optimize pass runs on a cadence, not per page. Only the page whose
commit triggers a pass pays for it, so `fts_optimize_ms` and
`vector_optimize_ms` are zero for most pages and large for a few. The one pass
at the end of a sync has no page and is not in these numbers.

## Fields reported beside the identity

These overlap other pages' work (preparation runs ahead of the consumer) or each
other, so they are **not** added to the sum:

| Field | Meaning |
| --- | --- |
| `walk_api_ops`, `walk_api_ms` | Notion requests the walk made to read this page (metadata when the search listing did not supply it, and Markdown content), including limiter wait and retries. The shared search listing is not attributed to any page. |
| `prepare_api_ops`, `prepare_api_ms` | Requests made authorizing relation and link targets while preparing the page. |
| `commit_api_ops`, `commit_api_ms` | Requests made on the commit path. Sync no longer rechecks the source before committing, so this must be 0; it is kept to show that. |
| `limiter_wait_ms` | Time the page's requests spent queued in the shared limiter's `admit()` (one request per 334 ms). It is part of the `*_api_ms` fields; subtract it for time on the wire. |
| `queue_wait_ms` | From the prepared page being handed to the bounded queue (including blocking while it is full) until the consumer received it. Large values can indicate a publication bottleneck. |
| `text_chunks`, `skipped_chunks` | Lexical chunks added or changed, and chunks reused unchanged. |

The old issue wording named "authorization/lifecycle walk, content fetch and
commit revalidation". Since the bulk tree-walk, one read returns metadata and
content, so those are the `walk` phase; authorization of relation targets is
`prepare`; revalidation at commit no longer exists, so `commit` stays 0.

Recording uses relaxed atomics on a per-page accumulator. It makes no Notion
request and takes no lock on the commit path; outside a sync it does nothing.

## Running totals

The periodic and final `cache_sync` progress line for the `indexing` phase adds
`page_totals`: `pages` plus the running sum of every numeric field above. Divide
by `pages` for an average.

The worker emits separate `operation: "semantic_projection"` events. A
`published` outcome gives the number of vectors published for that page.
`staged` gives progress retained for a later attempt, with a typed provider
reason when an embedding failed during the batch.
`embedding_deferred` reports a typed provider reason, including `InputLimit`.
`retry_deferred` reports an infrastructure failure in the retry loop.
The read-only status tool reports the current backlog.
[Synchronization status](knowledge-sync-status.md)

## Recipes

Average per phase over all pages of the journal window:

```sh
journalctl -u notion-knowledge -o cat --since today | jq -R -n '
  [inputs | fromjson? | select(.operation == "cache_sync" and ((.status // "") | startswith("page_")))] as $p
  | ["wall_ms", "consumer_wait_ms", "embed_ms", "lance_merge_ms", "fts_optimize_ms", "vector_optimize_ms", "receipt_ms", "other_ms", "queue_wait_ms", "walk_api_ms", "prepare_api_ms", "commit_api_ms", "limiter_wait_ms", "walk_api_ops", "prepare_api_ops", "commit_api_ops", "text_chunks", "skipped_chunks"]
  | map(. as $k | {key: $k, value: (($p | map(.[$k]) | add) / ($p | length))})
  | from_entries + {pages: ($p | length)}'
```

Median and 90th percentile of each phase for the first 50 pages (the baseline
procedure for the performance tickets):

```sh
journalctl -u notion-knowledge -o cat --since "<release start>" | jq -R -n '
  def pct($q): sort | .[((length - 1) * $q) | floor];
  [inputs | fromjson? | select(.operation == "cache_sync" and ((.status // "") | startswith("page_")))] | .[:50] as $p
  | ["wall_ms", "consumer_wait_ms", "embed_ms", "lance_merge_ms", "fts_optimize_ms", "vector_optimize_ms", "receipt_ms", "other_ms", "queue_wait_ms", "walk_api_ms", "prepare_api_ms", "commit_api_ms", "limiter_wait_ms"]
  | map(. as $k | {key: $k, value: {p50: ($p | map(.[$k]) | pct(0.5)), p90: ($p | map(.[$k]) | pct(0.9))}})
  | from_entries'
```

For a quick look at the latest totals instead:

```sh
journalctl -u notion-knowledge -o cat | jq -R 'fromjson? | select(.page_totals) | .page_totals' | tail -n 1
```

The service unit name is deployment specific. Lines that are not JSON are
skipped by `fromjson?`.
