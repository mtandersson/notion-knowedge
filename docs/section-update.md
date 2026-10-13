# Scoped one-section Notion Markdown edit (#79 / #312)

This completes the application-side section-edit flow building on the pure
section selector from #311. Production MCP section mutation remains DISABLED.
No page-wide markdown replace or caller-supplied byte-offset edit is exposed.

## Upstream semantics, checked 2026-10-13

Notion API version 2026-03-11 supports synchronous
PATCH /v1/pages/{page_id}/markdown with type=update_content and an array
of old_str/new_str pairs. This operation is the recommended targeted update.
See https://developers.notion.com/reference/update-page-markdown
(legacy replace_content_range is *not* used).

The adapter sends exactly one non-retried PATCH with:
- type: update_content
- update_content.content_updates: one {old_str, new_str,
  replace_all_matches:false}
- update_content.allow_deleting_content: false
- allow_async: false

The adapter checks the synchronous page_markdown receipt's identity and
completeness, but only a subsequent independent GET can prove success.
No whole-page replacement, no destructive override, no network PATCH retries.

## Server-only workflow

Call update_section_once with:
- a fresh, authenticated NotionRead and the restricted NotionSectionWrite port
- a root-scoped RootScopeGate constructed from trusted server configuration
- the existing cross-process durable WriteIdempotencyStore (#78)
- a canonical target PageId, explicit literal ATX SectionAnchor (level + heading)
- replacement body, expected RFC3339 page revision and exact full UTF-8
  Markdown SHA-256, and a unique idempotency key.

Order: authorize physical ancestry -> reserve in durable ledger -> verify
caller revision against complete authoritative content -> select exact unique
section using #311 -> require unique nonempty old_str across entire page ->
revalidate physical scope -> immediate authoritative preflight again ->
durably mark attempt UNCERTAIN -> one network PATCH -> fresh complete readback
matching the planned full Markdown byte-for-byte -> scope revalidation ->
record VERIFIED durable receipt. Only VERIFIED receipts may be replayed.
Timeouts, upstream conflicts after dispatch, malformed receipts and readback
mismatches retain UNKNOWN status; explicit reconciliation is required. Do not
resubmit the PATCH with the same or a new key without investigating the page.

## Fail-closed constraints

- Missing, duplicate, fenced, quoted, nested, unsupported enhanced Notion
  headings and escaped sibling headings are rejected by #311.
- An empty original section body is *not* sent as old_str: Notion requires a
  nonempty source string. The pure planner accepts it, but the range-write
  workflow explicitly reports SelectionRejected until a safe provider
  insertion primitive is separately reviewed.
- If the selected original body occurs more than once anywhere in the page,
  reject instead of using replace_all_matches or guessing a position.
- The adapter rejects malformed IDs/inputs and any incomplete mutation receipt.
  The shared transport disables retries for non-idempotent writes.
- The read adapter rejects truncated Markdown or unresolved block IDs. Child
  deletion stays disabled even if Notion gains new APIs.
- There is no Notion atomic compare-and-swap supported by this contract.
  Another editor can race after the last revision read. The server-side
  search-and-replace and strict full-page readback detect many races but
  cannot make the upstream write transactional. Ambiguous results require
  reconciliation; do not claim atomic isolation.

## Activation remains opt-in

No new MCP tool, tool description, route or permission is added. Any future
production exposure must explicitly wire authenticated root scope, durable
shared ledger, confirmation policy, audit, rate and ownership checks, and a
fresh caller revision. Keep #302 for subsequent concurrency integration.
