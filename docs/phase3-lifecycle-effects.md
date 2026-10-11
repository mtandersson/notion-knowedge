# Authoritative page lifecycle effects (#252)

`AuthoritativePageRefresh::apply_lifecycle` is the reusable production component
for a page selected by a webhook hint, manual request or reconciliation. It
reads current `PageLifecycle` evidence with the configured Notion credential and
`LifecycleScope`; no event name, discovery absence, incoming root/workspace or
cached source timestamp determines the effect.

Compose the handler exactly as in [authoritative refresh](phase3-authoritative-refresh.md):
a trusted workspace/root/exclusion/generation snapshot, real `NotionClient`,
`GuardedChunkTable`, and `IndexCommitCoordinator` bound to the same persistent
SQLite database and Lance directory. Supply the configured compatible
`EmbeddingProvider`, current database-wide reconciliation lease, stable durable
operation ID and current clock:

```rust,ignore
handler.apply_lifecycle(&page, &operation_id, provider.as_ref(), lease, clock).await?;
```

Allowed active pages enter the existing canonical source normalization,
heading-aware chunking, stable fingerprint and incremental refresh pipeline.
Moves between configured roots validate old trusted provenance against the
configured root set and replace citations with the current physical root.
Unchanged body chunks preserve identity and reuse vectors even when root/title
metadata changes. Restoration after a tombstone follows the same path.

Only affirmative inactive or complete outside/excluded evidence can remove a
page. The handler requires existing owned chunks or a durable page checkpoint,
verifies existing workspace/root provenance, and accepts page/generation-bound
non-superseded durable receipts when an interrupted initial refresh/removal left
no chunks or checkpoint. It prepares an empty complete-page
snapshot and revalidates the entire negative evidence inside the external index
guard. Missing/inaccessible/incomplete ancestry fails without removal. A 404
never means delete. Unowned hints cannot create tombstones. Tombstones live in
the index-bound SQLite state, with the observed selected-page source revision.

Deletion removes actual Lance rows, maintains lexical/vector indices and only
then commits the tombstone and Applied receipt in SQLite. The same directory
guard, pinned table version and database-wide lease fence serialize removal and
refresh. Changed source, ancestry, policy generation, ownership or table version
rejects the proposal; callers discard it and retry from authoritative reads.
An overlapping refresh cannot resurrect a removed page with a stale proposal.

A pending/failed lifecycle receipt may replace its action and proposal for the
same page/generation under that guard: a page can be restored after a delete
failed, or deleted after an interrupted refresh. A terminal Applied receipt
remains immutable and redelivery reports AlreadyApplied; a distinct new event
owns subsequent changes. Superseded receipts and wrong-page/generation reuse
remain conflicts. Index apply before SQLite acknowledgement is replayable with
the same operation ID after reopening both stores; the complete-page mutation
converges on current source state.

The method returns classified Source/Index/Conflict/Unavailable errors without
acknowledging inbox delivery. Failures inside the owned commit retain durable
classified receipts; source/preparation failures before a receipt exists remain
the caller's retry responsibility. Use the bounded retry/debounce contracts of
#247/#54. Running worker dispatch and durable inbox completion belong to #248;
this component does not start a background worker or enable server retrieval.

Notion observations are sequential, not atomic compare-and-swap. A source edit
immediately after the last revalidation still needs later event/reconciliation.
Configured scope generations must change whenever authority changes, and all
index writers must use the same guard. After an initial crawl, create the vector
index through the guard as described by refresh composition; both indexed and
brute-force vector queries observe row removal.

Credential-free integration tests in
`crates/retrieval/tests/authoritative_refresh.rs` use actual Notion HTTP,
SQLite journal/sync state, LanceDB lexical/vector search and a counting embedding
provider. They cover current hints, delete/outside/exclusion/restore, unowned
removal, two-root metadata moves, duplicate/out-of-order hints, inaccessible
ancestry, source/provider failures, stale owner/generation, source changes at
commit, concurrent refresh/delete and reopen after index apply before ack.

```sh
nix develop .#spike --command \
  cargo test -p notion-knowledge-retrieval --features local-lancedb \
  --test authoritative_refresh --locked
```

