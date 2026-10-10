# Phase 3: authoritative page refresh into real SQLite/LanceDB (#315)

`AuthoritativePageRefresh` is the reusable production application boundary
shared by future webhook worker #248, lifecycle effects #252, and authoritative
reconciliation #242. Available on Unix with the `local-lancedb` feature.

## Trust and composition

Instantiate with an integration-credential-bound `NotionClient`, actual
`GuardedChunkTable`, persistent `IndexCommitCoordinator`, configured
`LifecycleScope`, matching `ReconciliationScope` and chunk configuration.
The constructor rejects mismatched trusted workspace, policy fingerprint, or
generation. The operator must ensure the Notion credential belongs to the
selected workspace; caller/event IDs never supply authority.

For each selected page hint, call:

```rust
refresh.refresh_page(
    &selected_page_id,
    &configured_root_page_id,
    &durable_webhook_event_id,
    embedding_provider,
    current_reconciliation_lease,
    trusted_monotonic_clock,
).await?;
```

A caller must claim each durable webhook/reconciliation work item using the
existing inbox/worker retry machinery and **acknowledge only after** an applied
or safely already-applied result. Errors leave work retryable through that
shared lifecycle; this module does not create an unrelated queue or launch an
always-on worker.

The operation uses the real persisted page rows (including properties, links,
physical source/container metadata and fingerprints), not caller-supplied
previous chunks. `NotionClient::prepare_scoped_page` authorizes physical root
ancestry and exclusions, fetches one complete authoritative Notion page twice,
reuses `identify_chunks` stable IDs and returns a canonical document/chunks.
No relation, link or page hint is ever followed to expand authority.

If the full canonical page and persisted checkpoint are unchanged, return a
no-effect result. Otherwise, `GuardedChunkTable::prepare_page` embeds **only**
changed/new chunk content and reuses existing vectors for identical chunks.
Metadata, title, timestamp, URL and properties update citations without
re-embedding, and removed chunks are excluded from the complete snapshot.

Finally `commit_page` serializes a real SQLite receipt and LanceDB mutation
under the **cross-process directory-inode guard** from #254. Its async precommit
check rereads fresh authoritative metadata and Markdown and reauthorizes
physical ancestry under that guard, rejecting any change to selected ID,
source revision, title, properties, URL, content or scope. The guard compares
the pinned Lance table version and current SQLite fence; a conflict requires
discarding the stale proposal and starting over from new authoritative reads.

A completed index effect precedes durable SQLite checkpoint/receipt. A crash
after effects but before checkpoint remains safely replayable through the
guarded complete-page upsert and stable event receipt identity. No direct
unserialized write or source-side mutation exists in this workflow.

## Limits

- Notion does not provide an atomic page snapshot/CAS. A change after the final
  precommit read can be picked up by the next webhook/reconciliation event.
- This refresh path indexes **allowed active pages only**. Authoritative
  inactive/out-of-scope deletion, tombstones and recovery dispatch belong to
  #252; uncertain Notion 404/403 and webhook hints NEVER trigger speculative
  deletion in this module.
- Worker startup, queue scheduling, bounded retry and dead-letter reporting
  belong to #248/#57. Periodic full reconciliation belongs to #242/#243.
- All participating production writers must use the same #254 external guard;
  legacy direct-index writers do not gain cross-process fencing automatically.
- `notion-knowledge-notion/test-fixtures` is a test-only opt-in feature that
  forces an explicitly supplied local TCP port on 127.0.0.1; it is not
  enabled by normal production dependencies.

## Reproducible end-to-end test

The integration harness in `crates/retrieval/tests/authoritative_refresh.rs`
uses a strict local HTTP Notion response server, persistent SQLite, real
LanceDB, provider-compatible 3D counting embeddings, and actual lexical and
vector searches. No Notion credentials or model assets are required.

```sh
cargo test -p notion-knowledge-retrieval --features local-lancedb \
  --test authoritative_refresh --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Full CI also runs Qwen/LanceDB checks, smoke image, secret scan and
dependency audit. No Phase 3 epic closure should be inferred merely from this
leaf: #55/#56/#57/#58/#59 remain separately verifiable work.
