# Guarded targeted append workflow (#78)

`notion_knowledge_core::append::append_once` implements the exact, opt-in,
single-page append application workflow. It combines existing authoritative
`NotionRead`/`NotionWrite`, the shared live `RootScopeGate`, the fresh
revision preflight, and the **durable**, separate `WriteIdempotencyStore`.

## Target and position

- `page_id` is mandatory and names **one** existing Notion page. It is
  checked against trusted physical ancestry before reading or writing.
- `markdown` is inserted at the **end** of the page by the provider's
  `insert_content` operation. The service does not replace, delete, or reorder
  earlier content, or choose a section based on textual matching. The caller
  explicitly supplies desired leading/trailing newlines.
- `expected_last_edited_time` is required, with an optional exact UTF-8
  Markdown SHA-256 precondition.
- `idempotency_key` is required even for the first attempt; there is **no**
  unkeyed best-effort write path.

## Safety ordering

1. Validate input and verify target's **fresh physical root ancestry**, using
   the trusted integration/workspace root policy from #87.
2. Derive a durable idempotency claim scoped to the trusted workspace and
   bound to page ID, Markdown, revision preconditions and `end` position.
   First reservation is the only state that permits a future PATCH. A verified
   key replays its receipt; an in-progress or unknown key returns
   `OutcomeUnknown` and **never** repeats the operation.
3. Read the authoritative source and check every supplied precondition.
   Re-read the exact full Markdown before mutation. Any archived page,
   revision mismatch, source read error or scope change prevents PATCH.
4. Revalidate ancestry, then **durably mark the reservation uncertain before
   the first network mutation**. A crash, timeout or 5xx after this point never
   causes automatic retry. The Notion adapter uses a single PATCH.
5. Re-read the complete page authoritatively. Only a readback with the *exact
   original Markdown prefix*, followed by the *exact inserted Markdown*, an
   active matching page ID and a trusted Notion URL becomes a verified receipt.
   No unrelated original bytes may be changed.
6. Revalidate the approved scope and persist that verified receipt **before**
   returning success. If any post-PATCH verification or ledger write fails,
   return `OutcomeUnknown`. Reconciliation must be deliberate and based on
   authoritative evidence; never infer failure from a network timeout.

The ledger must be durable and must survive restarts: use
`SqliteIdempotencyStore::open_existing` in production, not the disposable
retrieval cache. Initial setup must use `initialize_new` explicitly.
A lost ledger is not proof that no operations committed.

## Boundaries

This is a **core application workflow**, not a newly enabled MCP mutation
tool. The existing `knowledge_append` schema is still development-only
and cannot mutate Notion; upcoming MCP composition must supply a trusted
write credential, scope gate, durable ledger, explicit write authorization
and user confirmation policy. #302 also tracks the remaining optimistic
concurrency integration.

Notion does **not** offer a transactional authorization+revision+append CAS
through these primitives. A concurrent edit or move in the last preflight/PATCH
window is still possible. Conservatively strict readback may report an unknown
result if Notion normalizes Markdown, even after a successful append; in that
case manually reconcile, do not retry the same key or switch keys without
explicitly checking the source.

## Verification

```sh
cargo test -p notion-knowledge-core --test append --locked
cargo test --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

Full CI also exercises release builds, container smoke, and security scanning.
