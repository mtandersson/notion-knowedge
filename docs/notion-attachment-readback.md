# Bounded native-attachment read-back (#74)

Internal Notion adapter behavior for both image (#71) and file (#72) attachment flows. No public MCP file mutation tool is activated.

## Success contract

A successful `attach_uploaded_image` or `attach_uploaded_file` still requires an explicit operator-scope-approved page, prior confirmed upload, one native block append, a valid single-block append receipt and a separate authenticated Notion `GET /v1/blocks/{created_block_id}`. Returned stable page/block/upload IDs and `verified: true` are possible only after that authoritative GET matches the caller-specific strict predicate:
- canonical created block ID, native block kind, active status and correct physical parent page;
- original `file_upload.id` if Notion exposes it, or the Notion-resolved HTTPS hosted-file representation;
- exact optional rich-text caption; for document files, original safe filename.

A mismatched block, missing source, missing caption, malformed JSON or wrong parent is **never** accepted as successful attachment.

## Eventual consistency / failures

`NotionClient::verify_attached_block` is shared by both flows. It performs up to **three GET attempts**. Only HTTP 404 (block not yet visible) or a syntactically complete but mismatched HTTP 200 may prompt a new read. Delays between attempts are **150 ms** then **300 ms**; the existing shared transport still enforces Notion rate limits and its own bounded read-only policy. Reads are capped at 64 KiB. 401, 403, 429, server failures, malformed and oversized results fail immediately through sanitized typed errors (no raw response, tokens or signed URLs).

If the created block never appears or remains mismatched, return `BackendErrorKind::Conflict` with the verifier's operation ID. No `verified: false` success is emitted: unverified is a **failure**, even if the initial append may already have committed. This is an important non-atomic outcome and must trigger read-only reconciliation of the existing block ID before any new mutation.

**Never retry the append/create/upload** in this helper. The same attachment can exist remotely even after a timeout or exhausted readback. Explicit idempotency and more general reconciliation/orchestration remain separate concerns.

Tests use credential-free local Notion HTTP mocks for both image and document paths, including first-read 404 then eventual visibility, persistent 404 exhaustion with exactly one PATCH, stale 200 then match, persistent mismatched 200, early permission failure, malformed JSON, and invalid block IDs.

Validation commands:
```bash
cargo fmt --all --check
cargo test -p notion-knowledge-notion --locked
cargo clippy -p notion-knowledge-notion --locked --all-targets -- -D warnings
```

GitHub issue: https://github.com/mtandersson/notion-knowedge/issues/74
