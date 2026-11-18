# Native Notion image attachment (#71)

This is an **adapter-only** primitive, not a public MCP mutation tool.
`NotionClient::attach_uploaded_image(target, upload, caption)` attaches
one previously uploaded PNG, JPEG, GIF or WebP to an **explicit page UUID**
on any page the token can write (no root gate, #691). It does not
accept arbitrary block IDs, caller-supplied URLs, or external image references.

## Guard order

1. Reject invalid target UUID, unsupported MIME, an unconfirmed upload receipt,
   empty upload and invalid/bidi/control-character caption **before network**.
   Strip only leading/trailing caption whitespace, cap the caption at 2,000
   Unicode characters, serialize it as Notion rich text (never raw JSON).
2. No ancestry walk or root gate runs: Notion's own permission check for the
   token decides whether the page is writable (owner decision, #691).
3. `PATCH /v1/blocks/{page_id}/children` with exactly one native
   `image` block containing `image.type=file_upload`,
   `image.file_upload.id={uploaded_id}` and optional rich-text caption.
   Uses the same integration credential, shared rate-limit transport and
   Notion API version. The append is never retried after ambiguous failures.
4. Require a single image block creation receipt with the correct parent and
   a valid returned block UUID. `GET /v1/blocks/{id}` must confirm the
   native image, its live parent, expected caption and supported Notion-hosted
   `file` URL or the exact `file_upload` ID. The URL is never returned or
   logged. Only then return `{page_id, block_id, upload_id, verified:true}`.

Read-back failure can occur **after the append has committed**. Such errors
must be reconciled by looking up the existing Notion block, not replayed as
another append. This primitive makes no claim of exactly-once semantics;
there is no MCP exposure and no multipart image upload here.

Issue #74 will add a general reusable cross-file verification workflow and
bounded eventual-consistency retries; issue #72 handles non-image file blocks.

Credential-free HTTP mock tests check success, exact native payload,
malformed input before network use, malformed creation receipt, and mismatched readback.

Validate with:
```bash
cargo test -p notion-knowledge-notion --locked
cargo clippy -p notion-knowledge-notion --locked --all-targets -- -D warnings
```

References: [Notion block format](https://developers.notion.com/reference/block),
[Append block children](https://developers.notion.com/reference/patch-block-children).
