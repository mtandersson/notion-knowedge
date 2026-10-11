# Notion single-part upload primitive (#69)

The server-only `NotionClient::upload_single_part` takes the request-owned
`DownloadedFile`, a declared filename/MIME and an explicit
`FileValidationPolicy`. It first reopens and reads at most the configured
validation limit plus one byte, requires an unchanged file length, and applies
the existing signature/UTF-8/filename/MIME checks **before creating any upload**.

It then uses the **same integration credential** and shared Notion request
admission/metrics for both steps:

1. `POST /v1/file_uploads` with `mode: single_part`, sanitized `filename`
   and canonical `content_type`; accept only a UUID `file_upload` receipt
   with `status: pending`.
2. `POST /v1/file_uploads/{id}/send` with streaming multipart `file`
   contents read from a separately reopened temp-file handle. No
   `part_number` or explicit multipart boundary header is supplied.
   Accept only a matching UUID with `status: uploaded` and compatible length.

The adapter returns only a normalized `UploadedFile` receipt after the
second success. Errors are typed and sanitized (no temporary source URL,
Notion token, raw upstream response body, or file bytes in diagnostics).
One-shot multipart HTTP requests run under shared request pacing but **never
retry** ambiguous transport failures or server errors; 429 response admission
extends the rate-limit clock without replaying streamed bytes.

This module **does not attach files to pages**, enable the MCP upload tool,
authorize a target page or implement multipart uploads. Scoped native block
attachment/readback remains #71/#72/#74, ChatGPT download/wiring remains
separate, and large-file multipart is #70. An upload failing at any step is
never reported as attached or successful.

Credential-free local HTTP fixture tests verify the two authenticated API
calls, normalized receipt, streamed multipart payload, validation-before-effect,
and rejection of failed or malformed upstream outcomes.

Run `cargo test -p notion-knowledge-notion --locked` and
`cargo clippy -p notion-knowledge-notion --locked --all-targets -- -D warnings`
in the repository's pinned Nix shell. Production Notion credentials are not
used in these tests.

Notion protocol references:
- https://developers.notion.com/reference/create-file
- https://developers.notion.com/reference/upload-file
