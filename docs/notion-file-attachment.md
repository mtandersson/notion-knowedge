# Native Notion document attachments (#72)

The server-only `NotionClient::attach_uploaded_file` adds a **native Notion file block** to an explicit page, using a previously confirmed `UploadedFile` receipt. This is an internal adapter primitive; it does **not** enable the public ChatGPT MCP upload tool.

## Supported files and preflight

Only PDF (`.pdf`, `application/pdf`), UTF-8 plain text (`.txt`, `text/plain`), Markdown (`.md`/`.markdown`, `text/markdown`), and CSV (`.csv`, `text/csv`) from the existing file-validation pipeline are supported. Other formats, including **images** and executable/active formats, must use their own path or fail closed. Uploaded metadata must have `status=uploaded`, a valid UUID, nonzero size, an aligned MIME/extension and a safe short basename. Caption is optional, trimmed and serialized through Notion rich text, never interpolated as raw JSON; control and bidi characters are rejected.

## Authorization and mutation

- The explicit target is a canonical **page** UUID (not arbitrary block/database IDs). A trusted server-owned `LifecycleScope` supplies workspace/root authorization. Its `RootScopeGate` resolves live physical Notion ancestry, checks root/exclusions and **revalidates** current physical ancestry before every append. No user-provided root label can expand authorization.
- `PATCH /v1/blocks/{page}/children` appends exactly one native `file` block containing `file.type=file_upload`, `file.file_upload.id={upload_id}`, a human-readable sanitized `name`, and optional `caption`.
- A valid returned list of exactly one native block with matching parent is required. A separate `GET /v1/blocks/{block_id}` must confirm its native file type, parent, canonical filename and caption. Both a native file-upload reference to the expected ID and a Notion-resolved HTTPS file URL are supported for the API's post-upload representations. HTTPS URLs are **not** emitted.
- Only confirmed readback returns `AttachedFile {page_id, block_id, upload_id, verified:true}`. Missing/invalid readback fails; never advertise a successful attachment based on the append receipt alone.

Like #71's image primitive, the operation does not retry ambiguous writes. If append succeeded but the reply or readback failed, manual/reconciliation readback is needed to avoid duplicate attachments. The physical revalidation limits, but cannot eliminate, TOCTOU races. Future #74 handles generalized bounded eventual-consistency readback and failure reconciliation.

## Tests

Credential-free mocked Notion HTTP tests cover PDF and Markdown attachment, optional caption, exact API request and stable receipt, forbidden root, changed ancestry, image-MIME and invalid filenames, malformed block receipt, incorrect block parent and mismatch between upload identity/readback. No live credentials or real Notion documents are used.

```bash
cargo test -p notion-knowledge-notion --locked
cargo clippy -p notion-knowledge-notion --locked --all-targets -- -D warnings
```

Official API references: [Block object](https://developers.notion.com/reference/block), [Append block children](https://developers.notion.com/reference/patch-block-children).
