# ChatGPT single-file MCP input (#66)

The staged `knowledge_upload_file` MCP descriptor exposes a required **top-level**
`file` input and declares `_meta["openai/fileParams"] = ["file"]`. ChatGPT can
send a single attached file directly to this semantic tool; no Google Drive
staging, sandbox path or separate in-chat image serialization is part of
the input contract.

The file object declares all four properties:

| Property | Required | Purpose |
| --- | --- | --- |
| `download_url` | yes | Temporary file retrieval URL from ChatGPT |
| `file_id` | yes | ChatGPT-managed opaque file reference |
| `mime_type` | no | Optional MIME hint; not authoritative |
| `file_name` | no | Optional untrusted filename hint |

Only the first two are required **inside the file object**. Exactly one
object is accepted, not an array. The server validates the structured
parameter, including sensible metadata length limits, but does **not** follow
the URL, download bytes, write to storage or attach anything to Notion yet.
A valid tool call returns a clearly marked `file_upload_unavailable` *tool
error*, not success. A missing file or invalid schema is a structured
`invalid_params` error. Neither path logs or echoes a temporary file URL.

## Security and rollout boundary

Never treat a supplied URL, MIME hint, filename or `file_id` as access
permission. Issue #67 owns bounded and SSRF-safe file download, #68 owns MIME
and size validation, and #69–#75 own Notion upload/attachment, read-back and
cleanup. Authorization, allowed Notion root, user consent and safe audit
requirements are separate prerequisites before the complete mutation tool
can succeed. The discovery descriptor alone does not make file ingestion live.

Current default MCP transport advertises the file *input contract* in addition
to the existing search/get tools, so user-facing clients should not interpret
discovery as proof of an operational upload. The tool description explicitly
warns that it is a staged unavailable capability. Do not use it as the
preferred route for real attachments until the downstream work is completed.

## Tests

Run:

```sh
cargo test -p notion-knowledge-mcp --locked
cargo test -p notion-knowledge-server --test stdio --test http --locked
cargo fmt --all -- --check
```

The schema unit tests check `_meta`, every declared property and required key,
single-file decoding, optional metadata and rejection of extra/missing fields.
The HTTP and stdio process smoke tests assert the same tool descriptor through
the actual server discovery response. No credentials or external file fetch
are involved.

Reference: [OpenAI plugin file-parameter contract](https://developers.openai.com/plugins/reference#file-parameters).
