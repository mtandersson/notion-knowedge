# Pre-upload file validation (#68)

The reusable `notion-knowledge-notion::file_validation` module rejects invalid
inputs **before** a future Notion File Upload request. It does not turn on file
upload tools or access private files by itself.

Call `FileValidationPolicy::default().validate(filename, declared_mime, bytes)`
after a bounded download and before any upstream upload creation. The
`ValidatedFile` receipt contains the sanitized filename, canonical MIME type,
and measured byte length. Upload exactly the same bytes that were validated.
A metadata-only declared MIME check is **not** sufficient.

## Allowlist

| Input format | MIME | Extensions | Content check |
| --- | --- | --- | --- |
| PNG | `image/png` | `.png` | PNG 8-byte header |
| JPEG | `image/jpeg` | `.jpg`, `.jpeg` | JPEG SOI header |
| GIF | `image/gif` | `.gif` | GIF87a/GIF89a header |
| WebP | `image/webp` | `.webp` | RIFF/WEBP identifier |
| PDF | `application/pdf` | `.pdf` | PDF header |
| Text | `text/plain` | `.txt` | valid UTF-8, no binary/control bytes |
| Markdown | `text/markdown` | `.md`, `.markdown` | valid UTF-8, no binary/control bytes |
| CSV | `text/csv` | `.csv` | valid UTF-8, no binary/control bytes |

The text MIME types optionally accept a single `charset=utf-8` parameter.
Unknown, executable, HTML, SVG, archive and macro-enabled formats are denied.
The extension, declared media type and observed bytes **all** must agree.
Signature checks are a quick spoofing defense, not an antivirus scanner or
proof that the content is harmless. Externally sourced data must still be
handled as untrusted.

Filenames are reduced to the last path component, unsafe characters and bidi
overrides are replaced, reserved Windows names are prefixed, and the final
UTF-8 filename is capped at 128 bytes while retaining the safe extension.
Validation errors contain only a fixed safe message, never user filenames,
file contents or signed URLs.

## Notion upload size policy

Default `FileValidationPolicy` is **5 MiB**, conservative for the Notion Free
plan. `for_single_part(max_bytes)` permits an explicitly configured positive
limit no greater than **20 MiB** for verified single-part paid-plan/API support.
A caller must check the actual Notion workspace plan and API limits before
overriding the default. Multipart uploads are **not** implicitly enabled and
must follow a separate flow; a Notion plan-limit error must not trigger an
unvalidated multipart retry. Validate size before making a Notion API call.
Streaming downloads also require their own network/byte/time caps (#67).

Upstream reference: [Notion file uploads](https://developers.notion.com/reference/file-upload);
[Notion pricing](https://www.notion.com/pricing).

## Boundary and next integration

This module is an adapter-level primitive; the eventual file transport and
attachment features (#69-#75) **must explicitly call it** before uploading.
Root authorization, signed URL download protections, file cleanup and
read-back verification are tracked separately. No current MCP tool accepts
files merely because this component exists.

Verification:

```sh
cargo test -p notion-knowledge-notion --locked file_validation
cargo test --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```
