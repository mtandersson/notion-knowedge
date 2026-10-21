# File-ingest cleanup and minimal audit (#75)

The internal downloaded-file boundary owns each request's `tempfile::NamedTempFile`. It is unlinked on success, validation/scope/audit failure, timeout, cancellation and Rust unwinding while the process remains alive. No raw download URL, query string, signed header or source filename is written into the temporary file's **metadata**, audit event or error. A process crash is inherently outside RAII guarantees; startup janitor/controlled staging-dir policies are separate operational tasks.

The **`file_ingest_audit`** module exposes only two trusted constructors. `successful_attachment_event` requires a verified native file/image attachment from an authoritative block readback; `attachment_event` admits non-success outcomes (denied/failed/indeterminate) but deliberately refuses a success claim based on an upload receipt. They construct the existing privacy-safe `AuditEvent`, whose schema stores only an operator-created correlation ID, bounded timestamp, actor/tool/outcome, validated target Notion UUID and file **kind/size**. There is no slot for a filename, signed URL, token, MIME parameters, content, arbitrary error strings or upstream response. Hash/MIME are optional under the issue's acceptance criteria and are intentionally **not** durably persisted because the existing audit store lacks explicit privacy/retention policy for those values.

The existing `AuditStore` is the durable storage contract; producing an event does **not** prove that it was recorded. The [installed ingestion runtime](installed-file-ingestion.md) writes admission and truthful terminal audit and reports whether terminal persistence succeeded. Its trusted local transport boundary remains separate from private ChatGPT OAuth readiness.

Credential-free tests cover success and all explicit error/cancellation paths already in `file_download.rs`, plus Rust unwind, validation rejection, denied scope and audit write failure fixture, and fail-closed typed event construction.

When a trusted request authority is supplied, native file effects require its
current mutation permission after transport admission and before each actual
send. Complete upload and attachment read-back receipts are checked again before
success. Loss of authority stops the ordered batch, retains any already-known
upload/block IDs and earlier verified items, and records denial without claiming
that an accepted effect was rolled back. No subsequent source download or
provider effect is attempted. Local temporary files still follow the cleanup
contract above; this does not introduce remote upload deletion or install the
OAuth workflow tracked in #477.
