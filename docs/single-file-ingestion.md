# Audited single-file ingestion application

`core::file_ingest::ingest_file` is the reusable application boundary for one
small supported image or document. `notion::file_ingest::NotionFileIngestAdapter`
joins the request-scoped HTTPS downloader, byte-signature/UTF-8 validator,
configured integration's single-part upload and scoped native attachment with
bounded authoritative block readback.
[Plan-aware multipart and ordered batches](multipart-batch-ingestion.md) extend
this same application without activating an installed MCP upload tool. This application API is available to
trusted composition; installed MCP/server activation follows separately.

The composition supplies `FileIngestContext` from trusted authentication and
operator operation policy, a fresh `RootScopeGate`, and a durable `AuditStore`.
It supplies the adapter's integration, exact source-host download policy,
single-part plan limit and identical full physical root/exclusion policy.
`permitted` must reflect the effective transport/operation permission and
read-only mode. Neither tool arguments nor a temporary URL can construct this
authority. An integration token is never an MCP identity. Use `Server` or
`Automation` when appropriate; `ApprovedUser` requires independently validated
user authentication and must never be invented by the caller mapper.

Example composition uses `Arc::new(client.clone())` for the gate's lifecycle
provider and the same configured `client` for `NotionFileIngestAdapter::new`.
Supply `SqliteAuditStore` from the retrieval adapter in a protected durable
state directory, not a disposable index. Construct both from the same trusted
`LifecycleScope`; the application refuses unequal policies before download.

The sequence is durable intent admission, current physical target authorization,
HTTPS download, bounded observed-media/name/size validation, another durable
intent availability check, fresh scope revalidation, single create/send upload,
fresh permit revalidation, native image/file append (which independently
reauthorizes/revalidates), bounded block GET verification, and terminal audit.
A moved/excluded/unverifiable target prevents the next effect. These checks
cannot provide an atomic Notion transaction lock against concurrent moves.

Invalid target text is never returned as a stable reference; the target slot is
absent until a canonical UUID has been validated. `FileIngestRequest` owns the
active temporary URL without implementing Debug.
Optional filename/MIME hints remain optional: absent MIME is inferred from an
allowed signature or inspectable UTF-8; absent names use `uploaded` plus a
supported extension. Every declaration and name still passes the existing
validator against those same bounded bytes. Text without a MIME hint uses a supported text filename extension when
present and otherwise defaults to plain text. HTTP source
Content-Type is untrusted and is not proof of observed media. Validation scans to EOF with a bounded 64 KiB working buffer; download and send stream.
The conservative default plan limit is 5 MiB and the single-part validator
cannot exceed 20 MiB. Multipart/batches use the documented application extension.

Native readback supports both an exact `file_upload.id` and Notion's hosted
`file` form. For a hosted GET, the HTTPS origin/path and any non-signing query
fields must match the authoritative append receipt's hosted resource. Recognized
AWS signature/expiry fields may rotate; an object `versionId` remains part of
the identity. Neither signed URL is persisted, returned or debug formatted.
Malformed URLs, duplicate resource parameters and unknown rotating query fields
cannot prove identity. A hosted append receipt followed by an exact upload-ID
GET can verify that stronger identity. If the append receipt contains only an
upload ID (or omits its source) and GET resolves to an opaque hosted URL, there
is no proven link between that URL and the upload: bounded readback returns
`Indeterminate` with known references rather than claiming verification or
replaying the append. This explicit ambiguity also applies to hosted origin,
path or resource-version changes.

Notion's official [file retrieval guide](https://developers.notion.com/guides/data-apis/retrieving-files)
describes API-uploaded files becoming hosted `file` objects after attachment;
its [file-object reference](https://developers.notion.com/reference/file-object)
documents ephemeral signed URLs. The hosted fixtures model this response form
without using private workspace credentials.

`Verified` **and** `terminal_audited` mean the whole workflow completed.
`VerifiedAuditFailed` retains a confirmed external target/upload/block receipt
but represents failure of durable terminal audit. `Indeterminate` means an
external effect may exist without verified attachment. `Denied` or `Failed`
can still carry an already-created upload reference. Known upload and block
UUIDs survive send, attachment receipt and readback failures; missing references
are never fabricated. Detailed component failures are conservative after a
write attempt; there is no blind non-idempotent replay, audited rollback claim,
or success based solely on an HTTP status. No temporary signed source or Notion
readback URL is returned or persisted.

Intent and terminal events include only server correlation ID, trusted actor,
target UUID, finite outcome and validated kind/byte count. Initial audit failure
prevents all network activity; the second admission failure prevents upload.
Terminal persistence failure preserves the actual external effect state and
reports `terminal_audited=false`. Dropping the in-flight application future
unlinks temporary bytes and best-effort records failed (before effects) or
indeterminate (after effects). Crash recovery must treat a remaining attempted
record as requiring reconciliation; no source URL survives for background retry.

Credential-free tests execute real HTTPS response streaming through a strictly
`cfg(test)` loopback/certificate seam, the real Notion HTTP adapter, native block
receipts/readback, and file-backed SQLite audit. Test-only TLS credentials
identify only `fixture.example` and authenticate no deployed service. The seam
has no production environment selector. Tests cover image/document success,
optional hints, initial and changed scope, MIME/size/expiry, upload/readback
ambiguity and receipt-bound hosted resource changes, audit failure, timeout/cancellation and temporary cleanup, including
persisted metadata secret/payload exclusion.

```sh
nix develop .#spike --command cargo test -p notion-knowledge-notion file_ingest::tests --locked
```

Notion's official [small-file lifecycle](https://developers.notion.com/guides/data-apis/uploading-small-files)
distinguishes create, send and attach, requires multipart/form-data for send,
and supports native blocks using the upload ID. The application retains this
separation and the existing pinned API version; no provider endpoint or upstream
credential is selected from request input.
