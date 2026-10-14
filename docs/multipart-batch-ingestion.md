# Plan-aware multipart and ordered file ingestion

The trusted `NotionFileIngestAdapter::new` remains single-part and conservative
(5 MiB by default). Operator composition can explicitly call `with_multipart`
with a validated `MultipartPolicy`; request arguments cannot select a plan.
The maximum is configurable up to the adapter's conservative **100 MiB** cap,
not the provider's full paid-plan maximum. The switch threshold is configurable
from **5 through 20 MiB**, with a part size from **5 through 20 MiB** that must
not exceed the threshold. These constraints guarantee single-part coverage up
to the switch and at least two parts above it. Nonfinal parts have the configured
size; the last part may be smaller. The download policy must separately permit
the intended maximum; its stricter cap always wins.

For example, `MultipartPolicy::new(80 * MIB, 20 * MIB, 10 * MIB)` keeps files
through 20 MiB single-part; `MultipartPolicy::new(30 * MIB, 5 * MIB, 5 * MIB)`
switches sooner. Invalid zero/oversized parts, gaps above the single-part ceiling,
and incompatible maxima are rejected during construction. Policy fields are
private. The installed server validates explicit operator plan/part settings at startup; request inputs cannot select a plan. See [installed runtime](installed-file-ingestion.md).

Notion's [larger-file guide](https://developers.notion.com/guides/data-apis/sending-larger-files)
requires nonfinal parts of 5–20 binary MiB, completion after every part is sent,
and an uploaded final status before attachment. The adapter sends in order,
checks exact part counts, completes once and independently GETs final upload
status. Send, complete and GET failures preserve the known upload UUID and
return honest indeterminate effects; no mutation is blindly replayed. The
existing pinned API version and rate limiter are retained.

Validation scans the entire owned file with a fixed 64 KiB working buffer.
PNG, JPEG, GIF, WebP and PDF use the existing signatures. Plain text, Markdown
and CSV also validate UTF-8 and allowed controls through EOF, carrying incomplete
UTF-8 scalars across chunk boundaries. Optional hints retain existing inference,
MIME/extension matching and bounded safe filename normalization. Upload always
streams from an independent file cursor; it does not buffer the whole payload.

`FileIngestBatch::new` admits a nonempty ordered set with one common canonical
target, bounded metadata and valid captions. The server supplies `FileBatchPolicy`
(default five files, configurable one through a hard cap of twenty). Common
fresh target authority precedes any download; each item then performs the entire
[single-file application](single-file-ingestion.md) with fresh checks, durable
intent admission, validation, upload, native append/readback and terminal audit.
Only one downloaded payload is owned at a time. Successfully appended files
retain request order. References alone never grant target authority.

`results()` contains every original index. `outcome=None` means unattempted;
otherwise the single-file verified, failed, denied, indeterminate and
verified-audit-failed statuses and redacted failure classes apply. `partial_success()`
counts verified external attachments even when terminal audit fails. Earlier
attachments remain after a later failure; there is no atomic rollback claim.
Authority loss, upstream authentication/permission loss or any audit failure
stops the remaining items. Ordinary item failures may continue in order.

Keep the batch object outside its borrowed `execute` future to inspect results
following cancellation. A canceled item remains failed before external effects
or indeterminate after effects start. Upload/block UUIDs are checkpointed as
soon as authoritative receipts supply them, even during multipart sends or
native readback. Earlier completed results remain intact and later items stay
unattempted. Cancellation unlinks owned temporary bytes and best-effort records
a terminal audit under the same single-file guard; `terminal_audited=false`
means execution did not confirm terminal persistence to the caller. Execution
is one-shot, including after denial/cancellation: replay requires a new explicit
request and renewed authority, never a persisted or queued signed URL.

Credential-free joined HTTPS, Notion HTTP and durable SQLite fixtures prove
switch boundaries, ordered exact multipart bytes/fields, interruptions,
completion/final-status checks, mixed image/document order with distinct IDs,
later failures, scope/audit safety stops, cancellation checkpoints and cleanup.
No installed MCP upload/batch tool is activated by this application extension.

```sh
nix develop .#spike --command cargo test -p notion-knowledge-notion file_ingest::tests --locked
```
