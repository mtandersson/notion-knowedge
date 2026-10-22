# OAuth image and file ingestion

OAuth file ingestion requires explicit operator activation. Read-only tokens
remain read-only even when the server installs upload support. The supported
production topology uses the same sealed Notion grant, approved user and
workspace, physical root policy, and retrieval runtime as
[OAuth semantic writes](oauth-semantic-writes.md). It does not use an integration
credential or an integration-only refresh worker.

Build the Unix server with `webhook-worker` and first complete the semantic-write
configuration, including explicit ledger initialization and the compatible
operational-state schema. For an existing state database, use the separate
[offline migration command](operational-state-migration.md). Then add:

```sh
NK_FILE_INGESTION=true
NK_FILE_SOURCE_HOSTS=files.example.com
NK_FILE_AUDIT_FILE=/var/lib/notion-knowledge/writes/file-audit.sqlite
NK_FILE_MAX_BYTES=5242880
NK_FILE_TIMEOUT_SECONDS=30
NK_MAX_UPLOAD_FILES=4
NK_FILE_AUDIT_RETENTION_DAYS=30
```

Replace the example source hostname with the exact approved HTTPS download
hostname; wildcards and arbitrary signed-URL hosts are not permitted. The audit
parent must already exist with operator ownership and mode 0700; existing audit
files must be private (0600). The file audit must be distinct from the semantic
ledger/audit, operational state, sealed credentials and disposable index.

For an operator-verified paid plan, multipart can be configured explicitly, for
example with `NK_FILE_PLAN=paid`, `NK_FILE_MAX_BYTES=10485760`,
`NK_FILE_MULTIPART_THRESHOLD_BYTES=5242880` and `NK_FILE_PART_BYTES=5242880`.
Threshold and part bounds must be paired and compatible with the configured
maximum. The startup parser rejects unsupported combinations.

The server binds each upload request to middleware authorization. File metadata
cannot choose the Notion credential, workspace, audit actor, allowed roots, or
refresh authority. A ChatGPT file reference supplies download metadata; it is
not permission to fetch an arbitrary URL or mutate the target page. The secure
downloader retains its configured exact HTTPS hostname allowlist, address and
redirect restrictions, timeout and byte bounds. Filename and MIME validation,
input-count limits and configured multipart limits still apply.

Each accepted input is processed in order into a native image or file block.
Success requires authoritative block read-back. A successful upload receipt alone
does not prove that a page attachment exists. Per-file source outcomes and
local index outcomes are separate: a verified attachment can still have queued,
failed or unavailable indexing. Earlier verified inputs remain represented when
a later input fails; subsequent inputs can remain unattempted.

Current request authority is checked across asynchronous boundaries and before
provider effects. Revocation, expiry, grant replacement or cancellation stops
further effects; it cannot undo bytes or attachments Notion already accepted.
Known upload and block IDs are evidence for reconciliation, not authorization
to replay an uncertain request. Do not automatically resubmit an interrupted or
indeterminate batch: inspect the explicit target page and reconcile each item
first. Uploads do not inherit the semantic-write idempotency ledger's replay
contract merely because both tools share the OAuth runtime.

# Refresh and recovery

Affected-page refresh uses the shared durable grant-bound queue. It reads the
authoritative source and updates derived retrieval state; it never repeats an
upload or attachment. When attachment success is uncertain, refresh can be
conservative and must not be interpreted as proof of source success. Failures
before any possible page attachment do not establish a changed page.

Before source effects, the server durably accepts one held refresh intent for
the ordered batch and its target page. It renews bounded source ownership while
the request runs. Settled source work releases the intent for indexing; a
proven no-effect batch discards it. Cancellation or an indeterminate provider
result leaves the intent held. Expiry reports `recovery_required` and never
establishes that a remote request has finished.

Each result retains its ordinal `index` and separate `index_status`. A shared
batch receipt identifies the target and mutation digest. Held batch recovery
can also apply to unattempted later items; it does not claim those files were
uploaded. Upload-only uncertainty may conservatively require a page refresh
without proving any attachment exists.

Use the installed OAuth tool `knowledge_file_refresh` with the receipt's
`page_id` and `mutation_digest` to inspect indexing. Add `"retry": true` to retry
failed derived work after the source intent is released. This tool cannot
release quarantine or repeat attachments. If a disconnect lost the receipt,
the operator can list held intents through the configured command. See
[operator file recovery](file-recovery.md) for list/status and explicit
stopped-owner/provider-quiescence release.

Preserve operational queue state across restart and index rebuilds. Recovery
requires the original approved live grant and current physical root policy.
A replacement grant cannot adopt earlier work. Revocation or missing authority
fails closed while the durable intent remains available for authorized
reconciliation. Keep credential, operational state and audit storage separate
from the disposable index and from one another, including filesystem aliases.

# Privacy and cleanup

The audit actor is supplied by authenticated server composition. Durable file
audit contains restricted operational facts such as correlation, target, file
kind/size and truthful outcome. It does not retain signed URLs, file names,
credentials, downloaded bytes, source text or raw upstream errors. Protect and
retain these records according to the configured policy. Audit failure can block
new effects or leave an already verified attachment with failed terminal audit;
it must not be concealed as complete success.

Request-owned private temporary files are removed on normal completion,
failure and cancellation. This is local cleanup, not remote upload deletion.
A process crash is outside normal RAII cleanup; follow the operator's private
staging-directory policy. See [file audit and cleanup](file-ingest-audit-cleanup.md)
for the underlying contract.

Automated acceptance uses configured OAuth HTTP MCP with local providers. It
does not establish deployed ChatGPT connectivity, real disposable-Notion writes,
or retrieval quality acceptance. The separately authorized live checks in
#109/#110 and the remaining parent criteria still apply.
