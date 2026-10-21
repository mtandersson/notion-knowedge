# OAuth image and file ingestion

OAuth file ingestion requires explicit operator activation. Read-only tokens
remain read-only even when the server installs upload support. The supported
production topology uses the same sealed Notion grant, approved user and
workspace, physical root policy, and retrieval runtime as
[OAuth semantic writes](oauth-semantic-writes.md). It does not use an integration
credential or an integration-only refresh worker.

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

Admission to this queue still requires current request authority. If authority
is lost after an upstream effect, refresh may be unavailable. Retain the safe
audit evidence and reconcile the target after restoring an explicitly approved
grant. Do not bypass revocation, substitute a replacement grant for old queued
work, or interpret unavailable indexing as rollback of the source effect.

Preserve operational queue state across restart and index rebuilds. For work
already admitted, recover or retry the refresh through the existing grant-bound
worker without repeating source effects. A new grant requires its own approval
and matching scope; old work must fail closed when its recorded grant no longer
matches. Keep credential, operational state and audit storage separate from the
disposable index and from one another, including filesystem aliases.

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
