# Disposable guarded Notion write/upload smoke (#556)

`scripts/smoke-notion-write.py` extends the [read/index/search smoke](notion-read-smoke.md)
with installed production guarded writes and file ingestion. This implementation
and its credential-free HTTP tool fixtures **do not authorize live mutations**.
Keep #110 and #557 open until an explicitly authorized operator run and cleanup
have actual evidence. Never point this harness at ordinary user content.

## Private prerequisites

Use the shipping Unix `webhook-worker` server with the existing
[OAuth semantic-write configuration](oauth-semantic-writes.md) and
[OAuth file-ingestion configuration](oauth-file-ingestion.md). Complete the
existing authorization and initialization procedures first; the harness does
not create credentials, initialize a missing ledger, start a listener, expose a
service or change policy. Use one explicitly operator-owned, non-sensitive,
disposable root as the only configured physical root. Keep its subtree small.
The OAuth token needs `knowledge:read knowledge:write` and the original approved
Notion grant. Do not substitute an integration token or use a bearer fallback:
guarded semantic writes are installed only in the production OAuth composition.

Run on the listener's machine, through its existing `http://127.0.0.1:PORT/mcp`
(or `[::1]`) endpoint. The loopback client never follows redirects, reads proxy
settings or repeats a transport request. This restriction does not change the
server's authentication or root-scope checks. The repeated root UUID and private
settings are operator assertions, not independent proof of root ownership.

Ledger, semantic audit and file audit must already exist, be distinct private
files (0600), and have operator-owned private parent directories (0700), outside
disposable derived state. The harness reads these configured SQLite stores in
read-only mode to verify committed per-run semantic claims and terminal audit
records. Keep their exact paths synchronized with the running server's settings.
Do not copy the stores, discard them after failure or edit their rows.

Generate a one-pixel synthetic PNG and a short synthetic text document locally:

```sh
python3 scripts/smoke-notion-write.py \
  --prepare-sources /absolute/private-smoke/synthetic
```

The new directory and files are private. Publish **exactly those bytes** using
your existing private, short-lived HTTPS source setup on the server's configured
exact host allowlist. The harness does not start a file server, open a firewall,
make files public or weaken the installed secure downloader. Keep download URLs,
signing credentials and source setup out of transcripts, logs and Git. File audit
must confirm the expected observed kind and tiny byte count (68-byte image and
54-byte text); the server validates and downloads the actual source bytes.

Prepare `/absolute/private-smoke/settings.json` locally (0600) beneath an
operator-owned private directory. All UUIDs are canonical lowercase; configure:

```json
{
  "endpoint": "http://127.0.0.1:3000/mcp",
  "token": "LOCAL_OAUTH_ACCESS_TOKEN",
  "root_page_id": "DISPOSABLE_ROOT_UUID",
  "workspace_id": "TRUSTED_WORKSPACE_UUID",
  "ledger": "/absolute/private-write-stores/ledger.sqlite",
  "audit": "/absolute/private-write-stores/audit.sqlite",
  "file_audit": "/absolute/private-write-stores/file-audit.sqlite",
  "files": [
    {"download_url": "PRIVATE_HTTPS_IMAGE_URL", "file_id": "synthetic-image", "mime_type": "image/png", "file_name": "smoke.png"},
    {"download_url": "PRIVATE_HTTPS_DOCUMENT_URL", "file_id": "synthetic-document", "mime_type": "text/plain", "file_name": "smoke.txt"}
  ]
}
```

The URL placeholders must be replaced privately. The token and source URLs are
read from that file; none belong in CLI arguments or diagnostics. Use Python
3.11+ from the repository's Nix environment. Ordinary CI uses no credentials:

```sh
python3 scripts/test-smoke-notion-read.py
python3 scripts/test-smoke-notion-write.py
```

## Explicit authorized run

Only after separate live authorization, use a new diagnostics directory beneath
an existing 0700 evidence parent:

```sh
python3 scripts/smoke-notion-write.py \
  --allow-mutations \
  --settings /absolute/private-smoke/settings.json \
  --diagnostics /absolute/private-smoke/evidence/write-run-1 \
  --root DISPOSABLE_ROOT_UUID \
  --confirm-disposable-root DISPOSABLE_ROOT_UUID
```

The harness discovers installed tools, creates one uniquely named run child,
verifies its guarded receipt and matching committed ledger claim, then waits
for semantic search plus indexed/fresh get to verify its marker under the chosen
physical root. Production create verifies exact authoritative content, child
identity and **direct physical parent** before returning its verified receipt;
the harness relies on that installed contract, not a new raw Notion HTTP client.
It appends only to this verified child using a fresh authoritative revision
precondition and stable per-run idempotency key. It verifies refreshed retrieval
again before attempting one ordered image/file batch on the same child.

Upload acceptance requires two distinct authoritative attachment block IDs,
known upload IDs, exact target, verified outcomes and terminal audits. Production
ingestion verifies actual native attachment readback (including identity/type)
before reporting `verified`; pending upload status alone cannot pass. The
harness checks durable audit kind/observed byte counts, inspects the shared
`knowledge_file_refresh` receipt with `retry:false` until completed, and repeats
search/get checks. Source receipts remain separate from derived index status;
index failure cannot imply that a known source write or attachment failed.
`--wait-seconds` bounds each derived wait; only read/search/status calls repeat.

## Failures, reconciliation and cleanup

The private 0600 `result.json` retains run ID, phase/outcome, stable semantic
idempotency keys, known child/upload/block IDs, derived refresh receipt/status
and bounded static failures. It excludes tokens, URLs, marker text, source bytes,
raw tool replies and upstream errors. Protect these operational IDs too. Writes
are marked uncertain and flushed before invocation. A transport failure never
causes automatic write/upload retry, even with the same key. Existing evidence
directories are never reused. Known IDs from partial results are retained even
when a later result fails or terminal audit is unavailable.

Stop on any failure. Preserve evidence and the original ledger/audits/queue.
Inspect known IDs through the authorized Notion UI and existing operator
procedures. A lost create response may leave a child whose ID is unknown: use
the saved run title/key and the durable ledger to reconcile it; never guess its
ID or run the harness again to find it. For ambiguous uploads, use the recorded
per-file IDs and shared refresh receipt with the documented
[file recovery procedure](file-recovery.md). A derived retry cannot repeat a
source effect. Keep the original grant and do not edit private queue/ledger rows.

No automatic remote cleanup occurs. Only after authoritative inspection confirms
run ownership may the operator manually archive the exact
`cleanup_child_page_id`, including its synthetic attachments, through the Notion
UI. Never archive the configured root, arbitrary search hits or an unrelated
page. If that field is absent, reconcile before cleanup; an ID retained only in
an unverified/uncertain effect is not cleanup authorization. MCP archive/delete
is unavailable, and this harness adds no deletion bypass. Record actual live
run and cleanup evidence under #557/#110 before closing those acceptance gates.
