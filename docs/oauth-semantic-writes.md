# Production OAuth semantic writes

The Unix server built with `webhook-worker` can explicitly install
`knowledge_create_page` and `knowledge_append` for OAuth tokens carrying both
`knowledge:read knowledge:write`. The default remains read-only. Bounded
`knowledge_update_section` additionally requires `NK_DESTRUCTIVE_WRITES=true`
and a client supporting MCP form elicitation. Archive, delete, whole-page
replacement are unavailable. Image/file upload requires the separate explicit
[OAuth file-ingestion opt-in](oauth-file-ingestion.md).

Use the existing OAuth registration, sealed Notion grant, allowlisted workspace
and user, physical root scope, and initialized compatible retrieval/index state.
Do not configure integration credentials or an integration webhook worker
alongside OAuth content. OAuth file ingestion uses the request-bound factory
described in the file-ingestion guide.

Set these additional operator settings:

```sh
NK_READ_ONLY=false
NK_OAUTH_WRITES=true
NK_RETRIEVAL=true
NK_OAUTH_WRITE_LEDGER_FILE=/var/lib/notion-knowledge/writes/ledger.sqlite
NK_OAUTH_WRITE_AUDIT_FILE=/var/lib/notion-knowledge/writes/audit.sqlite
# Optional, only for bounded section updates:
NK_DESTRUCTIVE_WRITES=true
```

Keep the existing `NK_WEBHOOK_INDEX_DIR`, `NK_WEBHOOK_QWEN_ASSETS` and
`NK_WEBHOOK_STATE_FILE` retrieval configuration. The OAuth refresh dispatcher
uses those polling and timeout settings without integration credentials. It
shares the exact coordinator and model with request-bound search. Existing
index authority and embedding metadata are validated before writable access;
an absent or incompatible index fails startup.

The write files need existing operator-owned private parent directories (0700)
and private files (0600). Ledger, audit, index state and credential files must
be separate; hard-link aliases are rejected. Keep ledger and audit outside the
disposable index directory. Create the ledger once, under the complete intended
configuration, with:

```sh
notion-knowledge-server --initialize-oauth-write-ledger
```

This command refuses an existing path. Ordinary HTTP startup opens an existing
compatible ledger and never recreates a lost one. Back up and preserve this
ledger across index rebuilds, restarts and grant replacements. Losing it loses
proof that an uncertain upstream write must not be repeated. Audit records are
restricted metadata, not source text or credentials; audit failure blocks a new
mutation. Start HTTP using `notion-knowledge-server --http`.

Discovery is token-specific. Operator write activation does not add write scope
to existing read-only tokens. Each invocation binds its source, scope and queue
to trusted middleware authorization. Tool arguments cannot select credentials,
workspace policy, refresh authority or approval. A section prompt displays the
exact target, heading, old body and replacement; only its authenticated peer's
explicit acceptance can approve that intent. Fresh scope/revision/content checks
run again after approval.

Write and index statuses are separate. A verified write can report queued,
running, completed, failed or unavailable indexing. Refresh uses the normal
canonical hash/diff path; it cannot repeat an upstream mutation. Replaying a
verified idempotency key returns its receipt and current refresh state. A
cancelled, denied or interrupted reserved attempt may require reconciliation;
retrying that same key never silently sends a new effect. Revocation or grant
replacement after an effect reached Notion is an unknown outcome, not rollback.

SIGTERM/SIGINT or worker failure stops admission and cancels refresh. HTTP,
workers and owned native effects share the configured shutdown deadline. A
failed drain exits unsuccessfully and leaves durable recovery work.

Automated evidence uses the actual configuration parser, OAuth login/token
flow, MCP HTTP/SDK handling, local fake Notion provider and guarded SQLite/Lance
state. It is separate from the operator-controlled deployed ChatGPT smoke and
real disposable-Notion acceptance (#109/#110); this change does not establish
that live cutover or retrieval quality acceptance.
