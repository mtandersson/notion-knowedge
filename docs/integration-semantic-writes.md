# Guarded writes through a private integration-backed connection

The [private ChatGPT tunnel](https://github.com/mtandersson/notion-knowedge/issues/559)
can use the existing guarded create/append runtime through the loopback HTTP
bearer transport. This mode uses the configured Notion integration and fixed
workspace/root policy; it does not claim an OAuth person's identity or grant.

Activation is explicit and off by default. Use a Unix `webhook-worker` binary,
configure trusted integration credentials/scope, `NK_HTTP_AUTH=bearer` with a
private local token, `NK_RETRIEVAL=true`, and either the existing verified
webhook worker or periodic reconciliation. Set `NK_READ_ONLY=false` and
`NK_INTEGRATION_WRITES=true`, then supply absolute, private durable paths in
`NK_INTEGRATION_WRITE_LEDGER_FILE` and `NK_INTEGRATION_WRITE_AUDIT_FILE`.
Do not combine this mode with OAuth discovery. Ordinary stdio startup does not
install this HTTP worker composition.

Initialize the new ledger once using `--initialize-integration-write-ledger`
through the same private runtime environment. Existing ledgers are never
replaced. Keep the ledger and audit distinct from each other, operational state,
file audit, credential storage and the disposable index. Preserve these stores
across cache rebuilds. A service restart or cache loss never authorizes replay
of an indeterminate source effect.

The server installs `knowledge_create_page` and `knowledge_append` only through
the trusted composition. Existing read-only/capability/root gates, raw source
revision/hash checks, verified readback, idempotency and audit still apply.
Audit records use the automation actor, not an OAuth-approved-user claim.
Section replacement is not installed by this composition. File ingestion has
its separate explicit activation and audit configuration.

Queries and post-write refresh share the existing sync worker's model, chunk
table, coordinator and operational queue. A verified write and its local index
status remain separate outcomes. The worker drains admitted mutation refresh
even when periodic reconciliation runs without webhook ingress; this does not
enable webhook processing when its worker is disabled.

This configuration enables a capability; it does not authorize a live source
mutation. Use local providers for implementation tests, and obtain the intended
source-write authorization separately before real acceptance.
