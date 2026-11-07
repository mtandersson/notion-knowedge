# Optional private ChatGPT installation through Secure MCP Tunnel

## Product verification and installation acceptance

The owner clarified on 2026-11-07 that the tunnel is an installation choice,
not a product readiness prerequisite. Verify the product through its local MCP
stdio or protected HTTP transport: initialization, tool schemas, real scoped
search/read results, citations, authorization negatives, enabled mutation/file
behavior and recovery as applicable. Use the real configured adapters and
approved test scope; a configuration check or unavailable bootstrap alone is
insufficient. These local checks can establish product behavior without a
ChatGPT account, tunnel runtime key, workspace association or browser interaction.

For an installation that chooses ChatGPT, separately verify that installation's
transport, account/workspace access and discovered catalog using the procedure
below. Local success does not establish actual ChatGPT connectivity. Conversely,
an expired tunnel credential or pending ChatGPT setup does not block product
tests or a completed documentation PR. Issue #559 tracks the remaining live
installation evidence; merging this runbook does not close it or authorize
primary-connector cutover.

The owner selected this deployment for #559 on 2026-11-07:

```text
ChatGPT (No authentication) → Secure MCP Tunnel → loopback MCP
                                                   ↓
                                            Notion integration
```

The tunnel authenticates to OpenAI using its runtime credential. A separate
local bearer protects the HTTP hop from the tunnel client to the MCP server.
The Notion integration token stays on the server. None of these credentials
belongs in ChatGPT's OAuth client fields. This deployment needs no MCP OAuth
callback, public authorization endpoint, or inbound firewall opening.

The implemented [OAuth connection](chatgpt-oauth-acceptance.md) remains a
separate supported choice. Its person/grant/expiry guarantees do not apply to
this shared, operator-scoped tunnel connection. Access instead depends on the
tunnel's organization/workspace permissions, the local credential and the
server's fixed Notion scope. Loopback alone does not exclude other local
processes. This procedure starts read-only and does not authorize source writes,
uploads, additional roots or primary-connector cutover.

## Prepare the private endpoint

Use a reviewed, pinned feature-enabled executable and the approved model/index
assets. Record their identities privately. Configure the existing trusted
integration-backed retrieval path with the approved workspace and roots;
enable `NK_RETRIEVAL=true` only with compatible assets and index state.

For HTTP, set `NK_HTTP_HOST=127.0.0.1`, a free local `NK_HTTP_PORT`,
`NK_HTTP_AUTH=bearer`, and a randomly generated `NK_HTTP_BEARER_TOKEN` in a
private runtime environment file. Keep `NK_READ_ONLY=true`,
`NK_OAUTH_WRITES=false` and file ingestion off. Omit OAuth discovery/login/grant
settings from this separate environment. The server rejects simultaneous
bearer and OAuth discovery configuration. See [bearer configuration](development-bearer-auth.md)
for token syntax and request-level checks.

Use the same measured resource envelope as the actual retrieval workload;
validate model load, readiness and recovery under those limits. A configuration
`--check` does not load the model or establish usable retrieval. Keep durable
source-effect records and existing OAuth grant state intact.

Bound connections before they consume application descriptors. The application
body/session budgets run after HTTP connection admission; they cannot protect
against incomplete headers exhausting the listener's descriptors. Use a measured
private proxy envelope from [resource containment](production-resource-limits.md),
or an equivalently verified transport boundary. Preserve the local bearer and
backend Host/Origin policy through that hop. Keep operator health access direct
and private so proxy saturation does not consume its capacity.

## Configure the tunnel

Follow current [Secure MCP Tunnel guidance](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels)
and the installed client's help. Reuse only the intended tunnel, confirm its
actual target ChatGPT workspace association and Read + Use access, and keep
runtime keys private. A successful local doctor check does not prove cloud
credential validity or workspace association.

Select the client's plain HTTP/no-OAuth profile and point its main MCP channel
at the loopback `/mcp` endpoint. Configure `mcp.extra_headers.Authorization`
and `mcp.discovery_extra_headers.Authorization` as private `env:` or `file:`
references containing the complete `Bearer …` header value. Never embed the
literal credential in a tracked profile or command argument. Keep its operator
health/admin listener on loopback too. Direct stdio is another supported private
transport, but does not use the HTTP bearer settings above.

With the inspected client v0.0.19, static MCP headers are scoped to the configured
server origin/path; discovery requests receive the discovery header map too.
Forwarded connector headers take precedence over static headers. An existing
OAuth connection can therefore replace the local `Authorization` header:
verify **No authentication** on the actual selected connection. Keep
`log.http_raw_unsafe` / `LOG_HTTP_RAW_UNSAFE` disabled; raw HTTP dumps can include
the final credential and source bodies. Treat client logs as private.

Verify unauthenticated `/mcp` requests return 401, then verify authenticated
initialization and tool discovery. Run the owned tunnel client and check its
actual readiness and polling. An expired runtime key requires replacement;
restarting the daemon cannot repair it. Do not stop or modify unrelated tunnels.

## Per-installation ChatGPT acceptance

In ChatGPT's custom MCP connection UI, select **Tunnel**, the verified tunnel,
and **No authentication**. These are separate selections; tunnel transport does
not automatically change an existing OAuth connection's authentication choice.
See [custom MCP connections](https://developers.openai.com/api/docs/guides/custom-mcp-server).

Keep the local URL and its authority unchanged while testing. The server's
loopback Host/Origin checks remain active behind the tunnel; forwarding the
private endpoint does not authorize a public Host or browser Origin. Diagnose
a 403 at those checks separately from a 401 at the local bearer gate.

Record the deployed revision, date, selected connection and sanitized outcomes:

| Check | Required live evidence |
| --- | --- |
| Discovery | This actual connection initializes and lists the expected read tools; no write tools are exposed. |
| Useful reads | A fresh multi-word search and multi-character source-ID get return the expected authorized fixture and citation. A local schema check or an answer without a new tool call is insufficient. |
| Scope | Caller input cannot widen configured roots; out-of-scope content is denied without disclosure. |
| Refresh | Refresh the connection's discovered tools, open a new conversation, and repeat the real reads using the refreshed schema. |
| Restart | Restart only the owned endpoint/tunnel, then verify recovery and a new successful tool call. |
| Disable | Disable the selected connection or stop its owned tunnel; new calls through that route fail. Restore it and verify recovery. This is transport denial, not OAuth token revocation. |

Keep raw logs, source IDs, page bodies, signed media URLs and credentials local.
Store only sanitized observations in the repository. Mark missing evidence
explicitly; local HTTP checks and the earlier development spike cannot establish
actual ChatGPT acceptance. Full approved-root sync, write/file acceptance and
primary cutover remain tracked separately and are not waived by this transport
choice. #559 stays open until its live installation checks and teardown are
verified. This acceptance is required for claiming that installation works,
not for accepting locally verified product behavior.

## Verified local checkpoint — 2026-11-07

The reviewed shipping artifact for merged PR #634 passed an operator-local,
read-only HTTP check under a 4 GiB memory limit with swap disabled. Semantic,
lexical and hybrid search, indexed and fresh reads, and retrieval of the retained
fixture's two existing attachments passed. Unauthenticated requests returned
401; an unknown reference was denied. Only read tools were advertised, and
durable source-effect records were unchanged. The temporary listener was stopped
after verification; the existing executable and service were preserved.

This establishes the checked local read-only retrieval behavior, not untested
write/file operations or full approved-root sync. Actual ChatGPT tunnel discovery,
reads, refresh, restart and disable/re-enable checks remain pending in #559.
The prepared installation's runtime credential was rejected as expired; a valid
credential, workspace association and access still need verification. The
runbook can be reviewed and merged independently of those operator steps.

A separate temporary loopback proxy check demonstrated connection/rate rejection
and a five-second incomplete-header deadline. During 1,100 owned connection
attempts, backend readiness stayed available and authenticated MCP discovery
recovered afterward. Backend descriptor use peaked at 19 with upstream
connections bounded at eight. This verifies that temporary proxy configuration;
the final tunnel route and its effective limits still need acceptance.
