# Private ChatGPT connection through Secure MCP Tunnel

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

Verify unauthenticated `/mcp` requests return 401, then verify authenticated
initialization and tool discovery. Run the owned tunnel client and check its
actual readiness and polling. An expired runtime key requires replacement;
restarting the daemon cannot repair it. Do not stop or modify unrelated tunnels.

## Actual ChatGPT acceptance

In ChatGPT's custom MCP connection UI, select **Tunnel**, the verified tunnel,
and **No authentication**. These are separate selections; tunnel transport does
not automatically change an existing OAuth connection's authentication choice.
See [custom MCP connections](https://developers.openai.com/api/docs/guides/custom-mcp-server).

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
choice. #559 stays open until its live checks and teardown are verified.
