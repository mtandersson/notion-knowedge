# Production ChatGPT OAuth connection acceptance

Use this read-only operator procedure after [production setup](oauth-production-setup.md).
It prepares #559's live acceptance; this checklist itself does not establish a
working production connection. #117's [HTTP tests](oauth-e2e-tests.md) use a fake
Notion provider. #216's [ChatGPT experiment](chatgpt-spike.md) demonstrated a
private development connection without production OAuth. Neither proves this flow.

## Prepare the connection

Record the deployed commit, binary feature set, date, ChatGPT account/workspace
route and a nonsecret fixture page already readable in the approved scope.
Retain actual workspace/user IDs and private source material locally. Choose one
connection label so evidence can distinguish it from the ordinary Notion app.
This procedure does not authorize deployment, public exposure, new credential
recipients, source edits, uploads or additional corpus ingestion.

Complete the production guide's two registrations, private grant storage,
independently verified workspace/person allowlist and optional retrieval setup.
Use ChatGPT's exact displayed callback and a predefined public client with S256
PKCE and token authentication `none`. This server has no DCR or CIMD support;
stop if the account's connection UI cannot configure that client. Never substitute
the Notion client secret for an MCP client secret. Run `--check` through the
private service environment; success validates configuration, not connectivity.

Official OpenAI guidance checked 2026-10-26:
[authentication](https://developers.openai.com/plugins/build/auth),
[custom MCP connections](https://developers.openai.com/api/docs/guides/custom-mcp-server),
and [Secure MCP Tunnel](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels).
Account/workspace policy can restrict adding or using connections. Confirm the
actual account's supported route before starting any endpoint.

## Select and verify transport

For approved HTTPS deployment, use the production guide's canonical issuer and
resource, TLS proxy, Host/Origin restrictions and session/SSE forwarding. Keep
the backend and diagnostic endpoints private. Verify both well-known documents
and the unauthenticated `/mcp` challenge over the intended route: their canonical
issuer/resource and authorization/token/revocation endpoints must match the
registration. A metadata 200 alone is not authorization evidence.

For private MCP access, Secure MCP Tunnel can carry discovery and MCP traffic,
but does **not** automatically tunnel the authorization server. Our browser
entry `/authorize`, Notion callback `/oauth/notion/callback`, token endpoint and
issuer metadata belong to the same server. Before attempting consent, map how
the browser, Notion redirect and ChatGPT token client reach their advertised
HTTPS URLs. A tunnel to loopback `/mcp` alone does not establish that reachability.
Any proxy route still needs the production Host/Origin and credential safeguards.
Do not change canonical OAuth URLs to loopback or disable validation to repair it.

Confirm the tunnel is associated with the intended ChatGPT workspace, the
operator has appropriate access, and the owned tunnel client is ready using its
local `doctor`/health surfaces. Keep runtime tunnel credentials separate from
Notion credentials. The #216 noauth sample is development evidence, not a
production OAuth tunnel recipe. Record this production combination as unverified
until the actual browser/token flow succeeds. If it needs a new HTTPS route or
credential recipient, obtain the applicable explicit authorization first.

## Run and record live acceptance

Use only the approved connection in a new ChatGPT conversation. Keep raw browser
traces, tokens, cookies, authorization codes, private IDs and provider responses
out of committed evidence. Record sanitized outcomes using the table below;
leave unexecuted steps marked **not run** rather than treating a fixture as proof.

| Step | Required observation |
| --- | --- |
| Discovery | Actual ChatGPT connection obtains metadata from the selected route; record configured issuer/resource aliases and any sanitized failure class. |
| Browser login | ChatGPT launches MCP authorization, then real Notion consent; consent uses the independently configured person/workspace. Record successful return to ChatGPT without URLs containing codes/state. |
| Identity boundary | Under a separately approved test identity, wrong workspace and wrong authorizing person each fail closed, without a usable MCP connection. Do not weaken the allowlist or collect another person's credentials. If unavailable, mark this live evidence missing and retain local rejection-test evidence separately. |
| Tool discovery | Record actual advertised names and enabled tools in ChatGPT. Expect `knowledge_search`, `knowledge_get` and `knowledge_sync_status`; optional tools depend on explicit installed configuration. Leave graph and mutation features off for this read-only acceptance. |
| Useful read | Explicitly ask this connection to call `knowledge_get` for the approved fixture, or `knowledge_search` with a known query when retrieval is installed. Capture actual tool name/arguments with private values redacted, success class and expected citation match. A discovered tool or a model answer without a fresh call is insufficient. |
| Expiry/reconnect | After the MCP token expires, confirm authorization is denied and reconnect through browser login; capture the actual client's behavior rather than assuming seamless refresh. |
| Revocation | Use an approved client/operator procedure from the revocation guide, then confirm the old authorization no longer permits reads and a fresh approved login works. Record which mechanism was exercised. |
| Discovery refresh | Follow the refresh procedure below and record the actual refreshed names/schema, or record that no schema change was available to test. |

For each row record UTC timestamp, pass/fail/not-run, sanitized observation and
local evidence reference. Include deployed revision, transport route, exact scope
configuration revision, limitations and teardown outcome in the final report.
Do not commit raw consent screenshots or full logs without reviewing them for
secrets and private content. #109 stays open until all original live criteria
are supported; unavailable identities or browser steps remain explicit gaps.

## Reauthentication and revocation

The current MCP access token lasts **600 seconds** and lives only in process
memory. The server advertises only `authorization_code`, issues no MCP refresh
token and accepts no MCP refresh-token grant. Reauthorization requires the
browser flow; restarting discards MCP tokens, sessions and pending transactions.
Persisted Notion consent does not make an old MCP token valid. Server-side
[Notion refresh](notion-oauth-refresh.md) rotates upstream credentials separately;
it does not renew the ChatGPT MCP token.

[Local revocation and forced reauthentication](oauth-revocation.md) describe
`POST /revoke`, exact-token session/SSE cancellation and offline
`--notion-oauth-forget`. Removing a connection in ChatGPT is not evidence that
it called `/revoke`; observe the actual client behavior. Session deletion alone
is not token logout. Operator forget requires stopping credential owners and
keeping the same private environment, key and allowlist. It removes local grant
state, not remote Notion consent. Provider-side removal is a separate operator
action. Never restore an old grant to undo revocation.

## Stale tools and troubleshooting

Open the connection's details in ChatGPT app settings and refresh the app to
fetch current tool definitions, descriptions and server instructions. Check
whether expected tools are enabled there. After refresh, select this connection
in a new conversation and require an explicit fresh tool call. A previous answer
can remain in conversation context even when discovery has changed.

If that fails, confirm the deployed revision and actual server configuration,
listener readiness and selected connection/route before reconnecting. A server
restart invalidates old sessions and tokens: complete browser authentication
again. For a tunnel, check only its owned client's readiness and workspace
association. Avoid deleting shared tunnel configuration as a troubleshooting step.
A `401` calls for valid authorization; `503` on reserved login routes indicates
incomplete configuration. Consult the production guide for provider/identity and
retrieval failures. Do not enable anonymous access, change allowlisted IDs or
send upstream credentials to ChatGPT to bypass a failure.

Static bearer authentication remains **development/emergency fallback only** in
a separate loopback configuration. It cannot establish the production OAuth
person/workspace boundary. Stop only temporary processes owned by this test,
confirm their listeners close and preserve unrelated services, tunnel identities,
private state and authoritative Notion content.
