# Production OAuth setup for ChatGPT and Notion

This is a single-person, single-process, read-only MCP deployment. ChatGPT
receives server-issued MCP tokens; the server alone holds Notion credentials.
A successful Notion login grants access only when its exact workspace and
**authorizing person** match policy configured before login.

Use a dedicated service identity, private local storage and a controlled HTTPS
reverse proxy. Do not share the grant state across replicas. Browser transactions,
authorization codes, MCP tokens and sessions live in memory; restarting requires
ChatGPT to authenticate again. The sealed Notion grant persists across restart.

## Register the two OAuth sides

Choose one canonical public hostname. This guide uses
`https://knowledge.example.com` as issuer and
`https://knowledge.example.com/mcp` as the protected resource.

| Registration | Exact callback |
| --- | --- |
| Notion public connection | `https://knowledge.example.com/oauth/notion/callback` |
| ChatGPT predefined public MCP client | The exact redirect URI displayed by ChatGPT for this MCP connection |

Create a **public** Notion connection, configure its OAuth redirect above and
keep its OAuth client secret server-side. Grant only the read capabilities
needed by this service. During consent, share the intended root pages.
Notion describes public connection setup, consent and the token response in
[Authorization](https://developers.notion.com/guides/get-started/authorization).

Before exposing the service, independently verify the intended `workspace_id`
and `owner.user.id` using an existing audited authorization/admin inventory.
The OAuth response's `owner.user.id` identifies the authorizing person;
`bot_id` and the top-level ID from `/users/me` identify the connection bot and
must not substitute for that person. Workspace names, email addresses and page
IDs are not allowlist identifiers. If these identifiers are unknown, keep the
service unexposed until an operator obtains and verifies them independently.
Do not enroll the first caller or copy identity values from denied-callback logs.

Create a ChatGPT custom MCP connection with the resource URL above and OAuth
using a **predefined public client**, PKCE S256 and token authentication `none`.
Choose a client ID and use it on both sides; there is no MCP client secret.
Copy ChatGPT's exact displayed callback into server configuration before linking.
[OpenAI authentication guidance](https://developers.openai.com/plugins/build/auth)
documents predefined clients and PKCE. This server implements neither CIMD nor
DCR nor RFC 9207 issuer response identification; do not enable those modes or
assume the stable ChatGPT callback is applicable. If the available ChatGPT UI
cannot configure a predefined public client, stop and resolve that compatibility
rather than inserting a Notion secret into the MCP client configuration.

## Bootstrap private state and configuration

Run these as the dedicated service identity, choosing persistent private paths:

```sh
umask 077
mkdir -p "$HOME/.config/notion-knowledge/secrets" "$HOME/.local/state/notion-knowledge"
chmod 700 "$HOME/.config/notion-knowledge" "$HOME/.config/notion-knowledge/secrets" "$HOME/.local/state/notion-knowledge"
openssl rand -out "$HOME/.config/notion-knowledge/secrets/grant.key" 32
chmod 600 "$HOME/.config/notion-knowledge/secrets/grant.key"
```

Generate the key **once**: it is 32 raw bytes, not hex or base64. Keep a protected
key backup separately from grant backups. Never commit keys, secrets or grant
files. The state file is initially absent; do not create an empty JSON file.
The service creates sealed state only after approved consent. Existing corrupt,
foreign or insecure state fails startup rather than being overwritten.

Supply this environment through a private service environment/secret manager.
Replace every placeholder; load the Notion client secret there without putting
it in shell history, command arguments or repository files.

```sh
NK_HTTP_HOST=127.0.0.1
NK_HTTP_PORT=3000
NK_NOTION_AUTH=none
NK_OAUTH_ISSUER=https://knowledge.example.com
NK_OAUTH_RESOURCE=https://knowledge.example.com/mcp
NK_OAUTH_CLIENT_ID=knowledge-chatgpt
NK_OAUTH_CLIENT_REDIRECT_URI=<exact-ChatGPT-displayed-callback>
NK_NOTION_OAUTH_CLIENT_ID=<Notion-public-connection-client-id>
NK_NOTION_OAUTH_CLIENT_SECRET=<secret-from-private-service-environment>
NK_NOTION_OAUTH_REDIRECT_URI=https://knowledge.example.com/oauth/notion/callback
NK_NOTION_ALLOWED_WORKSPACE_ID=<independently-verified-workspace-UUID>
NK_NOTION_ALLOWED_USER_ID=<independently-verified-authorizing-person-id>
NK_NOTION_GRANT_KEY_FILE=<absolute-home>/.config/notion-knowledge/secrets/grant.key
NK_NOTION_GRANT_STATE_FILE=<absolute-home>/.local/state/notion-knowledge/grant.json
```

These are service environment entries, not a shell script to source unchanged.
The two grant paths must be distinct and controlled by the service identity.
Do not configure `NOTION_TOKEN`, integration authentication or static HTTP bearer
authentication for this OAuth service. Configuration is immutable during a run;
change policy by stopping and restarting the service. The grant runtime holds an
exclusive lifetime lease on its state; never delete its `.lock` sidecar while
any runtime or credential probe is running.

The commands below assume the built binary is installed on the service PATH
(or replace its name with the absolute build output path). Validate the supplied
environment and then start it:

```sh
notion-knowledge-server --check
notion-knowledge-server --http
```

`--check` validates configuration and configured sealed-store/key safety. It does not prove provider connectivity,
consent, index compatibility or a working ChatGPT connection.

## Enable useful content deliberately

OAuth without content scope exposes no private knowledge. To enable scoped content,
configure `NK_NOTION_SCOPE_WORKSPACE_ID` equal to the allowlisted workspace
and `NK_NOTION_SCOPE_ROOTS` as the operator-approved physical root page IDs.
Use the generation/exclusion settings in [root scope configuration](mcp-root-scope-enforcement.md).
The consented connection must actually have permission to read those pages.

Search and `knowledge_get` (including `freshness: "fresh"`, which first expands
indexed references) require a Unix binary built with
`webhook-worker`, an existing compatible private committed SQLite/Lance index,
FTS and trained vector indexes, and pinned offline Qwen assets:

```sh
nix develop .#spike --command cargo build -p notion-knowledge-server --features webhook-worker --locked
```

Set `NK_RETRIEVAL=true`, `NK_WEBHOOK_STATE_FILE`, `NK_WEBHOOK_INDEX_DIR` and
`NK_WEBHOOK_QWEN_ASSETS` as described in [installed retrieval](installed-retrieval.md),
with the exact committed workspace/root/generation binding. Read activation does
not populate, migrate or repair storage. Prepare/update the index through the
separate trusted operator indexing workflow; stop conflicting credential users
and writers as required by their ownership rules. OAuth content does not enable
webhook workers, reconciliation schedules, file ingestion or mutation tools.
The stock Docker image/Compose bootstrap does not package this production
retrieval path; see [container limitations](container.md).

## Put the loopback service behind HTTPS

Keep port 3000 inaccessible to remote clients. Terminate trusted TLS at the proxy
and route only the OAuth/MCP paths to this same process. Use the [NGINX example](examples/oauth-nginx.conf) inside its `http {}` context,
on a dedicated HTTPS listener. Adapt its hostname and certificate paths before
exposure; its explicit Origin allowlist admits no Origin, the public service
origin and `https://chatgpt.com`. Review any additional trusted origin explicitly. The proxy must reject foreign public Host and Origin values **before**
rewriting the approved request to the loopback Host/Origin accepted by the
backend. Blindly removing Origin would bypass this boundary.

Preserve Authorization, cookies, MCP protocol/session headers and SSE streaming;
disable buffering and sensitive access logs (queries, authorization codes,
cookies, bodies and credentials). Keep diagnostics and operator endpoints private.
Public discovery values come from configured canonical URLs, never request Host
or forwarded headers. Inspect both well-known documents over trusted HTTPS and
confirm their exact issuer, resource, callback and endpoint URLs.

## Link, verify and reconnect

Start the server and proxy with the complete environment, then link the custom
MCP connection in ChatGPT. The browser goes to this server, then Notion; consent
must use the configured person and workspace. On success, the callback seals the
Notion grant before issuing a short-lived MCP authorization code. ChatGPT
exchanges that code using PKCE and sends its MCP bearer on resource requests.

Before using private content, perform an operator smoke with a known nonsecret
fixture: discover tools, read its page, and search it if retrieval is installed.
Confirm excluded roots are unavailable and an unapproved workspace/person cannot
link. Inspect sanitized errors and logs without recording tokens or private text.
The credential-free [OAuth end-to-end suite](oauth-e2e-tests.md) verifies local
protocol behavior; it does not establish a real ChatGPT/Notion production smoke.
Record the actual deployed-client smoke separately before relying on the service.

For logout, the client sends its exact MCP token to the advertised `POST /revoke`;
this cancels its sessions and SSE without contacting Notion. Session deletion
alone is not token logout. See the safe form contract in
[logout and forced reauthentication](oauth-revocation.md).

To force a new upstream login, stop the server, run the installed binary with
the **same private service environment and identity**, then restart:

```sh
notion-knowledge-server --notion-oauth-forget
notion-knowledge-server --http
```

The forget command refuses a busy runtime and removes only a valid configured
sealed grant. Keep the key and allowlist; complete browser login again as the
same approved person/workspace. It is local invalidation, not remote Notion
consent revocation. Remove the connection in Notion separately when necessary.
Do not restore an old grant to undo logout: local storage is not rollback-resistant.

## Troubleshooting and fallback

- `401` on `/mcp`: absent, expired, revoked or incompatible MCP authorization;
  reconnect through browser login. A session ID is not authentication.
- `503` on reserved OAuth routes: incomplete login configuration. Check the
  complete client, callback, allowlist and grant-store settings above.
- Denied workspace/person: verify independently configured IDs and selected
  account; do not weaken policy to accept the failed caller.
- Inactive/unknown-expiry/refresh failure: authorization fails closed. Provider
  active introspection is required when expiry is unknown; ambiguous rotation may
  require a new login. No integration-token fallback is attempted.
- Retrieval unavailable/startup rejected: inspect scope binding, private ownership,
  committed storage, FTS/vector indexes and model metadata. Do not silently
  overwrite or rebuild incompatible state to make startup pass.

Static `NK_HTTP_AUTH=bearer` is **development/emergency fallback only**, on a
separate loopback configuration. It is not this production OAuth connection and
does not provide the Notion-person/workspace login and lifecycle described here.
