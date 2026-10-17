# Notion OAuth authorization redirect (#121)

The separate `notion_oauth_redirect` module prepares Notion's consent URL
**only after** the #120 MCP public-client Authorization Code + S256 PKCE engine
accepts an MCP authorization request. It cannot authenticate a caller by itself.

## Configuration

Set the following **operator-controlled** environment variables alongside
the existing `NK_OAUTH_ISSUER` and `NK_OAUTH_RESOURCE` discovery settings:

```sh
NK_OAUTH_ISSUER=https://auth.example.com
NK_OAUTH_RESOURCE=https://mcp.example.com/mcp
NK_NOTION_OAUTH_CLIENT_ID=<your public Notion integration client ID>
NK_NOTION_OAUTH_REDIRECT_URI=https://auth.example.com/oauth/notion/callback
```

The Notion client ID and redirect URI must be supplied **together**; either
missing value fails startup. The registered Notion callback must **exactly**
equal the canonical HTTPS issuer plus `/oauth/notion/callback`. Callback
fragments, query strings, userinfo, alternative origins and unregistered
paths are rejected. This avoids open redirects and attacks relying on
`Host`, `Forwarded` or a browser-supplied callback. Configure this exact
HTTPS callback in the Notion public integration's settings. The client ID is
public. The redirect core alone needs no confidential client secret; the
optional browser entry point below requires the full server-only callback
configuration. The secret is never placed in a URL and is reserved for the
server-only exchange in #122.

## Consent and state

Notion's fixed endpoint is `https://api.notion.com/v1/oauth/authorize`.
`NotionOAuthRedirect::begin(flow, request)` first validates the whole
registered ChatGPT MCP request through the #120 `AuthorizationFlow::begin`
gate. It then produces a separate 256-bit random Notion anti-CSRF `state`
and stores only `SHA256(state)` as the index to its unapproved MCP
transaction handle. The URL contains only `owner=user`,
`response_type=code`, the fixed Notion `client_id`, percent-encoded,
operator-registered Notion callback URI and the fresh `state`. It contains
neither the MCP transaction secret nor any Notion client secret/token.

Notion doesn't accept a standard arbitrary OAuth `scope` parameter for
per-request capabilities: integration capabilities are configured in the
Notion developer settings, and the consenting user selects which pages are
shared. For this read-only MCP, configure **read content** and only the other
capabilities strictly required for enabled tools; avoid broad writes and
workspace-wide access. Scope/root/operation checks still must happen server
side after the OAuth grant is approved.

The state expires at **120 seconds**, each state is **single use**, maximum
**256** are pending, expired states are rejected, and process restart
invalidates all states. Concurrent callback requests atomically remove the
entry, so only one can acquire the corresponding pending MCP handle. Secrets
are redacted from `Debug`; neither query strings nor state are logged by
this module. The eventual HTTP adapter must also redact incoming callback
query strings, error bodies and proxy request logs.

**Very important:** `take(state)` returns only a correlation handle, never
a trusted identity or a token. The server-side Notion callback in #122 must
validate and consume state *before* exchanging the Notion code, validate the
canonical Notion `owner.type=user` plus exact operator allowlisted
`owner.user.id` and `workspace_id`, and persist a live grant before
calling `AuthorizationFlow::approve`. A forged or replayed callback must
not invoke that method.

## Browser entry point

To mount `GET /authorize`, additionally register one MCP public client and
configure the complete server-only callback identity policy:

```sh
NK_OAUTH_CLIENT_ID=chatgpt
NK_OAUTH_CLIENT_REDIRECT_URI=https://chatgpt.example.com/callback
NK_NOTION_OAUTH_CLIENT_SECRET=<server-only secret>
NK_NOTION_ALLOWED_WORKSPACE_ID=<operator-approved workspace ID>
NK_NOTION_ALLOWED_USER_ID=<operator-approved user ID>
```

Use the actual exact callback supplied by your MCP client, not the example.
The two `NK_OAUTH_CLIENT_*` settings are paired. Missing discovery or Notion
callback policy rejects startup. Without this registration, `/authorize`
continues returning 503. No client registration is learned from a request.

The HTTP entry point requires all eight parameters: `response_type=code`,
`client_id`, `redirect_uri`, canonical `resource`, `state`, S256
`code_challenge`, `code_challenge_method=S256`, and `scope=knowledge:read`.
It rejects missing, repeated, unknown, oversized or malformed parameters and
invalid registered-client/PKCE values with a sanitized 400 response, without
redirecting to any caller-supplied URI. Accepted requests return 302 to the
fixed Notion consent endpoint, with `Cache-Control: no-store` and
`Referrer-Policy: no-referrer`. Existing backend Host/Origin checks apply.

Each accepted browser request also sets an independent 256-bit random
`__Host-nk-oauth` cookie (`Secure; HttpOnly; SameSite=Lax; Path=/;
Max-Age=120`). Only its digest is bound to the pending Notion state. HTTP callbacks call `take_browser(state, cookie)`: the unbound `take`
cannot consume browser transactions, and a missing/wrong cookie cannot
consume another browser's state. Both secrets expire after 120 seconds and
state is single-use. A new login replaces the cookie; this deliberately
supports one pending login per browser. The cookie never appears in the
Notion URL. Use HTTPS at the configured issuer; do not weaken cookie security
for a plain-HTTP development browser.

## Installed composition

With complete client/callback/sealed-store configuration, `/authorize` starts
Notion consent, the [callback](notion-oauth-callback.md) saves the allowlisted
grant, and [`POST /token`](oauth-code.md) redeems its MCP code with PKCE S256.
The [MCP transport](oauth-mcp-sessions.md) requires live grant-bound authorization;
[local revocation](oauth-revocation.md) closes token-owned sessions. Partial
configuration leaves callback/token routes unavailable. Follow
[production setup](oauth-production-setup.md) for both exact registrations.

Unit tests verify the fixed endpoint, correct URL encoding, lack of
scope/secret injection, two independent states/transactions, wrong MCP client
or PKCE, invalid callback/client settings, expiry, replay and restart.
The config parser independently checks all environment-variable combinations.

```sh
nix develop --command cargo test -p notion-knowledge-server notion_oauth_redirect --locked
nix develop --command cargo test -p notion-knowledge-server notion_oauth_redirect_requires --locked
nix develop .#format --command cargo fmt --all -- --check
```

The process-level `oauth_discovery` suite also tests the actual HTTP redirect,
security cookie, parameter denial, Host protection and continued token/MCP denial.
