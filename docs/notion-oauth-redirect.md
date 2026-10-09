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
public; its confidential client **secret is deliberately not configured here**,
is not placed in an URL and belongs to the server-only exchange in #122.

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

## Intentional deployment gate

The existing discovery router still returns **503** on `/authorize`
and `/token` and **401** on `/mcp`. There is currently **no**
`/oauth/notion/callback` route. The new component is reusable core with
validated startup configuration and unit tests, not a connected browser flow.
An HTTP route must not bypass the missing #122 trusted Notion callback,
#123–#126 grant storage/identity policy and #127–#129 tokens/session
authorization and negative-path verification. The standalone #121 code is
ready for that later composition, but **production remote MCP access is
still prohibited**.

Unit tests verify the fixed endpoint, correct URL encoding, lack of
scope/secret injection, two independent states/transactions, wrong MCP client
or PKCE, invalid callback/client settings, expiry, replay and restart.
The config parser independently checks all environment-variable combinations.

```sh
nix develop --command cargo test -p notion-knowledge-server notion_oauth_redirect --locked
nix develop --command cargo test -p notion-knowledge-server notion_oauth_redirect_requires --locked
nix develop .#format --command cargo fmt --all -- --check
```
