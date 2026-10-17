# Notion OAuth callback and confidential exchange (#122)

The browser-bound callback is mounted only with explicit MCP client
registration and sealed grant storage. `/authorize` starts consent; a successful
callback persists the approved grant before returning a PKCE-bound MCP code.
[`POST /token`](oauth-code.md) checks live grant evidence before minting an MCP
bearer. [Session authorization](oauth-mcp-sessions.md),
[revocation](oauth-revocation.md) and [request-bound content](oauth-content.md)
complete the resource boundary. See [production setup](oauth-production-setup.md).

## Startup configuration

Alongside the HTTPS discovery and Notion redirect settings from
[the previous step](notion-oauth-redirect.md), configure the following
operator-held **private** settings:

```sh
NK_NOTION_OAUTH_CLIENT_SECRET=<server-side Notion OAuth client secret>
NK_NOTION_ALLOWED_WORKSPACE_ID=<immutable approved Notion workspace id>
NK_NOTION_ALLOWED_USER_ID=<immutable approved owner.user.id>
```

If any of these three is present, all three and the existing
`NK_NOTION_OAUTH_CLIENT_ID` and `NK_NOTION_OAUTH_REDIRECT_URI` are required
at startup. No owner/workspace values are learned from the first OAuth
authorization or from untrusted client data. `Config` holds the secret in
a redacted wrapper and error messages name only invalid keys, not values.
A staged redirect-only configuration without these settings remains
possible but **never enables callback handling or token issuance**.

The HTTP callback also requires the [fixed MCP client registration](notion-oauth-redirect.md#browser-entry-point)
and the paired `NK_NOTION_GRANT_KEY_FILE` / `NK_NOTION_GRANT_STATE_FILE`
[sealed store settings](notion-grant-store.md). It shares the same store
instance as the server credential runtime. Without the full composition,
`GET /oauth/notion/callback` returns a sanitized 503; it cannot fall back
to an ephemeral or plaintext grant.

## Callback state, exchange and typed grant

1. The internal `NotionCallback::complete(raw_query)` method accepts only a
   bounded, well-formed callback query with one `state`, one `code` or
   one `error`. Duplicate parameters, mixed error+code, unrecognized
   parameters, invalid percent encoding, controls, missing/oversized fields
   and malformed codes fail closed. Error descriptions are discarded.
2. The callback **atomically consumes** the fresh, 120-second, single-use
   Notion anti-CSRF state from #121 and recovers its linked pending MCP
   transaction **before any token exchange**. Forged, expired or replayed
   state can never trigger a Notion network request.
3. The confidential server requests
   `POST https://api.notion.com/v1/oauth/token` using HTTP Basic with the
   registered Notion client ID and its separate client secret. The JSON
   body contains only `grant_type=authorization_code`, the Notion code
   and **exact original registered redirect URI**. The request is HTTPS,
   has a 12-second timeout and follows **no redirects**. The production
   token URL is fixed and cannot come from incoming HTTP headers or query.
4. Provider status/error bodies and transport diagnostics are discarded,
   never logged or returned. Success bodies are bounded to 64 KiB before
   typed parsing. Response must contain a nonempty opaque Notion access
   token, `token_type=bearer`, a valid `bot_id`, a nonempty
   `workspace_id`, `owner.type=user` and a valid `owner.user.id`.
   Optional refresh token, if present, must also be well formed.
   Unknown additional Notion fields are safely ignored.
5. The returned user and workspace must **exactly match** the immutable
   allowlist. A different user in the same workspace or the same user
   in a different workspace is denied. Email, display name, bot ID and
   integration-token get-self identity can never substitute for the owner.
   The typed successful result (`VerifiedCallback`) holds the still
   unapproved MCP transaction plus private `NotionGrant` credentials.
   Both types redact their `Debug` representation and are not serializable.
   The method never calls `AuthorizationFlow::approve` or issues an MCP
   token merely because Notion exchanged a code.

## Sealed callback persistence (#298)

The server-internal NotionCallback::complete_and_persist(raw_query, store) now composes
the existing single-use callback validator with the AES-256-GCM grant store.
It rejects a mismatched Notion client **before** consuming callback state or
contacting Notion. After confidential code exchange and immutable owner/workspace
verification, the store independently rechecks the owner, workspace and
existing bot identity under its write lock, before atomically persisting the
full access/refresh-token pair and expiry metadata. Failed validation or
corrupt state does not replace the existing grant. The new grant receives a
fresh grant ID and authorization epoch and survives a process restart.

## Public HTTP callback

The exact registered path is `GET /oauth/notion/callback` on the canonical
HTTPS issuer. The trusted proxy uses the backend Host/Origin policy already
required by discovery; spoofed authorities are rejected before callback work.

The adapter accepts exactly one `__Host-nk-oauth` cookie across all Cookie
headers. It requires that independent secret to match the digest attached to
the fresh Notion state. Missing, malformed, duplicate or mismatched cookies
cannot exchange a code or consume another browser's transaction. The internal
unbound callback method cannot consume browser-started state. The Notion state
is consumed atomically before provider I/O, including provider-denial responses.

At most four callback exchanges run simultaneously per server. Overload returns
503 before consuming state. Provider calls retain their 12-second timeout and
bounded response size. On verified success the shared sealed store independently
checks the immutable identity and bot binding and commits the credentials before
the MCP code engine may approve the original transaction. Storage failure or a
changed grant fails closed. The original MCP redirect, state, read scope, S256
challenge and resource remain bound to the resulting code; neither incoming
callback fields nor Notion's response can choose a client destination.

Success returns 302 to the exact registered MCP client destination, preserving
registered query parameters and adding only the MCP `code` and original `state`.
The cookie is cleared. All outcomes use `Cache-Control: no-store` and
`Referrer-Policy: no-referrer`. Failures use one static user-facing message,
never a provider body, callback query, secret, path or identity. The server's
payload-free HTTP operation logs retain status and correlation ID for denials;
operators must also redact callback queries and redirect Locations in proxy logs.

Verified owner/workspace policy failures additionally emit a fixed
`notion_oauth_workspace` or `notion_oauth_user` operation with outcome `rejected`
at warning level. These events carry the same server-generated correlation ID
as the enclosing HTTP rejection, never the supplied/configured IDs, tokens,
provider error, URL or callback fields. When both identity claims mismatch,
the workspace rejection is recorded first. Malformed provider grants remain
fail-closed and cannot be mistaken for a verified allowlist match. Set
`NK_LOG_LEVEL=warn` (or more verbose) to retain these security denials.

The callback itself returns no MCP access token; the separate token endpoint
requires the original PKCE verifier and current grant binding. The installed transport and content adapters perform live grant, token,
scope/session and revocation checks on resource operations. A lifetime state
lease supports a single credential runtime; multi-process deployment is unsupported.

## Verification

The callback tests use a local fake HTTP provider (in tests only; the
production token endpoint cannot be overridden). They verify the outgoing
server-side POST and Basic authorization, exact redirect, typed grant,
redacted secrets, mismatching user/workspace rejection, malformed provider
bodies, provider denials, bad owner types, forged and replayed state, strict
query handling, configuration incompleteness and error sanitization.

```sh
nix develop --command cargo test -p notion-knowledge-server notion_oauth_callback --locked
nix develop --command cargo test -p notion-knowledge-server notion_callback_credentials --locked
nix develop .#format --command cargo fmt --all -- --check
```

HTTP tests exercise the production discovery/browser router over real local
sockets with a test-only fake provider, proving cookie/state admission, code
exchange, exact client redirection, sealed identity, replay denial, user/workspace
and malformed-grant rejection, provider failures and bounded callback admission.
The binary-level discovery suite proves callback denial without sealed storage.
The production Notion endpoint cannot be overridden by configuration.

The [joined acceptance suite](oauth-e2e-tests.md) covers token/resource/revocation
paths. These fixture tests make no claim that a real external connection has
completed; follow [production setup](oauth-production-setup.md) for that operator smoke.
