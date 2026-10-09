# Notion OAuth callback and confidential exchange (#122)

**Status:** staged, server-internal callback verifier. No public callback route is
mounted. OAuth discovery still denies `/authorize` and `/token` (503), and
`/mcp` (401). The application **must not** expose a private ChatGPT MCP
resource until approved grants, durable storage, revocation and
request/session-level token enforcement are in place (#123–#129).

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

**Crucial next gate:** the grant store in #123 must protect credentials,
record exact identity and integration binding, assign a durable
`grant_id`/authorization epoch, and check the configured Notion roots
and operations. Only then can the workflow re-enter the #120 authorization
engine. #127/#128 must reject revoked/stale grants on every MCP operation.
The current design keeps callback **unmounted** until this is safe.

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

Future #129 integration tests must exercise production HTTP routing,
proxy headers and logging, Notion error/callback handling, durable
transactions/revocation, single-user deny matrix and ChatGPT MCP
session enforcement. These tests are not a substitute for a complete
remote login verification.
