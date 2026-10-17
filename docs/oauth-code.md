# Authorization Code + PKCE HTTP flow (#120)

The reusable Rust engine in `crates/server/src/oauth_code.rs` supplies the
security-sensitive **Authorization Code + PKCE S256** mechanics. It is intended
for ChatGPT's public OAuth client and the MCP resource (RFC 7636, RFC 8707)
and depends on the [approved Notion identity model](oauth-trust-model.md).

The public HTTP composition now connects the registered-client authorization
request, browser-bound Notion callback, sealed allowlisted grant and MCP code
redemption. Discovery settings alone still enable no login: explicit client,
Notion callback policy and sealed storage are required. **MCP resource access
remains denied** until #127/#128 install request/session enforcement and
revocation. A token response alone is not permission to expose private tools.

## Transaction phases

1. A statically registered public `Client` binds one client ID, **exact**
   registered HTTPS redirect URI, and one configured HTTPS MCP resource.
   `AuthorizationFlow::begin` validates `response_type=code`, the exact
   client ID/redirect/resource, a client state, `scope=knowledge:read`,
   and exactly `code_challenge_method=S256` with a well-formed base64url
   SHA-256 challenge. Missing PKCE or `plain` is rejected; the resource
   indicator is required in both authorization and token exchanges.
2. `begin` produces a 256-bit unguessable **server-side transaction handle**
   with a 120-second deadline. A handle is not an OAuth code, token, client
   login or proof of Notion approval. The Notion callback (#121/#122)
   separately correlates its own anti-CSRF state before asking the
   code engine to continue.
3. Only after an authoritative Notion OAuth callback has been independently
   verified may the integration call `approve` with the live trusted
   `grant_id`, `authorization_epoch`, `owner.type=user`,
   `owner.user.id` and `workspace_id`. Both identity IDs must match
   server-configured immutable allowlist values. No automatically learned
   identity is accepted. This phase atomically consumes the transaction and
   issues a new 256-bit single-use authorization code (120-second TTL), while
   preserving the client state and exact redirect URI.
4. The token exchange `redeem` checks `grant_type=authorization_code`,
   identical client, exact redirect and MCP resource, valid verifier (43–128
   unreserved ASCII characters), and constant-time comparison of
   `base64url(SHA256(code_verifier))` with the stored challenge. The code is
   consumed under one mutex before verifying ownership, preventing races,
   replay and unlimited wrong-verifier attempts. The current approved grant
   must still match the stored grant ID, epoch, workspace and user.
5. An approved exchange creates a new **opaque MCP-only Bearer token** with a
   600-second TTL, one resource, `knowledge:read` scope and the grant binding.
   Notion access/refresh tokens never enter this engine or the MCP token.
   `verify` checks the current live grant ID/epoch and expiry, but does **not**
   replace production route/session authorization (#127/#128).

Transaction/code/token map keys are SHA-256 digests; bearer values and codes
are never in ordinary error or Debug output. The Linux/macOS runtime reads
256 bits from the operating system CSPRNG (`/dev/urandom`); entropy failure
aborts issuing secrets. Pending transactions and codes are capacity-bounded
(512 each), tokens are capped at 2048, and expired records are removed at
writes. Restart intentionally invalidates the process-local records.

## Verified callback-to-code composition

`NotionCallback::complete_persist_and_approve` now connects a single-use,
validated Notion OAuth callback to the in-memory PKCE engine **only after**
writing the authoritative callback grant to the versioned encrypted store.
The client cannot supply the approved grant ID, epoch, workspace or user;
those are read from the returned sealed-store snapshot. The store and the
PKCE engine independently apply the immutable owner/workspace allowlist.
A foreign Notion app is rejected before consuming state or contacting the
provider, storage errors never issue a code, and replayed callback state
fails before reaching the grant store. A replaced grant makes any code
invalid when the token endpoint rechecks the current store generation.

The public browser adapter uses the correlated-cookie variant of this method.
The unbound internal API cannot consume browser-started state. See the
[Notion callback contract](notion-oauth-callback.md).

## HTTP code redemption

`POST /token` is installed with the same explicit MCP client, Notion callback
policy and sealed grant settings used by the browser callback. Without those
settings it returns a sanitized 503. The existing Host/Origin guard applies;
issuer, client, callback and audience are never derived from request headers.

Send `Content-Type: application/x-www-form-urlencoded` (optional UTF-8 charset),
with **exactly** these form fields, not JSON, URL query parameters, cookies,
client secrets or an Authorization header:

```text
grant_type=authorization_code
code=<the MCP code from the registered client callback>
client_id=<the exact registered NK_OAUTH_CLIENT_ID>
redirect_uri=<the exact registered NK_OAUTH_CLIENT_REDIRECT_URI>
resource=<the configured NK_OAUTH_RESOURCE>
code_verifier=<the original public client's PKCE verifier>
```

The body is capped at **8 KiB** (413 for excess data). Missing, malformed,
unknown or repeated fields, including percent-encoded duplicate keys, fail
closed. Unsupported media types, compressed content, query/body mixing and
client credentials are rejected. The server reads the current sealed grant
and atomically claims the code against its identity/epoch and PKCE binding before any provider call. It then
requires the exact upstream grant to be active, refreshing expired/near-expiry
credentials through the shared runtime when appropriate. Only verified live
evidence can mint the MCP bearer, and the code deadline/binding is rechecked
after the provider response. Missing, inactive or mismatched grants cannot
issue a token; unreadable/corrupt state or provider failure returns a sanitized
503. A valid code is consumed even if later provider verification fails. It
never falls back to an operator integration token or to an identity supplied
by the client.

Success returns only `access_token` (new opaque MCP token), `token_type=Bearer`,
`expires_in=600`, and `scope=knowledge:read`. Neither Notion credential nor
identity metadata is returned. Responses and OAuth errors use
`Cache-Control: no-store` and `Pragma: no-cache`; error bodies contain a fixed
OAuth error code only. Codes are short-lived and single-use, including under
simultaneous redemption; an incorrect well-formed verifier consumes its code.
The endpoint shares the bounded four-operation admission pool with callbacks.

MCP bearer verification is not installed by this delivery: all MCP HTTP
methods still return 401, even with a newly issued token. `/revoke` is still
503. The token's upstream-grant binding must be checked on **every** resource
operation and session continuation in #127, with durable revocation in #128.
Process restart invalidates all pending codes and MCP tokens; it does not make
an old sealed grant into an authenticated session.

## Trust boundaries and remaining work

- #121/#122 and #123/#125/#126 supply the separate Notion flow, browser
  correlation, confidential exchange and encrypted allowlisted grant. Never
  pass a user-controlled HTTP identity to `approve`.
- #124 supplies provider-compatible active-state verification and server-side
  credential rotation; a saved approval is not proof that a Notion credential
  is permanently live.
- #127/#128 must persist/revoke grant epochs and MCP tokens, validate them on
  every transport operation, bind SSE/session continuity, revoke on logout,
  and protect against concurrent refresh/revocation and multiple server
  processes. The process-local engine does not solve distributed/HA security.
- #129 and #130 must exercise the public HTTP end-to-end login and configured
  HTTPS proxy with negative-path tests before access is enabled.

The implementation tests the RFC 7636 Appendix B verifier/challenge vector,
valid flow, incorrect or missing PKCE, strict client/redirect/resource,
wrong Notion user/workspace, single-use callback/code, replay, invalid
verifier, expiry, epoch rotation and process restart. Engine unit tests use synthetic grants. Real-socket HTTP tests additionally
exercise ChatGPT-style PKCE through a strict local fake Notion provider,
sealed grant commit and public code redemption, including deterministic code
expiry, malformed forms, replay, grant replacement and continued resource denial.
These fixtures use no real credentials and do not prove a live external
ChatGPT connection or private MCP resource readiness.

```sh
nix develop --command cargo test -p notion-knowledge-server oauth_code --locked
nix develop --command cargo fmt --all -- --check
```
