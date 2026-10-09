# Authorization Code + PKCE core (#120)

The reusable Rust engine in `crates/server/src/oauth_code.rs` supplies the
security-sensitive **Authorization Code + PKCE S256** mechanics. It is intended
for ChatGPT's public OAuth client and the MCP resource (RFC 7636, RFC 8707)
and depends on the [approved Notion identity model](oauth-trust-model.md).

**It is deliberately not a complete external login.** The #119 production
discovery router continues to return 503 at `/authorize` and `/token` and
401 on `/mcp`. There is still no upstream Notion authorization callback,
durable approved grant, or production HTTP token validation. This component
must not be repurposed as an anonymous passwordless login, a first-login
identity enrollment, or permission to expose private MCP tools.

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
   login or proof of Notion approval. The future Notion callback (#121/#122)
   must separately correlate its own anti-CSRF state before asking the
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

## Trust boundaries and remaining work

- #121/#122 must implement the separate Notion OAuth redirect/callback
  including independent anti-CSRF state, provider code exchange and
  authoritative identities. **Never** pass a user-controlled HTTP claim to
  `approve` as a verified grant.
- #123–#126 must provide encrypted/durable approved-grant storage, rotation
  and explicit immutable workspace + authorizing user enforcement.
- #127/#128 must persist/revoke grant epochs and MCP tokens, validate them on
  every transport operation, bind SSE/session continuity, revoke on logout,
  and protect against concurrent refresh/revocation and multiple server
  processes. The process-local engine does not solve distributed/HA security.
- #129 and #130 must exercise the public HTTP end-to-end login and configured
  HTTPS proxy with negative-path tests before access is enabled.

The implementation tests the RFC 7636 Appendix B verifier/challenge vector,
valid flow, incorrect or missing PKCE, strict client/redirect/resource,
wrong Notion user/workspace, single-use callback/code, replay, invalid
verifier, expiry, epoch rotation and process restart. These unit tests use
synthetic approved-grant input, **not** real Notion credentials. They do not
claim a completed ChatGPT login until the remaining integration work ships.

```sh
nix develop --command cargo test -p notion-knowledge-server oauth_code --locked
nix develop --command cargo fmt --all -- --check
```
