# OAuth trust and single-identity binding

**Status:** security design for [#118](https://github.com/mtandersson/notion-knowedge/issues/118), 2026-10-09. **Not implemented.** This is a contract for the child tickets in [#117](https://github.com/mtandersson/notion-knowedge/issues/117), not permission to enable remote/private access. Review with [the threat model](threat-model.md) and [ADR 0001](adr/0001-runtime-and-component-boundaries.md). The current bootstrap has no authenticated MCP application principal; `NK_NOTION_AUTH=integration` and its get-self probe are **not** a production OAuth grant.

## Principals and separate trust boundaries

```text
ChatGPT (public OAuth client) -- MCP auth code + PKCE --> server authorization endpoint
ChatGPT -- MCP-scoped access token --> MCP resource server (every HTTP operation)
                                            |
                        server-side approved grant record / epoch
                                            |
Server (confidential Notion OAuth client) -- Notion OAuth --> Notion authorization server
Server -- separately stored Notion access token --> Notion API (scoped reads/writes)
```

The server owns two different roles: OAuth authorization server/resource server **to ChatGPT**, and confidential OAuth client **to Notion**. Notion is not the MCP token issuer. The Notion token must never be accepted as an MCP bearer, passed through to ChatGPT, returned in tool output, embedded in a page, placed in a URL or stored in LanceDB. Neither a successful ChatGPT login nor a Notion grant alone authorizes data access: the final decision also requires the configured owner/workspace, a live matching grant and application root/operation scope. HTTP proxy TLS, Host/Origin checks, an MCP session ID, a valid webhook signature and a user's email address do not establish that authorization. Stdio is a separate trusted-local execution boundary with credentials supplied by the operator, not an HTTP OAuth bypass exposed over a network.

## Authoritative identity and approval

Configure **exactly one** immutable allowed Notion `workspace_id` and **exactly one** `owner.user.id` (canonical nonempty Notion identifiers) from operator-managed configuration. An admin/operator must provision these values out of band; never learn, replace or broaden them from the first successful OAuth callback or a webhook. Reject missing, malformed, ambiguous or mismatching values before storing a usable grant or minting an MCP access token. Compare canonical IDs, not a display name, email, workspace name, avatar, `bot_id`, integration ID, page URL or MCP client account. In the Notion token response require `owner.type == "user"` and a nonempty `owner.user.id` as well as a nonempty `workspace_id`; an owner of another type is not silently equivalent. `bot_id` is useful for integration correlation but **not** a substitute for the authorizing user ID.

The exact configured pair must be rechecked on **each** new Notion authorization, refresh/rotation, MCP token issue, login restoration and permission-bound request. Notion's OAuth token response identifies the user who authorized an integration and delegates an upstream capability; OAuth itself is not a general-purpose identity login for the MCP, and it does not decide whether that user is the operator approved for this service. A different user in the same workspace, or the same user in another workspace, is unauthorized even with a fully valid Notion access token.

The approved grant is additionally associated with the expected Notion OAuth client/integration, the configured roots/exclusions and a durable server-generated `grant_id` and monotonic `authorization_epoch`. A changed identity, installation/client, roots policy or replaced/revoked upstream grant must not inherit prior access merely because a token string is valid. Exact representation/storage is delegated to #123 and #127; the binding invariants are mandatory.

## ChatGPT → MCP authorization boundary (#119, #120)

1. Advertise the canonical HTTPS MCP protected resource and its authorization server using **OAuth protected-resource metadata (RFC 9728)** and authorization-server metadata (RFC 8414 or supported OIDC discovery), consistent with the supported MCP specification. Validate the configured public issuer and externally visible canonical resource URI, including reverse-proxy behavior. Do not derive security-critical redirect URIs, issuer or resource from untrusted `Host`/`Forwarded` headers.
2. Require Authorization Code + **PKCE S256** for a public ChatGPT client; reject `plain`, missing PKCE, implicit/password grants and authorization-code replay. Validate registered client identifiers/client metadata and exact registered redirect URI rules; never use a wildcard redirect. A client ID is not client authentication.
3. Bind each short-lived, single-use code to client ID, exact redirect URI, challenge, requested canonical `resource` (RFC 8707), granted scopes, and the server-side pending login/Notion authorization transaction. Correlate browser callbacks with unpredictable, single-use, expiring `state` to prevent login CSRF/code injection; keep browser/session cookies `Secure`, `HttpOnly`, appropriate `SameSite` and session-rotated as needed. Do not put bearer secrets in front-channel URLs.
4. Mint an MCP token **only after** the approved Notion identity/workspace and live grant are verified. Every token carries or server-side resolves its immutable issuer, canonical MCP audience/resource, client ID, scopes, stable `grant_id`, `authorization_epoch`, unique revocable token ID and expiry. Opaque tokens with a protected server-side lookup or signed audience-constrained tokens are both possible; no unvalidated, self-describing JSON or unbound client tokens.
5. On **every HTTP MCP route**, including initialize, POST, GET, DELETE, tool calls, session continuation and SSE reconnect, require and validate the MCP token: issuer, intended resource/audience, expiry/not-before, client binding, approved grant/epoch and revocation. Do not treat `Mcp-Session-Id`, cookie, Origin, Host or earlier initialization as sufficient. Session state is always owned by the verified token/grant/epoch; cross-token session reuse fails. Authentication failures return 401 with appropriate challenge metadata; authenticated insufficient privilege returns 403. Before parsing sensitive bodies or invoking any tool, enforce authorization.
6. Enforce requested scopes and configured root/operation policy **server-side** for each call. OAuth authentication does not supersede the existing Notion ancestry/scope checks; stale local index hits cannot authorize out-of-scope reads.

The canonical `resource` parameter is present in the client authorization **and token** requests, and the server issues/verifies a token exclusively for that resource. Distinguish the OAuth server's issuer from the MCP resource URI. Reject foreign issuer/audience and upstream Notion bearer tokens (no token passthrough or confused deputy). Explicitly review token format, key rotation, token entropy, expiry and refresh-token policy in #120/#127.

## MCP → Notion authorization boundary (#121–#126)

The server initiates a separate Notion OAuth authorization using its confidential client configuration, fixed registered redirect URI and a distinct unguessable, one-time, expiring state tied to the pending approved MCP authorization. Only the server exchanges the returned code using Notion's HTTPS token endpoint. Reject provider errors, missing or repeated callbacks, state mismatch, redirect mismatch, token exchange failure and unrecognized response shape. A forged URL parameter cannot supply an identity.

After exchanging the code, validate Notion's returned `workspace_id` and `owner.user.id` against the **operator allowlist** before activating the grant. Record the grant's authorization epoch, OAuth client/integration and minimum safe operational metadata; encrypt or otherwise protect access/refresh tokens in a dedicated grant store (#123). Do not log token response bodies, redirect query parameters, access tokens, refresh tokens, state, signed URLs or grant-store encryption keys. Keep grant secrets out of the retrieval/index DB and all MCP results.

Rotate refresh/access token material atomically while preserving the stable trusted identity and epoch. Validate every refresh response's available identity metadata, and when identity claims are absent, verify identity using an authoritative provider-supported method **before** treating the refreshed grant as approved; do not assume identity from a cached name or an unchecked response. Failure or ambiguity means no token issuance and no new source request with that grant. Serialize concurrent refresh/revocation, prohibit stale refresh success from resurrecting a revoked or replaced grant, and avoid logging provider errors with secrets. If the provider cannot guarantee the required identity invariant during rotation, require controlled reauthorization rather than proceeding.

Integration-token/bootstrap mode is distinct from this interactive OAuth chain and must not make its get-self response count as owner approval. A token's upstream Notion capabilities still require explicitly configured root restrictions and least-privilege content/file permissions.

## Durable binding, lifecycle and revocation (#123, #124, #127, #128)

Model the approval as explicit durable states, not inferred from possession of either bearer:

```text
Unconfigured -> PendingOAuth -> Approved(grant_id, epoch)
PendingOAuth -> Rejected/Expired (no issued MCP token)
Approved -> Rotating -> Approved(same grant_id, epoch; same trusted identities)
Approved -> Revoked / ReauthRequired (epoch invalidated)
ReauthRequired -> PendingOAuth -> Approved(new grant_id/epoch after full allowlist check)
```

- Every MCP token/session references a **specific** durable grant identity and authorization epoch, not simply "current credential". Authorization looks up that live approved record and compares the configured workspace/user. Binding is never based only on a refresh token fingerprint or `bot_id`.
- **Refresh** retaining the identical approved identity may keep an epoch *only when* the grant continuity is proven and persisted atomically. A changed Notion account, workspace, OAuth client, root policy or grant replacement requires a new epoch and invalidation of incompatible MCP tokens/sessions, even if the same browser remains logged in.
- **Logout, upstream revocation, forced reauthorization, local operator revocation or detected mismatch** invalidates the grant and associated MCP tokens, sessions, pending codes and refresh token families. Persist revocation **before** acknowledging the request or returning success. Never silently fall back to another Notion grant or integrate a new one from an attacker-controlled callback.
- Each request checks the live epoch/status; any cache of this authorization decision needs an explicitly documented, short revocation bound and must be tested. No indefinitely cached valid token or open SSE connection may outlive revocation: reauthorize active streams on resume and terminate them on revocation/expiry, subject to a documented upper bound.
- Deny safely when the grant store is unavailable/corrupt, identity verification fails, configured allowlist changes or a token references a nonexistent epoch. Prevent a partial grant-save from issuing a functioning MCP token. Re-authentication must never migrate existing sessions to a new owner/workspace.
- Make token/session/code IDs unguessable, bounded-lived and single-use where applicable. Use minimal, nonsecret audit entries (hashed/correlation IDs, state transition class, timestamp); never persist full bearer values in telemetry, errors, SQLite sync checkpoints or index content.

## Denial matrix for implementation and end-to-end verification (#129)

| Input / event | Required outcome |
| --- | --- |
| Notion OAuth succeeds for **wrong user** but correct workspace | Reject; no approved grant, MCP token or session |
| Correct user in **wrong workspace** | Reject; no approved grant, MCP token or session |
| Missing/null `owner.user.id` or `workspace_id`, or `owner.type != user` | Fail closed; do not substitute email or bot ID |
| Correct identities but missing configured allowlist | No first-login bootstrap; deny |
| Forged/replayed state, reused code, wrong redirect, missing/wrong PKCE | Deny before issuing token; sanitize failures |
| Valid Notion token used as MCP bearer; MCP token for another audience/issuer | 401; no token passthrough |
| Valid MCP token with changed grant epoch, revoked/expired grant, or altered scope | 401/403 as appropriate; no tool backend call |
| Old session ID with different client/token/grant | Deny; no cross-session adoption |
| Concurrent refresh races with logout/replacement | Revocation wins; old token and session unusable |
| Rotated grant with changed user/workspace or ambiguous refresh identity | Fail closed; no successful rotated authorization |
| User is approved but requested page is outside configured roots | Deny at application policy; no source read/index mutation |
| Missing authorization on HTTP initialize, GET/SSE, POST or DELETE | 401; no protocol-state side effect |
| Revoked long-lived SSE connection | Terminates or fails further sensitive data, within explicit revocation bound |

Use strict fake-provider HTTP fixtures and deterministic clocks for positive and negative cases; then run a credential-controlled live end-to-end flow (#129/#130) without exposing real identities or secrets in logs. Verify behavior for simultaneous sessions, restart, cache invalidation, token expiry, and arbitrary source/transport failures. This design document alone makes **no** runtime-security claim.

## Implementation sequencing and deployment gate

- #119/#120: resource/AS discovery, correct resource indicators, code + PKCE, HTTP enforcement.
- #121/#122: separate Notion redirect, callback correlation and code exchange.
- #123/#124/#125/#126: secret persistence/rotation and explicit allowlisted workspace/user.
- #127/#128: atomic grant-epoch binding across MCP tokens/sessions and revocation.
- #129/#130/#109: negative-path integration tests, deployment/proxy guidance and real ChatGPT connection verification.

**Do not** expose private tools or production HTTP to remote/untrusted clients until all relevant #117 controls, safe application scope checks and deployment reviews have shipped. Host/Origin checks, a private tunnel, the local demo in [chatgpt-spike.md](chatgpt-spike.md), TLS alone and an otherwise valid Notion OAuth login do **not** waive this gate. See also #84 for explicitly development-only bearer fallback.

## Sources / compatibility

- [MCP authorization specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization): transport boundary, protected resource discovery and audience checks; check the version used by the deployed client at implementation time.
- [RFC 9728](https://www.rfc-editor.org/rfc/rfc9728): protected resource metadata; [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414): authorization-server metadata; [RFC 8707](https://www.rfc-editor.org/rfc/rfc8707): resource indicators.
- [Notion: Create a token](https://developers.notion.com/reference/create-a-token): upstream exchange response including `workspace_id` and `owner.user.id`; [Notion authorization guide](https://developers.notion.com/guides/get-started/authorization).
- [ADR 0001](adr/0001-runtime-and-component-boundaries.md), [threat model](threat-model.md), [integration identity probe](notion-identity.md), [ChatGPT spike](chatgpt-spike.md): existing architecture and **current-vs-planned** separation.
