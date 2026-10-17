# OAuth discovery only (#119)

This is a **staged discovery implementation**, not OAuth authorization. It
publishes RFC 9728 Protected Resource Metadata and RFC 8414 Authorization Server
Metadata for the planned [OAuth trust model](oauth-trust-model.md). It exists so
a generic OAuth client can discover the intended issuer and endpoints, without
silently making the current unauthenticated MCP bootstrap a remote OAuth server.
**Discovery configuration alone issues no OAuth token, code, Notion grant or MCP session.**
An optional [browser entry point](notion-oauth-redirect.md#browser-entry-point)
can start upstream consent; the [public callback](notion-oauth-callback.md)
also requires sealed storage and returns a PKCE-bound MCP code only after
persisting the allowlisted Notion grant. The [token endpoint](oauth-code.md)
can redeem that code. With complete configuration, the
[OAuth MCP transport](oauth-mcp-sessions.md) validates live token/grant evidence
and binds sessions to exact tokens. Explicit matching scope and compatible
existing index state enable [request-bound content](oauth-content.md).

## Explicit configuration

The existing loopback HTTP bootstrap remains the default when both variables
are unset. To opt into closed discovery mode, configure **both**:

```sh
NK_OAUTH_ISSUER=https://auth.example.com
NK_OAUTH_RESOURCE=https://knowledge.example.com/mcp
```

These are trusted operator-defined, canonical **HTTPS** URIs: the issuer is a
root origin (no suffix), and the resource is an origin ending in `/mcp`.
There is no implicit config based on `Host`, `Origin`, forwarded proxy
headers, Notion OAuth responses or the listener's loopback address. Invalid,
incomplete, non-HTTPS, userinfo, query or fragment-bearing config makes the
startup/--check fail before binding; it cannot fall back to anonymous HTTP.

The canonical HTTPS origins are public endpoint identities, **not** the local
listener URLs. Configure TLS and proxy routing separately; this application
still binds to `NK_HTTP_HOST:NK_HTTP_PORT` (default `127.0.0.1:3000`).
The trusted reverse proxy must use the already-supported backend Host authority
for forwarding so the local Host/Origin guard admits the request. Do not expose
the backend listener or add permissive global Host/Origin exceptions. Neither
setting grants permission to publish private content.

## Discovery and current denial behavior

| Method and local path | Current response |
| --- | --- |
| `GET /.well-known/oauth-protected-resource/mcp` | 200 JSON: the exact configured MCP resource and its approved authorization-server issuer |
| `GET /.well-known/oauth-protected-resource` | 200 JSON: same RFC 9728 document for root fallback discovery |
| `GET /.well-known/oauth-authorization-server` | 200 JSON: issuer, authorization/token/revocation endpoints, authorization code and S256 capabilities |
| `GET /authorize` | 503 by default; explicit [MCP client registration](notion-oauth-redirect.md#browser-entry-point) enables a browser-bound Notion consent redirect only |
| `GET /oauth/notion/callback` | 503 without complete sealed-storage/browser configuration; otherwise strict cookie/state verification and allowlisted callback exchange |
| `POST /token` | 503 without complete browser/storage configuration; otherwise strict form-only code + S256 redemption against the current sealed grant |
| `POST /revoke` | 503, no token revocation claim |
| `GET/POST/DELETE /mcp` | 401 challenge without valid token/grant evidence; otherwise the SDK transport enforces exact-token session ownership and read-only scope; configured content uses request-bound authority |

All listed routes apply the existing loopback/backend Host and Origin
restrictions; discovery responses specify `Cache-Control: no-store`. Even a
fabricated Bearer token or old MCP session ID cannot access a tool in discovery
mode. The advertised OAuth paths are deliberately reserved and closed until
the subsequent implementation; metadata alone is not a successful OAuth login.

**Production rollout is blocked** on #124 (provider-compatible refresh),
#127–#128 (token/session binding, revocation), #129 (negative-path tests) and
#130 (deployment configuration). This issue does not close the parent #117.

## Verification

```sh
nix develop --command cargo test -p notion-knowledge-server --test oauth_discovery --locked
nix develop --command cargo test -p notion-knowledge-server --locked
nix develop --command cargo fmt --all -- --check
```

The integration test starts the actual HTTP binary with no credentials,
follows both well-known discovery documents as a generic OAuth client, verifies
endpoint consistency and S256, then proves that every advertised authorization
route is unavailable and all MCP request methods reject requests. Host/Origin
spoofing and malformed/partial configuration are negative-path tests. Existing
anonymous bootstrap tests still run without the explicit discovery variables.
