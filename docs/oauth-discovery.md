# OAuth discovery and endpoint configuration

The server publishes RFC 9728 Protected Resource Metadata and RFC 8414
Authorization Server Metadata. Discovery settings alone issue no token, code,
Notion grant or MCP session. Complete browser/client/store configuration enables
[Authorization Code + PKCE](oauth-code.md), [Notion consent](notion-oauth-callback.md),
[token-bound sessions](oauth-mcp-sessions.md), [local revocation](oauth-revocation.md)
and explicit [request-bound content](oauth-content.md).
Start with [production OAuth setup](oauth-production-setup.md).

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
| `POST /revoke` | 503 without complete configuration; otherwise bounded form-only local token revocation and session cancellation |
| `GET/POST/DELETE /mcp` | 401 challenge without valid token/grant evidence; otherwise the SDK transport enforces exact-token session ownership and read-only scope; configured content uses request-bound authority |

All listed routes apply the existing loopback/backend Host and Origin
restrictions; discovery responses specify `Cache-Control: no-store`. Even a
fabricated Bearer token or old MCP session ID cannot access a tool in discovery
mode. Partial configuration keeps reserved login routes closed; metadata alone
is not a successful OAuth login. Fully configured routes are covered by the
[joined end-to-end suite](oauth-e2e-tests.md). Follow the
[production guide](oauth-production-setup.md) for HTTPS routing and onboarding.

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
