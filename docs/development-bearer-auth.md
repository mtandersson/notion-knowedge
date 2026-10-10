# Explicit development bearer authentication (issue #84)

This is a **local development, diagnostics, or emergency fallback** for the
Streamable HTTP `/mcp` resource. It is **not** the production ChatGPT OAuth
flow described in [#117](https://github.com/mtandersson/notion-knowedge/issues/117)
and [the OAuth trust model](oauth-trust-model.md). Static bearer credentials do
not establish a Notion user/workspace, grant epoch, scopes or token revocation.
Do not treat this mode as a substitute for those production controls.

## Configuration

The default `NK_HTTP_AUTH=none` preserves the existing local bootstrap.
Authentication is enabled only when explicitly requested:

```sh
export NK_HTTP_AUTH=bearer
export NK_HTTP_BEARER_TOKEN="$(openssl rand -hex 32)"
# Keep NK_HTTP_HOST on loopback (default 127.0.0.1).
cargo run -p notion-knowledge-server -- --http
```

`NK_HTTP_BEARER_TOKEN` must contain 32–256 URL-safe ASCII characters
(letters, digits, `-`, `_`, `.`, or `~`), have no whitespace, and should
be generated randomly rather than memorized. Store it in an operator-managed
secret store or private process environment; do not commit, print or place it
in a URL. The server rejects a provided token without `NK_HTTP_AUTH=bearer`,
a missing/invalid token in bearer mode, a non-loopback bind in that mode,
and simultaneous OAuth discovery settings. The token has no bearing on the
separate upstream `NOTION_TOKEN` used for Notion API reads.

Every HTTP `/mcp` operation, including initialize, POST tools, GET/SSE,
session continuation and DELETE, must send exactly one:

```http
Authorization: Bearer <generated-token>
```

Missing, malformed, duplicate and incorrect credentials receive HTTP
`401 Unauthorized` with a fixed Bearer challenge and `Cache-Control:
no-store`. The middleware executes before JSON body parsing or MCP session
handling, and neither client credentials nor configured tokens appear in
error bodies or startup diagnostics. A valid token still passes through the
existing MCP protocol, Origin/Host enforcement, Notion root scoping, and
tool-level read/write policies; a session ID is not an authentication token.

This option listens only on a **loopback address**. For an emergency client
on another machine, terminate HTTPS and network authorization at a trusted
local reverse proxy; never expose plaintext bearer traffic on the internet.
Rotating the configured credential requires restarting the server. No
per-session revocation, user identity, authorization scopes, or OAuth refresh
is provided.

## Independent health exposure

`NK_HTTP_HEALTH_AUTH=none` (default) keeps `/livez`, `/readyz` and
`/health` independent of MCP bearer authorization. They retain the existing
Host/Origin guard and only publish sanitized dependency state. Set
`NK_HTTP_HEALTH_AUTH=bearer` to require the **same** developer bearer on
all three health routes. This is valid only with `NK_HTTP_AUTH=bearer`.
The built-in container `--healthcheck` automatically sends the token for
its local liveness probe when this is enabled.

Webhook ingress `/webhooks/notion` keeps its separate Notion signature
verification and is never authorized by a developer bearer header.
Stdio mode is unchanged and does not use this HTTP-only fallback.

## Verification

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p notion-knowledge-server --locked
```

The `bearer_fallback` integration tests cover authenticated MCP
initialization/continuation, unauthorized and duplicate headers,
GET/DELETE, Host/Origin enforcement, redacted replies and both health modes.
