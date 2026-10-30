# Local browser OAuth test

This test exercises the real Notion consent/callback and sealed grant on one
Linux machine without a certificate, model, index, public listener or tunnel.
It does not prove remote ChatGPT connectivity or production resource acceptance.

Configure the Notion **public OAuth connection** with the exact redirect URI
`http://localhost:3300/oauth/notion/callback`. Keep its client secret in your
private runtime environment file. Supply the independently approved workspace
and person IDs as for [production OAuth](notion-oauth-callback.md).

For this local test, configure:

```dotenv
NK_OAUTH_LOCAL_HTTP=true
NK_HTTP_HOST=127.0.0.1
NK_HTTP_PORT=3300
NK_OAUTH_ISSUER=http://localhost:3300
NK_OAUTH_RESOURCE=http://localhost:3300/mcp
NK_NOTION_OAUTH_REDIRECT_URI=http://localhost:3300/oauth/notion/callback
NK_OAUTH_CLIENT_ID=knowledge-chatgpt
NK_OAUTH_CLIENT_REDIRECT_URI=http://localhost:3301/callback
NK_READ_ONLY=true
NK_RETRIEVAL=false
```

Continue supplying `NK_NOTION_OAUTH_CLIENT_ID`,
`NK_NOTION_OAUTH_CLIENT_SECRET`, `NK_NOTION_ALLOWED_WORKSPACE_ID`,
`NK_NOTION_ALLOWED_USER_ID`, and the separate private grant key/state paths.
The existing [grant storage instructions](notion-grant-store.md) apply; do not
regenerate a key for an existing grant or pre-create an empty grant state file.
The server does not load dotenv files itself. Use a service environment file,
never shell-source untrusted dotenv text or put secrets in command arguments.

Start the configured server with `--http`, then run:

```sh
python3 scripts/local-oauth-test.py
```

Open the printed authorization URL in a browser **on the same machine**. Select
the approved workspace and pages, then approve Notion consent. The local test
client receives the MCP code, checks state and exchanges it with S256 PKCE.
It reports success without printing or saving the code, bearer or Notion tokens.
The bearer is discarded and the callback listener exits; the server retains the
encrypted Notion grant. No content writes or uploads are requested. Stop the
server after testing if it is not needed.

The local callback listener binds IPv4 loopback on port 3301 for up to five
minutes. The server binds IPv4 loopback on 3300; use the exact `localhost` URLs
above in the browser. Keep both ports free. In Docker on Linux use host
networking with the server itself bound to `127.0.0.1`; a bridge listener on
`0.0.0.0` deliberately fails local-mode validation even if published on loopback.

HTTP is permitted only in this explicit mode, with literal `localhost`, a
nonzero explicit port matching the listener, a same-origin `/mcp` resource and
an exact registered localhost `/callback` client redirect. Host/Origin checks,
state, PKCE, owner allowlisting, encrypted grants and upstream Notion HTTPS
remain enforced. Local browser correlation uses a distinct host-only
`nk-oauth-local` HttpOnly/SameSite=Lax cookie without Secure. Production keeps
the Secure `__Host-nk-oauth` cookie and its existing HTTPS validation.
Turn off `NK_OAUTH_LOCAL_HTTP` and configure real HTTPS origins and the actual
client callback before remote deployment.
