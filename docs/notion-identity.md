# Integration identity probe

The composition root constructs `NotionClient` only from validated integration
configuration for the explicit `--notion-identity` command. Default startup,
`--check`, `--diagnostics` and both MCP transports continue without contacting
Notion. No token argument, public endpoint override or automatic `.env` loader
is provided. Supply `NOTION_TOKEN` through your secret environment/service
manager, with `NK_NOTION_AUTH=integration`.

The thin REST adapter sends a bearer token and explicit `Notion-Version:
2022-06-28` to `https://api.notion.com/v1/users/me`. The version is deliberately
pinned for this stable user endpoint; data-source API work will evaluate newer
versions separately. The official [retrieve your token's bot user reference](https://developers.notion.com/reference/get-self)
documents the get-self endpoint and bot response. This verifies credentials,
not access to specific pages, configured roots or approved OAuth grants.

TLS uses rustls with certificate verification, requests time out after 15 seconds,
and redirects are refused so credentials cannot be forwarded to another host.
Client debug output and the sensitive authorization header redact credentials.
Raw HTTP bodies and transport errors never become application errors or logs.
A successful response must contain a nonempty bot-user ID. The CLI deliberately
does not print private identity metadata.

Failures use the core sanitized backend contract: 401 is `Unauthenticated`,
403 `PermissionDenied`, 429 `RateLimited`, network failures and 5xx
`Unavailable`, and malformed responses/unexpected statuses `Internal`.
There are no automatic retries. Retry policy and read/write ports belong to
subsequent tickets. No identity request uses live credentials in ordinary CI;
mock HTTP tests exercise request headers, success, authentication/transient
failure distinctions and malformed identities.
