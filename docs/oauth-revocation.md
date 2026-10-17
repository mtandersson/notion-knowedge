# OAuth logout and forced reauthentication

The configured OAuth server supports local MCP logout through `POST /revoke`.
This endpoint does not contact Notion or revoke the upstream Notion consent.
It remains usable when the provider is unavailable. The registered public MCP
client sends its own opaque MCP access token, never a Notion access or refresh
token.

## Client logout

Read `revocation_endpoint` from the configured authorization-server metadata.
Send an HTTPS POST with exactly `Content-Type: application/x-www-form-urlencoded`
and these form fields:

| Field | Value |
| --- | --- |
| `client_id` | The exact configured `NK_OAUTH_CLIENT_ID` |
| `token` | The MCP access token being discarded |
| `token_type_hint` | Optional; `access_token` is appropriate |

No client secret or Authorization header is used. Keep token values out of
URLs, shell arguments, request logs and telemetry. The endpoint follows
[RFC 7009](https://www.rfc-editor.org/rfc/rfc7009.html): successful revocation,
an unknown token and an already-revoked token produce the same empty 200
response. An unrecognized hint does not change token lookup. Malformed,
ambiguous or oversized forms and incorrect client registration are rejected.
Responses prohibit caching. Request bodies and concurrent revocation work
are bounded; capacity exhaustion is retriable.

Revocation removes that exact token from the process-local authorization
records and signals cancellation to its existing request evidence. All SDK
sessions owned by the token are closed; their SSE streams terminate and their
capacity is released. Another token is not logged out merely because it
references the same approved Notion grant. Deleting an MCP session alone
closes that session without revoking the token; clients should revoke their
token when logging out.

There is no authorization cache grace interval. After cancellation, new MCP
requests are denied locally, and pending requests must pass cancellation
checks before dispatch or private content disclosure. Existing SSE bodies
observe cancellation while polling and discard remaining buffered content.
Bytes already delivered to a client cannot be recalled. Already-started
provider operations may finish under their existing timeout; revocation does
not launch detached, unbounded work or allow their result to restore access.
If an interrupted upstream rotation has an ambiguous outcome, authorization
fails closed and a fresh login may be necessary.

## Operator reset

A client logout leaves the sealed upstream grant available for other valid
MCP tokens. To discard that grant and require a new allowlisted Notion login,
stop the server and run the installed binary with the same OAuth configuration
and service identity:

```sh
notion-knowledge-server --notion-oauth-forget
```

The command accepts no other arguments. Run it through the existing service
environment or secret manager; keep secrets out of command-line arguments.
Restart the server afterward and reconnect the MCP client through browser login.

The reset and the production credential runtime must acquire the same exclusive lifetime state lease; a reset must
refuse while a server or credential probe owns that lease.

The reset validates the existing sealed grant, removes only the configured
grant state and durably commits its removal. Corrupt or foreign state is refused
without overwriting it; investigate and recover it under operator control. A
missing state is already unapproved and can be reset safely. The empty
`<state-file>.lock` sidecar remains in place: never delete or replace it while
credential users are running, because its inode owns the process lock.
Keep the separate encryption key, client registration and immutable workspace/user
allowlist. Restarting also discards process-local MCP tokens,
authorization codes and browser transactions. Complete a new browser flow
using the same approved Notion workspace and authorizing user. A different
identity must still be rejected; a missing grant never enrolls the next caller.

This is local invalidation, not a provider-side revocation request. Remove the
connection in Notion separately if upstream consent should also be withdrawn.
The file-backed runtime supports one active process per state file; do not
share it across independent replicas or restore an old sealed grant as a
revocation recovery mechanism. Secure state and backups against unauthorized
replacement. The local lease is not rollback-resistant external storage.
