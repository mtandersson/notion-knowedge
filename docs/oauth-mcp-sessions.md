# OAuth MCP transport and session binding (#431)

With complete browser/grant configuration, `/mcp` mounts the real Streamable
HTTP SDK behind live authorization. Discovery alone still authenticates nobody.
Every POST, GET/SSE/reconnect and DELETE requires an issued opaque MCP bearer;
Notion tokens, cookies and session IDs cannot substitute for it.

The gate rejects malformed/duplicate authorization, unknown or expired tokens,
wrong resource/scope, changed grant binding, and cross-token session IDs locally
before provider work. It then reserves finite admission capacity, obtains an
active grant from the shared refresh coordinator, and rechecks the token and
binding. Provider failure denies dispatch without static-token fallback.
Responses use fixed sanitized messages; challenges point at protected-resource
metadata. Admission exhaustion returns 503 and `Retry-After`.

Bounded SDK session entries retain exact-token digest and grant ID/epoch/user/
workspace evidence. A second token for the same grant cannot use another token's
session. Initialization binds the SDK-returned ID before exposing it. Subsequent
requests authenticate again. Session deletion, idle sweeping and cancellation
retain the existing session and stream accounting. The request carries typed,
non-secret `HttpAuthorization` evidence into SDK request Parts extensions.

The request, JSON body, session, stream and tool limits remain in force. Provider
admission also covers GET and DELETE, preventing an unbounded queue behind a
slow introspection request. An SSE body's absolute deadline is the earlier of
its token expiry and configured stream lifetime, including time spent awaiting
SDK dispatch. Disconnect or expiry drops the stream permit.

The only supported scope is `knowledge:read`: OAuth tool discovery and dispatch
remain read-only regardless of operator mutation settings. Explicit matching
scope and compatible existing retrieval state enable
[request-bound content](oauth-content.md). Missing configuration/context remains
fail-closed; static integration-backed handlers cannot substitute for these ports.
Joined acceptance and remaining lifecycle gates still belong to #127/#117.
Durable logout/revocation and cancellation of already in-flight work after
upstream revocation remain #128; expiry of an existing stream is enforced here.

Verification uses real local HTTP sockets, fake local Notion providers, the
production router/budget composition and actual SDK sessions. Run:

```sh
nix develop --command cargo test -p notion-knowledge-server --locked
nix develop --command cargo test -p notion-knowledge-mcp --locked
nix develop --command cargo clippy -p notion-knowledge-server -p notion-knowledge-mcp --all-targets --locked -- -D warnings
```
