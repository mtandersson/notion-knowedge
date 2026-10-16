# SDK session and SSE resource budgets — #398

The installed HTTP transport uses a finite capacity-owning SessionManager
wrapper around the rmcp 3.5.0 LocalSessionManager. The SDK worker and
registry entry are removed on DELETE, idle timeout and process shutdown.
Active POST/GET/reconnect streams mark their owning session in-use;
client disconnect releases that lease.

| Setting | Default | Valid range | Effect |
| --- | --- | --- | --- |
| `NK_MCP_MAX_SESSIONS` | 32 | 1–512 | Live legacy MCP worker/session capacity |
| `NK_MCP_SESSION_IDLE_SECONDS` | 300 | 1–3600 | Idle deadline and periodic worker eviction |
| `NK_MCP_MAX_STREAMS` | 16 | 1–256 | Concurrent HTTP GET/POST response stream permits |
| `NK_MCP_STREAM_MAX_SECONDS` | 300 | 1–3600 | Maximum wall-clock SSE stream lifetime |

A quota rejection occurs before SDK initialize or tool dispatch, responds
with fixed 503 JSON-RPC plus `Retry-After: 1`, and leaks no session ID,
content or credential. DELETE bypasses SSE limits so clients can cleanly
close even when all stream slots are held. Existing authenticated access,
Host/Origin checks and root/operation policy are unchanged.

SSE permits are held **by the response body** until EOF, disconnection or
deadline; POST reserves the capacity *before* the tool executes. The
response age is a hard cutoff rather than an inactivity timeout. Clients
can reconnect (up to quota) as usual after a maximum-age disconnect.
The separate short-lived HTTP request and webhook pools from #397
continue to protect request buffering and durable receipts.

This is a per-process availability boundary, not a promise of OS-level
containment. Expensive concurrent backend operations, stdio, file/temp
storage and deployment quotas are tracked in #399/#400. The 2026-07-28
stateless MCP path cannot create legacy sessions, but shares the HTTP
stream quota.

Verification: session quota/idle/DELETE and SSE reconnect/timeout tests,
Cargo fmt, Clippy, workspace and feature tests, and required CI gate.
