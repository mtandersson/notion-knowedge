# Installed ingress admission — #397 (part of #396)

This is a **request-dispatch** limit, not proof that session, SSE response, stdio,
OS, index or file-resource quotas are complete. Keep security epic #396 open
until #398–#400 are verified.

| Setting | Default | Valid range | Protected work |
| --- | --- | --- | --- |
| `NK_MCP_MAX_IN_FLIGHT` | 8 | 1–256 | MCP HTTP POST body validation/dispatch and DELETE dispatch |
| `NK_WEBHOOK_MAX_IN_FLIGHT` | 4 | 1–128 | Separate webhook body read, signature check and receipt-dispatch wait |

Limits are **per process**; no in-process queue is built up when full. Overload
is HTTP 503; the MCP response is a fixed, non-sensitive JSON-RPC error with
`Cache-Control: no-store` and `Retry-After: 1`. Never copy request payloads,
signed URLs or token values into denial output. A request still waiting for its
MCP POST body has a 30-second receive deadline; this does **not** impose a
deadline on mutations already dispatched to the server. Clients must not
automatically repeat an indeterminate mutation.

The admission pool is installed *after* the optional development bearer check
and *before* MCP body buffering/SDK dispatch. The webhook pool is independent;
diagnostics and /livez have no MCP admission permits and keep their existing
Host/Origin and optional credential gates. An overloaded MCP route therefore
cannot consume webhook admission slots. Excess webhooks receive 503 for Notion
to redeliver; durable deduplication remains the existing webhook contract.

Session/idle and GET/POST-stream lifetime controls now live in
[session budgets](session-resource-limits.md). Weighted HTTP/stdio tool work and
backend ownership after cancellation live in [work admission](mcp-work-admission.md).
A timed-out webhook waiter retains capacity through actual receipt completion;
it never replays that receipt. See [integrated load acceptance](security-resource-acceptance.md)
for evidence and remaining deployment-level limits under #416/#13.

Verification: `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets --locked -- -D warnings`,
`cargo test -p notion-knowledge-server --locked`, and full PR CI.
A dedicated unit test holds one MCP request, proves immediate safe rejection
without invoking a second handler, verifies /livez isolation, and tests recovery.
