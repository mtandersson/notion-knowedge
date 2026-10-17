# Installed resource security acceptance (#396 / #400)

These controls bound admitted application work per process. Public ChatGPT
OAuth remains separately gated by #117; successful local load tests confer
no remote authorization.

## Inventory and ownership

| Boundary | Finite default / lifecycle | Evidence |
| --- | --- | --- |
| HTTP MCP POST/DELETE | 8 dispatches; zero queue; POST body deadline 30s, 4 MiB | `tests/admission.rs`, `tests/http.rs` |
| HTTP session initialize/continue/DELETE | 32 SDK sessions, 300s idle; DELETE releases worker | `tests/session_bounds.rs` |
| GET/SSE/reconnect and POST responses | 16 response slots, 300s hard age; body owns slot through disconnect/EOF | `tests/session_bounds.rs` |
| HTTP and trusted stdio tools | 16 weighted units; status 1, get 2, graph 3, search 4, upload 8; zero queue | MCP `tests/tool_admission.rs`, `src/tool_budget.rs` |
| Operational metrics | independent one-slot SQLite snapshot budget; owner retained through blocking completion after scrape cancellation | server `http::tests::cancelled_metrics_waiter_keeps_backend_slot_until_snapshot_finishes` |
| Cancelled status inspection | backend task owns separate capacity through blocking SQLite/index completion | server `sync_status::tests` |
| Webhook ingress | independent 4 slots; 64 KiB, 10s input deadline; bounded owner retains capacity through receipt/setup publication after cancellation or 20s response timeout | server `webhook::tests`, `tests/admission.rs` |
| Retrieval and graph | calls share tool units; graph depth <=2, result <=100, fanout <=256; Qwen model permit remains inside blocking inference | MCP related tests; retrieval Qwen tests |
| Files | upload weight 8; serial bounded batch, operator count/size/time policy; exact host admission and pinned public DNS; private temporary ownership and audit receipts | `http::ingestion_tests`, downloader and ingestion tests |
| Index/webhook worker | separately serialized worker, shared commit coordinator and durable journal; no client-timeout replay | worker, shutdown and reconciliation tests |

Rejected calls fail before arguments, authorization adapters or effects.
HTTP overload returns fixed 503 (MCP includes Retry-After); tool overload is
`tool_busy: retry later`. There is no admission queue. Weighted admission is
fail-fast, not FIFO fairness: sustained heavy reads can starve uploads. Tune
capacity and isolate workloads operationally if write latency matters. Health,
management and webhook routes do not consume MCP capacity.

## Reproducible load gate

Run in the pinned development environment:

```sh
cargo test -p notion-knowledge-server --test admission --test session_bounds --locked
cargo test -p notion-knowledge-server --lib cancelled_ --locked
cargo test -p notion-knowledge-mcp --test tool_admission --locked
```

The authenticated burst gate sends 32 initializations against two sessions,
holds both SSE responses without consuming them, rejects 32 mixed tool calls,
checks finite current/peak metrics, admits a signed durable webhook while MCP
is saturated, checks health and metrics access, then deletes sessions and
verifies new admission. A second installed HTTP gate holds a real search adapter and upload application
at a combined 12 weighted units, rejects excess mixed calls before arguments
or adapters, then releases search and exercises scoped SQLite graph navigation
while upload remains active. It verifies adapter invocation counts and recovery.
Companion tests exercise idle expiry, stream deadline,
reconnect, cancellation of real blocking work and permit recovery. Installed
file integration tests separately verify native effects, audit/indeterminate
outcomes, read-only/scope denial and cleanup. These fixtures use no live Notion
credentials and make no throughput or real-world capacity claim.

## Tuning and observation

See [ingress](ingress-admission.md), [sessions](session-resource-limits.md),
[tool work](mcp-work-admission.md) and [file settings](file-ingestion-settings.md)
for validated overrides. Start with defaults, reduce concurrency before raising
memory limits, and measure production model/index footprints with private
representative data. Changing settings requires a restart.

`nk_resource_active`, `nk_resource_peak`, and `nk_resource_rejected_total`
have only the fixed `resource` labels `mcp_request`, `webhook_request`,
`session`, `stream`, `tool_work`, `metrics_snapshot`. Tool values count weighted units; other values
count owners. Stream ownership includes pending dispatch reserved for a response,
not just SSE bodies. Peaks/rejections reset on restart; scrapes are concurrent
snapshots, not transactionally aligned across series. Alert on sustained rejects
and active values near configured capacity; correlate webhook queue age and
Notion retries rather than retrying unknown mutation outcomes.

## Deployment guardrails and residual risk

Application permits cannot bound accepted TCP connections, HTTP headers, SDK
buffers, one expensive query, disk growth, total CPU/memory or a malicious local
stdio producer. Deadlines require executor/socket progress; a peer that stops
reading can retain buffered bytes until proxy/socket cleanup. No public remote
DoS containment is claimed by these in-process tests.

Before remote deployment, [#416](https://github.com/mtandersson/notion-knowedge/issues/416) under #13 must measure and enforce:

- cgroup memory and CPU quotas including model loading/index rebuild peaks;
- process/file-descriptor and PID ceilings, leaving headroom for health/receipt
  work and recovery;
- separate bounded temporary storage and state/index volume capacity monitoring;
- trusted proxy connection, header/body, slow-client and response/write deadlines,
  per-client connection/rate quotas and bounded upstream queues;
- private authenticated monitoring, alerts for rejection/queue age/disk pressure,
  and verified recovery after quotas are hit.

For an initial local fixture deployment, CPU 2, memory 4 GiB, 256 PIDs,
1024 file descriptors and a 64 MiB temporary mount are reasonable starting
ceilings **without a loaded model**; they are not validated production sizing.
Production Qwen/index workloads need measured larger limits. Keep the development
Compose loopback-only; resource quotas alone do not make OAuth available.

Operational SQLite metric snapshots have a separate process-wide capacity of
one, with no queue. Their owned permit stays inside the blocking query after
scrape cancellation. Concurrent snapshots receive fixed HTTP 503 with
`Retry-After: 1` and `Cache-Control: no-store`; liveness and in-memory metrics
without a configured store remain independent. This bound is intentionally
fixed because the SQLite connection already serializes these aggregates.
