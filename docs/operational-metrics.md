# Operational Prometheus metrics (#107)

The HTTP server exposes `GET /metrics` in Prometheus text format (0.0.4). This endpoint uses **the same host/origin guard and optional `NK_HEALTH_REQUIRES_BEARER` authentication as `/health`**, rather than the MCP authorization surface. Keep the scrape endpoint private to loopback or a trusted network, and configure a short scrape timeout.

Metrics are process-local, monotonic since startup unless explicitly described as gauges. No metric has user-controlled labels. Never add page IDs, workspace IDs, query text, credentials, request paths, signed URLs, correlation IDs or exception messages to metric labels.

- `nk_http_requests_total{outcome}` and `nk_http_request_duration_seconds` cover HTTP requests (including rejected requests, health checks and scrapes).
- `nk_notion_requests_total{outcome}`, `nk_notion_attempts_total`, `nk_notion_retries_total` and `nk_notion_rate_limits_total` cover provider response outcomes and rate-limited transport attempts.
- `nk_search_requests_total{mode,outcome}` and `nk_search_duration_seconds{mode}` cover all three search modes. Denials, validation failures after parsing, cancelled requests and backend failures are counted as failed search operations. Query text and results are not retained.
- `nk_webhook_admissions_total{outcome}`, `nk_index_commits_total{outcome}` and `nk_index_last_commit_timestamp_seconds` summarize existing structured event hooks. Timestamp is zero until a successful index commit in the current process.
- When a durable webhook SQLite inbox is configured, `nk_webhook_queue_outstanding` counts persisted pending **and running** webhook events. `nk_webhook_oldest_outstanding_age_seconds` measures the age of the earliest outstanding event (omitted when no pending events), and `nk_index_last_change_age_seconds` measures time since latest persisted page/index version update (omitted when uninitialized). These are direct read-only SQL snapshots, not estimated from in-process counters. A failed snapshot causes HTTP 503, not a misleading zero.

Histogram duration buckets are fixed at 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5 and 10 seconds, plus `+Inf`. No ephemeral per-user or per-request time series are produced. Alerts may use `rate(nk_http_requests_total{outcome="failed"}[5m])`, `rate(nk_notion_rate_limits_total[5m])` and `nk_webhook_oldest_outstanding_age_seconds` where the optional sample exists.

Operational caveats: these metrics expose current server activity, not the full local-index architecture when adapters are absent; readiness remains separate. They are not a substitute for authentication or root authorization. The queue gauges require configured SQLite state and should not be interpreted as full-page indexing backlog across unrelated stores.

Resource admission adds `nk_resource_active` and `nk_resource_peak` gauges and
`nk_resource_rejected_total` counters. Fixed labels are `mcp_request`,
`webhook_request`, `session`, `stream`, `tool_work`; tool work counts weighted
units, others count owners. Peaks and rejections reset at process restart.
Stream counts include reservations during request dispatch, and backend webhook
owners retain their count after caller timeout. See [resource acceptance](security-resource-acceptance.md)
for load commands, tuning and deployment limitations.
