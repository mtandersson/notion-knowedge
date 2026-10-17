# Shared MCP tool-work admission (#399)

The application `KnowledgeServer::call_tool` now owns a **weighted, zero-queue
process-local budget**, independent of HTTP request admission and session/SSE
lifetime budgets. The same handler method is used for stdio and Streamable
HTTP, and its cloned session handlers share the same `Arc` budget.

Operator setting: `NK_MCP_TOOL_CAPACITY` (default **16**, valid **1–128**).
Weights (units): sync status **1**, knowledge get **2**, related graph **3**,
semantic/lexical/hybrid search **4**, file ingestion or future write **8**.
Each weight is capped at the configured capacity, permitting operators to
set a small server for development. Calls fail immediately with a structured
MCP tool result `tool_busy: retry later` when no quota is available.
A denied call never parses user arguments, enters root authorization, starts
search/model work, or starts a file write.

The owned guard lasts until the *entire call-tool future* returns or is
cancelled. This limits overlapping search, graph traversal, Notion refresh,
file download/upload and serialisation work; no deadline is imposed on
previously admitted mutations. In particular, uncertain Notion writes are
**never automatically retried or replayed** by this quota layer. File
ingestion continues using its pre-existing one-shot batch and per-file
receipts; root scope authorization and mutation permissions are unchanged.

This complements rather than duplicates earlier limits:
- #23: Notion API limiter and bounded Retry-After/backoff (unchanged)
- #73: bounded file batches and partial-failure receipts (unchanged)
- #93: HTTP request, webhook, result and metadata byte caps (unchanged)
- #112: shutdown/in-flight durable work is tracked separately (still open)

The existing webhook index worker is separately serialized and has its own
inbox timeout and retry policy. MCP capacity is **not** an OS memory or CPU
quota, and it cannot stop one admitted operation from using too much memory.
Deployment memory/CPU quotas and adversarial saturation monitoring belong
to #400. No retries or timeouts were added to mutation futures.

Tests exercise a mixed search/graph/status/upload budget, denied admission
without executing a second effect, and permit release on cancellation.

## Cancellation and local status inspection

Local sync-status inspection has a second, shared backend budget equal to
`NK_MCP_TOOL_CAPACITY`. Admission occurs before spawning an inspection task.
That task retains its permit through SQLite/index reads and their blocking
completions, even when the HTTP/stdio caller is cancelled. Saturated status
inspection reports unavailable fields (the existing empty status contract),
without queuing additional work. This bounds detached read-only inspections;
it introduces no mutation retry or deadline. A deterministic regression holds
an actual blocking inspection, cancels its caller, verifies rejection of
additional work and releases the backend before asserting recovery.

Webhook request capacity also stays owned by a detached, bounded task through
body admission, setup-file publication and durable SQLite receipt completion.
The response waiter ends after 20 seconds (including the independent 10-second
body deadline); timeout or client disconnect never frees backend capacity early.
Excess deliveries fail with 503 and rely on existing deduplication on redelivery.
