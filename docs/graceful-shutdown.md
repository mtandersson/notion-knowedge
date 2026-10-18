# Graceful shutdown (#112)

The server handles SIGTERM (including container stop) and SIGINT. Configure
`NK_SHUTDOWN_TIMEOUT_SECONDS` to an integer from 1 through 3600; the default
is 30 seconds. Invalid values fail configuration validation.

On shutdown, HTTP admission stops, existing connections drain, and the indexing
worker stops taking new work. The server waits for worker cleanup and owned
index commits under the same deadline. The timeout is a total cleanup budget,
not a separate allowance for each component. Requests reaching the HTTP router
after cancellation receive HTTP 503.

An owned native commit retains its index guard while it runs. Cancellation
does not release that guard or imply that a cross-store effect was rolled back.
A successful drain awaits the owned work before normal process exit.

If the deadline expires, the server exits unsuccessfully. Runtime teardown does
not add an unbounded wait for blocking native tasks. This is an incomplete
shutdown: preserve the SQLite state, index, binding metadata and operation
receipts for recovery. An interrupted SQLite transaction rolls back, but a
Pending receipt can still describe an effect already applied to LanceDB.
Do not discard it or infer that an acknowledged webhook was fully indexed.

Index and reconciliation operator commands use the same configured shutdown
budget. A deadline failure produces structured JSON with the command's
`operation` and `status: "shutdown_timeout"`, with exit status 1. Other shutdown
supervision errors use `status: "shutdown_failed"`. Neither result confirms
successful completion of the command.

Give the deployment's termination grace period enough time for this budget
and process startup/exit overhead. SIGKILL cannot run cleanup. After an
interruption, reopen the existing matched state and index through the guarded
runtime so durable claims and receipts can be recovered; never delete their
identity files to force startup. See [index commits](index-commits.md),
[webhook processing](notion-webhooks.md), and
[state recovery](local-state-recovery.md).

Process tests and disposable SQLite/index fixtures establish local shutdown
and restart behavior. They do not establish a deployed orchestrator's grace
period or live Notion delivery; verify those separately in the target deployment.
