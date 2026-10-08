# Notion webhook boundary

`POST /webhooks/notion` is a separate route on the HTTP listener. It is disabled
by default. It authenticates Notion independently of MCP browser Host/Origin
validation: deploy behind a trusted HTTPS proxy with request/concurrency limits.
It is not an MCP authorization credential or proof that a page is in scope.

## Setup

The [official subscription setup](https://developers.notion.com/reference/webhooks)
sends an unsigned one-time JSON `verification_token`. An operator must paste that
token into the Notion connection's subscription verification form. The token is
also the key for subsequent HMAC-SHA256 delivery signatures.

1. Set `NK_WEBHOOK_MODE=setup` and `NK_WEBHOOK_CANDIDATE_FILE` to an absolute path
   in a private, operator-owned directory. Start the server with `--http`.
2. Register its public HTTPS `/webhooks/notion` URL in Notion. The endpoint
   captures only the exact setup object into a private (0600 on Unix) file.
   Publication is atomic and never overwrites an existing file or symlink.
3. Inspect the candidate privately, verify the intended subscription in the
   Notion UI, then configure the confirmed token through your secret environment
   as `NK_WEBHOOK_VERIFICATION_TOKEN`. Set `NK_WEBHOOK_MODE=verified` and the
   subscription's `NK_WEBHOOK_WORKSPACE_ID`, `NK_WEBHOOK_INTEGRATION_ID`, and
   `NK_WEBHOOK_SUBSCRIPTION_ID` (hyphenated UUIDs). Set `NK_WEBHOOK_STATE_FILE`
   to an absolute SQLite path in private, persistent local storage. Restart.
4. Remove the candidate file securely when no longer needed. Keep all token files
   outside Git and avoid terminal/session logging of their contents.

An unsigned setup request cannot establish trust: an attacker can race to fill
an exposed setup path. Confirm the token against the intended Notion UI and
subscription before promotion. If the candidate is suspect, stop setup, remove
it privately and request a new token. Setup never enables event admission or
replaces a configured signing token. Prefer a short setup window and restrict
proxy access where feasible. Existing candidate paths return 409; storage errors
return 503 without exposing file paths or token values.

## Delivery

Verified mode requires exactly one `X-Notion-Signature: sha256=<64 hex digits>`.
HMAC verifies the received bytes with a constant-time MAC comparison before JSON
parsing. Re-serialized JSON is not used. Bodies are limited to 64 KiB, including
chunked bodies; content encodings are rejected, reading takes at most ten seconds.
The typed envelope validates UUIDs, RFC3339 time, a positive delivery attempt and entity kind,
then checks the configured workspace, integration and subscription. Unknown
lowercase event names remain representable for future subscriptions.

Only minimized event identity, time, entity and delivery metadata cross the
core admission port. Full payloads, authors, data, signatures and tokens are not
logged or retained in that port. An event is a hint for later authoritative,
scoped Notion reads, never an instruction to index arbitrary supplied content.

Verified HTTP composition now opens the SQLite inbox before serving. The state
file is required; invalid configuration or an unavailable database stops startup.
The inbox uses the operational-state database with schema migration v3, preserving
page state, crawl checkpoints, index versions and the reconciliation journal.
It acknowledges only a committed receipt. SQLite work runs on blocking threads;
a cancelled request may still commit, and a retry safely finds the same event.
Failure/timeout returns 503. Auth failures return 401, scope failures 403,
malformed envelopes 400, oversized bodies 413 and unsupported encoding 415.

Deduplication uses canonical lowercase `(workspace_id, subscription_id, event_id)`.
The first envelope's identity, timestamp, event type, entity and attempt are retained;
redeliveries may vary their positive attempt number but cannot overwrite the hint
or reset pending/running/succeeded/failed state. A conflicting immutable hint
returns 503, retaining the original. No raw payload, author data, signature,
verification token or free-text failure is stored. The inbox has no automatic
retention policy yet; monitor disk capacity and keep the database backed up.

`WebhookInbox` supports scoped claims of pending work and recovery of expired
running work. Claims have a durable increasing generation and bounded lease;
completion from an expired or replaced owner fails. Successful and failed events
remain durable and inert on redelivery. Failures store only a typed class.
Claims are scoped to workspace/subscription; consumers must still validate their
integration and current allowed roots through authoritative Notion reads. Inbox
claims do not grant an indexing lock: workers must separately acquire the shared
index-writer fence before effects. Effects and SQLite completion are not atomic;
recovered work may execute again, so downstream writes must be idempotent.
Automatic refresh, debounce and bounded retries remain #54–#57.

Legacy identity-only `webhook_events` rows are preserved, but cannot reconstruct
pending hints or prove matching payloads. Their IDs conservatively reject new
receipts, even in another subscription. Do not delete them to enable blind
replay: first reconcile affected sources authoritatively and decide an explicit
operator migration/retention policy. This upgrade does not claim recovery of
payloads the old ID-only API never persisted.

[Official delivery semantics](https://developers.notion.com/reference/webhooks-events-delivery)
allow delayed, unordered and repeated deliveries, with retries over approximately
24 hours. No event-timestamp freshness restriction is applied to receipt.

Fixture tests run with `cargo test -p notion-knowledge-server --locked`; normal CI
needs no Notion credentials or live subscription.
