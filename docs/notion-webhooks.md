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
Page debounce and bounded recovery are described below; authoritative refresh and
production dispatch remain #55/#56 and #248.

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

## Bounded inbox recovery and operator commands

Schema v4 persists a processing-cycle counter separately from lifetime processing
attempts, Notion's original `attempt_number`, and ownership generation. Each claim,
including takeover after a crash, consumes one attempt. Defaults allow five attempts;
retryable outcomes delay eligibility by 5, 10, 20, then 40 seconds (capped at 300).
Permanent outcomes and exhausted work stay failed. Expired final claims become
failed when the next scoped claim sweeps them; delayed/failed work does not block
other eligible events. Clock values are Unix seconds; invalid or overflowing times
fail without completing the claim. Policies allow 1–100 attempts and 1–86400-second
base/cap, with cap at least base. Policy is persisted per event, so changing an
operator policy does not silently change another event's budget.

The production binary provides local-only commands, requiring an **existing absolute
SQLite file**. Scope UUIDs and event UUIDs are case insensitive. Arguments are exact
positional forms; output contains identifiers, sanitized enum classes and counters,
never raw bodies or free-text source failures:

```sh
notion-knowledge-server --webhook-failed /absolute/state.sqlite WORKSPACE_UUID SUBSCRIPTION_UUID 100
notion-knowledge-server --webhook-inspect /absolute/state.sqlite WORKSPACE_UUID SUBSCRIPTION_UUID EVENT_UUID
notion-knowledge-server --webhook-retry /absolute/state.sqlite WORKSPACE_UUID SUBSCRIPTION_UUID EVENT_UUID GENERATION 5 5 300
```

List returns the oldest failed events, bounded to 1–1000; inspect addresses any
selected event directly, including pending/running events. Retry requires the
observed generation from inspect and a failed event. It atomically increments the
generation, starts a new bounded cycle with the supplied attempts/base/cap, and
retains lifetime counts, original hints and last failure classification. Concurrent
or repeated requeues and stale worker completion fail. Delivery duplicates never
reset counters or state. The existing `finish(..., Some(failure))` API deliberately
means permanent failure; automatic delayed retries use `complete(Retryable(...))`.

Upgrade preserves v3 hints, receipts, page and reconciliation state. Existing rows
retain lifetime generation as the best available prior-claim count; rows previously
claimed begin with one consumed cycle attempt. Existing failed rows stay inert until
an explicit operator retry, and existing running leases remain valid with their
original generation. These commands do not dispatch source/index processing; #248
adds that composition after its dependencies. Inbox ownership does not fence an
in-flight index write: processors still require shared index serialization and
idempotent apply-before-ack.

## Durable page debounce

Verified HTTP admission atomically coalesces `page.content_updated` and
`page.properties_updated` with `entity.type=page`. Notion's supported
[content-update payload](https://developers.notion.com/reference/webhooks-events-delivery)
identifies the page even when the edit adds/removes blocks; no block ID is used
as a page key. Other kinds remain ordinary inbox events for the dispatcher.
Generic block hints are not silently interpreted as pages. This grouping does
not authorize a page or eliminate the authoritative reads and root checks.

Set `NK_WEBHOOK_DEBOUNCE_MS` (default 5000, 1–60000 milliseconds) and
`NK_WEBHOOK_MAX_DELAY_MS` (default 30000, quiet period through 300000 milliseconds).
Both are validated at startup. The first pending event persists its window;
newer events extend the trailing quiet deadline up to the original burst cap.
Older events join the batch without postponing it; receipt duplicates never
change deadlines. Latest metadata is selected by parsed UTC instant, with event
ID as a deterministic tie break. Full changed content is not retained.

Schema v5 adds page work and event membership without altering existing receipts,
recovery counters or reconciliation state. Keys include canonical lowercase page
ID, workspace and subscription. The SQLite `WebhookDebounce` port claims a due
page as one immutable event snapshot. Different pages can have independent leases.
Single-event claims/completion cannot take page-owned events. A new event received
during a refresh remains pending in a successor batch, so the old completion cannot
consume it. Completion verifies the actual stored snapshot and ownership generation.

Each member consumes its existing bounded processing budget when claimed. Success
or permanent failure completes just that snapshot; retryable members remain grouped
with their own persisted backoff. A fresh event can proceed while older members wait
for retry eligibility, without accelerating their retry clocks. Expired page claims
recover with a new generation and consume another attempt; exhausted members become
failed and detach. Operator requeue of failed members resumes ordinary inbox work.
The quiet window applies to delivery bursts; crash recovery is immediately eligible
under the same bounded policy as ordinary inbox recovery. Cancellation needs no
memory-only timer: another process recovers the persisted lease. Clock values for
page work are Unix milliseconds; event retry counters/deadlines retain Unix seconds.

This delivers real production admission and the durable batch lifecycle. It does
not yet execute source refresh or index writes: #55/#56 implement effects and #248
composes the worker. A page lease is not an index-writer fence; effects still need
shared index serialization and idempotent apply-before-ack. Real binary HTTP tests
verify signed mixed bursts, two pages and restart retention against the SQLite
adapter, and deterministic SQLite tests cover deadlines, offsets, successor batches,
expiry boundaries, retry budgets and crash recovery without live Notion credentials.
