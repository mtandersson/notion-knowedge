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
production dispatch is opt-in as described below.

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
original generation. These commands operate on the same state consumed by the opt-in serving worker. Inbox ownership does not fence an
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

This delivers real production admission and the durable batch lifecycle. The opt-in worker below consumes these batches through the authoritative refresh/lifecycle handler. A page lease is not an index-writer fence; effects still need
shared index serialization and idempotent apply-before-ack. Real binary HTTP tests
verify signed mixed bursts, two pages and restart retention against the SQLite
adapter, and deterministic SQLite tests cover deadlines, offsets, successor batches,
expiry boundaries, retry budgets and crash recovery without live Notion credentials.


## Production serving worker

Build the Unix production binary with the matched toolchain:

```sh
nix develop .#spike --command cargo build -p notion-knowledge-server --features webhook-worker --locked
```

The ordinary binary remains model-free. Explicit `NK_WEBHOOK_WORKER=true` requires
this feature, `--http`, verified webhook configuration, integration credentials,
and matching `NK_NOTION_SCOPE_WORKSPACE_ID` and webhook workspace. Configure
`NK_NOTION_SCOPE_ROOTS` with trusted comma-separated canonical page UUIDs and
`NK_NOTION_SCOPE_GENERATION` with a positive generation (default 1). Optional
`NK_NOTION_SCOPE_EXCLUDED_PAGES` and `NK_NOTION_SCOPE_EXCLUDED_DESCENDANTS` take
1–100 canonical UUIDs each. Root/exclusion authority comes from this operator
configuration and live Notion ancestry, never event names or stored index links.
The credential must belong to that intended workspace; webhook IDs alone cannot
prove credential identity.

Set `NK_WEBHOOK_INDEX_DIR` to an existing absolute local directory and
`NK_WEBHOOK_QWEN_ASSETS` to the absolute directory containing the exact pinned
[offline Qwen assets](local-qwen.md). Startup loads and validates the real model,
binds the index to the same `NK_WEBHOOK_STATE_FILE`, and creates a new empty
`chunks` table only when absent. Existing tables must match the production vector
identity; missing/corrupt assets, incompatible tables, foreign bindings or a busy
startup index writer stop serving with a sanitized error. No download, destructive
rebuild, credential fallback or success placeholder is used. `--check` validates
configuration syntax; serving startup additionally validates assets/storage.
Existing experimental/spike vector identities are incompatible and require an
explicit separate rebuild. This worker binding uses policy `webhook-v1`, a scoped
root/exclusion digest, and the configured generation/epoch; all future indexing
consumers must open the same binding and acquire the same SQLite writer lease.

`NK_WEBHOOK_POLL_MS` is 10–60000 (default 1000); `NK_WEBHOOK_TIMEOUT_SECONDS` is
1–300 (default 60). One processor alternates ordinary event and due-page selection,
sleeping once between dispatches. Empty/delayed queues and busy index writers do
not spin or consume claim budgets. Source/index processing has a bounded deadline;
claims and the shared writer lease last that deadline plus 30 seconds. Startup
and restart claim durable due work, including expired leases. A crash retains the
original processing-cycle budget. Retry policy and operator commands above remain
unchanged; changing worker poll/deadline settings does not reset event budgets.

Supported page hints are `page.created`, `page.content_updated`,
`page.properties_updated`, `page.deleted`, `page.undeleted` and `page.moved`.
Every batch member must have the trusted integration and page identity. Other
hints become permanent sanitized `Source` failures. Supported hints all use the
same authoritative lifecycle read: present/allowed pages refresh canonical
metadata/content/chunks; positively inactive or moved/excluded owned pages remove
local chunks. Inaccessible/ambiguous source responses never authorize deletion.
Unknown or out-of-scope pages and invalid/unsupported source contracts fail
permanently; ordinary source availability, index, conflict and storage failures
consume the persisted retry budget and backoff. Exhaustion is durable dead letter.
Notion's HTTP retry policy remains bounded independently of the inbox policy.

Processing acquires the database-wide indexing fence before claiming/effects.
Stable hashed member IDs bind operation receipts across restart. Actual commit
checks both the writer fence and inbox generation/expiry, including the page
lease's millisecond deadline. Source revalidation under the real directory guard
polls that ownership/cancellation clock at most every 20 milliseconds. Directory
lock waits also poll the clock; timed-out observers cannot accumulate detached
unbounded lock waiters. On shutdown no new claims start, source preparation is
cancelled, and any operation-owned source revalidation loses authority and releases
its lock. An already authorized filesystem write may finish; its cross-process
lock remains held through completion, and an incomplete receipt is reconciled
idempotently after restart. Filesystem/SQLite IO and an already running CPU model
forward pass are not forcibly interrupted; Qwen retains its bounded permit until
that call completes. The deadline bounds async scheduling and source/lock waits,
not a hard wall-clock promise for a stalled kernel or CPU forward pass.

Effects and inbox acknowledgement remain separate commits. Replay may run the
handler again after effects-before-ack; durable receipts and canonical unchanged
checks prevent duplicate chunks/embeddings while current authority is rechecked.
Completion uses the immutable batch snapshot, so newer arrivals and delayed older
members retain independent work/budgets. Losing an inbox claim leaves completion
to its current/recovered owner. Unexpected worker storage failure cancels HTTP
serving and returns failure for the service manager to restart.

At info/debug level the running worker emits payload-free JSON summaries containing
`operation=webhook_worker`, state, cycle/lifetime attempt counts, retry deadline and
sanitized last failure enum. IDs, payloads, exception text, content and credentials
are absent. `NK_LOG_LEVEL=off` suppresses these summaries. The local operator
inspect/failed commands expose retained retry/dead-letter state including counters.

Credential-free verification uses a fixture Notion HTTP transport, real webhook
HTTP signature admission, actual worker lifecycle, SQLite and the guarded Lance
adapter with a deterministic embedding provider:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --locked
```

Tests cover healthy searchable canonical content alongside isolated dead letters,
transient retry, permanent unknown hints, exhausted budgets, operator recovery,
debounce, restart/effects-before-ack, shared-writer/inbox fencing and shutdown during
owned source revalidation. Searchability is verified through actual Lance FTS;
production MCP retrieval composition remains separate work. The fixture provider
is not a live-model quality test or live Notion smoke. No external credentials are
required. Optional real-Qwen smoke remains documented separately.
