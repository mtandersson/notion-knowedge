# Shared request admission and retries

Every identity, exact metadata/Markdown read, discovery request (including
read-only data source POST queries), and create/append/replace mutation uses
one transport on `NotionClient`. Clone that client to share both its admission
clock and cumulative `request_metrics()` counters. Construct one client per
integration at composition; separately constructed clients and other processes
cannot coordinate their budgets. There is no distributed/workspace limiter.

Admission spaces starts by at least 334 ms (conservatively below the ordinary
3 requests/second budget), without bursts. A 429 or 529 extends the shared
cooldown. Notion specifies integer seconds for `Retry-After`; valid values are
minimum waits, never shortened. Missing/invalid values use bounded equal jitter.
Repeated rejection uses at least the corresponding exponential backoff.
A documented `public_api_request_blocked` reason fails immediately; bounded
error bodies are inspected only for this classification and discarded.

At most three retries follow the initial attempt. Replay-safe reads retry
transport failures and HTTP 500/502/503/504, with exponential equal jitter
(125–250, 250–500, 500–1000 ms). Other status codes fail immediately.
Mutation requests retry only explicit admission rejection (429/529), never
ambiguous transport failures, 5xx, interrupted successful responses or parse
failures. A failed write may already have happened: reconcile before a caller
retries. No idempotency guarantee or automatic 503 write reconciliation is
invented. Read response-body interruptions currently fail to the caller rather
than restarting a partially read response.

Each HTTP attempt retains its 15-second timeout. Admission and queue waits
are bounded to 30 seconds per attempt. A longer Retry-After returns a sanitized
`RateLimited` error with its delay immediately, retaining the full cooldown for
clones; a caller must defer and resubmit later. Bounded attempts and waits make
the operation finite; these are not a transaction deadline for multi-request
crawls or replacement verification. Cancellation consumes no future queue slot.

`RequestMetrics { attempts, retries, rate_limits }` contains only cumulative
integer counts, shared across clones. Consumers may export those fields in
metrics/logs. The adapter emits no tokens, request URLs, IDs, bodies, or raw
upstream error text. This change introduces no automatic payload logging.

Official [request limits](https://developers.notion.com/reference/request-limits)
checked 2026-10-03 document per-connection limits, integer Retry-After, 429/529
admission retries for every SDK method, the permanent workspace block exception,
and the special reconciliation requirement before retrying a write returning
503. The conservative ordinary-plan budget applies even to higher-plan limits.
Workspace-wide throttling can still happen and is handled by shared cooldown.

## Fixture design

Use the explicit `with_fast_fixture_transport()` helper in Notion unit fixtures
that exercise parsing, scope decisions or persistence. It is compiled only for
unit tests or the `test-fixtures` feature. It removes admission pacing, automatic
transport retries and provider consistency delays, while retaining every real
HTTP request, response and metric. Cross-crate integration fixtures use the
loopback-only `with_loopback_fixture(port)` entrypoint to this same policy.
Do not use an ordinary production client merely to obtain a mock endpoint, and
do not add sleeps to let a fixture reach an expected state.

Timing contracts use normal production policy under `test_clock::TestClock`:
334 ms admission, retry bounds/jitter and upstream cooldown remain unchanged.
The controlled clock inhibits Tokio auto-advance while real socket I/O is
pending, records admitted requests and response barriers, and advances only
registered provider waits after responses arrive. This prevents request timeout
or admission assertions from depending on host scheduling. The clock crosses
Tokio's millisecond timer-wheel tick explicitly; no wall-clock wait is involved.
`TestClock::park` establishes a pending admission before cancellation or defer,
instead of relying on a task having run after a scheduler yield.

Cancellation fixtures wait on request notifications or actual partial-file
state before dropping work or advancing its deadline. Stalled fixture bodies
stay pending until explicitly cancelled; they do not sleep for seconds. The
400 ms streaming-test watchdog only bounds progress of real socket/filesystem
I/O before the partial-byte barrier; it neither delays the test nor supplies
its timeout behavior. Real HTTP/TLS, private temporary files and SQLite/Lance
integration stay real. Provider admission, retry and consistency time are virtual
or disabled in non-timing fixtures. There is no environment/runtime bypass and
production constructors retain all configured limits.

Run unit HTTP and controlled-clock contracts with
`cargo test -p notion-knowledge-notion --features test-fixtures --lib --locked`.
Compare the final test execution summary separately from Cargo compilation time;
keep all existing cases and request-count assertions when improving fixtures.
