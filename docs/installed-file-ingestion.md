# Installed audited file ingestion

`knowledge_upload_file` runs the real one-shot `FileIngestBatch` through the
shared server root-scope composition. It downloads from admitted HTTPS hosts,
validates observed bytes, chooses single or explicitly configured multipart,
appends a native image/file block, verifies readback, and records durable
payload-free audit. There is no Drive staging or background signed-URL retry.

Enable `NK_READ_ONLY=false` and `NK_FILE_INGESTION=true` with the integration,
full physical scope, exact source hosts and protected audit settings described
in [startup settings](file-ingestion-settings.md). Use a private operator-owned
directory (mode 0700) and a separate audit SQLite path. Startup opens the audit
store privately and fails if it cannot be used. `--check` validates configuration
without starting a transport or contacting Notion. The startup-owned file count,
byte/timeout/plan/part/retention settings are shared by discovery and execution.

Stdio is a trusted local operator transport. HTTP additionally needs explicit
`NK_HTTP_AUTH=bearer`, a strong locally supplied `NK_HTTP_BEARER_TOKEN`, and a
loopback listener. The existing middleware authenticates every session request;
knowing a session ID does not authorize another call. Unauthenticated HTTP and
OAuth discovery do not install ingestion, even with write mode enabled.
The actor is `server`, representing operator policy, never an invented OAuth
user. Default read-only and operation allow/block policy reject calls before
any adapter. Missing ingestion settings return `file_upload_unavailable`.

A request supplies required canonical `target`, optional bounded `caption`,
and exactly one of legacy `file` or ordered `files`. File metadata grants no
scope authority. The canonical target and full roots/exclusions are rechecked
against Notion before download, upload and append. Caller-supplied roots,
actors, plan settings and provider endpoint selectors are unavailable.

The structured result has `files` in original index order, `partial_success`,
and an optional batch `failure`. Each item reports `index`, `status` and
`terminal_audited`; attempted items include known `target`, `upload_id`,
`block_id` and an allowlisted failure class. Status is `verified`,
`verified_audit_failed`, `failed`, `denied`, `indeterminate`, or `unattempted`.
A pre-batch authority denial leaves all items unattempted and reports `denied`.
Earlier verified attachments remain after ordinary later failures. Authority,
authentication or audit loss stops remaining work. References are returned only
when receipts are known; no rollback or atomic batch guarantee is claimed.

MCP cancellation is observed using the SDK request token. The application keeps
the request-owned batch outside its cancellable execution future, retaining known
upload/block receipts and earlier indexed results in a safe result snapshot with
`cancelled=true`. Dropping execution stops further effects, unlinks request-owned
temporary files, and records
failed or indeterminate audit according to whether an effect might have occurred.
A canceled caller may receive no response. Inspect authoritative Notion and the
audit before manually supplying fresh input; no replay or recovery queue is run.

## Integrated acceptance evidence

Credential-free tests live in `crates/server/src/http/ingestion_tests.rs`. They
install real `Config`/root-scope/application composition and join secure source
HTTPS, the normal rate-limited Notion HTTP adapter and durable SQLite. Fixtures
use typed seams compiled only for tests; no production environment selects a
fixture endpoint. Append target, upload identity and caption are checked, and
readback identity derives from the actual received append.

Run with the repository toolchain:

```sh
nix develop .#spike --command cargo test --locked -p notion-knowledge-server --lib ingestion_tests
nix develop .#spike --command cargo test --locked -p notion-knowledge-server --test stdio --test http --test config
```

| Original criterion | Installed evidence |
| --- | --- |
| #66 file parameters | Both native transport tests discover the actual upload tool; parser/schema and real-process stdio tests retain `_meta["openai/fileParams"]`, legacy input and bounded arrays. |
| #67 secure temporary download | Both native transport tests use real TLS source streaming and assert owned files are removed; existing downloader tests exercise host/DNS/redirect/size boundaries. |
| #68 MIME/extension/size | Native image/document tests traverse observed-byte validation; multipart selection checks exact observed thresholds. Existing validator tests cover rejection boundaries. |
| #69 single-part | `installed_transports_select_single_and_multipart_from_observed_bytes` asserts single mode and exact transmitted bytes at threshold. |
| #70 multipart | Same installed wire test asserts multipart mode, exact ordered parts and part numbers; existing joined adapter tests cover malformed/final-status failures. |
| #71 native image | `installed_stdio_native_image_and_file_join_https_notion_and_durable_audit` and `installed_authenticated_http_attaches_and_rejects_unauthenticated_requests` verify actual native image append/readback. |
| #72 native file | Same tests verify the native document file block. |
| #73 multiple files | `installed_transports_ordered_batch_keeps_success_and_reports_source_failure` checks original indices, verified first attachment, expired second reference and partial success. |
| #74 readback | Native transport tests require actual append-bound block readback and verified stable upload/block references; cancellation test reaches real blocked readback. |
| #75 cleanup/audit/privacy | Native transport and batch fixtures inspect safe durable outcomes and private-file cleanup; cancellation fixture sends wire `notifications/cancelled` during multipart send/readback and requires truthful indeterminate audit without replay. |

Scope/exclusion/read-only/blocklist and unsuitable HTTP composition are checked
by `installed_transport_scope_and_effective_policy_deny_before_source_or_writes`.
Unauthenticated HTTP initialization and reuse of an authenticated session are
rejected on the actual HTTP wire. Existing subprocess tests retain missing
configuration, read-only and invalid startup checks.

These fixtures verify local stdio and loopback bearer operation. A private real
ChatGPT connection, OAuth authorization (#117), a live allowed root and native
Notion UI/readback remain separate operational gates. No production ChatGPT
OAuth readiness is claimed and no private API credentials are needed by CI.
