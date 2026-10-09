# Authoritative backend contract

`notion_knowledge_core::backend` owns the async, object-safe `NotionRead`,
`NotionWrite` and combined `NotionBackend` ports. The Notion adapter implements
these ports; application, index and MCP callers depend only on core. No SDK,
HTTP response, MCP transport, LanceDB type or credential crosses this boundary.

Reads return fresh `Page` metadata or `PageContent` with normalized Markdown.
These authoritative records are separate from disposable `IndexedDocument`
records: no embedding state or content hash is required for a page fetch.
Property values reuse the provider-independent core normalization contract.
IDs and timestamps are preserved; validation and Notion wire conversion belong
to the adapter.

Writes distinguish explicit full replacement, append and creation under an
explicit parent page. Each mutation returns the authoritative page ID and URL.
A read-only consumer accepts `dyn NotionRead` without acquiring a write
capability. Adapters must also enforce configured authorization on writes and
reject destructive unsupported Markdown conversions. The production `NotionClient` implements both ports using the operations from
#24–#27. Replacement requires explicit opt-in and verifies fresh content;
creation and append fetch complete authoritative metadata after their receipts.
If that read fails after a mutation, the error operation is
`notion.create.verify` or `notion.append.verify`: the mutation has already
returned a successful receipt, so callers must reconcile rather than replay.
The typed `committed_page_id` preserves the validated successful mutation
receipt's target for both creation and append. Callers can refresh that exact
page without replaying POST/PATCH. Its absence does not prove that an ambiguous
mutation failed to commit. The lower-level inherent methods retain their
receipt-only return types for callers that need them.

All operations return `BackendError`: a normalized category, operation label,
and optional rate-limit retry duration. Adapters must not retain raw response
bodies, credentials or page content in errors. Retry advice does not establish
idempotency: creation and append must not be blindly replayed. Authentication,
HTTP error mapping and retry policy are implemented in #22–#23.

The boxed Send futures work through trait objects without a runtime-specific
contract or an async-trait dependency. The core integration tests substitute an
in-memory adapter, exercise distinct replacement/append semantics, accept a
read-only capability, and observe missing-page and create-rate-limit failures.
Run them with `cargo test -p notion-knowledge-core --test backend --locked` in the
pinned Nix development environment.

## Integrated backend verification (#3)

The native implementation tickets #21–#27 are complete. The production ports
are exercised through `dyn NotionBackend` against a local mock HTTP upstream:
authenticated exact metadata/content reads, explicit-parent creation,
non-destructive append, conservative replacement with fresh verification, and
sanitized read-after-create failure without mutation replay. No live credentials
or Notion mutations are required. Run
`cargo test -p notion-knowledge-notion --locked` in the supported Nix shell.

The same adapter's remaining boundaries are covered by its focused tests:
identity validation/redaction (#22), shared rate admission and Retry-After,
read retries versus mutation non-replay (#23), page metadata completeness (#24),
enhanced Markdown and explicit unsupported/truncated reads (#25), disabled
replacement and fail-closed destructive conversion (#26), and create/append
request validation and receipt checks (#27). The core mock contract tests
verify capability separation without coupling applications to HTTP.

This completes the backend adapter objective, not the later semantic MCP tools,
OAuth, scoped agent-write policies or real-account production smoke in their
own epics. No unperformed live-account verification is implied.

The metadata-only `PageLifecycle` companion port supplies
[authoritative lifecycle scope evidence](notion-lifecycle.md) and revalidation
without widening `NotionRead` into index mutation authority.
