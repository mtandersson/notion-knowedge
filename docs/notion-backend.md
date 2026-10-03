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
reject destructive unsupported Markdown conversions. The concrete conversion,
access checks, read-after-write behavior and API operations are subsequent
implementation tickets #24–#27; this contract makes no live Notion requests.

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
