# notion-knowedge

[![CI](https://github.com/mtandersson/notion-knowedge/actions/workflows/ci.yml/badge.svg)](https://github.com/mtandersson/notion-knowedge/actions/workflows/ci.yml)

Local-first Notion knowledge MCP for ChatGPT.

Notion is the authoritative source of truth. Local retrieval state is derived,
disposable, and rebuildable. The architecture decision is recorded in
[ADR 0001](docs/adr/0001-runtime-and-component-boundaries.md).

> The repository name currently uses `notion-knowedge`; package names use
> `notion-knowledge-*`.

## Development environment

The supported development setup is **Nix-first** and uses a tracked
`.envrc`.

Prerequisites:

- Nix with flakes enabled
- direnv
- optionally nix-direnv for faster cached shell activation

After cloning:

```sh
direnv allow
```

If nix-direnv is installed, `.envrc` uses its `use flake` integration.
Otherwise it falls back to `nix print-dev-env`.

No separately installed global Rust toolchain is required. The pinned Nix
flake supplies Cargo, rustc, rustfmt, Clippy, and the native tools needed by
the current workspace.

To enter the same environment manually:

```sh
nix develop
```

Do not put secrets in `.envrc`. Local secret-bearing `.env*` files are
ignored by Git.

## Commands

Run the MCP server over stdio:

```sh
cargo run -p notion-knowledge-server
```

The server accepts newline-delimited MCP JSON-RPC on stdin and writes protocol
responses to stdout. Startup diagnostics and errors go to stderr. It completes
MCP initialization and supports tool discovery for both `knowledge_search` and
`knowledge_get`; calls return explicit unavailable errors until the corresponding
retrieval adapters are configured. The process exits when
the connected client closes stdin after initialization. Use `--http` to serve
the same MCP handler over Streamable HTTP at `/mcp`:

```sh
cargo run -p notion-knowledge-server -- --http
```

The default endpoint is `http://127.0.0.1:3000/mcp`. `NK_HTTP_HOST` and
`NK_HTTP_PORT` set the listener address. HTTP supports initialization, tool
discovery and calls, stateful sessions, SSE responses, and session deletion.
Protocol failures return structured JSON-RPC errors. Ctrl-C stops the listener
and cancels active sessions. Both transports share the [semantic search contract](docs/knowledge-search.md);
valid search calls in the default bootstrap return an explicit retrieval-unavailable tool error. The isolated [semantic MCP spike](docs/semantic-mcp-spike.md) explicitly configures real local semantic retrieval through this same handler and transport wiring.
Calls to unknown tools return protocol errors.

The HTTP listener validates Host against loopback names and the configured IP,
and validates browser Origin against the configured HTTP authority or localhost
at the configured port. Clients without Origin are accepted. For remote access,
place this listener behind a trusted HTTPS proxy; public hostname routing and
MCP authentication are separate deployment work.

For MCP Inspector-style clients, build the binary with
`cargo build -p notion-knowledge-server` and configure the stdio command as the
absolute path to `target/debug/notion-knowledge-server`, with no arguments.
Supply configuration through the client's environment settings. A local MCP
client configuration has this shape (replace the example path):

```json
{
  "mcpServers": {
    "notion-knowledge": {
      "command": "/absolute/path/to/notion-knowedge/target/debug/notion-knowledge-server",
      "args": []
    }
  }
}
```

One-shot configuration and composition check (does not start a transport):

```sh
cargo run -p notion-knowledge-server -- --check
```

Type-check the full workspace:

```sh
cargo check --workspace --all-targets --locked
```

Run tests:

```sh
cargo test --workspace --locked
```

The transport process smoke tests can also be run alone:

```sh
cargo test -p notion-knowledge-server --test stdio
cargo test -p notion-knowledge-server --test http
```

Format and lint:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Production build:

```sh
cargo build --workspace --release --locked
```

## Diagnostics

Use `--version` for the Cargo release identity, `--diagnostics` for a one-shot
composition report, and HTTP `GET /health` for current serving-process dependency
health. The bootstrap reports unavailable adapters honestly (HTTP 503). See
[diagnostic semantics and orchestration access policy](docs/diagnostics.md).

## Container

The current server is packaged in a pinned, non-root container with stdio and
HTTP support. See [container build, run and verification instructions](docs/container.md).
This bootstrap does not yet load models or process indexes; #104 remains open
for the adapter integration in #145.

## Continuous integration

The permanent GitHub Actions workflow at `.github/workflows/ci.yml` runs on every
pull request targeting `main`, every push to `main`, and manual
`workflow_dispatch` runs. PRs select relevant checks from changed inputs; main
and manual runs execute the full canonical checks above.

CI selects checks conservatively from changed PR inputs and retains fresh dependency
and secret scans on every PR. Main pushes and manual runs execute all checks.
The always-running **CI gate** is the stable check to require when branch protection
is available. See [CI selection and verification](docs/ci.md) for the path map,
check migration, and actual-run evidence.

CI retains separate check names for formatting, Clippy, type
checking, unit tests, the release build, final-image container smoke tests,
dependency vulnerabilities, and committed secrets.
The release-build job also uploads
a short-lived Linux server artifact for downstream validation and future
release automation.

The normal CI path does **not** use Notion credentials or other repository
secrets. Integration tests that eventually require real external credentials
should remain separate controlled jobs rather than broadening the trust surface
of ordinary pull-request CI.

GitHub Action dependencies are pinned to immutable commit SHAs. Cargo dependency
downloads are cached using the lockfile-derived cache key. Compiled outputs use
[compatible source snapshots](docs/cargo-cache.md), refreshed after successful
source changes; the Nix flake remains the source of truth for the toolchain.

CI uses [smaller pinned format/security shells and fresh Nix downloads](docs/nix-ci.md),
selected from measured restore/save overhead.

Security scans use pinned Nix tools, fresh RustSec advisories, and complete Git
history with redacted findings. See [security scanning](docs/security-scanning.md)
for local commands, gate verification, and narrowly reviewed exceptions.

## Agent guidance and skills

Shared agent configuration is intentionally agent-neutral:

- `AGENTS.md` is the canonical repository guidance file.
- `.agents/skills/` is the canonical location for shared repository skills and
  their supporting resources.
- `CLAUDE.md` is a compatibility symlink to `AGENTS.md`.
- `.claude/skills` and `.codex/skills` are compatibility symlinks to
  `.agents/skills`.

Edit the canonical files, not the compatibility links. The layout check used by
CI verifies the link targets, the five expected skills, and the
`pick-tickets/agents/openai.yaml` metadata from a fresh checkout.

Run the same check locally with:

```sh
./scripts/check-agent-layout.sh
```

## Configuration

The server reads environment variables once at startup and validates them
before reporting readiness. `--check` performs the same validation and exits
with status 0 on success or 2 on a configuration error. Normal startup also
exits with status 2 for invalid configuration. Errors name the setting and
constraint without printing the supplied value. Credentials are redacted in
configuration debug output.

The bootstrap needs no configuration or credentials by default. These settings
establish the composition-root configuration for the HTTP transport
and Notion adapter; the stdio server does not open a listener. Neither transport
makes Notion requests during normal startup. Use the explicit on-demand probe
`cargo run -p notion-knowledge-server -- --notion-identity` to verify the configured
integration against Notion. It requires `NK_NOTION_AUTH=integration` and
`NOTION_TOKEN` from the secret environment, prints only a success message or a
sanitized failure class, and exits without starting an MCP transport.

| Variable | Default | Accepted values |
| --- | --- | --- |
| `NK_HTTP_HOST` | `127.0.0.1` | An IPv4 or IPv6 address, without a port or IPv6 brackets; hostnames are not supported. |
| `NK_HTTP_PORT` | `3000` | An integer from 1 to 65535. |
| `NK_NOTION_AUTH` | `none` | `none` or `integration`. This selects upstream Notion credentials, not MCP client authentication. |
| `NOTION_TOKEN` | Unset | Required when `NK_NOTION_AUTH=integration`; must be nonempty and contain no whitespace or control characters. Ignored when authentication is `none`. |

An explicitly empty optional setting is invalid; defaults apply only when a
variable is unset. OAuth configuration will be introduced with the OAuth
implementation.

See [`.env.example`](.env.example) for a sample. The server does not load `.env`
files automatically: export variables in your shell or configure your service
manager to supply them. For example, validate an HTTP bind override:

```sh
NK_HTTP_HOST=::1 NK_HTTP_PORT=3001 cargo run -p notion-knowledge-server -- --check
```

Supply a real Notion integration token through your local secret environment
only after selecting `NK_NOTION_AUTH=integration`. Keep secret-bearing `.env`
files out of Git and never put secrets in `.envrc`.

## Workspace

```text
crates/
  core/       domain types, application services, ports
  notion/     authoritative Notion adapter
  retrieval/  LanceDB/SQLite/embedding adapters
  mcp/        transport-independent semantic MCP surface
  server/     binary composition root and transport wiring
```

Dependency direction is defined by ADR 0001. In particular, `core` must not
depend on MCP, Notion, LanceDB, SQLite, or HTTP implementation types.

The [canonical indexed content contract](docs/indexed-content.md) defines the
versioned page/chunk records shared by discovery and retrieval. The
[heading-aware Markdown chunker](docs/markdown-chunking.md) produces semantic
chunk drafts with citation metadata and original source offsets. The
[versioned fingerprint stage](docs/content-fingerprints.md) assigns content hashes
and reconciles stable chunk identities against a preceding snapshot.

The [authoritative backend contract](docs/notion-backend.md) defines typed
read/write capabilities and normalized failures independently of API transports.

## Retrieval evaluation

A bilingual, privacy-conscious fixture corpus and graded query judgments live in
[`eval/retrieval/`](eval/retrieval/README.md). Dataset integrity tests run with the
normal workspace tests; retrieval benchmarking is a separate follow-up.

## Security boundaries

The [threat model](docs/threat-model.md) records current protections, production
release requirements, residual risks and review triggers for MCP, Notion, local
state, webhooks and file ingestion. Review it when enabling or changing a trust
boundary; the current bootstrap does not implement MCP authentication.

## Architecture decisions

Architecture Decision Records live in [`docs/adr/`](docs/adr/README.md).
The ADR README documents when to write one, numbering, status transitions,
review expectations, and how decisions are superseded.

The [integration identity client](docs/notion-identity.md) documents the API
version, failure classes and credential boundary.
The [shared rate limiter and retry policy](docs/notion-rate-limits.md) covers
every Notion request and exposes payload-free retry counters.
The [page write primitives](docs/notion-writes.md) document Markdown creation
and append semantics.

The [scoped root crawler](docs/notion-discovery.md) documents read-only discovery,
physical ancestry boundaries and the `--crawl-dry-run` command.

The [exact page metadata read](docs/notion-pages.md) documents IDs, links,
normalized properties and response completeness.

The [authoritative content read](docs/notion-content.md) preserves nested
enhanced Markdown and surfaces unsupported or incomplete content explicitly.

The [scoped canonical document smoke](docs/discovered-documents.md) assembles
exclusion-aware discovery, exact content, relationships, chunking and stable
fingerprints into a read-only canonical snapshot.

The [embedding provider contract](docs/embeddings.md) defines asynchronous batch
execution and persisted provider/model/vector-space identity independently of
local model runtimes.

The [production LanceDB chunk table](docs/lancedb-chunks.md) persists canonical
chunks with explicit vector identity, queryable source filters and stable-ID upserts.

The [one-page Notion/vector experiment](docs/notion-vector-spike.md) connects an
explicitly selected page to the local Qwen/LanceDB spike without broadening
discovery authority. It remains separate from the production MCP runtime.

The [bounded ChatGPT experiment](docs/chatgpt-spike.md) records the private
connection, actual source-linked answers, manual-refresh demonstration, unsupported
question behavior and production follow-up decisions.

The [local Qwen embedding provider](docs/local-qwen.md) implements the production
embedding port with pinned offline assets and bounded CPU batches. Enable the
optional `local-qwen` retrieval feature and use the matched `.#spike` toolchain;
the bootstrap server is not yet wired to load the model.

The [production semantic vector adapter](docs/semantic-search.md) binds a configured
embedding provider to the indexed LanceDB table and filters before top-k selection.

The [hybrid search service](docs/hybrid-search.md) combines lexical and semantic
candidates with configurable reciprocal-rank fusion and per-result path provenance.
