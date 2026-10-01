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
MCP initialization and supports tool discovery; the tool catalog is currently
empty while semantic knowledge tools are implemented. The process exits when
the connected client closes stdin after initialization. Use `--http` to serve
the same MCP handler over Streamable HTTP at `/mcp`:

```sh
cargo run -p notion-knowledge-server -- --http
```

The default endpoint is `http://127.0.0.1:3000/mcp`. `NK_HTTP_HOST` and
`NK_HTTP_PORT` set the listener address. HTTP supports initialization, tool
discovery and calls, stateful sessions, SSE responses, and session deletion.
Protocol failures return structured JSON-RPC errors. Ctrl-C stops the listener
and cancels active sessions. Both transports currently share an empty tool
catalog; calls to unknown tools return protocol errors.

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

## Container

The current server is packaged in a pinned, non-root container with stdio and
HTTP support. See [container build, run and verification instructions](docs/container.md).
This bootstrap does not yet load models or process indexes; #104 remains open
for the adapter integration in #145.

## Continuous integration

The permanent GitHub Actions workflow at `.github/workflows/ci.yml` mirrors the
canonical checks above on every pull request targeting `main`, every push to
`main`, and manual `workflow_dispatch` runs.

CI exposes separate required-check-friendly jobs for formatting, Clippy, type
checking, unit tests, the release build, and final-image container smoke tests.
The release-build job also uploads
a short-lived Linux server artifact for downstream validation and future
release automation.

The normal CI path does **not** use Notion credentials or other repository
secrets. Integration tests that eventually require real external credentials
should remain separate controlled jobs rather than broadening the trust surface
of ordinary pull-request CI.

GitHub Action dependencies are pinned to immutable commit SHAs. Cargo dependency
downloads are cached using the lockfile-derived cache key; the Nix flake remains
the source of truth for the toolchain.

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
makes Notion requests yet.

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

## Retrieval evaluation

A bilingual, privacy-conscious fixture corpus and graded query judgments live in
[`eval/retrieval/`](eval/retrieval/README.md). Dataset integrity tests run with the
normal workspace tests; retrieval benchmarking is a separate follow-up.

## Architecture decisions

Architecture Decision Records live in [`docs/adr/`](docs/adr/README.md).
The ADR README documents when to write one, numbering, status transitions,
review expectations, and how decisions are superseded.
