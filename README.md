# notion-knowedge

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
the connected client closes stdin after initialization. Streamable HTTP is
implemented separately by #18.

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

Run tests:

```sh
cargo test --workspace
```

The stdio process smoke test can also be run alone:

```sh
cargo test -p notion-knowledge-server --test stdio
```

Format and lint:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

Production build:

```sh
cargo build --workspace --release
```

## Configuration

The server reads environment variables once at startup and validates them
before reporting readiness. `--check` performs the same validation and exits
with status 0 on success or 2 on a configuration error. Normal startup also
exits with status 2 for invalid configuration. Errors name the setting and
constraint without printing the supplied value. Credentials are redacted in
configuration debug output.

The bootstrap needs no configuration or credentials by default. These settings
establish the composition-root configuration for the upcoming HTTP transport
and Notion adapter; the current stdio server does not open a listener or make
Notion requests.

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

## Architecture decisions

Architecture Decision Records live in [`docs/adr/`](docs/adr/README.md).
The ADR README documents when to write one, numbering, status transitions,
review expectations, and how decisions are superseded.
