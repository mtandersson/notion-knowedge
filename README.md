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

Run the bootstrap server process:

```sh
cargo run -p notion-knowledge-server
```

The bootstrap intentionally does not expose an MCP transport yet. It stays
running so the development command has the same process lifecycle as the
future server. stdio and Streamable HTTP are implemented by #17 and #18.

One-shot composition smoke check:

```sh
cargo run -p notion-knowledge-server -- --check
```

Run tests:

```sh
cargo test --workspace
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
