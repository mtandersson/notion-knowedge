# ADR 0001: Runtime and component boundaries

- **Status:** Accepted
- **Date:** 2026-10-01
- **Issue:** #14

## Context

The project is a local-first MCP server for personal knowledge where **Notion
remains authoritative** and all local search/index state is disposable and
rebuildable.

The server must expose the same application behavior over both stdio and
Streamable HTTP, support local hybrid retrieval, preserve provenance, and keep
embedding execution replaceable.

## Decision

Use **Rust** as the initial runtime and **embedded LanceDB OSS** as the primary
retrieval backend.

Use **SQLite** for operational state that is not naturally retrieval data, such
as sync checkpoints, webhook/event deduplication, retry/job state, tombstones,
index metadata, and credential metadata.

Notion is always the source of truth. LanceDB contains derived data only and
must be safe to delete and rebuild from Notion. Secrets and access tokens must
never be stored in LanceDB.

### Initial workspace boundaries

```text
crates/
  core/
    domain types
    application services
    ports/traits

  notion/
    Notion REST/OAuth adapter
    implements core Notion ports

  retrieval/
    LanceDB retrieval/index adapter
    SQLite operational-state adapter
    embedding-provider implementations
    implements core retrieval/state/embedding ports

  mcp/
    semantic MCP tools
    request/response mapping
    transport-independent MCP application surface

  server/
    binary composition root
    configuration
    stdio transport
    Streamable HTTP transport
    HTTP auth/OAuth plumbing
    lifecycle/health
```

### Dependency direction

```text
                    server (composition)
                     /             \
              transports           adapters
                 |                 /   \
                 v                v     v
                mcp ----------> core <--- notion
                                 ^
                                 |
                             retrieval
```

Rules:

1. `core` depends on no MCP, Notion, LanceDB, SQLite, or HTTP implementation
   types.
2. `core` owns application contracts/ports, including `NotionBackend`,
   `RetrievalIndex`, `EmbeddingProvider`, and operational state/sync
   interfaces as needed.
3. `notion` implements the authoritative backend contract using the Notion
   API.
4. `retrieval` implements disposable local indexing/search and operational
   state.
5. `mcp` maps semantic MCP tools to application services and contains no
   transport-specific business logic.
6. `server` is the composition root. It chooses stdio or HTTP and wires
   concrete adapters into the same application/tool layer.
7. Neither stdio nor HTTP handlers call Notion or LanceDB directly.

### Retrieval

The initial retrieval implementation is **LanceDB OSS embedded in-process**.

LanceDB-specific types stay inside the retrieval adapter. The application sees
only `RetrievalIndex` contracts so another backend can be evaluated without
rewriting MCP or Notion layers.

### Embeddings

Embedding generation is behind `EmbeddingProvider`. Implementations may use a
remote embedding API or a local model such as Qwen3-Embedding-0.6B.

Model name, version, and vector dimension are index metadata. A provider/model
change that makes vectors incompatible requires an explicit re-index.

### Authority and freshness

- Notion IDs, URLs, edit timestamps, and hashes are preserved as provenance.
- Local normalized documents/chunks are derived cache state.
- Critical reads and writes can refresh from Notion instead of trusting stale
  index content.
- Local retrieval state can be deleted and rebuilt without authoritative data
  loss.

### Authentication boundaries

Authentication is separate at each hop:

```text
ChatGPT -- MCP-scoped credential --> notion-knowledge
notion-knowledge -- Notion credential --> Notion API
```

The Notion adapter may support an internal integration token for bootstrap and
headless use plus Notion OAuth for interactive/default operation. A Notion
credential is never accepted as the MCP bearer credential and is never exposed
through MCP results or logs.

Remote MCP authentication belongs at the HTTP/server boundary and must not leak
into core business logic.

## Alternatives considered

### TypeScript

The strongest alternative: excellent MCP support, the official Notion
JavaScript SDK, and first-class LanceDB support.

Not selected initially because Rust provides a compact single-service
deployment, first-class LanceDB integration, and a strong fit for the long-lived
local indexing, file, and sync workload. Reconsider if Rust's Notion/API
integration becomes a material blocker.

### Go

Excellent server/MCP ergonomics and deployment model.

Not selected while embedded LanceDB remains preferred because Go is not a
first-class LanceDB OSS SDK target. Reconsider if retrieval moves to a
Go-friendly embedded backend or separate search service.

### Python

Excellent ML/retrieval experimentation ecosystem and first-class LanceDB
support.

Not selected as the initial server runtime because embedding execution is
abstracted and the core architecture does not require Python-specific ML
libraries.

### SQLite + FTS5 + sqlite-vec

A credible embedded retrieval alternative and useful benchmark/fallback.

Not selected initially because LanceDB already provides an embedded
retrieval-oriented store with first-class Rust support. The `RetrievalIndex`
boundary deliberately keeps this option open.

## Consequences

### Positive

- one primary systems runtime
- no external retrieval database required
- transport-independent application logic
- local retrieval remains disposable and rebuildable
- embedding implementation remains replaceable
- workers/services can be split later if operational evidence requires it

### Tradeoffs

- Notion integration uses a thin typed Rust REST/OAuth adapter rather than the
  official JavaScript SDK
- Rust can carry a higher implementation cost than TypeScript
- LanceDB remains a technology choice to validate through later retrieval
  evaluation tickets

## Revisit when

Revisit this decision if a focused implementation spike finds a material blocker
in Rust's Notion/MCP/LanceDB stack, or if retrieval benchmarks show the embedded
LanceDB approach is materially worse than an alternative for the real corpus.

## Related work

- #15 bootstraps this workspace
- #17 and #18 add stdio and Streamable HTTP transports
- #21 defines the Notion backend contracts
- #36–#43 implement local state, LanceDB, and embeddings
- #94–#103 evaluate retrieval quality
