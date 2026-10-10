# Phase 3: authority-verified page preparation (#314)

This independently reusable adapter prepares the real canonical Notion side
of the production refresh pipeline. It implements the first child of #255
without lying about completing guarded incremental indexing. #315 must consume
it with the actual SQLite/LanceDB commit authority from #254.

## Contract

Call \`NotionClient::prepare_scoped_page(selected, root, scope, previous, config)\`
only from trusted server-side composition. The caller supplies:
- An explicit canonical page ID, plus a root ID contained in the **trusted**
  \`LifecycleScope\` associated with the Notion integration credential.
- Previously persisted \`IndexedChunk\` records for **that exact page/root and
  workspace** from the authoritative local index, not caller-supplied stale
  metadata. Corrupt previous IDs/content hashes are rejected.
- Explicit \`ChunkConfig\`. No unbounded discovery, relation traversal or
  helper-wide crawl is performed to authorize or refresh one selected page.

The adapter first calls \`RootScopeGate::authorize\` using the real
\`NotionClient\` lifecycle implementation. It verifies the selected page's
complete physical ancestry, configured root membership, exclusions and active
state BEFORE fetching body Markdown. Database/data-source IDs are taken only
from the verified ancestry. Then it fetches the complete exact authoritative
Notion Markdown and typed page metadata, runs:
\`IndexedDocument\` + \`extract_relationships\` + \`chunk_document\` +
\`identify_chunks(previous)\` + per-chunk link extraction.

The second complete Notion read must equal the first (title, properties,
timestamp, URL, active state AND Markdown). The original lifecycle proof
is revalidated with live authoritative physical metadata. Inaccessible,
excluded, moved or racing pages fail closed. Links and relation targets are
recorded as untrusted index edges, **never followed** for authority.

The result \`ScopedPageSnapshot\` contains a document and chunks. Metadata-only
changes propagate updated title/property/URL/citation fields and retain stable
content IDs. Removed sections are omitted from the prepared complete snapshot,
but **this adapter never removes or mutates index rows itself**.

## Remaining integration responsibility

This is a **prepared proposal, not a commit permit**. Notion does not expose
a transactional snapshot and the page can change after the last read. #315
must revalidate the source's revision and lifecycle (under #254's external
cross-process commit guard), prepare real provider-compatible vectors, upsert
only changed chunks, remove obsolete rows, checkpoint safely, and supply the
credential-free SQLite/LanceDB/embedding end-to-end integration harness.

To run focused tests (no external Notion credentials or model assets):

\`\`\`sh
cargo test -p notion-knowledge-notion prepare::tests --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
\`\`\`

No webhook dispatch, source mutations, deletion, embeddings, or production
scheduling are enabled by this PR.
