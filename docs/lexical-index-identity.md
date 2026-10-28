# Lexical index identity

The opt-in guarded lexical API requires `.nk-lexical-binding-v1.json` in the
coordinator-owned derived index directory. This is trusted operator metadata,
never a tool input. `GuardedChunkTable::bind_with_lexical_plan` requires a plan
constructed from a verified immutable dictionary. Missing, noncanonical,
unknown-field, corrupt, linked, oversized, or mismatched metadata fails closed.
The raw binding constructor rejects an installed lexical binding. No search or
open creates or repairs this file.

The canonical binding separates unchanged embedding/vector metadata from
lexical storage and query-plan identity. Storage includes the schema, four
field/index names, Swedish positional profile, raw identifier semantics, pinned
backend versions and storage implementation digest. Plan identity binds the
verified asset digest, importer/build/Unicode identity, planner/executor source,
lockfile, limits, bypass, score and tie behavior. The binding also retains the
verified initial source snapshot digest, table, workspace, roots, scope policy,
physical directory device/inode, generation and epoch. `plan().sha256()` provides
payload-free identity for later evaluation provenance.

Every guarded table open checks the pinned metadata. Page preparation retains
that exact binding and checks it again after embedding. Commit checks prepared
and current binding under the coordinator fence, including after authoritative
source validation and before effects. Enabled maintenance uses the positional
profile; raw startup/commit paths cannot silently downgrade it. Original
citations and vector metadata are unchanged.

`LexicalIndexBinding::for_candidate` only constructs canonical metadata bytes;
it does not write a file or authorize stamping an existing table. Historical #503 implemented offline sibling preparation with exact row/vector
verification. That is an existing component, not the required recovery procedure.
The supported replacement is single-process cache reset and authoritative
Notion resync (#577), with obsolete transition removal in #578. Historical
#504 activation/rollback requirements are superseded. The existing component
checks do not prove that complete reset/resync is implemented.
These primitives do not install expansion in the server (#492), automatically
migrate existing state, or establish retrieval quality (#493).

Verification:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-lancedb --lib lexical_identity --locked --offline
```
