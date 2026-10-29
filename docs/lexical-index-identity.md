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
table, workspace, roots, scope policy,
physical directory device/inode, generation and epoch. `plan().sha256()` provides
payload-free identity for later evaluation provenance.

Every guarded table open checks the pinned metadata. Page preparation retains
that exact binding and checks it again after embedding. Commit checks prepared
and current binding under the coordinator fence, including after authoritative
source validation and before effects. Enabled maintenance uses the positional
profile; raw startup/commit paths cannot silently downgrade it. Original
citations and vector metadata are unchanged.

`LexicalIndexBinding::for_cache` only constructs canonical metadata bytes;
it does not write a file or authorize stamping an existing table. The former
sibling preparation, activation, and rollback framework has been removed.
`GuardedChunkTable::create_fresh_lexical` creates the configured FTS profile
and binding only when both the table and binding are absent, under the current
coordinator lease. It refuses existing rows rather than relabeling their vectors.
`LexicalCachePlan` retains the verified dictionary handle and its exact identity.
Payload format 2 removes the obsolete initial-source handoff digest. The artifact
filename stays stable so an older file is refused and reset instead of ignored.
The supported recovery is [cache reset and authoritative Notion resync](cache-reset-and-resync.md).
Legacy transition journals trigger a rebuild; they are never resumed.
`IndexRuntime::recover_lexical_cache` accepts the verified plan as trusted
configuration. Missing or incompatible cache metadata resets derived state and
resyncs the current authorized Notion scope. Operational credentials, external
effect ledgers and admitted refresh work remain intact. Permission and arbitrary
storage failures remain errors. Content reads are refused while rebuild is
pending; startup verifies the rebuilt search layout and probes a real cached
page ID through native FTS before admitting reads. Empty authorized caches are
valid. A healthy read-only reopen neither resyncs nor writes.

The runtime retains the same immutable dictionary handle and plan used to bind
its cache. This typed preparation seam does not install expansion in shipping
search (#492) or establish retrieval quality (#493).

Verification:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-lancedb --lib lexical_identity --locked --offline
nix develop .#spike --command cargo test -p notion-knowledge-server \
  --features webhook-worker --test reconciliation_command lexical_cache_ --locked --offline
```
