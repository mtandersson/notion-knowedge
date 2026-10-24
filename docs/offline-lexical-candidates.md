# Offline lexical candidate preparation

The `lexical-candidate` operator example prepares and verifies a **sibling**
derived index and can seal a Prepared activation journal. It never activates a generation, changes coordinator authority,
renames the active directory, calls Notion, or loads credentials. Activation,
interrupted-transition reconciliation and rollback remain #504. Production
installation and complete quality measurement remain #492/#493.

Stop the serving process and every index/operational writer before running it.
The operation obtains the existing physical directory's exclusive advisory lock
and an exclusive SQLite transaction for its entire lifetime. New guarded reads
and writes cannot enter; live reconciliation or held-file leases reject the
operation. Operational SQLite and its recovery backup must be outside both disposable
derived directories, with private owner directories. The operator opens only
existing authority; missing state is never initialized. The old derived index stays in place. Grant stores,
write/idempotency ledgers, audits and source effects are never reset or replayed.
Acknowledged pending refresh work remains byte-identical in the operational DB.

Preparation retains a create-new 0600 SQLite backup, verifies its complete digest
and SQLite integrity under the exclusive rollback-journal transaction, and only
then creates a private sibling directory. WAL databases are rejected by this
backup protocol. Existing outputs are never overwritten. Reads and preparation
fail closed on physical authority or schema/vector incompatibility; use the
explicit Notion-authoritative normal rebuild/re-embedding path in that case.
Changing persisted identity fields to force compatibility is not a rebuild.

The derived directory is copied with regular-file/no-link checks and bounded
streaming. Before any full-row query, a bounded native length/null metadata
projection checks the conservative UTF-8/vector/row envelope and total row cap. Every source and copied row is validated against the same workspace,
roots, schema and embedding identity. Complete typed field bytes, original JSON
source fields and exact f32 vector bits are compared by an order-independent
multiset digest before and after rebuilding all four native FTS indexes. All
stable page/chunk IDs receive native exact-ID probes. Verification compares the
actual persisted native tokenizer/position/filter settings against the same
approved parameter builder used to create the indexes. All copied and rebuilt
files and directories are synced before publishing canonical binding and ready
metadata; the sibling parent is synced before preparation reports success. Source and operational identities are revalidated before publication.

The default limits are 100,000 filesystem entries, 32 directory levels, 128 GiB
of files, one million rows, 16 MiB per returned Arrow batch, and 256 MiB of
conservatively charged retained chunk identifiers. Limit exhaustion fails the
whole preparation; it never publishes a partial ready generation. These are
trusted offline bounds, independent of installed per-request retrieval budgets.

`OfflineLexicalSource` is a source-only migration handle. It can read a prior
plan/storage implementation digest only when the known schema, backend,
tokenizer profile, embedding text contract, workspace/roots and physical
coordinator generation remain compatible. It never executes the old plan and
cannot serve searches or commit page writes. Normal runtime binding remains
exact and rejects that older plan. New asset/plan versions follow the same
candidate workflow; the new asset must load as an immutable verified bundle.

## Operator commands

Build the importer and candidate tool from the same checkout revision. Prepare
and pin the public dictionary bundle as described in [lexical assets](lexical-assets.md).
Use its actual SHA-256, and supply the existing trusted source identity in a
local configuration file. The configured embedding identity must exactly match
the table; no embedding provider runs during copying.

```json
{
  "index_directory": "/private/nk/index",
  "state_file": "/private/nk/operational.sqlite",
  "table": "chunks",
  "workspace": "configured-workspace-id",
  "roots": ["configured-root-id"],
  "exclusions": [],
  "policy": "existing-policy-identity",
  "scope_generation": "existing-scope-generation",
  "generation": "existing-index-generation",
  "epoch": 1,
  "next_generation": "lexical-candidate-2",
  "next_epoch": 3,
  "embedding": {
    "schema_version": "1",
    "provider_id": "existing-provider",
    "model_id": "existing-model",
    "model_version": "existing-model-version",
    "dimension": 1024
  },
  "asset": "/private/nk/eng-swe.bundle",
  "asset_sha256": "replace-with-the-verified-bundle-sha256",
  "candidate": "/private/nk/lexical-candidate-2",
  "backup": "/private/nk/operational-before-lexical-2.sqlite",
  "activation": {
    "archive": "/private/nk/index-before-lexical-2",
    "backup": "/private/nk/operational-at-activation-2.sqlite",
    "quarantine_generation": "lexical-quarantine-2"
  }
}
```

The candidate must be a new sibling of the active derived directory. Choose a
fresh, never-used next generation and larger epoch. The configuration is local
operator input and is never accepted from an MCP request.
Preparing an activation journal requires a next epoch at least two greater than
the current epoch, leaving the intervening epoch for quarantine. Candidate-only
commands also accept configurations without the optional `activation` object.

```sh
nix develop .#spike --command cargo run -p notion-knowledge-retrieval \
  --features local-lancedb --example lexical-candidate --locked --offline -- \
  prepare /private/nk/lexical-preparation.json

nix develop .#spike --command cargo run -p notion-knowledge-retrieval \
  --features local-lancedb --example lexical-candidate --locked --offline -- \
  verify /private/nk/lexical-preparation.json
```

`verify` is read-only under offline ownership: it reopens the ready marker,
checks canonical binding/plan/physical authority and the actual retained backup,
reopens the complete source/candidate rows and indexes, compares every vector and
citation field again, and rejects changed/missing/corrupt state. A ready marker
alone is not activation permission. Activation must revalidate under its own
fence before admission; a successful earlier verification is not that fence.
Output contains local operational metadata and digests, not source text or credentials.

### Prepare an activation journal

After candidate preparation and verification, keep every server, webhook,
reconciliation, file and mutation process stopped. Make the configuration private
and explicitly acknowledge that shutdown:

```sh
chmod 600 /private/nk/lexical-preparation.json
nix develop .#spike --command cargo run -p notion-knowledge-retrieval \
  --features local-lancedb --example lexical-candidate --locked --offline -- \
  prepare-activation /private/nk/lexical-preparation.json \
  --all-source-processes-stopped
```

The tool requires an operator-owned, singly linked regular configuration file
with no group/other permissions. It re-verifies the sibling, creates a fresh
activation-time SQLite backup capped at 2 GiB, seals the complete SQL manifest,
retains any prior completed transition history, and validates retained derived
work. The activation backup and unused archive paths are distinct from the
candidate, preparation backup and operational state. Invalid containment or
archive aliases fail before a backup is created.

Success leaves a durable **Prepared** journal and the unchanged source remains
admissible. This command does not quarantine, rename or activate a generation.
Failures retain artifacts; do not edit a journal or restore a backup to force a
retry. Activation, explicit interrupted-transition reconciliation and rollback
commands remain incomplete under #504, including rollback-compatible artifact
verification. See the [transition protocol](design/lexical-generation-transition.md).

## Interrupted preparation

Missing ready metadata means incomplete preparation. Keep the old index and
retained backup; no new generation has been adopted. Inspect the incomplete
sibling and retry with fresh candidate/backup names after recovering ownership.
The tool never removes or overwrites recovery copies or incomplete siblings.
Do not rename a candidate over the active directory or edit the coordinator
binding manually. A corrupt ready candidate also remains unavailable; verification
never repairs it or silently uses another generation.

Real process tests terminate preparation before copy, after copy, and immediately
before ready publication. They verify unchanged SQLite/backup bytes, rejection
of the incomplete candidate, continued old-generation reads, and successful
fresh preparation/verification. Separate process restart tests reopen completed
candidates. Corruption tests cover missing/noncanonical ready metadata, changed
assets/backups and actual vector mutation.

```sh
nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --features local-lancedb --lib offline_candidate --locked --offline
nix develop .#spike --command cargo test -p notion-knowledge-retrieval \
  --test index_commits offline_backup --locked --offline
```
