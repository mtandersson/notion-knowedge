# Isolated Notion read/index/search smoke (#555)

`scripts/smoke-notion-read.py` is an opt-in operator harness for the shipping
Unix `notion-knowledge-server` built with `webhook-worker`. It reuses production
manual reconciliation, index maintenance and stdio MCP semantic search/get.
It does not use the experimental Qwen/Lance spike executable or create a second
retrieval implementation. The #211/#216 read/index/search proof motivates these
checks; its development evidence does not establish this production acceptance.

## Prepare a separate disposable root

Use Python 3.11+ and the pinned build environment:

```sh
nix develop .#spike --command cargo build --locked \
  -p notion-knowledge-server --features webhook-worker
python3 scripts/test-smoke-notion-read.py
```

The credential-free tests run subprocess CLI and newline JSON-RPC fixtures to
check orchestration, confinement, source verification and failure retention.
They do not claim real Notion, neural inference or adapter integration evidence.
Existing [production container smoke](container.md) and
[reconciliation tests](manual-reconciliation.md) cover the real adapters with
local provider fixtures; an explicitly authorized real run remains #557/#110.

Select one operator-owned, non-sensitive disposable Notion page as its own
physical root. Put a distinctive synthetic ASCII marker (8–256 characters,
letters/digits/spaces/hyphens/underscores) in its small body, within one search
chunk. Keep this subtree small: reconciliation discovers and indexes the
entire physical subtree, while search/get acceptance targets the root page.
The harness never creates or edits the page. Only integration read permission
is necessary. Keep its token in local `NOTION_TOKEN`, outside command arguments,
Git and shell transcripts. The workspace UUID is trusted operator configuration,
not an independently verified workspace identity.

Provision a **separate** private index/state using the existing
[production worker initialization](container.md) under precisely this one root,
workspace and scope generation 1, then stop competing writers. Initialization
requires that documented worker configuration and is an explicit operator step.
The harness cannot bootstrap missing storage: production manual reconciliation
deliberately opens existing compatible authority. Do not copy or reuse another
root's production state to satisfy this prerequisite.

Use this layout beneath an operator-controlled mode-0700 directory:

```text
/absolute/private-smoke/data/                  0700
  index/                                      0700 (initialized compatible index)
  state/                                      0700
    state.sqlite                              0600 (initialized current schema)
/absolute/private-smoke/evidence/              0700 (existing private parent)
/absolute/separate-model-assets/               externally provisioned pinned assets
```

Ancestors must remain operator controlled, as required by the production storage
guard. The harness additionally refuses symlinked paths, public/group-accessible
state, overlapping model/data directories and diagnostics inside derived state.
It overrides inherited `NK_*` configuration with exactly one root, generation 1,
read-only tools, graph disabled and retrieval enabled. It starts no HTTP listener,
webhook worker, mutation runtime or model download.

## Explicit read-only run

Use the canonical hyphenated UUID in both root arguments. `--diagnostics` must
name a **new** directory beneath the existing private evidence parent. The
explicit repeated root is a safeguard against an accidental target, not an
independent proof that the page is disposable.

```sh
python3 scripts/smoke-notion-read.py \
  --binary /absolute/checkout/target/debug/notion-knowledge-server \
  --assets /absolute/separate-model-assets \
  --data /absolute/private-smoke/data \
  --diagnostics /absolute/private-smoke/evidence/run-1 \
  --root ROOT_UUID --confirm-disposable-root ROOT_UUID \
  --workspace TRUSTED_WORKSPACE_UUID \
  --marker 'NK555 synthetic saffron ravioli'
```

The harness creates a unique reconciliation run ID, applies a fresh authoritative
inventory to **derived local storage**, requires completed reconciliation with
zero errors/pending work, and runs `--index ensure`. It then initializes the
shipping stdio MCP and requires semantic search to return the selected page,
its marker, a finite score and its canonical Notion source link. Both indexed
and fresh `knowledge_get` must return the marker and matching page/root/link
provenance. Exit zero means these read-only checks passed. It does not establish
write/upload or cleanup acceptance.

## Retention and recovery

`result.json` is retained mode 0600 beneath a mode-0700 diagnostic directory.
It contains phase/outcome, a bounded static failure description, the known
reconciliation run ID, verified chunk identity on success, root/workspace IDs
and SHA-256 identities of the exact executable and three supplied model assets.
It contains no marker, source text, token or raw upstream output. These IDs are
still private operational metadata. Keep the matching code revision/build
record with the executable digest when collecting live evidence. Model hashes
identify supplied bytes; the production provider separately enforces its pinned
asset manifest. Output and diagnostics from child processes are never copied
into retained evidence.

A failed run leaves its diagnostic manifest and all derived state intact.
Configuration rejection before diagnostics creation emits only a bounded generic
error. Inspect the private environment and production diagnostics separately;
never publish raw logs or tokens. Use the recorded run ID with the documented
`--reconcile status`/`resume` procedure for interrupted reconciliation. The
harness never resumes/retries automatically or discards durable receipts. Start
a deliberate fresh run with a new diagnostic path only after reconciliation.

No cleanup runs automatically. Retain the named smoke data and evidence until
investigation is complete. An operator may deliberately remove only this known
disposable **derived** directory after stopping its processes; keep model assets,
other runtime state and authoritative Notion content. Live source cleanup and
any later write/upload approval belong to #556/#557, with #110 remaining open.
