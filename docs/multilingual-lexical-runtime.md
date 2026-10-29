# Opt-in bilingual lexical retrieval

The Unix `webhook-worker` build can explicitly enable bounded English/Swedish
literal dictionary expansion in configured retrieval. It defaults OFF. The
normal trusted workspace/root and retrieval/model settings remain required;
these options do not grant access or activate an unconfigured content backend.

| Setting | Value |
| --- | --- |
| `NK_LEXICAL_EXPANSION` | `true` or `false`; default `false` |
| `NK_LEXICAL_ASSET_BUNDLE` | Absolute path to a separately prepared local bundle |
| `NK_LEXICAL_ASSET_SHA256` | Exact lowercase bundle SHA256 from preparation |
| `NK_LEXICAL_MAX_REQUESTS` | Shared enabled-search admission, 1–16; default 1 |

Prepare the pinned public dictionary using [offline lexical assets](lexical-assets.md)
from the exact committed source, lockfile and pinned compiler used by the server:

```sh
nix develop .#spike --command cargo run -p notion-knowledge-retrieval \
  --example lexical-asset --locked -- \
  /absolute/path/freedict-eng-swe-2025.11.23.src.tar.xz \
  /absolute/path/new-lexical.bundle
```

The output path must not exist. Preserve the emitted JSON identity with the
bundle. It embeds original license and attribution: FreeDict/WikDict eng-swe
2025.11.23, Karl Bartel, Wiktionary via DBnary, CC BY-SA 3.0 Unported. Dictionary
data is separately distributed; nothing is downloaded during search. Keep
operator paths, credentials and approved scope configuration private.

For a container, mount the bundle read-only at an absolute path readable by
the existing unprivileged server user. Set the matching path/digest in private
deployment configuration. Prepare and consume the bundle with the same pinned
build; a different importer/compiler/source identity requires a new bundle.

Enabled startup validates assets before loading the model, then binds the
embedding, lexical-storage and plan identities. Integration-owned recovery uses
[cache reset and authoritative resync](cache-reset-and-resync.md). Incompatible
derived state is rebuilt from authorized Notion content; credentials, external
write/idempotency/audit records and admitted refresh work are preserved. Readers
refuse content while rebuilding. Read-only OAuth composition requires compatible
existing state; it does not acquire a global credential or initialize source
data outside its established authorization boundary.

Workers, manual/scheduled maintenance, index commands and status inspection use
the configured binding. Missing/corrupt assets, resource limits, deadlines or
incompatible state fail explicitly. Requested lexical/hybrid search does not
substitute vector-only or partial results. Healthy read-only reopen is a no-op.
Asset handles are immutable and shared for the process configuration; restart
with a new configuration to change assets.

One shared candidate reservation is acquired before model work/read fencing and
retained through merging. Each reservation covers the existing 96 MiB candidate
envelope. All enabled read modes share the pool. `NK_WEBHOOK_TIMEOUT_SECONDS`
supplies the whole search deadline (default 60, range 1–300 seconds). MCP
cancellation drops native search work without adding timeout/retry around
mutations. Hybrid rankers use one committed snapshot and existing RRF policy.
Canonical text, citations and raw exact-ID/phrase behavior are preserved; match
records retain public dictionary provenance.

Unsupported words/languages remain valid zero-expansion plans. Deterministic
planner limits still apply; complete linguistic coverage is not promised. Graph
boost remains separately default-off. Component tests do not establish retrieval
quality: production-evaluation wiring is #594 and unchanged complete-corpus
measurements and original gates remain #493.
