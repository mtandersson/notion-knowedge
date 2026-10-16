# Installed canonical aliases

The shared `IndexRuntime` installs canonical alias projection for authoritative
webhook, manual and periodic refresh/reconciliation. Aliases are derived local
names for canonical Notion page IDs; they grant no authority to read content.
There is no unscoped alias MCP tool.

## Startup sources

Index settings read these immutable operator variables once:

| Setting | Default | Meaning |
| --- | --- | --- |
| `NK_ALIAS_TITLE` | `true` | Include page titles; accepts exactly `true` or `false`. |
| `NK_ALIAS_PROPERTY_IDS` | unset (no properties) | JSON array of stable normalized Notion property IDs, for example `["alternate-names"]`. Display labels are not IDs. |

At least one source is required. At most 32 property entries of at most 128
bytes each are accepted, using `AliasSources` identifier validation. Duplicate
IDs are canonicalized; ordering does not change configuration identity.
Only configured text/string values are extracted. Bodies, relation IDs, numeric
values, people and unselected properties never become names. Extraction accepts
at most 128 aliases per page, each with at most 256 normalized characters.
Swedish letters are preserved; whitespace folds and Unicode lowercase matches
names. Empty or unsafe queries fail instead of matching everything.

These settings accompany `NK_WEBHOOK_INDEX_DIR` and `NK_WEBHOOK_QWEN_ASSETS` in
all installed indexing and retrieval configurations. Neither webhook hints nor
MCP requests can select or broaden sources, workspace or roots.

## Source changes and repair

Every fenced page commit installs exact aliases, relation projection, checkpoint
and receipt in the same final SQLite transaction. Current source/policy authority
remains held through acknowledgment. Alias-only changes require refresh even
when the body, chunks and edit timestamp are unchanged. Configuration identity
includes absent configured fields, so adding an absent field also requires a
projection update. Empty live pages and tombstones have explicit completeness
markers. Physical root transitions clean old aliases within the binding's owned
roots; unrelated workspace/root namespaces and graph families survive.

After changing configured sources, restart indexing callers with the same new
settings and explicitly reconcile existing state:

```sh
notion-knowledge-server --reconcile dry-run
notion-knowledge-server --reconcile rebuild-aliases alias-source-change-1
notion-knowledge-server --reconcile status alias-source-change-1
```

Use a fresh run ID. `rebuild-aliases` uses the existing owned maintenance guard,
writer lease and Inventory transaction to invalidate only alias completeness;
then normal authoritative reconciliation repopulates unchanged and empty pages.
It retains alias rows until each replacement commits, relations, links, immutable
receipts, webhook inbox and scheduling history. A normal new `apply` run also
recognizes changed configuration. Never delete SQLite wholesale. Stop other
callers or deploy their configuration together to avoid alternating policies.

Read-only open, startup, status and dry-run never migrate, invalidate or repair
state. Existing incompatible schemas fail closed; explicit writable
initialization performs supported migration. Pending or Failed work remains
retryable through the existing resume/retry workflow. Alias persistence failure
rolls back SQLite acknowledgment even if Lance effects already occurred.
Historical Applied receipts remain immutable; repairs receive distinct durable
IDs, and incomplete repairs replay before a reverted-source no-op can succeed.

## Trusted local lookup

A trusted local composition consumer calls `IndexRuntime::lookup_alias(name)`.
It binds workspace and allowed roots from validated runtime authority and reuses
`PageAliasStore` to return sorted distinct canonical page IDs across those roots.
Ambiguity returns every candidate; callers must select explicitly and still pass
normal authoritative content access checks. No title match grants access.

Lookup holds the current coordinator read guard, rejects pending/failed commit
effects and verifies exact current alias rows/configuration for every owned page.
A changed configuration or missing completeness marker returns a repair error,
including queries with no candidates. Lookup does not write or initialize state.
Standalone `PageAliasStore` remains a component port, not an authorization gate.

## Installed acceptance evidence

`crates/server/tests/reconciliation_command.rs` exercises the actual shared
`IndexRuntime`, real Notion HTTP fixtures and committed SQLite/LanceDB state:

- `installed_aliases_preserve_ambiguity_explicit_sources_rebuild_and_restart`
  covers Swedish/English names, repeated names, explicit property selection,
  alias-only edits, configuration-only changes, empty projections, removal,
  namespace isolation, read-only snapshots, explicit rebuild and reopen.
- `installed_alias_persistence_failure_replays_durable_repair_without_mutating_history`
  injects a real SQLite alias insertion failure after vector work, rejects lookup,
  reopens and replays the same durable repair after source reversion, preserves
  the original Applied receipt, and repairs changed configuration on replay.
- `actual_scheduled_http_manual_periodic_and_webhook_contention_preserves_retry_budgets`
  proves alias updates through actual supervised periodic, webhook and manual
  composition while retaining coordination/retry budgets.

These cover original #376 criteria: immutable explicit sources; joined fenced
lifecycle and no-op/config repair; atomic stale removal with retryable failure;
trusted scoped ambiguous lookup; committed bilingual/update/removal/replay/restart
behavior and operator documentation. Existing authoritative lifecycle and owned
alias-effect tests cover current policy/lease/generation, tombstone recovery and
final transaction failure. Parent #376 requires a separate final acceptance audit.
