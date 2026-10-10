# Canonical page aliases for graph lookup (#64)

The derived graph alias catalog maps explicitly configured alternative names to
**canonical Notion page IDs**, never to invented graph nodes, chunk IDs or
out-of-scope content.

## Source configuration

Use `AliasSources::new(include_title, property_ids)` with stable normalized
Notion property IDs (not display labels). Extraction reads only
`IndexedMetadata.title` and the selected `PropertyValue::Text` and
`PropertyValue::Strings`. Unconfigured fields, relation ID arrays, person IDs,
dates, numbers and body Markdown are not alias sources. No properties are enabled
by default. There is an explicit cap on configured property IDs and aliases/page.

Names are matched by collapsing Unicode whitespace and applying Unicode
lowercase (e.g. `ÖSTRA SJÖN` and `östra sjön`). No fuzzy or substring matching,
transliteration or guessed entity identity. Empty/unsafe/oversized aliases fail
validation; blanks are skipped when extracting configured properties.

## Storage, scope and ambiguity

`SqliteSyncStateStore` implements `PageAliasStore`. The new schema migration
`0008_graph_aliases.sql` stores a materialized derived alias index alongside
the local graph. A lookup is always scoped to the *trusted* workspace ID and
physical allowed root page ID supplied by the caller. It returns a sorted
list of **distinct canonical page IDs**. A repeated name on several pages is
ambiguous by design; there is no automatic single-result selection.

The `replace_indexed_aliases(metadata, sources)` convenience method derives
the names from one already-normalized page and calls the atomic replacement
transaction. Replacements remove aliases that disappeared or changed; an
empty replacement removes the page's mappings (e.g. after a tombstone).
Malformed names fail before any DELETE. Old migration histories remain valid,
and an invalid/newer migration history still fails closed.

The derived catalog exposes no MCP operation, Notion write endpoint or new
user-visible graph tool. **Index/orchestrator call sites must use this port
only after establishing physical-ancestry scope**, and must invoke it on
incremental page updates and removals. Live graph navigation is handled in
separate issue #63, rather than silently exposing untrusted aliases here.

## Validation

`cargo test -p notion-knowledge-retrieval --test aliases` covers Swedish and
English names, exact/property source selection, multi-page ambiguity, scope
isolation, stale removal/tombstones, malformed inputs and SQLite reopen.
Core extraction and alias storage are provider-independent and rebuildable.
