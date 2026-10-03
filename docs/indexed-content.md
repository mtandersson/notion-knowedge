# Canonical indexed content (schema 1)

`notion-knowledge-core::indexed` owns the shared cache contract for crawler,
chunker, embeddings and search. `IndexedDocument` is one normalized page;
`IndexedChunk` is one retrieval unit derived from that page. Both serialize to
JSON with an explicit `schema_version: "1"`. Unsupported or missing versions
are errors; storage readers must migrate explicitly or rebuild from Notion.
The schema version is independent of the application release version.

Both records carry `metadata` (page ID, optional block ID, citation URL, title,
ordered heading path, original Notion RFC 3339 `last_edited_time`, source
provenance and normalized properties), normalized `text`, `content_hash`, and
extracted `links`. Chunks additionally carry `chunk_id`. A page-level document
normally has no block ID and an empty heading path. A chunk retains page
metadata and identifies its source block when available; the heading path runs
from outermost to innermost heading. A chunk can therefore be cited or refreshed
without loading its document. Database and data-source IDs are optional for
pages outside databases; workspace/root IDs identify crawl provenance.

Properties are keyed by stable Notion property ID. Tagged `PropertyValue`
variants preserve null, text, number, checkbox, multi-value strings, date ranges,
page relations and people IDs. Formula/rollup values use their resolved value
type. Normalization determines supported property mappings; raw API objects and
credentials do not belong here. Link targets distinguish external URLs, pages
and blocks. Producers retain source order for links and headings. Unordered property
collections (multi-select, relations and people) use canonical sorted order; `BTreeMap` provides stable
property-key serialization.

These types describe data, not a validation or processing pipeline. Producers
must supply valid Notion IDs, URLs, timestamps, finite numeric values and
normalized text. Empty text and absent source-block/database metadata are
representable. Hash algorithm, canonical hash input, stable chunk-ID generation,
normalization and link extraction remain the responsibility of #30–#33; this
contract does not calculate them. The [heading-aware chunker](markdown-chunking.md)
produces lossless drafts with source offsets and citation metadata; IDs and
hashes remain a separate stage. Document hashes cover document content and
chunk hashes cover chunk content. Neither record contains vectors or storage
provider types. Additive optional fields can be compatible; changes to meanings,
required fields or tagged variants require an explicit schema migration/version.

Run contract tests with `cargo test -p notion-knowledge-core --locked` inside the
pinned `nix develop` shell. Tests cover the wire representation, Unicode/text,
typed properties, links, optional provenance, heading order and rejection of
unsupported versions.
