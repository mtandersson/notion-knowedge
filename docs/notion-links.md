# Relationship normalization

`notion_knowledge_notion::links::extract_relationships(&PageContent)` is a
pure normalization step over the authoritative content returned by
`read_content`. It leaves the Markdown and properties unchanged and makes no
requests. Index consumers can copy `Relationships.links` into the existing
canonical indexed document/chunk link field. `references` preserves every
occurrence, its raw target, resolution diagnostic and source: a byte range in
original Markdown or stable relation property ID and element position. The
containing page supplies the source-page identity. Duplicate occurrences remain
inspectable while `links` contains unique targets in first-occurrence order.

CommonMark inline/reference links, autolinks and images are parsed rather than
matched with regular expressions. Notion's enhanced `<mention-page url="…">`
and `<page url="…">` tags also resolve page targets, including self-closing
forms and decoded XML entities. Notion wrapper children and leading tabs are
handled in a temporary parser view with original byte offsets; fenced/inline
code and escaped markers remain literal. No rewritten text is persisted.

HTTPS Notion links resolve to lowercase hyphenated page IDs using the same
strict authority/ID policy as metadata reads. UUID block fragments produce
page/block targets. HTTP/HTTPS links on other hosts remain external, including
lookalike `notion.so.evil.test` hosts. Invalid Notion IDs/fragments, relative
URLs, unsupported schemes and missing/malformed enhanced URL attributes remain
unresolved with `InvalidTarget`. Resolution is syntactic: a well-formed ID
makes no claim about target existence or accessibility. No linked object is
fetched and no link expands crawler authorization.

`PageIds` properties produce page edges with relation provenance. Other
property variants do not produce relation edges. Database/data-source/user/
agent mentions (and child databases) remain recorded with `UnsupportedMention`:
the current canonical `LinkTarget` contract has only external, page and block
variants, so pretending a database or person is a page would corrupt a graph.
Date mentions are scalar values, not object references. Arbitrary HTML embeds,
synced-block references and unknown-block markers are not interpreted as page
edges. Graph storage, index orchestration and target-access checks belong to
later work.

Tests: `cargo test -p notion-knowledge-notion --locked`. They cover canonical
page/block IDs, external and deceptive authorities, reference links/autolinks,
code/escapes, nested enhanced tags, entities, malformed/unresolved targets,
duplicate occurrences, deterministic output and relation provenance. No live
Notion credentials are used.

Syntax source: [Notion enhanced Markdown](https://developers.notion.com/guides/data-apis/enhanced-markdown).
