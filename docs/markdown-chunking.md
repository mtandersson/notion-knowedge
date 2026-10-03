# Heading-aware Markdown chunks

`notion-knowledge-core::chunking::chunk_document` consumes an `IndexedDocument`
and produces ordered `ChunkDraft`s. Each draft clones citation/provenance,
properties and edit metadata and updates the heading path (inherited document
path followed by Markdown ancestors). Heading levels may skip; a heading replaces
ancestors at the same or deeper level. ATX and Setext headings work; inline
formatting is rendered to heading text. Code and nested list headings do not
change the surrounding path.

Draft text is an exact contiguous source slice, with UTF-8 byte offsets in
`source_bytes`. Whitespace is retained. Empty/whitespace-only sections produce
no chunks. Short semantic sections stay together; chunks never cross heading
boundaries. Chunk drafts deliberately have no manufactured IDs or hashes:
#32 owns those, and callers can extract per-chunk links with the original offsets.
They are the input to the existing `IndexedChunk` record, not a new persisted
schema or a wired indexing pipeline.

`ChunkConfig` defaults to a 2000 Unicode scalar character target and at most
200 characters of overlap. This is deterministic and provider independent, not
an estimate of any embedding model's tokens. Zero targets or overlap greater
than/equal to target are rejected. Large ordinary prose paragraphs split on
whitespace; short sections are still packed intact. Complete trailing blocks or
prose segments may repeat within the overlap budget. Overlap can be zero if no
whole safe segment fits; it never crosses a section boundary and always advances.

The size target is soft. Lists (including nested items), fenced/indented code,
tables, blockquotes, paragraphs containing inline Markdown/HTML or potential block markers (including
ordered-list tokens), repeated spaces/tabs or explicit hard breaks, and unbroken
words are atomic when splitting would damage syntax. They can exceed the target.
HTML/enhanced-Markdown containers are conservatively preserved through the last
matching closing tag; incomplete containers retain the remaining tail intact.
This intentionally favors losslessness over target adherence and may group
multiple same-name containers. Heading-looking text inside those containers
is not treated as an outer Markdown heading. Model adapters must apply their
own token limits/oversize policy; this stage never silently truncates content.

Run `cargo test -p notion-knowledge-core --locked`, `cargo fmt --all -- --check`
and `cargo clippy -p notion-knowledge-core --all-targets --locked -- -D warnings`
in the pinned Nix shell. Tests cover headings and inheritance, long Unicode
prose, bounded overlap, exact offsets, deterministic output and intact
code/list/table/enhanced-container syntax.
