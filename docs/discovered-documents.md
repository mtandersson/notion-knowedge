# Scoped canonical document smoke

`NotionClient::discover_documents` assembles the phase-1 read path for one explicit
root and trusted workspace provenance. It completes `crawl_with_exclusions`
before fetching content, then consumes only the report's allowed IDs. Each exact
read normalizes properties and preserves enhanced Markdown, IDs, authoritative
URL and edit timestamp. Local relationship extraction does not follow references.
The heading-aware chunker and versioned fingerprints produce schema-1
`IndexedDocument` and `IndexedChunk` records with citation metadata. Chunk links
come from that chunk's text plus the page's relation properties; document links
come from the complete page. Exclusions never expand authority through links.

The returned in-memory snapshot is complete or fails without returning any
partial records. Discovery, missing/inaccessible reads, incomplete Markdown,
invalid chunk configuration and a page archived after discovery fail the run.
There is no index writer, embedding call, persistent checkpoint or MCP tool here;
those belong to subsequent epics. Treat a failed run as no new scope snapshot.
The method does not reconcile a previous chunk snapshot: unchanged sections have
stable fresh IDs, while cross-edit reconciliation remains available through
`identify_chunks` in the owning downstream consumer.

Notion does not provide an atomic snapshot: content can move or change between
crawl and exact read. This smoke inherits the crawler's concurrency limitations;
repeat discovery to reconcile the current scope. Workspace ID is supplied trusted
provenance, not independently verified by this method. Database/data-source and
source-block IDs are absent rather than guessed from incomplete discovery data.
Multiple roots can be run separately; resolving overlapping-root provenance is
not part of this one-root contract. JSON includes private page content and URLs,
so store output only in an appropriate private location.

## Runnable read-only smoke

Supply `NOTION_TOKEN` through the secret process environment. Do not put credentials
in arguments or committed fixtures. In the pinned `nix develop` shell, run:

```sh
cargo run -p notion-knowledge-notion --example discover_documents -- \
  ROOT_PAGE_ID WORKSPACE_ID EXCLUDED_PAGE_ID
```

The exclusion argument is optional and repeatable. Successful output contains
`discovery`, `documents`, and `chunks`. Check that excluded IDs appear only in
skip records, schema versions are `1`, source root/workspace and edit timestamps
are retained, nested heading paths are ordered, and links never add documents.
Rerun unchanged source and compare content hashes and chunk IDs. No live Notion
credential run is required by ordinary CI or claimed by this fixture smoke.

## Automated integrated smoke

```sh
cargo test -p notion-knowledge-notion scoped_snapshot --locked
```

The test drives the actual assembled method against a strict HTTP fixture with
root metadata, an excluded child, enhanced Markdown, nested headings, an external
link, a page mention and a relation to an out-of-scope page. It allows only four
requests per snapshot: root discovery, root block listing, exact metadata and
Markdown. An excluded/linked target read fails the test. It verifies normalized
properties, links, timestamps, root/workspace provenance, canonical wire
roundtrips, per-chunk link locality and stable hashes/IDs across timestamp-only
rereads. Component suites additionally cover pagination, ancestry failures,
exclusion precedence, property completeness, unsupported Markdown and chunk edits.

## Epic #4 acceptance map

All seven implementation tickets are required, together with the integrated smoke:

| Ticket | Delivered contract exercised by the snapshot |
| --- | --- |
| #28 | Schema-1 document/chunk serialization and citation metadata |
| #29 | Configured-root physical discovery before content reads |
| #30 | Typed property values keyed by stable property ID |
| #31 | Nested heading-aware chunk drafts and per-chunk citations |
| #32 | Document/chunk content hashes and stable fresh chunk identities |
| #33 | External links, page mentions and relation targets without traversal |
| #35 | Excluded pages absent from content reads and canonical records |

This is the phase-1 canonical-source boundary in parent #1. Local index creation,
embedding, sync and semantic MCP retrieval remain their own implementation epics.
