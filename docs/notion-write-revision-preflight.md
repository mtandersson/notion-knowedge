# Fresh write-revision preflight (#301)

The provider-independent notion_knowledge_core::revision module compares a
caller-supplied expected revision with **fresh authoritative** NotionRead data.
It does not use the local search index, perform writes, grant authorization or
enable any production MCP write tool.

A caller must supply at least one precondition:

- last_edited_time: RFC3339 timestamp; compare parsed instants so equivalent
  offsets or fractional-second formats match.
- markdown_sha256: 64-character hex SHA-256 of the **exact UTF-8 Markdown**
  returned by an authoritative read. This is deliberately not the normalized
  search/content fingerprint. SHA-256 is not a secrecy mechanism.

If both conditions are given, both must match. With timestamp only, the guard
reads current metadata; a content hash requires a fresh full-content read.
Missing/malformed conditions, inaccessible or malformed upstream data, wrong
page identity and archived pages all fail closed. A revision mismatch returns
only page ID and current last-edited timestamp; the current Markdown and title
are not included in the conflict payload.

The caller must explicitly re-fetch the current source, reconcile the
requested edit, and re-submit a new precondition. **Do not silently retry**
non-idempotent writes, and do not expose a success receipt solely because
preflight passed.

## Limitation

The guard is **not atomic** with a later Notion PATCH. In particular, a writer
could change a page between fresh preflight and mutation. As of this feature,
the repository has not established an upstream conditional-write/CAS primitive.
Do not advertise race-free edits or enable production write tools using only
this guard. Integration and focused concurrent-edit tests are tracked in #302
alongside #78 and #79, and read-back/idempotency remain separately required.

## Checks

Run cargo test -p notion-knowledge-core --test revision --locked, ideally
inside nix develop, plus the core crate's broader tests before merge.
