# Revision-aware write integration and explicit conflict retry (#80 / #302)

The opt-in core `append_once` and `update_section_once` workflows accept
optional expected `last_edited_time` (valid RFC3339 instant) and/or expected
`markdown_sha256` (64 hexadecimal characters of the **exact fresh Markdown
bytes**). At least one is mandatory, and all supplied conditions must match.
Hash-only, time-only and combined conditions work for both workflows.

Flow: validate caller input -> authorize page ancestry -> reserve durable
unique intent/key -> fresh authoritative revision read -> full Markdown read
(and unique section selection if applicable) -> revalidate root scope ->
fresh immediate full-content revision comparison -> persist **uncertain** ->
single non-retried provider PATCH -> authoritative read-back -> verified
receipt or unknown. The final pre-PATCH read also compares the original
Markdown snapshot, so same-timestamp concurrent edits cannot pass silently.

A mismatch returns a typed `RevisionConflict(RevisionMetadata)` with only
the canonical page ID and current authoritative last-edited time, not page
Markdown, title, properties, upstream error text or credentials. Invalid
caller revisions and unverifiable upstream reads fail closed.

**Caller retry:** after an ordinary preflight conflict, no mutation was sent.
However, the original durable reservation is NOT automatically rearmed.
Explicitly re-fetch current source, review intended changes, supply a fresh
revision and use a new idempotency key for a consciously resubmitted intent.
The original key remains blocked. If a PATCH was dispatched, timeouts,
unexpected receipts and read-back failures are OutcomeUnknown and require
explicit reconciliation before any fresh attempt. There are no automatic
network retries.

The preview-only MCP semantic write schema documents `anyOf` timestamp or
hash and refuses a missing precondition, but production tool discovery stays
disabled, and no additional mutation route is exposed.

**Limit:** this is optimistic read/compare *before* the write, **not atomic
compare-and-swap** at Notion. An edit may race after the last fresh read but
before PATCH. No unsupported If-Match/conditional Notion mutation is assumed.
Read-back and durable idempotency make detectable outcomes explicit but do
not make competing Notion clients transactional. Do not claim linearizability.

Tests cover hash-only/time-only, missing/invalid preconditions, structured
conflicts, same-timestamp content changes, uncommitted key replay rejection,
caller-driven refresh with a new key, authority failure, exactly-one PATCH,
unknown outcomes and read-back correctness. Run:

    cargo test -p notion-knowledge-core --test append --test section_update --test revision --locked
    cargo test -p notion-knowledge-mcp --test write_catalog --locked

Do not enable production writes solely because these tests pass.
