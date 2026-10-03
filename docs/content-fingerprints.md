# Content fingerprints and chunk identity (version 1)

The core `fingerprint` module finalizes heading-aware drafts into schema-1
`IndexedChunk`s. Call `identify_chunks(drafts, previous_snapshot)` for each page,
then attach per-chunk links in the extraction stage (#33). It retains original
text and citation metadata; it does not embed, persist, or compare model versions.
Compare `content_hash` to reuse embeddings even when citation timestamps change.
`content_hash` also accepts document text for document-level fingerprints.
Chunk hashes additionally frame each inherited heading component, so changed
searchable context invalidates embeddings even when body text is unchanged.

Hashes are SHA-256 with the literal domain `nk-content-v1` followed by each
UTF-8 input's unsigned 64-bit big-endian byte length and bytes. Content canonicalization
converts CRLF/lone CR to LF and trims boundary newlines (section separators). It preserves other whitespace, case, Unicode,
Markdown, link labels and semantic URLs. For HTTP(S) URL spans delimited by
whitespace or Markdown/HTML quotes/brackets, recognized AWS SigV4 and Google
V4 signatures remove only their credential/signature/date/expiry/algorithm/
signed-header query fields (and AWS security token). This applies only when
`X-Amz-Signature` or `X-Goog-Signature` is present. Other query parameters,
path, host and fragment remain byte-for-byte, including retained query percent
encoding and plus signs. Recognized parameter names are literal ASCII names
compared case-insensitively; encoded names are conservatively retained. Other signing schemes are deliberately not
stripped: extending this allowlist requires versioning and tests. No blanket
query removal or whitespace collapsing can silently discard searchable text.
Canonicalization is fingerprint input only, not credential redaction of stored
text; upstream normalization and logging must enforce their security contracts.

Initial IDs use `nk-chunk-v1` and the same length-framed SHA-256 encoding of page
ID, optional block ID (empty when absent), ordered heading components, and
content hash. Identical duplicates get a local numeric suffix. Source offsets,
page-global positions, timestamps, title and properties are excluded. Distinct
unchanged sections therefore keep IDs even on a fresh snapshot after insertions.

Reconciliation groups by page/block/heading path. It first reuses previous IDs
for exact fingerprints (source order breaks duplicate ties). If exactly one old
and one new draft remain in a group, it reuses the old ID for an edit. Otherwise
new unmatched drafts get fresh IDs, reserving all previous IDs to avoid collisions.
Removed chunks are omitted. Pass only the immediately preceding snapshot with
unique IDs produced by this algorithm, not a history of snapshots. Duplicate IDs,
unknown ID prefixes, and hashes inconsistent with text/heading context return
`InvalidPreviousSnapshot`; rebuild rather than silently mixing versions.

Without stable per-section source block IDs, two identical repeated headings and
contents are indistinguishable; which duplicate survived cannot be established.
A singleton replacement under the same heading is treated as an edit. Multiple
simultaneously changed drafts cannot be reliably paired and get new identities.
Heading renames, moving between heading parents, and re-chunking that changes
content boundaries can change IDs. Exact unaffected drafts still match. These
are explicit limits of the available draft contract, not offset-based guesses.
Embedding model/config changes require independent invalidation in their owning
stage. Changes to canonicalization or framing require new algorithm prefixes
and explicit rebuild/migration; the persisted schema version is separate.

Verification: `cargo test -p notion-knowledge-core --locked`, formatting and
Clippy in the pinned Nix environment. Tests exercise unchanged/edit/insert/remove
runs, duplicate ambiguity, Unicode/line endings, metadata independence, signed
credential rotation, and semantic URL/label/content changes.
