# Manual refresh and disposable generation experiment (#215)

Notion remains authoritative. The isolated executable performs an explicit full
read and embedding of the same selected page into a **new index directory**.
`refresh-notion` validates the old generation's sidecar and table model/schema
identities before source reads or model execution. Incompatible identity requires
an explicit `create-notion` rebuild to a new path; it never mixes vectors. The
new path must not exist, so repeated refreshes replace the complete corpus by
new generations rather than append rows. The old generation remains available
if discovery, model execution or writing the new generation fails.

This experiment has no automatic path switch. Stop the old MCP process and
restart with the newly verified path. An already-open serving process continues
to use its old generation; a source edit alone cannot refresh derived data.
Do not serve an incomplete new directory. A successful create/refresh writes the
identity sidecar and reopens the table to verify its row count, unique canonical
chunk IDs and model/schema compatibility. `inspect-index` repeats these checks
without loading model assets, printing only counts and a SHA256 of sorted
canonical records. `verify-text INDEX ABSENT_TEXT PRESENT_TEXT` additionally
checks every stored canonical chunk for absence of the old distinctive phrase
and requires the new marker in at least one chunk, printing only booleans and
the same corpus digest. Equal hashes prove the same records, not numerical identity
of inference outputs or a production backup guarantee.

## Commands

Build with the matched pinned `nix develop .#spike` toolchain, as documented in
[Step 3](semantic-mcp-spike.md). Supply the authorized integration environment
and trusted workspace identity locally for source reads. Keep tokens, raw source
responses, logs and index directories outside Git. No token is required to
inspect or search a persisted generation.

```sh
exe=crates/retrieval/spikes/qwen-lance/target/debug/qwen-lance-spike
"$exe" inspect-index /absolute/original-index
"$exe" query-page /absolute/model-assets /absolute/original-index \
  'Vilken organisation var H. Norman Schwarzkopf Jr. chef för mellan 1988 och 1991?'
# After the explicitly approved manual source edit, search the old path again:
"$exe" query-page /absolute/model-assets /absolute/original-index \
  'Vilken organisation var H. Norman Schwarzkopf Jr. chef för mellan 1988 och 1991?'
"$exe" refresh-notion /absolute/model-assets /absolute/original-index \
  /absolute/refresh-1 ROOT_PAGE_ID SELECTED_PAGE_ID TRUSTED_WORKSPACE_ID
"$exe" query-page /absolute/model-assets /absolute/refresh-1 \
  'Vilken organisation var H. Norman Schwarzkopf Jr. chef för mellan 1988 och 1991?'
# Repeat a full refresh from unchanged Notion into another new generation:
"$exe" refresh-notion /absolute/model-assets /absolute/refresh-1 \
  /absolute/refresh-2 ROOT_PAGE_ID SELECTED_PAGE_ID TRUSTED_WORKSPACE_ID
"$exe" inspect-index /absolute/refresh-2
"$exe" verify-text /absolute/refresh-2 OLD_DISTINCTIVE_PHRASE NK215-B
"$exe" serve-stdio /absolute/model-assets /absolute/refresh-2
```

Every command opens a new process. For HTTP use `serve-http` with the same new
index path. Existing sessions should be stopped, not silently switched.
Exclusions remain optional final arguments and full fresh discovery still owns
selected-page authorization. This is one selected page, bounded to 32 chunks;
no additional distinct question is introduced.

For recovery, retain the authoritative Notion page and model assets. Delete only
the specifically named disposable **derived** generation after stopping its
serving process. Preserve the old/original generation as independent evidence.
For this experiment the disposable recovery path is `/tmp/nk215-rebuild-index`;
never apply removal to a repository, asset directory or parent of an index.

```sh
# Only after confirming this exact directory is the experiment's derived index:
rm -r -- /tmp/nk215-rebuild-index
"$exe" create-notion /absolute/model-assets /tmp/nk215-rebuild-index \
  ROOT_PAGE_ID SELECTED_PAGE_ID TRUSTED_WORKSPACE_ID
"$exe" inspect-index /tmp/nk215-rebuild-index
"$exe" query-page /absolute/model-assets /tmp/nk215-rebuild-index \
  'Vilken organisation var H. Norman Schwarzkopf Jr. chef för mellan 1988 och 1991?'
```

A failed creation can leave an incomplete new directory. Keep the old serving
path, inspect the failure, then deliberately discard only that known disposable
new directory. There is no automatic recursive deletion, transactionally atomic
cross-directory swap, cross-process lock or persisted active-generation pointer.
These limitations are explicit experimental boundaries.

## Actual process evidence (2026-10-05)

The user manually changed only the selected introduction's role wording from
`chef` to `befälhavare` and added the distinctive temporary marker `NK215-B`.
The historical organization and dates remained unchanged. An authenticated read
of the exact selected block confirmed the old distinctive phrase absent and the
exact requested replacement present. This experiment did not write Notion.
The selected page remains its own discovery root, as in Step 2.

The original fresh-process baseline returned the expected organization passage
first. A separate query process after the source edit still returned the original
passage from the old index, with the old phrase present and marker absent. Thus
source editing alone did not refresh existing derived state.

The first actual `refresh-notion` rediscovered one selected document, extracted
11 chunks, embedded them with the same pinned Qwen revision and reopened the new
index to verify 11 rows and 11 unique chunk IDs. A new query process returned the
edited introduction first, with `NK215-B` present and the old distinctive phrase
absent. Independent assertions compared its selected page ID, title, URL and
edit timestamp with current authoritative metadata, checked the present empty
heading path, stable canonical ID, finite score, and recomputed its content hash.
Both returned results belonged to the approved page, with distinct chunk IDs.
A separate model-free read of **all 11 stored canonical records** confirmed the
old distinctive phrase absent everywhere and the new marker present. The new introductory chunk
was `nk-chunk-v1:7a74759a31debb30267f0839926e1a29cf7ad988c69143887051587db2a87298`,
with score 0.8359098 and source edit time `2026-10-05T15:31:00.000Z`.

| Generation | Verified rows / unique IDs | Sorted canonical-record SHA256 |
| --- | --- | --- |
| Original, retained | 11 / 11 | `017078a09764c0891875769dbb6d0b1b69120b5c4f5c44b4b189f680bee37111` |
| First refresh | 11 / 11 | `bcc95d9a0ba00eb7c1a5bdb89f2aa0b68d24fa5043a9b1741934ab9af97e5f71` |
| Repeated full refresh | 11 / 11 | `bcc95d9a0ba00eb7c1a5bdb89f2aa0b68d24fa5043a9b1741934ab9af97e5f71` |
| Recovery after deletion | 11 / 11 | `bcc95d9a0ba00eb7c1a5bdb89f2aa0b68d24fa5043a9b1741934ab9af97e5f71` |

The second full refresh independently read and embedded the unchanged source,
then verified the identical canonical-record hash, 11 rows and 11 unique IDs;
no records were appended or duplicated. A separate executable process reopened
this repeated generation and returned the edited passage first with two distinct
chunk IDs, current title/URL/edit timestamp, correct content hash, marker present
and old phrase absent. It also verified absence of the old
phrase across every record and presence of the marker. A validated copy
of the first refreshed generation was placed at the documented disposable
`/tmp/nk215-rebuild-index`, checked, and deleted using that exact non-symlink path.
The recovery command then independently rediscovered and read the unchanged
Notion page and ran Qwen over all 11 canonical chunks; it did not restore or copy
vectors. The rebuilt table had the same canonical-record hash and 11 unique rows,
with the old phrase absent across all records and the marker present. The original
index and model assets remain intact. A new process reopened the rebuilt index
and retrieved the same edited passage first, with two distinct results and the
same verified current citation metadata/content hash. A final authenticated source
read compared all page and intro-block fields with the initial post-edit read;
only the response's per-request `request_id` was excluded. Source metadata,
edit timestamp and content remained unchanged throughout all three embedding runs.
Raw source/query responses
remain private in `/tmp`; ordinary CI uses no live Notion content or credentials.

The first refresh measured 36.169 s for asset verification/model loading and
662.491 s for embedding all 11 chunks. Its fresh query measured 34.990 s loading,
12.623 s embedding and 0.032 s scanning. The repeat refresh measured 36.154 s loading and 666.971 s embedding;
recovery measured 34.538 s loading and 666.746 s embedding.
These are single-host, unoptimized
`dev` (`debug = 0`) stage timings, excluding Notion read and process startup;
they are not production performance, cold OS-cache or peak-memory claims.

Actual failure checks rejected missing source credentials, missing model assets
after authorized discovery, and an existing output directory; the old canonical
digest and existing-target sentinel were unchanged. A disposable sidecar with the
same 1024 dimension but a different model revision failed before source/model
work with explicit rebuild guidance and no new index. Model-free specifications
also reject duplicate canonical IDs and prove order-independent corpus identity.


## Production scope

#42 can reuse full-generation separation and canonical records, but retains
incremental add/update/delete, stable identity reconciliation and efficient
selective embedding requirements. #43 can reuse strict persisted identity checks
and explicit rebuild choice, but retains production index version policy and
migration/compatibility decisions. #58 retains scheduled reconciliation, missed
event recovery and operational-state coordination. #111 retains production
backup/restore, recovery procedures, operational state and their wider evidence.
Rebuilding this tiny disposable index does not satisfy those complete tickets.
