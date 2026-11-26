# Trusted semantic mutation dispatch (#447)

`KnowledgeServer::and_mutations` accepts a trusted `MutationRuntime` containing
authoritative source ports, operator scope, durable write ledger, refresh queue,
audit store and server-owned actor. It is an application composition capability,
not a tool argument or shipping server setting. It does not disable read-only,
destructive-operation or capability allowlist/blocklist gates.

Default discovery stays read-only. The installed composition is the
[guarded integration write path](integration-semantic-writes.md).

With trusted capabilities and explicit writable mode, actual discovery includes
`knowledge_create_page` and `knowledge_update_page`. Bounded
`knowledge_update_section` additionally requires a trusted confirmation provider
and the independent destructive-operation flag. Archive and whole-page
replacement are not implemented, advertised or dispatched by this runtime, even
when the old development schema-preview flag is also set.

Inputs require canonical page/root UUIDs and an idempotency key. `root_page_id`
selects a configured root. Use `sources[].provenance.root_page_id` from
`knowledge_get` on the destination if it is indexed, or on an indexed page
under the intended root. The create destination is `parent_page_id`, and
append/section use `page_id`. Create also takes title and Markdown.
Append/section require at least
one authoritative `expected_last_edited_time` or exact
`expected_markdown_sha256`. Section uses `section_anchor: {level, heading}`,
selects one exact literal heading and preserves all unrelated bytes. Empty
replacement can clear only that selected body. Inputs cannot choose credentials,
workspace, actor, arbitrary HTTP operations or confirmation authority; unknown
fields, including caller-supplied `confirmation_id`, are rejected.

Creation uses the provider-neutral `core::create::create_once_and_refresh` over
the existing read/write ports. Each source operation remains separately
visible to a future request-bound adapter; wrapping an entire concrete workflow
in one stale credential check is unnecessary. It reserves the durable intent,
marks the outcome uncertain before one create effect, reads complete exact
title/Markdown, then records the verified receipt before refresh admission.
MCP validates the configured root selection before dispatch. Notion checks
whether the integration token may write to the destination. This path performs
no ancestry walk.
The older concrete Notion icon-capable helper remains a separate internal API;
the semantic create schema does not accept icons or arbitrary properties.

Append retains the existing guarded end-only workflow. Section confirmation
occurs only for a new reserved intent, bound to trusted scope, operation, target,
complete request fingerprint and idempotency key. Fresh
source preconditions are checked after approval may have waited. A verified
ledger replay returns its existing receipt without consuming another one-use
approval. A declined, failed or unknown reserved intent is not automatically
released, reprompted or repeated.

Every admitted mutation attempt requires a durable content-free audit intent before source
effects. Success/denial/failure/indeterminate outcomes receive a terminal audit;
disconnect/cancellation retains conservative indeterminate evidence. Audit actor
and correlation IDs come from trusted composition/context. Terminal audit failure
does not erase a verified Notion receipt. Shared weighted tool admission covers
the complete workflow, including approval and audit; it introduces no write retry
or timeout that could repeat an uncertain effect.

Results separate three states:

```json
{
  "write": {
    "status": "verified",
    "page_id": "<canonical-page-id>",
    "url": "<authoritative-Notion-link>",
    "last_edited_time": "<authoritative-revision>",
    "replayed": false
  },
  "index": {"status": "queued", "refresh_id": "<opaque-mutation-digest>"},
  "audit": {"status": "recorded"}
}
```

Index `queued`/`running`/`completed`/`failed`/`unavailable` is independent of
verified source success; `not_attempted` means this call did not reach admission,
not that no prior queued work exists. A root that does not match the configured
scope returns `write.status: not_authorized` with
`write.reason: root_not_configured` and a fixed guidance message before any
provider call or idempotency reservation. The caller can correct the root and
reuse the same key for the same intended mutation. Revision conflicts return only page ID and
current edit time, never source text. A request Notion definitely refuses
before any effect (400, 403 or 404 on the create POST, append PATCH or section
PATCH, or a refused preflight read) returns `not_authorized` or
`invalid_input` with `write.reason` `not_found`, `permission_denied`,
`invalid_request` or `parent_is_database` and a fixed message without ids.
The idempotency claim is released and the audit records `failed`, so the
same key can be retried. A create whose `parent_page_id` is a readable
database reports `parent_is_database` and points to `data_source_id`.
`outcome_unknown` requires authoritative
reconciliation and the original durable key. Index or audit repair must never use
a new mutation key to repeat the source write. A terminal audit failure retains
the verified receipt and marks the MCP result as an error with
`audit.status: unavailable`; replaying the original key can record the outcome
without repeating Notion effects. The [refresh contract](write-refresh.md)
defines separate indexing-only recovery.

Credential-free verification:

```sh
cargo test -p notion-knowledge-mcp --test mutations --locked
cargo test -p notion-knowledge-core -p notion-knowledge-mcp --locked
cargo clippy -p notion-knowledge-core -p notion-knowledge-mcp --all-targets --locked -- -D warnings
```

The MCP transport fixtures use production SQLite ledger/audit/queue
adapters and local authoritative source doubles. They cover all three mutations,
root selection, strict parsing/default/capability boundaries, exact read-back/direct
parent, confirmation changes/replay, durable reopen/privacy, source uncertainty,
index/audit failure, competing sessions and disconnect after effect. They do not
establish production ChatGPT mutation support.
