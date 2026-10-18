# Trusted semantic mutation dispatch (#447)

`KnowledgeServer::and_mutations` accepts a trusted `MutationRuntime` containing
authoritative source ports, operator scope, durable write ledger, refresh queue,
audit store and server-owned actor. It is an application composition capability,
not a tool argument or shipping server setting. It does not disable read-only,
destructive-operation or capability allowlist/blocklist gates.

Default discovery stays read-only. Existing OAuth HTTP contexts reject these
static capabilities regardless of builder order. Shipping OAuth write permission,
grant-bound refresh, trusted production confirmation and installed server
composition remain #448–#451; this component does not complete #441/#83/#10.
The separate file/image workflow remains #446.

With trusted capabilities and explicit writable mode, actual discovery includes
`knowledge_create_page` and `knowledge_append`. Bounded
`knowledge_update_section` additionally requires a trusted confirmation provider
and the independent destructive-operation flag. Archive and whole-page
replacement are not implemented, advertised or dispatched by this runtime, even
when the old development schema-preview flag is also set.

Inputs require canonical page/root UUIDs and an idempotency key. `root_page_id`
can only narrow the trusted operator roots; it cannot add authority. Create takes
`parent_page_id`, title and Markdown. Append/section take `page_id` and at least
one authoritative `expected_last_edited_time` or exact
`expected_markdown_sha256`. Section uses `section_anchor: {level, heading}`,
selects one exact literal heading and preserves all unrelated bytes. Empty
replacement can clear only that selected body. Inputs cannot choose credentials,
workspace, actor, arbitrary HTTP operations or confirmation authority; unknown
fields, including caller-supplied `confirmation_id`, are rejected.

Creation uses the provider-neutral `core::create::create_once_and_refresh` over
the existing read/write/lifecycle ports. Each source operation remains separately
visible to a future request-bound adapter; wrapping an entire concrete workflow
in one stale credential check is unnecessary. It reserves the durable intent,
revalidates parent scope, marks the outcome uncertain before one create effect,
reads complete exact title/Markdown, verifies fresh child scope and the exact
direct parent, then records the verified receipt before refresh admission.
The older concrete Notion icon-capable helper remains a separate internal API;
the semantic create schema does not accept icons or arbitrary properties.

Append retains the existing guarded end-only workflow. Section confirmation
occurs only for a new reserved intent, bound to trusted scope, operation, target,
complete request fingerprint and idempotency key. Fresh physical scope and
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
not that no prior queued work exists. Revision conflicts return only page ID and
current edit time, never source text. `outcome_unknown` requires authoritative
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

The twelve real MCP transport fixtures use production SQLite ledger/audit/queue
adapters and local authoritative source doubles. They cover all three mutations,
strict parsing/default/OAuth/capability boundaries, exact read-back/direct
parent, confirmation changes/replay, durable reopen/privacy, source uncertainty,
index/audit failure, competing sessions and disconnect after effect. They do not
establish installed production OAuth mutation support.
