# Root-scope authorization gate (#306 / parent #87)

This change adds a **provider-independent, fail-closed policy gate**
(`RootScopeGate`) that all future MCP operations can reuse. It is backed by
the existing `PageLifecycle` port and Notion's metadata-only physical
ancestry resolver; it makes no Notion mutations.

## Authorization invariant

A page UUID, indexed source reference, a search-result `root_page_id`, a
Notion relation or a user-supplied root claim **never confer authority**.
The trusted server composition must provide a canonical workspace UUID,
canonical allowed root-page UUIDs, exclusions and a monotonically updated
configuration generation.

For each requested page, `authorize` asks `PageLifecycle` for **fresh**
authoritative ancestry. The gate confirms:

- Returned policy matches the trusted configuration and selected page ID.
- Every parent edge is a contiguous physical page/block/database/data-source
  ancestry step that reaches the workspace; no missing, repeated, or forged
  root is accepted.
- At least one approved root is actually an ancestor (or the selected page),
  and the evidence's allowed roots match exactly those physically observed.
- The selected page and its ancestors are active; page/descendant/source-type
  exclusions are enforced on the entire chain.
- Unknown or inconsistent ancestry, unavailable metadata, or policy mismatch
  returns a sanitized denial; nothing is emitted from partially validated data.

The gate **does not cache positive access decisions**; each operation needs
a new `authorize`. This means moved pages cannot retain authority through an
old ancestry cache. The `AuthorizedPage` permit has private fields and can
only be created by the gate. Before a mutation or sensitive fresh read,
`revalidate` compares a **newly loaded current trusted policy** with the
permit's policy and asks the backend to recheck source evidence. It rejects
root/exclusion/generation changes and any changed ancestry. This is
defense-in-depth, not a Notion atomic authorization transaction; edits or
moves may occur between final check and external call.

Errors contain no page content, selected page ID, parent chain, credential,
or backend error body.

## Rollout

The production MCP tools are **not** yet wired to this new port. That is
tracked in child [#307](https://github.com/mtandersson/notion-knowedge/issues/307)
and is required before parent #87 may be closed or write workflows #78/#79
enabled. In particular, existing indexed-result root filtering alone is not
a substitute for fresh ancestry verification. The read-only default and
planned-write schema-preview behavior remain unchanged by this PR.

Run `cargo test -p notion-knowledge-core --test root_scope --locked`,
the existing Notion lifecycle tests, workspace Clippy/format, and full CI.
