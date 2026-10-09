# Authoritative page lifecycle evidence

`PageLifecycle` in core is a metadata-only authoritative read port implemented by
`NotionClient`. Supply a selected `PageId` and trusted `LifecycleScope` from the
composition root: credential-bound workspace UUID, scope generation, explicit
page roots and the same `ExclusionRules` used by discovery. This behavior is
reusable by page refresh (#55) and lifecycle effects (#252); it does not itself
extract content, embed, change an index or acknowledge webhook work.

`lifecycle` normalizes UUIDs/Notion links and deduplicates/sorts roots and rules.
The result preserves that exact scope, selected page revision and every observed
physical ancestor's kind, identity, revision, inactive state and parent. It
contains no titles, properties, Markdown, raw API responses or credentials.

The sequential current chain follows page, block, database and data-source
parents only. It continues through configured roots to a validated
`workspace: true` terminal, preserving all overlapping root provenance and
checking exclusions above nested roots. Root IDs designate pages; source types
apply to containers and their descendants. Page exclusions affect the named
page and descendants; descendants-only rules preserve the named page itself.
Exclusions win over any explicit/overlapping root. The nearest observed
exclusion supplies the deterministic reason; allowed roots are sorted by UUID.
No references, linked-source contents, global search, block children, source
queries or page content endpoints are used. Synced-block references and unknown
parent types (including agent parents) fail as unsupported; their targets are
never followed. Supported data-source-to-data-source and wiki database parents
follow physical `parent`, not the convenience `database_parent` shortcut.

## Safe outcomes

- `Allowed`: active page, complete active ancestry, one or more configured page
  roots and no exclusion. A restored page is evaluated using these same reads.
- `Inactive`: the selected page's authoritative boolean `in_trash`,
  `is_archived` or legacy `archived` is true. The adapter deliberately does not
  need access to ancestors of an affirmatively inactive selected page. Every
  present status flag must be boolean; at least one must be present.
- `OutsideScope`: a complete active current chain terminates at workspace and
  contains no configured root.
- `Excluded`: a complete active chain crosses a configured exclusion, including
  an exclusion above an otherwise explicit root.

Removal candidates are only affirmative inactive or complete outside/excluded
evidence. They are not blanket authorization: the consumer must bind the
selected identity to its own trusted current workspace/index scope, revalidate
and fence the actual commit. An inactive result does not authorize content reads.
A 404, denial, failed/missing/partial ancestor, unsupported relationship,
malformed metadata, cyclic chain, limit overflow, inactive ancestor of an active
page or observed concurrent change returns a sanitized `BackendError` with no
lifecycle result. Access failures retain their normalized kind; consumers must
retry/reconcile rather than interpret them as deleted. Webhook event type and
absence from a discovery report are never inputs to removal evidence.

Each object must have the expected kind/canonical UUID, RFC3339 edit revision,
boolean lifecycle status and a valid physical parent. Requests use API version
`2026-03-11`, the existing shared limiter/retry transport, its timeout, and a
2 MiB response-body limit. Traversal is limited to 256 nodes/depth (including
the selected page), hence at most 512 successful metadata reads per resolution,
plus the transport's bounded retry attempts. Large configuration sets are
rejected rather than silently truncated (256 root inputs, 100,000 exclusion IDs).
No durable progress is written; failed/interrupted reads authorize nothing.

## Revalidation and workspace provenance

Resolution rereads the chain in reverse order before returning evidence and
rejects observed changes. `revalidate_lifecycle` first compares the normalized
current workspace binding, generation, roots and rules, then resolves the
selected page again and compares the complete evidence. Changed source revision,
parent, status, roots, exclusion reason or ancestry rejects with `Conflict`.
Consumers must do this after preparing a page update/removal and before their
actual commit, while also checking their own selected page revision and current
scope generation under their external index serialization/fence.

Notion exposes only `workspace: true`, not a workspace UUID, in a physical parent.
The workspace UUID here is explicitly trusted configuration associated with the
integration credential. It is preserved and compared, not falsely inferred or
verified from that response. Replacing a credential/workspace binding requires
new trusted scope configuration and generation; webhook payloads cannot supply
or override it.

Neither the reverse pass nor successful revalidation creates an atomic Notion
snapshot: a move/edit can occur immediately after the final read, and revisions
may not encode every concurrent ancestor change. A SQLite lease alone cannot
fence a later LanceDB mutation. Downstream production workflows must retain their
commit guard, reject observed stale evidence and reconcile subsequent changes.
The searchable lifecycle behavior, including deletion and restoration effects,
remains #252 under open parent #56.

## Verification and protocol sources

`cargo test -p notion-knowledge-notion lifecycle --locked` runs strict,
credential-free HTTP fixtures against the actual adapter, including exact GET
routes/version/authentication, physical ancestry, exclusions, moves, inactive
state, inaccessible ancestors and revalidation races. No private workspace or
index is accessed.

The object fields and parent relationships follow Notion's official
[Parent](https://developers.notion.com/reference/parent-object),
[Page](https://developers.notion.com/reference/page),
[Block](https://developers.notion.com/reference/block),
[Database](https://developers.notion.com/reference/database) and
[Data source](https://developers.notion.com/reference/data-source) references.
