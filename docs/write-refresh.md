# Verified writes and durable local refresh (#440)

The internal `append_once_and_refresh`, `update_section_once_and_refresh` and
`NotionClient::create_scoped_page_once_and_refresh` workflows return a verified
Notion receipt plus a separate `IndexStatus` and refresh key. They retain the
guarded revision, physical ancestry and durable once-only ledger requirements.
Create verifies complete authoritative Markdown, metadata and fresh child scope;
append and section update reacquire scope after their own revision change.
Provider formatting differences or incomplete/ambiguous read-back fail closed.

The durable write ledger is separate from disposable retrieval state. Verified
ledger commit precedes refresh admission. If admission fails or the process
stops between those steps, replay the same original mutation key: its verified
receipt is replayed and admission retried, with no second Notion mutation.
Unknown outcomes remain reconciliation-only. Never switch mutation keys to
repair indexing.

The operational SQLite database has a `mutation_refresh` queue distinct from
authenticated webhook receipts. Admission binds a server-derived mutation
digest to one canonical workspace and page. Repeated admission returns its
existing state, including a failure or completion. It does not create upstream
webhook events, reset a failed attempt or mutate source content.

The existing supervised refresh worker consumes this queue alongside webhook
work. It uses the same `AuthoritativePageRefresh`, scoped authoritative reads,
normal content hash/chunk diff, embedding reuse and guarded commit coordinator.
The shared writer lease and per-operation claim generation/deadline fence
cancelled or expired workers at the actual commit boundary. After a crash,
expired ownership can be reclaimed up to three times; exhaustion becomes failed.
Repeated processing after a committed index receipt safely resumes that receipt.

`Queued` and `Running` do not claim a current index. `Completed` means the guarded
refresh committed. `Failed` means local refresh failed; `Unavailable` means
durable admission/status could not be established. Neither status rolls back a
valid Notion write or makes it safe to repeat. Trusted callers may inspect
`MutationRefreshQueue::status` and explicitly request
`SqliteSyncStateStore::retry_mutation_refresh` for a failed derived refresh.
Retry resets only the bounded index attempt budget and retains the fencing
generation; it has no source mutation capability.

Existing databases require explicit migration to schema 17 through their normal
operator-controlled migration path. Read-only opens do not migrate or repair.
Back up durable operational state and the separate write ledger before upgrades.

These are internal application/adapter capabilities. Default MCP discovery stays
read-only. Installed semantic tool dispatch, operator configuration and explicit
request-bound OAuth write authorization remain [#441](https://github.com/mtandersson/notion-knowedge/issues/441).
This child does not close parent #83/#10 or establish production ChatGPT writes.

Credential-free verification:

```sh
cargo test -p notion-knowledge-core --test append --test section_update --locked
cargo test -p notion-knowledge-notion --lib scoped_create --locked
cargo test -p notion-knowledge-retrieval --test mutation_refresh --locked
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --test webhook_worker mutation_refresh --locked
```
