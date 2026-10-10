# Privacy-aware audit events (#90)

This is the **audit event model and durable SQLite adapter** for future
authenticated Notion mutation tools. No production MCP write tools are wired
yet (#76–#83), so the default server does **not** emit audit records. This
component does not authorize operations, prove a Notion write happened, or
provide a public audit-query MCP tool.

## Contract and privacy boundary

`notion_knowledge_core::audit::AuditEvent` only accepts server-controlled,
closed-form metadata:

| Field | Accepted data | Never persist |
| --- | --- | --- |
| Time | Unix seconds from a trusted server clock | Source body or event payload |
| Actor | `approved_user`, `automation`, `server` | Names, emails, upstream user tokens |
| Tool | `page_create`, `page_append`, `page_replace`, `page_delete`, `page_move`, `file_attach` | Free-form method or request text |
| Target | Optional Notion page UUID | Page title, Markdown, content excerpts, URLs |
| Outcome | `attempted`, `succeeded`, `denied`, `failed`, `indeterminate` | Raw HTTP errors or stack traces |
| Correlation | Trusted generated 16–64 character safe identifier | Caller-provided query, token or URL |
| File | Optional image/other class and byte length | Filename, MIME parameters, file content, signed download/upload URL |

Fields are private, and the constructor rejects non-UUID target strings,
invalid correlation syntax and file facts for non-file operations. Errors
use fixed categories, not formatted upstream exception messages. A successful
`page_create` may begin without a known target; once Notion returns a page
UUID, the caller can attach that ID to the outcome audit record.

The actor is a *class*, not proof of user identity. The caller must first
enforce the allowlisted upstream user/workspace, authorization and root scope.
Only trusted code should mint correlation IDs. This event format intentionally
does not contain information needed to replay a mutation.

## Store, retention and operations

`SqliteAuditStore::open(path, AuditRetention::days(N))` creates its own
SQLite database/table, separate from **disposable LanceDB** and from the
existing sync journal migration version. Default retention is **30 days**;
accepted configuration is **1–3650 days**. `record(event, trusted_now_unix)`
performs retention deletion and insertion in one SQLite transaction.
`prune(now)` supports periodic cleanup even when no mutations occur.
Expired/future events are rejected, not misleadingly accepted then discarded.
Failures return only `InvalidInput` or `Unavailable`.

Keep the audit database and its parent directory private to the serving
operator (on Unix: directory `0700`, file `0600`; the adapter forces
`0600` on the database file at open), with encrypted/protected
backups if retained. The API does not create a new public HTTP route. In
deployments with multiple writers, SQLite handles transaction locking for a
*shared file*; do not put different writers on separate audit files. SQLite
transaction success is the local durability acknowledgment, not the same
transaction as the external Notion mutation.

**Audit-write failure policy:** for a planned write, check that durable audit
storage is available and record an `attempted` event before effects;
if the audit write fails, **deny the mutation**. After an attempted Notion
mutation, record `succeeded`, `denied`, `failed` or
`indeterminate` according to authoritative evidence. If that audit write
fails, surface an operator incident and reconcile the authoritative page;
never assume an ambiguous external write failed or blindly replay it. A
healthy audit log alone is not an authorization grant. This failure policy
must be enforced when MCP mutation workflows are wired; the present adapter
does not transparently intercept existing Notion client primitives.

## Verification

```sh
cargo test -p notion-knowledge-core audit --locked
cargo test -p notion-knowledge-retrieval audit --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Unit tests verify strict UUID-only target fields, non-file metadata rejection,
sanitized errors, configurable retention, rollback-safe SQLite insertion,
reopening persistence, and the absence of body, URL, filename and token columns.
No live Notion credentials or content are used. Integration with real
MCP writes and end-to-end audit evidence belongs to the mutation workflow
tickets; do not treat these component tests as that evidence.
