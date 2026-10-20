# Trusted section confirmation (#450)

The explicit `ElicitationConfirmation` provider obtains a user decision through
the authenticated MCP peer's form elicitation capability. It is created per
request from trusted HTTP middleware evidence and the current request-bound
source. Tool arguments, confirmation IDs and destructive tool annotations do
not grant approval. Clients without form elicitation support are denied.

The prompt shows the target page, exact heading, authoritative old section body
and proposed replacement body. Existing section bounds remain: the source
document is at most 1 MiB and replacement text at most 200,000 bytes. The
provider never truncates the approved effect. Scope identity includes workspace,
generation, roots and every exclusion rule. The immutable confirmation intent
also binds operation, target, revision/payload fingerprint and idempotency key.

Only an explicit accepted form with `approve: true` grants one local permit.
Missing, declined, cancelled, malformed or extra response fields do not approve.
The peer, request, session and source authority cannot be selected by tool JSON.
Approval cannot transfer to another invocation or a changed intent.

The configured timeout is bounded to five minutes. Request cancellation, token
revocation or expiry, grant replacement and failed live validation deny pending
approval. Live validation is itself bounded by the remaining approval and bearer
deadlines; a late result cannot extend approval. After approval, the section
workflow rechecks scope and fresh source revision/content before any effect.
The durable idempotency ledger still determines verified replay behavior.

This provider must be explicitly installed in trusted request composition.
It does not enable archive, delete or whole-page replacement. Shipping OAuth
mutation installation and the complete installed write/index acceptance remain
#451. Default OAuth discovery stays read-only until that composition ships.

Local tests use real MCP request/elicitation messages and Streamable HTTP with
the production OAuth gate and bounded session manager. They cover explicit
acceptance/refusal, foreign-peer replies, token logout, expiry, replacement,
timeouts and exact intent binding without real Notion writes. Real ChatGPT
connection and user-interface acceptance remain part of the live cutover work.

Protocol reference: [OpenAI MCP server guidance](https://developers.openai.com/plugins/build/mcp-server).
