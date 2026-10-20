# Request-bound OAuth write authority (#448)

OAuth scope issuance remains read-only by default. `NK_OAUTH_WRITES=true` is an
explicit operator policy permitting the scope set `knowledge:read knowledge:write`.
It requires `NK_READ_ONLY=false`, complete registered OAuth client and sealed
grant settings, and authoritative workspace/root scope with no integration
credentials. Unknown, duplicate, missing, write-only or malformed scope sets are
rejected. Either order of the two supported scope tokens is accepted and issuance
returns canonical read-then-write order. Existing read-only tokens retain only
read authority. OAuth metadata advertises the operator-supported scope tokens.

Scope issuance alone does not install mutations. The explicit complete
[production OAuth semantic-write configuration](oauth-semantic-writes.md)
installs create/append and optionally bounded section updates through the
request-bound factory. Static trusted capabilities cannot substitute for that
authority. Archive/full-page replacement and OAuth files remain unavailable;
file/image installation is tracked in #446.

Issued permissions are immutable, non-secret HTTP evidence and part of exact
session identity. Only trusted issuance supplies them; MCP arguments cannot add
permissions. A bound source exposes mutation ports only for a read/write token.
Each create, append or bounded section mutation obtains a temporary client from
the same active sealed grant as reads: exact grant ID, epoch, user and workspace.
No integration token is consulted. The reusable server-only
`RequestGrantAuthority` captures that exact credential snapshot for future
request-bound ports, including file adapters; it never adopts a different grant
or silently refreshes a client already waiting to send.

The concrete adapter explicitly marks mutation sends. After transport admission
or rate-limit backoff, authority is checked synchronously immediately before each
actual provider attempt. It checks token deadline/cancellation, known upstream
expiry and the exact current sealed credential snapshot. Read POSTs are not
classified as mutations by HTTP verb. Successful responses are checked again
before returning; the bound operation also checks after receipt/body/read-back.
The existing redacted request logging and attempt/retry metrics remain shared.
Future file adapters must classify their actual effects through the same guarded
mutation send path; this child installs no file adapters.

Expiry, logout or grant replacement after an effect may have reached Notion is
an unknown outcome, never proof that nothing changed. Known successful mutation
targets survive subsequent metadata read-back errors. The existing core workflows
persist uncertain state before the provider await; a reopened ledger or new OAuth
grant cannot repeat that intent. A provider 429/529 retry must revalidate authority
after its admission wait; ambiguous transport/server failures do not retry writes.

Local acceptance uses real OAuth HTTP/code issuance, sealed grants, loopback
Notion transport and SQLite mutation ledgers. Fixtures cover scope and identity
failures, disabled shipping tools, preflight revocation, held effects/read-back,
known upstream expiry during admission, revocation between rate-limit attempts,
and durable unknown-outcome replay. No real provider credentials or writes are
required.

```sh
nix develop --command cargo test -p notion-knowledge-server --locked
nix develop --command cargo test -p notion-knowledge-notion --locked
nix develop --command cargo test -p notion-knowledge-mcp --locked
```
