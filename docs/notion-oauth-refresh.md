# Notion OAuth access-token refresh and rotation (#124)

The shared server-only coordinator is installed in the browser/token/MCP
composition. Every authorized resource operation requires live grant evidence;
refresh alone is not caller authorization. See [production setup](oauth-production-setup.md).

## Refresh policy and provider request

- Use the *already configured* confidential Notion client from #122:
  `POST https://api.notion.com/v1/oauth/token` with HTTP Basic client ID
  and secret. The JSON body uses `grant_type=refresh_token` and only the
  opaque server-side `refresh_token`. Production HTTPS endpoint is fixed,
  HTTP redirects are disabled, timeout is 12 seconds and the provider
  success response is bounded to 64 KiB.
- Shared parsing requires `token_type=bearer`, a valid access token,
  `owner.type=user`, `owner.user.id`, `workspace_id` and `bot_id`.
  For refresh, also require a new nonempty `refresh_token`; the new refresh
  token may not be identical to the current one. `expires_in` is optional:
  the documented Notion response omits it, and sealed expiry stays `None`. Missing/ambiguous binding **fails closed**.
- `NotionRefresh::ensure_fresh()` loads the sealed grant from #123. If
  `expires_at_unix` is more than **300 seconds** away, return the
  private server-only grant without a network call. At/inside the
  300-second threshold (including already expired), perform a server-side
  refresh. An **unknown** expiry requires a confidential Basic-authenticated
  `POST /v1/oauth/introspect` for the exact access token before every use.
  Only boolean `active: true` succeeds; inactive, absent, malformed or failed
  responses deny without trying refresh to mask revocation. For active grants,
  a separate **24-hour server-owned rotation age** triggers proactive rotation;
  this cadence never asserts a provider expiry. Newly rotated unknown-expiry
  credentials are also introspected before release. If no grant exists, or
  rotation is due without a refresh token, require reauthorization. Upstream
  failures never return the old token.
- Exact **operator-allowlisted user/workspace**, previous stored
  user/workspace, and unchanged `bot_id` are mandatory before persistence.
  A refresh cannot silently switch to another Notion identity, integration
  or workspace. Errors are fixed, sanitized values; provider bodies,
  credential values and paths do not appear in logs.
- A shared `tokio::sync::Mutex` spans fresh-state re-read, token HTTP
  request and committed file write. This coalesces concurrent refreshes
  **within one process**: after first successful rotation, later callers
  read the newly stored pair and skip refresh while it remains sufficiently
  valid. Unknown-expiry callers still verify their own live active state.

The provider contract is based on Notion's [refresh response](https://developers.notion.com/reference/refresh-a-token)
and [introspection response](https://developers.notion.com/reference/introspect-token).
Introspection establishes active state only, not identity or expiry. Identity
continues to come from the sealed, allowlisted callback grant. Requests use
`Notion-Version: 2026-03-11`, fixed HTTPS endpoints, redirect denial, the
12-second timeout and 64 KiB response bound. No real credentials or live
provider calls are part of automated verification.

`NotionOAuthRuntime::ensure_active()` supplies server-only active grant evidence
for per-request resource authorization (#431) and request-bound content composition (#432). It introspects even a
known-expiry grant and rechecks the exact sealed snapshot after the network
request. The local bot-identity probe uses this stronger check too. This API does not itself authorize an MCP session; the transport enforces
token, grant and session ownership independently.

## Atomic sealed rotation

`GrantStore::rotate_refresh(expected, fresh)` uses its existing
process-local writer mutex and verifies the expected pre-refresh
`grant_id`, `epoch`, `issued_at_unix`, current access/refresh
credentials and all identity fields before modifying the grant. A
mismatched/stale snapshot or identical refresh token is rejected.
The entire new access+refresh token pair and new expiry are AES-256-GCM
encrypted in **one** versioned envelope, fsynced, and atomically renamed
over the previous file. Both tokens are never stored in separately
written files or in logs.

A refresh **preserves grant ID and authorization epoch**: changing a
Notion upstream credential is not a new MCP consent decision. A *new
OAuth authorization* through `save()`, by contrast, creates a new
grant ID and increments epoch. Resource authorization compares this stable binding and rejects revoked
tokens or incompatible grants on every call.

When the provider has rotated its refresh token but storing the new
pair fails, the call returns failure with **no credentials**. The
old persisted token may then be invalid; the operator may need a fresh
Notion authorization. Never fall back to another identity or assume
retrying an already consumed refresh token is safe.

## Deployment caveats

The mutex is attached to **one shared coordinator instance in one
process**. Calling `NotionRefresh::new` for every request, running
multiple workers/pods against the same file, or moving the state onto a
shared filesystem is **not safe**. The installed runtime acquires a lifetime exclusive state lease, rejecting a
second process or operator reset while active. It does not provide distributed
fencing, rollback-resistant storage or persistent MCP sessions. The browser,
token and content adapters share the same coordinator.

The version-1 sealed file format remains unchanged, so records made
under #123 can be refreshed in place. Since expired records need
renewal, startup now validates cryptographic integrity and owner
binding using `load_for_refresh()` *without* treating expiry as
malformed state. The runtime may only **use** a grant after
`ensure_fresh()` succeeds. The server-internal runtime is now wired at HTTP startup, including the configured callback, token exchange and request-bound content.

## Shared server runtime (#299)

The HTTP composition root constructs **one** `NotionOAuthRuntime` when
encrypted grant settings are configured, and retains its shared
`NotionRefresh<'static>` coordinator for the entire process lifetime.
Each internal `with_client` operation calls `ensure_fresh()` before
creating a temporary `NotionClient` with the decrypted server-only OAuth
access token. Before dispatch, the sealed grant's `grant_id`, epoch,
issuance time and complete access/refresh credential snapshot are checked
against the current state, rejecting stale/replaced grants. Every
refresh failure, missing grant, corrupt state or identity mismatch denies
the operation; no separate static `NOTION_TOKEN` fallback is considered.

An operator can test an actual Notion get-self request via
`--notion-oauth-identity` with the approved sealed grant. The command
checks the remote bot ID against the immutable verified callback grant
and reports **only** a fixed success/failure message, never any
credentials or provider responses.

The browser flow issues codes and `/token` uses live `ensure_active()` evidence
before minting a grant-bound MCP bearer. `/mcp` enforces live authorization and
exact-token session ownership; content ports require verified request-owned
evidence. [Local revocation](oauth-revocation.md) cancels that evidence and blocks
pending results before disclosure. Already-started bounded provider operations
may finish; bytes already delivered cannot be recalled. The legacy integration
token is a separate opt-in mode and NEVER a failed-refresh fallback.

Use one active process per private state file. The state lease excludes a second
runtime but does not protect against an operator restoring old state. Restore
protected key/state together or reauthorize after a lost key or ambiguous rotation.

## Verification

```sh
cargo test -p notion-knowledge-server notion_oauth_refresh --locked
cargo test -p notion-knowledge-server notion_grant_store --locked
cargo fmt --all -- --check
```

Local fake Notion token servers test confidential Basic/POST body,
single-call concurrent refresh, skip of healthy tokens, unknown expiry,
atomic persistence, grant/epoch preservation, owner/workspace/bot
denials, missing rotation, optional expiry, active-state denials, provider errors, absent state and
failure without stale-token fallback. Grant-store tests include
replacement invalidation and stale-snapshot compare-and-swap denial.
