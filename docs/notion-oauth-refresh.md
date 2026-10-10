# Notion OAuth access-token refresh and rotation (#124)

**Status:** server-internal, opt-in component, not a public OAuth login.
The authorization and MCP HTTP endpoints remain **fail-closed** until the
remaining authentication, authorization and revocation controls in #117 land.

## Refresh policy and provider request

- Use the *already configured* confidential Notion client from #122:
  `POST https://api.notion.com/v1/oauth/token` with HTTP Basic client ID
  and secret. The JSON body uses `grant_type=refresh_token` and only the
  opaque server-side `refresh_token`. Production HTTPS endpoint is fixed,
  HTTP redirects are disabled, timeout is 12 seconds and the provider
  success response is bounded to 64 KiB.
- Shared parsing requires `token_type=bearer`, a valid access token,
  `owner.type=user`, `owner.user.id`, `workspace_id` and `bot_id`.
  For refresh, also require a new nonempty `refresh_token` and explicit
  `expires_in`; the new refresh token may not be identical to the
  current one. Missing/ambiguous binding **fails closed**.
- `NotionRefresh::ensure_fresh()` loads the sealed grant from #123. If
  `expires_at_unix` is more than **300 seconds** away, return the
  private server-only grant without a network call. At/inside the
  300-second threshold (including already expired), perform a server-side
  refresh. An **unknown** expiry also requires refresh rather than being
  considered permanently valid. If no grant or refresh token exists,
  require reauthorization. Upstream failures never return the old token.
- Exact **operator-allowlisted user/workspace**, previous stored
  user/workspace, and unchanged `bot_id` are mandatory before persistence.
  A refresh cannot silently switch to another Notion identity, integration
  or workspace. Errors are fixed, sanitized values; provider bodies,
  credential values and paths do not appear in logs.
- A shared `tokio::sync::Mutex` spans fresh-state re-read, token HTTP
  request and committed file write. This coalesces concurrent refreshes
  **within one process**: after first successful rotation, later callers
  read the newly stored pair and skip refresh while it remains sufficiently
  valid.

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
grant ID and increments epoch. The later resource authorization code
(#127/#128) must compare this stable binding and reject revoked grants
on every call.

When the provider has rotated its refresh token but storing the new
pair fails, the call returns failure with **no credentials**. The
old persisted token may then be invalid; the operator may need a fresh
Notion authorization. Never fall back to another identity or assume
retrying an already consumed refresh token is safe.

## Deployment caveats

The mutex is attached to **one shared coordinator instance in one
process**. Calling `NotionRefresh::new` for every request, running
multiple workers/pods against the same file, or moving the state onto a
shared filesystem is **not safe**. Distributed locking/fencing,
rollback-resistant revocation and persistent session authorization
remain #127/#128 requirements. No live HTTP callback, authorization,
token issuing or private MCP route is enabled by this PR.

The version-1 sealed file format remains unchanged, so records made
under #123 can be refreshed in place. Since expired records need
renewal, startup now validates cryptographic integrity and owner
binding using `load_for_refresh()` *without* treating expiry as
malformed state. The runtime may only **use** a grant after
`ensure_fresh()` succeeds. The server-internal runtime is now wired at HTTP startup, but no OAuth
callback or authorized MCP credential dispatch route is enabled.

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

**This does not activate OAuth HTTP login or authenticated MCP access.**
The production OAuth discovery router still denies `/authorize`,
`/token` and `/mcp` pending #120/#125/#126/#127/#128 and #129. The
runtime is an internal credential composer and local operator probe;
it must be explicitly connected to authorized backend tool handlers
after the MCP authorization and durable revocation gates land. The
legacy `NK_NOTION_AUTH=integration` operator token remains a separate
opt-in mode and is NEVER a fallback for failed OAuth refresh.

**Deployment/security:** one shared coordinator per HTTP process, one
writer for the private sealed file. Do not scale it across workers/pods
without distributed fencing. Snapshot checks prevent known stale
generations at dispatch, but cannot guarantee revocation of an
already in-flight request; #127/#128 own session- and per-request
authorization/revocation enforcement. Restore key and state together
or reauthorize after a lost key or failed provider rotation.

## Verification

```sh
cargo test -p notion-knowledge-server notion_oauth_refresh --locked
cargo test -p notion-knowledge-server notion_grant_store --locked
cargo fmt --all -- --check
```

Local fake Notion token servers test confidential Basic/POST body,
single-call concurrent refresh, skip of healthy tokens, unknown expiry,
atomic persistence, grant/epoch preservation, owner/workspace/bot
denials, missing rotation/expiry, provider errors, absent state and
failure without stale-token fallback. Grant-store tests include
replacement invalidation and stale-snapshot compare-and-swap denial.
