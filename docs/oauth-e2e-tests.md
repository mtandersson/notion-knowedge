# OAuth end-to-end acceptance tests

The seven single-user authorization criteria in #129 are covered through real
HTTP requests to the production OAuth router and MCP SDK. The joined lifecycle
test follows browser PKCE initiation, the Notion callback, allowlisted sealed
storage, token issuance, useful content reads, refresh, logout and a fresh login.
The rejection tests exercise the same HTTP boundaries before private content
can become available.

Test names below are under `oauth_authorize::tests`. The browser and token tests
live in [tests.rs](../crates/server/src/oauth_authorize/tests.rs), resource tests
in [resource.rs](../crates/server/src/oauth_authorize/tests/resource.rs), joined
content tests in [content.rs](../crates/server/src/oauth_authorize/tests/content.rs),
and logout tests in [revocation.rs](../crates/server/src/oauth_authorize/tests/revocation.rs).

| Acceptance criterion | Test and observable evidence |
| --- | --- |
| ChatGPT-style PKCE → Notion OAuth → allowlist → MCP token | `browser_callback_persists_allowlisted_grant_then_redirects_with_bound_code` checks registered redirect/state, saved owner/workspace, read scope, token lifetime and exclusion of upstream credentials. `content::http_oauth_lifecycle_reads_refreshes_revokes_and_reauthorizes` continues that HTTP browser/token flow through SDK initialization and useful lexical, semantic, hybrid and fresh content reads. |
| Wrong Notion workspace rejected | The foreign-workspace case in `denied_provider_identity_and_errors_never_persist_or_redirect` gets 400 with no client redirect and no saved grant. |
| Wrong Notion user rejected | The foreign-user case in `denied_provider_identity_and_errors_never_persist_or_redirect` gets the same denial without saving a grant. Missing identifiers and wrong owner type are also rejected. |
| Invalid state and PKCE verifier rejected | `unknown_and_cross_login_states_preserve_both_legitimate_browser_transactions` rejects a well-formed unknown state and both cross-login state/cookie combinations before provider IO, then completes both legitimate transactions through token issuance and SDK initialization. `invalid_browser_correlation_never_exchanges_or_consumes_another_login` covers missing/invalid/duplicate cookies. `token_endpoint_rejects_bound_field_changes_and_expired_or_replayed_codes` rejects a well-formed incorrect verifier without a provider probe. |
| Expired and replayed authorization codes rejected | `token_endpoint_rejects_bound_field_changes_and_expired_or_replayed_codes` advances the injected clock and rejects expired or consumed codes. `browser_callback_persists_allowlisted_grant_then_redirects_with_bound_code` rejects token-code and callback replay. `concurrent_token_redemption_issues_at_most_one_bearer` permits exactly one competing redemption. |
| Refresh-token path exercised | `content::http_oauth_lifecycle_reads_refreshes_revokes_and_reauthorizes` advances the provider credential clock during an existing token/session, reads actual guarded index content, and verifies a rotated pair with unchanged grant/epoch and absent upstream expiry. `token_exchange_supports_documented_unknown_expiry_and_atomic_refresh` also covers documented no-expiry and near-expiry responses. `resource::provider_errors_and_failed_resource_refresh_never_dispatch_or_change_session_binding` covers failed refresh and recovery. |
| Revoked MCP token cannot call tools | `content::http_oauth_lifecycle_reads_refreshes_revokes_and_reauthorizes` posts to `/revoke`, then gets 401 for an actual `knowledge_get` with no provider/token/page IO, completes a fresh browser login and useful read, and checks the old bearer remains denied. `revocation::logout_ends_existing_streams_and_denies_every_session_without_provider_io` verifies live SSE termination and session capacity recovery. `content::revoked_token_cannot_disclose_private_content_from_an_already_started_read` withholds private bytes from a paused backend operation. |

## Run the acceptance coverage

From the checkout root, run the default HTTP rejection and transport tests:

```sh
nix develop .#default --command cargo test -p notion-knowledge-server --locked oauth_authorize::tests
```

The joined content lifecycle requires the production retrieval feature and the
spike environment's native toolchain. Run this as well to cover useful reads,
guarded storage and refresh/logout on that path:

```sh
nix develop .#spike --command cargo test -p notion-knowledge-server --features webhook-worker --locked oauth_authorize::tests
```

A default-only run omits the LanceDB-backed joined tests. CI also runs the full
server suites and strict lint; focused verification can use:

```sh
nix develop .#default --command cargo fmt --all --check
nix develop .#spike --command cargo clippy -p notion-knowledge-server --all-targets --features webhook-worker --locked -- -D warnings
```

## Fixture boundaries

Every Notion response and request in these tests belongs to a private loopback
fake provider. No real credentials, Notion consent, external embedding service
or authenticated upstream call is used. The confidential client still sends
the real token-request shape to that fake provider. Browser redirects are
inspected and followed manually so the registered client and Notion HTTPS URLs
never cause external requests. The installed OAuth router, callback verification,
sealed grant store, token and session middleware, MCP SDK, request-owned source
adapters and tool dispatch are the production implementations.

The joined tests seed actual guarded LanceDB and SQLite state using the commit
coordinator, a fixed local embedding provider and canonical relation receipts.
They exercise real lexical/vector retrieval and graph-enabled composition;
query operations must leave that stored authority unchanged. Injected clocks
cross code and credential lifetime boundaries without wall-clock sleeps. Paused
local IO exposes replacement, revocation and cleanup races deterministically.

These tests establish the local HTTP single-user authorization contract. They
do not verify a real ChatGPT installation, real Notion account consent or a
public HTTPS reverse proxy. Deployment configuration and external client
onboarding remain separate acceptance work in #130. See [local logout and
operator forget](oauth-revocation.md) for process ownership and local/provider
revocation distinctions.
