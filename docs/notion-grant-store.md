# Encrypted Notion OAuth grant store (#123)

**Status:** encrypted persistence is installed in the explicit
[browser callback](notion-oauth-callback.md), not just an internal primitive.
The callback and credential runtime share one store instance. A verified,
allowlisted Notion grant must be durably saved before the original MCP PKCE
transaction can receive an authorization code. Failed storage never returns
a code. MCP access tokens and private MCP resource access remain disabled;
those are separate #120/#124/#127–#130 release gates under #117.

## Key, directory, and startup

Set the preexisting Notion OAuth callback registration and immutable approved
identity settings from [#122](notion-oauth-callback.md), then configure **both**:

```sh
NK_NOTION_GRANT_KEY_FILE=/run/secrets/notion-grant-aes256.key
NK_NOTION_GRANT_STATE_FILE=/var/lib/notion-knowledge/oauth-grant.bin
```

Generate the encryption key with a CSPRNG (`openssl rand -out
/run/secrets/notion-grant-aes256.key 32`), keep it **outside** the data
directory and never commit it. Supply it through the orchestrator's
secret mount/secret manager, backed by a separately access-controlled and
recoverable key. The key is **exactly 32 raw bytes**, must be a regular file,
must not be a symlink, and must be accessible to the service with restrictive
permissions (e.g. `0400`/`0600`, no group/other permission). Do not
confuse this with the Notion OAuth **client secret**: two distinct secrets.

The private data directory must already exist with no group/other write bits
(e.g. `0700`). The grant file itself must be a regular, non-symlink file
with `0600` permissions. Both configured paths must be absolute, safe paths.
A missing state file is treated as **no grant**, never automatic first-login
enrollment. A missing/malformed key, wrong file mode, symlinked state,
corrupted ciphertext, wrong key, wrong Notion integration/client ID or
mismatched approved workspace/user all **fail closed with sanitized errors**.
Startup calls `Config::validate_grant_store()` before HTTP/stdio/diagnostics
and `--check` execution when the store is configured. Startup validates the encrypted grant and identity even when its access token
has expired, permitting the server-only [refresh coordinator](notion-oauth-refresh.md)
to attempt renewal. Expired grants require a successful refresh; unknown-expiry
grants require positive live introspection before use and rotation when due. Errors contain neither raw paths nor plaintext
secrets.

Keys and state must be backed up/recovered **together** under independent
access control. Losing the encryption key makes stored tokens permanently
unreadable. Key rotation currently requires controlled offline
re-encryption/re-authorization; changing the key in place is rejected, and
automatic online rotation is NOT implemented here.

## Authenticated sealed format

Every on-disk grant is:

```text
8-byte ASCII magic/schema marker "NKGRANT1"
12-byte fresh CSPRNG nonce
AES-256-GCM ciphertext + 16-byte authentication tag
```

The AEAD authenticated associated data binds this exact application's
`notion-knowledge/notion-oauth-grant/v1` namespace. The *entire* JSON
record is encrypted, including token material and identity metadata:

```text
schema: 1
grant_id: 128-bit random UUID-like opaque hex ID (new for each save)
epoch: monotonically increasing u64 per replacement
notion_client_id, workspace_id, owner_user_id, bot_id
issued_at_unix
expires_at_unix: optional
access_token
refresh_token: optional
```

The callback parses optional upstream `expires_in` (1 second through one
year), which is recorded as `issued_at_unix + expires_in`. When Notion
doesn't supply a lifetime, the expiry is **explicitly unknown**, not
invented. Stored expiry, if present, is checked on load. The grant is still
not granted to MCP until #124/#127 verify live validity and revocation.

Payloads are limited to 32 KiB. Encrypt first, write to a `0600`
temporary file in the same private directory, `fsync` contents, atomically
rename, then `fsync` the directory. Existing state is validated before
saving/replacing; corrupt/foreign grant state is never overwritten
silently. Secrets are absent from `Debug`, structured logs and audit
responses. Test fixtures assert that neither Notion tokens nor owner IDs
appear in plaintext on disk.

## Boundary and limitations

- The internal callback-to-store boundary from #298 and the installed
  browser callback require a matching confidential Notion client before code
  exchange, then save verified grants atomically. Browser callbacks also
  require the independent secure cookie bound to the single-use Notion state. A preexisting bot identity must match
  the new verified grant; switching integration bots requires an explicit,
  controlled state reset rather than a silent account change.
- The store only accepts an already typed, verified `NotionGrant` and
  independently compares owner+workspace against the immutable operator
  policy before storing. Every load rechecks the configured Notion client,
  workspace and authorizing user. Replacing a grant rotates its `grant_id`
  and increases the epoch; no caller automatically inherits the new grant.
- **An encrypted file is not proof of a live OAuth grant.** The store cannot
  mint a ChatGPT/MCP token. The callback may call `AuthorizationFlow::approve`
  only after successful persistence and a current-generation check. Code
  redemption must re-read the current approved grant; #127 must enforce live
  resource authorization, revocation and root-policy checks.
- Refresh rotation preserves the grant ID and epoch, atomically replaces the
encrypted access/refresh pair and compares the expected prior token snapshot.
See [#124 refresh](notion-oauth-refresh.md) for why a shared in-process
coordinator is required and why refresh failures deny stale credentials.

The current writer uses a process-local mutex. **Multi-process
  write fencing, crash-safe refresh/rotation, rollback prevention,
  and durable revocation** remain open in #124/#127; do not share the
  state file between active writers. An attacker with write access
  to the file and an older authenticated copy could roll back epochs:
  strictly protect file integrity and implement external durable
  epoch/revocation fencing before remote production use.
- For a multi-pod deployment use one coordinated secret manager/transaction
  store; do not put the local encrypted file on an unsafe shared filesystem.
  Kubernetes Secret mounts are suitable for the key when their access
  permissions are restricted, but the exact 32-byte raw key and file
  permissions must match the above contract.

## Tests

```sh
cargo test -p notion-knowledge-server notion_grant_store --locked
cargo test -p notion-knowledge-server sealed_grant_configuration --locked
cargo fmt --all -- --check
```

Tests cover encrypted persistence and reload, stable random IDs/epoch
rotation, owner binding, changed client/secret key, ciphertext tampering,
symlink and unsafe permissions, missing key, and startup configuration.


## Final storage integration assessment (#123)

The original staged store and internal callback commit were insufficient to
prove installed persistence. The public composition merged in
[PR #425](https://github.com/mtandersson/notion-knowedge/pull/425) now supplies
that missing boundary: the HTTP server constructs the credential runtime,
passes its exact shared store to the browser login adapter, and refuses to
mount a usable callback without configured sealed storage. Missing state is
an unapproved login, not authentication. Existing corrupt or foreign state
fails startup, and state corrupted after startup cannot be overwritten by a
callback or used to obtain an MCP code.

| Storage acceptance requirement | Current evidence |
| --- | --- |
| Access/refresh credentials encrypted at rest | `GrantStore::write_record` seals the complete record with AES-256-GCM, a fresh nonce and authenticated application/version binding; `encrypted_round_trip_is_durable_and_secrets_never_on_disk` checks ciphertext, reload and absence of plaintext credentials/identifiers. |
| Workspace, authorizing user and expiry metadata retained | Typed `NotionGrant` fields are encrypted together in `Record`; each load rechecks the configured Notion client/user/workspace. `None` expiry remains explicitly unknown. |
| Tokens absent from logs and audit | Credential containers redact Debug and are not transport-serializable; provider failures are discarded; callback HTTP errors are static, with no provider body, query, identity, secret or path. |
| Explicit schema/version | Magic `NKGRANT1`, schema `1`, AEAD namespace and strict record deserialization bind the format. Wrong schema, key or tampering fails closed. |
| Safe missing/corrupt startup state | `Config::validate_grant_store` runs before HTTP, stdio and `--check`. Missing state means no approved grant; missing key, unreadable/corrupt state, wrong owner/client or unsafe permissions fail. `sealed_grant_startup_rejects_unsafe_state_before_check_or_transport` checks real binary refusal and sanitized stderr for missing/wrong key, corrupt/foreign state, unsupported schema and unsafe state permissions; store/configuration tests cover the remaining path/format boundaries. |
| Installed confidential callback persists before MCP approval | `browser_callback_persists_allowlisted_grant_then_redirects_with_bound_code` exercises the production HTTP router; `failed_sealed_commit_never_returns_an_mcp_authorization_code` proves a bad commit returns no redirect/code and preserves suspicious state. Binary tests verify full configured callback wiring and refusal without storage. |

Reproduce the relevant checks in the pinned environment:

```sh
nix develop --command cargo test -p notion-knowledge-server --locked
nix develop --command cargo fmt --all -- --check
nix develop --command cargo clippy -p notion-knowledge-server --all-targets --locked -- -D warnings
```

This assessment completes the installed **storage** contract. It does not
claim that discovery, a saved grant or an MCP authorization code permits a
private tool call. Provider-compatible refresh, writer/revocation fencing,
code redemption, token/session enforcement and complete end-to-end production
acceptance remain tracked by #117. In particular, the current file writer is
process-local: the deployment must not introduce concurrent independent
writers or restore an old grant as a way to restore authorization.

### Provider lifetime and rotation

Notion's documented refresh response may omit `expires_in`; sealed
`expires_at_unix` remains absent in that case. Before use, the shared refresh
coordinator requires positive provider introspection of that exact access token.
Its separate 24-hour proactive rotation cadence is server policy, not provider
expiry. Both rotated tokens are committed together while grant ID and epoch
remain stable. Inactive or ambiguous unknown-expiry grants deny without rotation.
See [refresh and active-state policy](notion-oauth-refresh.md).
