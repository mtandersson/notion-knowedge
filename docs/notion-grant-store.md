# Encrypted Notion OAuth grant store (#123)

**Status:** server-internal persistence, **not** public login or authorization.
The OAuth discovery router remains fail-closed: `/authorize` and `/token`
return 503, and `/mcp` returns 401 in OAuth mode. The grant store is NOT
connected to MCP bearer issuance or live Notion calls. #124/#127/#128 must
implement serialized token refresh, durable revocation and every-request grant
and session enforcement before private HTTP access.

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
and `--check` execution when the store is configured. Startup with an
expired grant also fails closed, requiring operator-managed reauthorization;
no silent fallback to a new account. Errors contain neither raw paths nor
plaintext secrets.

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
issued_at_unix, expires_at_unix: optional
access_token, refresh_token: optional
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

- The store only accepts an already typed, verified `NotionGrant` and
  independently compares owner+workspace against the immutable operator
  policy before storing. Every load rechecks the configured Notion client,
  workspace and authorizing user. Replacing a grant rotates its `grant_id`
  and increases the epoch; no caller automatically inherits the new grant.
- **An encrypted file is not proof of a live OAuth grant.** This component
  cannot mint a ChatGPT/MCP token and does not call
  `AuthorizationFlow::approve`. #127 must bind these records to live
  application authorization and enforce revocation and root-policy checks.
- The current writer uses a process-local mutex. **Multi-process
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
