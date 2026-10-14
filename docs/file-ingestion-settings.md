# Startup file ingestion admission

The installed upload contract accepts a required canonical lowercase UUID `target`
and optional caption (at most 2,000 Unicode characters, no controls or directional
formatting characters), separately from the legacy `file` or ordered `files` input.
Input references confer no authority. Runtime download/attachment remains unavailable
until the audited ingestion composition ships; `--check` validates settings only.

`NK_MAX_UPLOAD_FILES` is parsed once at startup, accepts 1–16 (default 4), and
sets both discovery `maxItems` and request admission. Invalid values fail startup.
Standalone MCP handlers have the same deterministic default. The narrower MCP
4/16 policy is compatible with the reusable core's 5/20 policy; future composition
must explicitly pass the admitted MCP maximum to the core batch policy.

Ingestion settings require explicit `NK_FILE_INGESTION=true`, `NK_READ_ONLY=false`,
`NK_NOTION_AUTH=integration`, a token and the existing full authoritative physical
workspace/root/exclusion configuration. Exclusions remain server-owned; an empty
exclusion policy is valid. No request can supply provider endpoints or authority.
Settings below without explicit ingestion opt-in fail startup.

| Setting | Contract |
| --- | --- |
| `NK_FILE_SOURCE_HOSTS` | Required, comma-separated 1–32 exact DNS names for HTTPS sources; no URL, wildcard, port or IP |
| `NK_FILE_TIMEOUT_SECONDS` | 1–120; default 30 |
| `NK_FILE_PLAN` | `free` (default) or explicit `paid` |
| `NK_FILE_MAX_BYTES` | Positive; default 5 MiB; free capped at 5 MiB, paid at 100 MiB |
| `NK_FILE_MULTIPART_THRESHOLD_BYTES` | Paired with part bytes, paid only, 5–20 MiB, strictly below maximum |
| `NK_FILE_PART_BYTES` | 5–20 MiB and no larger than threshold; existing adapter enforces at most 1,000 parts |
| `NK_FILE_AUDIT_FILE` | Required absolute durable file path, existing private owner directory, no symlink ancestors; existing file must be owner-controlled and private |
| `NK_FILE_AUDIT_RETENTION_DAYS` | 1–3,650; default 30 |

Paid limits through 20 MiB may use single-part upload; higher limits require
multipart settings. The maximum 100 MiB and minimum 5 MiB part size imply at most
20 parts. Configuration rejects contradictory bounds before serving. Audit paths
must be in persistent private operator storage, separate from disposable indexes;
validation checks metadata without opening or initializing the database. Current
path validation requires Linux `/proc/self` ownership evidence and fails closed
elsewhere. Runtime activation must still open durable audit storage successfully
before any effects and retain transport authentication and operation-policy gates.

All configuration errors use setting names and static reasons. File request Debug
is fully redacted; signed URLs, hints, captions and rejected target values are never
copied into MCP errors or logs. No OpenAI API call or credential is needed to validate
this contract. Run `cargo test -p notion-knowledge-server -p notion-knowledge-mcp`
and `cargo run -p notion-knowledge-server -- --check` in the pinned Nix environment.
