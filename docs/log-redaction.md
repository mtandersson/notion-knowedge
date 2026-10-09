# Operator diagnostic redaction (#89)

Short stderr diagnostics must pass through the server's
\`redact::diagnostic\` formatter. It is applied to error output from the
configuration, local webhook recovery, Notion identity/crawl and HTTP
composition-root paths. It is intentionally *not* a general data-loss
prevention system.

The formatter removes:

- Whole HTTP(S) URLs (including query parameters and private path segments),
  e.g. Notion temporary signed links and ChatGPT file download URLs.
- Bearer/Basic authorization values and common OAuth/token/key-value fields,
  including quoted JSON, headers and raw query-like fragments.
- Common Notion integration token prefixes (\`ntn_\`, \`secret_\`).
- The exact values of the operator's integration, webhook verification and
  Notion OAuth client secrets, even if the values use unrecognized formats.
- Newlines in diagnostic strings, to avoid forging additional log records.

The formatter preserves ordinary non-sensitive error text. Tests include
signed URL query strings, arbitrary operator tokens, HTTP authorization,
OAuth JSON and multiline output.

## Boundaries

**Do not log raw headers, callbacks, signed download URLs, request bodies,
Notion provider responses, encrypted grant state or private page content.**
Structured error kinds and operation identifiers remain the primary
diagnostic surface. Sanitization is a defense-in-depth output boundary, not
permission to pass untrusted payloads to the logger.

The exact-value protection covers the configured *environment* secrets above.
Rotated upstream OAuth access/refresh tokens are **not** obtainable via this
allowlist: keep using their non-serializable, redacted types and never log raw
provider values. Future logging/telemetry (#106), file ingestion (#67/#75) and
privacy-audit work (#90) must reuse this formatter for any
operator-visible string, add relevant registered secrets, and include
negative-path regression tests.

This utility sanitizes only user-facing/operator-facing diagnostics; it does
not sanitize intentional authoritative crawl JSON output, persistent SQLite
state or MCP tool results. No guarantee is made about independent proxy or
third-party loggers, which must be configured separately.

## Verification

\`\`\`sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p notion-knowledge-server --lib --locked redact
cargo test --workspace --locked
\`\`\`
