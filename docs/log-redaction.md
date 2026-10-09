# Safe logging and URL/credential redaction (#89)

Sensitive page text, upstream HTTP response bodies, request bodies, webhook
payloads, full download URLs, signed Notion URLs and raw authorization values
**must never be passed to logs or error objects**. Prefer typed errors with a
fixed operation and error class, as in the Notion backend. The sanitizer below
is a final defense, **not** permission to log arbitrary private data.

## Shared utility

Use \`notion_knowledge_core::redaction::redact_for_log\` at a diagnostic
boundary when formatting a potentially untrusted error. The CLI entrypoint
routes printable startup/HTTP/operator errors through its \`safe_error\` helper.
This protects against a future upstream error type accidentally embedding a
URL or a recognizable token. It does not modify MCP results, Notion responses
or authoritative search data.

The sanitizer:

- replaces complete \`http://\` and \`https://\` URLs, including query strings
  and signed download parameters, with \`[REDACTED_URL]\`;
- replaces \`Bearer\` values, common credential assignment/header fields
  (including \`NOTION_TOKEN\`, \`access_token\`, \`refresh_token\`,
  \`client_secret\`, \`api_key\`, \`authorization\`) and recognizable
  \`secret_\`, \`ntn_\`, \`sk-\` token forms with \`[REDACTED_SECRET]\`;
- bounds the resulting diagnostic to 4,096 Unicode characters plus a
  truncation marker. Nonsecret UTF-8 diagnostic context is preserved.

**Limitations:** Unknown token formats, values without a recognizable key or
prefix, arbitrary JSON/page content and private text can still be sensitive.
Do not dump upstream errors, headers, file bodies, page titles or JSON just
because the sanitizer is present. In particular, existing Notion transport
errors intentionally discard HTTP response bodies and return an allowlisted
error class instead. The log sanitizer is intentionally **not** used to turn
private source URLs into acceptable structured audit identifiers.

## Verification

From the pinned development shell:

\`\`\`sh
nix develop --command cargo test -p notion-knowledge-core redaction --locked
nix develop --command cargo test -p notion-knowledge-server startup_error_path --locked
nix develop --command cargo fmt --all --check
nix develop --command cargo clippy --workspace --all-targets --locked -- -D warnings
\`\`\`

The unit fixtures cover Notion tokens, bearer authorization, JSON grant fields,
ChatGPT temporary download URLs, Notion signed-file URLs, UTF-8 diagnostic
text and bounded logging. A server binary test exercises the same sanitizer
as the actual error-printing helper. Tests contain synthetic values only.
