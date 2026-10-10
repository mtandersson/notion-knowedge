# Request-scoped temporary file downloads (#67)

`notion_knowledge_notion::file_download::DownloadPolicy` is the reusable
download boundary for ChatGPT file references. It does not enable the upload
tool, grant Notion scope, or call an upload API. Authenticated ingestion must
invoke this boundary before #68 validation and #69–#75 attachment.

Construct the policy from **operator-controlled exact source hostnames**, a
positive byte limit and a whole-request deadline (up to 120 seconds). Do not
derive allowed hosts or limits from the tool request. Confirm intended file
providers before setting the allowlist; there is no wildcard or arbitrary-URL
fallback. Example policy: 5 MiB and 30 seconds for a verified source host.

`download(&temporary_url)` accepts HTTPS on port 443 only, with no userinfo,
fragment, whitespace or control characters. It resolves the allowed hostname
fresh for the active request, rejects mixed public/private DNS answers, and
pins all admitted addresses into a new request-local client. TLS still
authenticates the original hostname. IP literals, loopback, private, link-local,
multicast, reserved/documentation and transition ranges are denied. IPv6 uses
global unicast only. No environment proxy, automatic redirects, Referer,
credential headers, cookie jar or automatic retry is enabled.

Only HTTP 200 complete, uncompressed responses are admitted. Declared length
is checked before reading; each received chunk is counted independently, so
chunked bodies cannot bypass the configured cap. Partial responses, compressed
responses, empty files and truncated transfers fail. DNS, connection and the
entire streaming operation are covered by the deadline.

The body is streamed to a random private tempfile rather than accumulating a
full body in memory. `DownloadedFile` owns that file and exposes its measured
size and a read handle. Keep ownership through validation and upload; dropping
it unlinks the tempfile. Failed or cancelled operations also drop ownership.
The source URL, source filename, response headers and raw HTTP errors are never
stored in the result, logs, an audit event or SQLite. This API does not log.
Process termination can leave a private temporary file: operational cleanup
and any lifecycle beyond request ownership remain #75. A host crash cannot
provide an RAII cleanup guarantee.

Errors have fixed safe categories. Network/server/429 failures and timeouts are
retryable **within the current request**; authorization, missing/expired source
references require a new file reference. Redirects and validation failures are
not retried. Never queue a signed URL for background retry or include it in
error diagnostics. Receiving bytes is not proof of MIME safety or authorization
to attach them: #68 and the authenticated scope/workflow controls still apply.

Verification:

```sh
nix develop --command cargo test -p notion-knowledge-notion --locked
nix develop --command cargo clippy -p notion-knowledge-notion --all-targets --locked -- -D warnings
nix develop .#format --command cargo fmt --all -- --check
```

Tests exercise the real client/streaming code against local HTTP fixtures for
chunked size limits, complete bytes, redirects, partial/encoded/truncated
responses, status classification, timeouts, cancellation and private-file
cleanup. Tests alone override HTTPS and public-IP admission for the local
fixture; production URL and address policy have separate negative tests.
No real signed URL, private Notion data, credentials or provider request is used.
