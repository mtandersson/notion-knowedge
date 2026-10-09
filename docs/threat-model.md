# MCP, Notion and local-index threat model

Reviewed baseline: 2026-10-02, main `04c638f`, for
[#91](https://github.com/mtandersson/notion-knowedge/issues/91).
OAuth discovery implementation reviewed 2026-10-09 for [#119](https://github.com/mtandersson/notion-knowedge/issues/119);
see [staged OAuth discovery](oauth-discovery.md). OAuth trust-boundary addendum reviewed 2026-10-09 for [#118](https://github.com/mtandersson/notion-knowedge/issues/118);
see [OAuth trust and single-identity binding](oauth-trust-model.md).
This is a security design and release checklist, not evidence that the planned
controls have shipped or permission to expose private knowledge publicly.
[ADR 0001](adr/0001-runtime-and-component-boundaries.md) defines the architecture;
source and tests take precedence over this snapshot.

## Scope and current implementation

The intended deployment is one operator, one explicitly approved Notion user
and workspace, and configured Notion roots. Multi-user isolation is outside
this model; adding it requires a new review. Notion is authoritative. Derived
local content is disposable but remains confidential, including embeddings,
page titles, graph edges, checkpoints and backups.

Explicit HTTPS [OAuth discovery mode](oauth-discovery.md) now advertises RFC 9728/RFC 8414 metadata while blocking all MCP methods with a Bearer discovery challenge and returning 503 on reserved authorization/token/revocation endpoints. This is **not** login or token authorization; the existing anonymous bootstrap is unchanged when discovery is unset. A trusted proxy still must preserve the restricted backend Host/Origin rules. It must not be exposed as a private knowledge service until #120–#130 ship.

The default bootstrap serves stdio/HTTP and the MCP tool catalog; retrieval
calls remain unavailable until adapter composition is enabled. The Notion,
retrieval and embedding adapter implementations exist, but default startup does
not make Notion requests, open index/state databases or load a model. Explicit
commands and composed services have their own documented boundaries.

The opt-in [webhook endpoint](notion-webhooks.md) implements setup candidate
capture, authenticated delivery validation and durable minimized SQLite receipt
with scoped identity deduplication. Verified deliveries are acknowledged after
commit; admission failure returns 503. Scoped authoritative refresh is pending. Application
MCP authorization, OAuth and other release requirements below are not implied
by the existence of adapter implementations or this document.

The [owned commit coordinator](index-commits.md) now supplies cooperative
local index serialization and trusted SQLite/index/table/workspace/scope bindings.
Independent-process tests cover aliases, conflicting state databases, caller
cancellation, lease expiry and process death; checkpoint-failure tests preserve
ambiguous pending work for replay. Local storage owners and all index writers
must participate. This does not isolate raw SQLite/LanceDB writers or malicious
filesystem replacement; actual LanceDB integration remains #257.

Implemented protections and their limits:

- [Startup configuration](../crates/server/src/config.rs) defaults to loopback,
  validates settings and redacts token Debug output and configuration errors.
  `NK_NOTION_AUTH` selects upstream credentials; it does not authenticate MCP
  clients. Token validation checks syntax, not identity or grant scope.
- [HTTP wiring](../crates/server/src/http.rs) retains SDK Host checks and
  configured Origin checks, including DNS rebinding protection. Clients with
  no Origin are accepted; a non-browser caller can supply an allowed Host.
  These checks are not authentication. Sessions are protocol state, not proof
  of identity. The JSON POST validation buffer is capped at 4 MiB for
  `application/json`; this is not complete request/result/session/rate limiting.
  [Transport tests](../crates/server/tests/http.rs) cover those current gates.
- [Health diagnostics](diagnostics.md) expose fixed identity and dependency
  states without tokens, signed URLs or upstream errors. `/health` has explicit
  Host/Origin gates, requires no session or credential, and is intended for
  private operator/probe access. Placeholder adapters report degraded health;
  this is not a liveness endpoint or evidence of application authorization.
- [Container packaging](container.md) runs UID/GID 65532, excludes runtime
  secrets/data from build context and documents loopback publication and a
  read-only filesystem. The reserved volume directories are not yet consumed.
  Non-root execution does not isolate data from the host administrator.
- [CI scanning](security-scanning.md), delivered in
  [#92](https://github.com/mtandersson/notion-knowedge/issues/92), gates known
  vulnerable dependencies and detectable committed secrets. It does not detect
  every leak, unsafe runtime behavior or compromised upstream dependency.

## Assets, actors and trust boundaries

Protect confidentiality of page content and file bytes; credentials and signed
URLs; local index/state/models and backups; tool results, diagnostics and audit
metadata. Protect integrity of Notion mutations, grant identity, scope,
provenance and sync state, plus availability of the service and local machine.

Assume malicious internet callers and websites, an otherwise valid but
unapproved Notion OAuth user, and adversarial page/file content (including
content placed by a collaborator). The authorized agent can make mistakes or
follow injected instructions. Dependency/model publishers and the embedding
service are additional supply-chain/data-processing principals. The operator,
host OS and selected HTTPS proxy are trusted to protect secrets and storage;
host compromise remains outside the application's containment guarantee.

```text
Untrusted caller/browser -- HTTPS + MCP token --> trusted proxy/server boundary
Trusted local MCP client -- process stdin/stdout --> server (stdio boundary)
Server -- separate Notion grant --> Notion API (external authority)
Notion -- authenticated webhook --> event queue --> scoped source refresh
Server <--> derived LanceDB / SQLite state / protected grant store (host storage)
Server -- validated temporary HTTPS URL --> file source --> Notion upload
Server -- document chunks --> local model OR explicit remote embedding provider
Server -- minimized tool results --> MCP client / ChatGPT (external recipient)
```

These flows describe the target composition. Stdio/HTTP and opt-in webhook
setup/authentication and durable inbox receipt are shipped; automatic webhook
processing and scoped refresh remain pending. Each arrow
crosses a boundary even when the same process wires the components. Content
crossing from Notion, files or retrieval into the agent is data, never an
instruction or authorization grant. Sending a tool result to ChatGPT transfers
that information outside the local machine; local indexing alone does not make
the full workflow local or private.

## Threats and mitigation order

The order below starts with exposure and credential compromise, then privileged
application actions and ingestion, then persistence and operations. All linked
mitigation tickets remain open at this baseline unless explicitly marked shipped.
These are requirements for the affected capability before production enablement,
not priority labels or a claim that completing one ticket secures the system.

| Boundary / attack path | Required mitigation and backlog | Verification needed before enablement |
| --- | --- | --- |
| Remote caller initializes a session or replays a stolen token to read/mutate knowledge; HTTPS proxy is mistaken for authorization | Production OAuth under [#117](https://github.com/mtandersson/notion-knowedge/issues/117): separate MCP and Notion credentials, server-scoped access tokens, strict issuer/origin and redirects, PKCE S256, short-lived single-use codes, random expiring state. [#118](https://github.com/mtandersson/notion-knowedge/issues/118) defines the [separate MCP/Notion trust and grant binding contract](oauth-trust-model.md); [#119](https://github.com/mtandersson/notion-knowedge/issues/119)–[#122](https://github.com/mtandersson/notion-knowedge/issues/122) implement discovery and both flows. Static bearer ([#84](https://github.com/mtandersson/notion-knowedge/issues/84)) is development/fallback only, not the production design. | Reject unauthenticated calls on every operation and session/SSE path; reject bad redirects/state/verifiers and expired/replayed codes; verify the deployed HTTPS proxy and discovery configuration with [#129](https://github.com/mtandersson/notion-knowedge/issues/129), [#130](https://github.com/mtandersson/notion-knowedge/issues/130) and [#109](https://github.com/mtandersson/notion-knowedge/issues/109). |
| An unapproved user completes valid Notion OAuth, or a replaced grant leaves an old MCP token usable | Stable `workspace_id` and `owner.user.id` must match explicit configuration, with missing/mismatched identity failing closed ([#125](https://github.com/mtandersson/notion-knowedge/issues/125), [#126](https://github.com/mtandersson/notion-knowedge/issues/126)). Bind every token/session to that approved grant ([#127](https://github.com/mtandersson/notion-knowedge/issues/127)); rotation must be atomic and revalidate identity ([#124](https://github.com/mtandersson/notion-knowedge/issues/124)); revocation/re-authentication must invalidate incompatible access ([#128](https://github.com/mtandersson/notion-knowedge/issues/128)). | End-to-end denied user/workspace, absent identifiers, concurrent rotation, replaced grant, revoked token and documented bounded revocation-cache tests. Successful OAuth alone must never authorize a caller. |
| Notion token, refresh token or signed URL leaks through storage, logs, errors or tool output | Tokens remain server-side and never enter LanceDB or MCP results (ADR 0001). [#22](https://github.com/mtandersson/notion-knowedge/issues/22) verifies integration credentials; [#123](https://github.com/mtandersson/notion-knowedge/issues/123) requires encrypted grant storage or a secure secret store. Configuration redaction is shipped; central structured secret/URL redaction is still [#89](https://github.com/mtandersson/notion-knowedge/issues/89). Minimize logging/audits with [#90](https://github.com/mtandersson/notion-knowedge/issues/90), [#106](https://github.com/mtandersson/notion-knowedge/issues/106), [#107](https://github.com/mtandersson/notion-knowedge/issues/107). | Inspect success/failure/retry logs, tool results, DB records and backups for representative tokens, URL query parameters and private bodies; corrupt/missing grant storage must fail safely. Revoke exposed credentials; deleting a log/commit is insufficient. |
| Injected page text tells the agent to export secrets, attach a private file elsewhere or overwrite a page; direct page ID bypasses root checks | Treat retrieved text as untrusted and preserve provenance ([#49](https://github.com/mtandersson/notion-knowedge/issues/49)); enforce root scope across reads, search expansion, writes and files at the application boundary ([#87](https://github.com/mtandersson/notion-knowedge/issues/87), [#50](https://github.com/mtandersson/notion-knowedge/issues/50)). Read-only and operation policy are server-enforced ([#85](https://github.com/mtandersson/notion-knowedge/issues/85), [#86](https://github.com/mtandersson/notion-knowedge/issues/86)), not tool annotations. Explicit targets/replacement semantics are [#26](https://github.com/mtandersson/notion-knowedge/issues/26), [#27](https://github.com/mtandersson/notion-knowedge/issues/27), [#71](https://github.com/mtandersson/notion-knowedge/issues/71), [#72](https://github.com/mtandersson/notion-knowedge/issues/72). | Deny out-of-scope direct IDs and stale/unverifiable ancestry; exercise malicious document instructions and disabled tools through both transports, proving no backend mutation. Agent-side confirmation is useful but cannot replace these controls or prevent all authorized-client exfiltration. |
| An authorized agent overwrites a concurrent edit, repeats a timed-out mutation or treats an unverified write as success | Narrow semantic writes ([#76](https://github.com/mtandersson/notion-knowedge/issues/76)), conflict detection against expected source metadata ([#80](https://github.com/mtandersson/notion-knowedge/issues/80)), payload-bound idempotency keys with explicit TTL ([#81](https://github.com/mtandersson/notion-knowedge/issues/81)), separately disabled/confirmed destructive actions that fail closed when confirmation is unavailable ([#82](https://github.com/mtandersson/notion-knowedge/issues/82)). Source read-back precedes final success; reindex outcome is reported separately ([#83](https://github.com/mtandersson/notion-knowedge/issues/83), file read-back [#74](https://github.com/mtandersson/notion-knowedge/issues/74)). | Concurrent edit returns conflict; timeout/retry does not duplicate append/upload; reused key with different payload is rejected; declined confirmation is never auto-retried. Test source-write success with index failure and unverified attachment failure without hiding either outcome. |
| A signed file URL is replayed, logged, redirected to metadata/internal services or resolves to a private address; large/malicious content exhausts resources | Request-only URL handling and bounded streaming ([#67](https://github.com/mtandersson/notion-knowedge/issues/67)); expected HTTPS sources, DNS destination checks, private/link-local/loopback blocking and redirect revalidation ([#88](https://github.com/mtandersson/notion-knowedge/issues/88)); MIME/content, size and filename checks ([#68](https://github.com/mtandersson/notion-knowedge/issues/68)). Do not forward Notion/MCP authorization headers to file hosts; signed URLs are temporary capabilities, not stable provenance. Temporary bytes must be cleaned on success/failure and only safe metadata retained ([#75](https://github.com/mtandersson/notion-knowedge/issues/75)). | SSRF tests at each resolved connection/redirect, including IPv4/IPv6 and changing DNS; timeout/oversize/truncated input tests; no credential forwarding or URL persistence. File ingestion stays disabled until these controls and scoped upload checks exist. |
| Forged/replayed webhook triggers index poisoning or excessive refreshes; crash loses acknowledged events | A separate verified endpoint ([#52](https://github.com/mtandersson/notion-knowedge/issues/52)), minimized durable event identity/deduplication ([#53](https://github.com/mtandersson/notion-knowedge/issues/53)), debounce and bounded retries ([#54](https://github.com/mtandersson/notion-knowedge/issues/54), [#57](https://github.com/mtandersson/notion-knowedge/issues/57)). Events are hints to fetch authoritative scoped Notion content ([#55](https://github.com/mtandersson/notion-knowedge/issues/55)); never proof of MCP authorization. | Reject invalid verification/signatures without logging secrets; duplicate and interrupted delivery tests; unauthorized page events cannot cause out-of-scope indexing. Verify the actual upstream verification protocol during #52 implementation. |
| Deleted/moved/revoked content remains searchable; tampered cache or checkpoint becomes authoritative | Shared scope policy ([#87](https://github.com/mtandersson/notion-knowedge/issues/87)), tombstones/deletes ([#42](https://github.com/mtandersson/notion-knowedge/issues/42), [#56](https://github.com/mtandersson/notion-knowedge/issues/56)), reconciliation ([#58](https://github.com/mtandersson/notion-knowedge/issues/58)) and explicit fresh reads ([#51](https://github.com/mtandersson/notion-knowedge/issues/51)). SQLite/LanceDB schema and compatibility checks ([#36](https://github.com/mtandersson/notion-knowedge/issues/36), [#37](https://github.com/mtandersson/notion-knowedge/issues/37), [#43](https://github.com/mtandersson/notion-knowedge/issues/43)); recovery procedure ([#111](https://github.com/mtandersson/notion-knowedge/issues/111)). | Deny results outside current scope, including stale caches; test move/delete, corrupted/incompatible state and authoritative rebuild. Protect host volume/backup permissions and encryption according to operator policy; rebuildability does not erase disclosed content or secure backups. |
| Session/SSE floods, tool expansion, file bursts or webhook queues exhaust CPU, memory, disk or upstream quota | Comprehensive payload/result limits ([#93](https://github.com/mtandersson/notion-knowedge/issues/93)), bounded file counts ([#73](https://github.com/mtandersson/notion-knowedge/issues/73)), backend retry/rate control ([#23](https://github.com/mtandersson/notion-knowedge/issues/23)), queue retries and shutdown ([#57](https://github.com/mtandersson/notion-knowedge/issues/57), [#112](https://github.com/mtandersson/notion-knowedge/issues/112)). Deployment must also bound session concurrency, request time and storage; current buffer cap alone does not cover these. | Load/failure tests with finite queues, deadlines, connections and resource quotas; confirm shutdown preserves acknowledged work. Session/concurrency quotas require explicit implementation/review before remote exposure, even if payload tests pass. |
| Dependency/model compromise executes code; remote embedding sends private chunks outside the host | Shipped [#92](https://github.com/mtandersson/notion-knowedge/issues/92) scans and pinned builds reduce known supply-chain risk. [#38](https://github.com/mtandersson/notion-knowedge/issues/38), [#39](https://github.com/mtandersson/notion-knowedge/issues/39), [#145](https://github.com/mtandersson/notion-knowedge/issues/145) must review model provenance/loading, external assets and runtime permissions. ADR 0001 allows remote providers; enabling one requires explicit operator data-sharing approval and a refreshed model. | Validate actual model assets/runtime in the final image, least filesystem/egress privileges and dependency exceptions. Do not load untrusted executable model code or assume embeddings are anonymous. Document recipient, retention and transmitted fields before any remote provider use. |

## OAuth trust-binding addendum (2026-10-09, #118)

The full target authentication chain is defined in [oauth-trust-model.md](oauth-trust-model.md).
It does **not** describe shipped controls. Notion's OAuth token response must
identify **both** the explicitly configured `workspace_id` and `owner.user.id`;
neither an OAuth callback, email, workspace name, bot ID nor the first login may
enroll or replace the allowed identity. A valid upstream Notion grant is necessary
but insufficient to authorize a ChatGPT MCP token. MCP tokens have their own
issuer, **canonical MCP resource/audience** (RFC 8707), client/scopes, expiry,
revocation identity and durable Notion grant/authorization-epoch binding. A
foreign-audience or upstream Notion bearer must never authenticate MCP requests.

Every HTTP operation and continued SSE/session use must revalidate the live
token/grant binding independently of session IDs and Host/Origin checks.
Grant revocation/replacement, changed identity or root policy, ambiguous Notion
refresh, and corrupt/unavailable authorization storage fail closed. Concurrent
refresh cannot undo logout/revocation; stale sessions cannot silently adopt a
new grant. Before exposure, #119–#130 must verify these denial paths, code +
PKCE S256, single-use state/code, redirect and token binding, upstream secret
storage, session invalidation, bounded revocation and safe proxy discovery.
These are **requirements**, not a claim that the current HTTP server enforces
MCP authorization.

## OAuth discovery boundary review (2026-10-09, #119)

The new [OAuth discovery router](oauth-discovery.md) activates only when the
operator supplies **both** canonical HTTPS origins. It publishes fixed metadata
and a 401 Bearer resource-metadata challenge on every MCP request instead of
mounting the protocol service. Authorization, token and revocation endpoints
are explicitly not implemented (503), so this does not make the deployment
OAuth-capable. Invalid/ambiguous origins fail startup; `Host` and `Origin`
spoofing are denied for the new routes. This code has no Notion credentials,
no grant storage, no issued tokens and no access to protected MCP tools.
Integration tests exercise real HTTP discovery and deny paths. Remaining
risk: callers may discover an unavailable AS; reverse-proxy HTTPS and the
actual OAuth authorization implementation remain future work, not a security
waiver. Record changed routing and testing with #119 and keep #117 open.

## Notion upstream OAuth redirect boundary (2026-10-09, #121)

The [Notion redirect component](notion-oauth-redirect.md) is currently only a
server-side helper. It binds the fixed Notion `owner=user` authorization URL
to a separate 256-bit random, short-lived, single-use state and a separately
validated MCP PKCE pending transaction. The redirect URL does not expose the
MCP transaction, confidential Notion client secret or any bearer token.
The exact registered HTTPS callback must match the canonical MCP issuer;
no browser-driven callback/Host/Forwarded override is permitted.
Notion's integration permissions are configured out of band, and users select
shared content at consent; there is no request-scoped elevation of permissions.
The redirect helper cannot establish the real authorizing Notion identity.
Until #122 supplies the provider callback, authoritative token exchange,
allowlisted owner/workspace validation and #123–#129 bind live durable grant
and MCP sessions, the public auth/token endpoints stay 503 and MCP stays 401.
Treat callback state and query strings as secrets in future proxy logging.

## Confidential Notion OAuth callback review (2026-10-09, #122)

[Notion callback verification](notion-oauth-callback.md) consumes the
independent expiring, single-use state **before any outbound token exchange**.
Only the confidential server can send the Notion client secret in Basic auth
to a fixed HTTPS endpoint, with redirects disabled, 12-second timeouts and
bounded response bodies. Parsed grants require `owner.type=user` and exact
operator-pinned workspace/user IDs before returning a still **unapproved**
MCP transaction. Typed secrets have redacted diagnostics; provider errors and
callback query values are never logged or returned. Testing uses a local
fake Notion provider and negative-path cases; it does not expose a live HTTP
callback. A successfully exchanged upstream token does not create an MCP
authorization until #123/#127 store and enforce durable grant/epoch and
revocation. This component is **not** permission to expose private MCP tools.

## Bounded residual risks and release decision

The documented bootstrap operating envelope accepts local protocol access
without MCP authentication only while the catalog is empty, no private source
is read and no mutation/file/webhook/index capability exists. Use a trusted
local stdio client, or loopback HTTP/loopback-published container on a trusted
single-operator host. Untrusted local processes can still contact that HTTP
listener and exhaust resources. Host administrators can read process memory,
environment and mounted files; Debug redaction is not secret-memory isolation.
These are explicit local-development assumptions, not production risk acceptance.

A proxy with HTTPS alone does not meet remote-access requirements. Do not expose
this unauthenticated bootstrap to the internet or enable private knowledge tools
under its current access policy. Production release requires the authentication
and identity-binding chain, scoped operations, privacy controls and bounded
resources above, plus the protections for each enabled ingestion/storage path.
Open backlog links are tracking references, not automatic waivers. Read-only
mode reduces mutation risk but still permits disclosure of every readable result.

Even with planned controls, an approved MCP client can copy returned information
and an approved user can authorize a harmful in-scope operation. Root scope
cannot prevent all prompt injection. Upstream outages, compromised host/proxy
and unknown supply-chain flaws remain residual risks. Before production, the
operator must record the actual configuration, enabled capabilities, evidence,
remaining risks, owner and review/expiry date in the release issue. Only the
operator can accept that deployment's risks; this document grants no waiver.
Rotate/revoke exposed MCP and Notion grants and review local copies/backups
when responding to disclosure.

## Architecture change review

The author of a boundary-changing PR owns updating this model and its mitigation
links; an independent reviewer checks it before merge. The deployment operator
owns release evidence and any explicit residual-risk acceptance. Review is
required when adding a semantic tool, changing OAuth/grant/session handling,
root scope, webhook/file ingestion, database/grant storage, embedding/model
provider, logs/telemetry, proxy/network exposure or container/worker topology.
Also review after an incident, new relevant advisory or changed upstream protocol.
[#118](https://github.com/mtandersson/notion-knowedge/issues/118) adds the
[documented OAuth trust model](oauth-trust-model.md) and the dated review above.
Subsequent #119–#130 implementations must revisit this baseline and record
negative-path evidence before production enablement.

Every affected PR must identify changed assets/actors/flows; update current
versus planned controls; link mitigation issues and negative-path evidence;
state remaining risks and enabled deployment envelope; and request independent
security review. Record the PR/date/baseline here when material assumptions
change. Follow the [ADR process](adr/README.md) for architectural decisions;
update this model in the same PR, rather than rewriting an accepted ADR's history.
A reviewer should block capability enablement when a required mitigation is
missing, even if ordinary CI is green. Security controls should gain behavioral
negative-path tests in their implementation tickets; this documentation change
adds no tests that merely assert prose.

The opt-in [webhook boundary](notion-webhooks.md) implements bounded raw-byte
HMAC verification and separate candidate setup capture. Unsigned setup does not
establish trust. Verified HTTP admission commits minimized hints to SQLite before
acknowledgement. Duplicate and abrupt-restart tests cover retained pending work;
claim generations fence completion after recovery. Queue retention/concurrency
limits and authoritative scoped refresh remain release gates. Deleting SQLite
state loses pending hints even though authoritative Notion content survives.
