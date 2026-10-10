# Identity and health diagnostics

The root `Cargo.toml` workspace package version is the release source of truth.
The shared core identity supplies the MCP `serverInfo`, startup diagnostic,
`--version` and health JSON. `--version` works without runtime configuration.
Container builds must pass the same Cargo version as `VERSION`; the Dockerfile
rejects a missing or mismatched version instead of labelling a different binary.

## Running-process health

With `--http`, the server exposes three unauthenticated orchestration/diagnostic
endpoints on the same listener. They are deliberately separate so an upstream
Notion outage cannot create a container restart loop.

### Liveness: `GET /livez`

Liveness answers only whether the HTTP process can serve requests. It does not
inspect Notion or the local index and therefore remains HTTP 200 when either is
degraded:

```json
{
  "server": {"name": "notion-knowledge", "version": "0.1.0"},
  "transport": "http",
  "status": "alive"
}
```

Use liveness for restart decisions and the image-level Docker `HEALTHCHECK`.
Responses use `Cache-Control: no-store`.

### Readiness: `GET /readyz`

Readiness gates traffic on required **local** serving dependencies. Today that
means the retrieval index snapshot. The remote Notion API is intentionally not
a readiness dependency: temporary upstream failure must not remove an otherwise
usable local knowledge service from rotation.

A ready response is HTTP 200 with `status: "ready"`. An unavailable, unknown,
or unconfigured local index returns HTTP 503 with `status: "not_ready"`:

```json
{
  "server": {"name": "notion-knowledge", "version": "0.1.0"},
  "transport": "http",
  "status": "not_ready",
  "dependencies": {"index": "unavailable"}
}
```

The current bootstrap has no index adapter, so `/readyz` deliberately returns
503 until #145 wires the production index. Future required local dependencies
must be added to this gate explicitly rather than inheriting aggregate upstream
health implicitly.

### Dependency diagnostics: `GET /health`

`/health` remains the full dependency snapshot for operators and diagnostics.
Both Notion and the index must be healthy for HTTP 200; other combinations return
HTTP 503 while preserving each dependency state:

```json
{
  "server": {"name": "notion-knowledge", "version": "0.1.0"},
  "transport": "http",
  "access": {"read_only": true},
  "status": "degraded",
  "dependencies": {"notion": "unconfigured", "index": "unavailable"}
}
```

Dependency states are `healthy`, `unconfigured`, `unavailable`, and
`unknown`.

The `access.read_only` boolean reports the effective `NK_READ_ONLY` MCP
tool-access policy (default `true`). In read-only mode the shared MCP
handler advertises and dispatches only the audited read tools, and rejects
mutation/file-upload tool names before reaching backend adapters. Explicit
`NK_READ_ONLY=false` does not make currently unimplemented write tools
available. This flag does not control webhook admission or operator CLI
actions; those have independent authorization boundaries.

All three endpoints use bounded in-memory snapshots and perform no upstream I/O. Their JSON is allowlisted and cannot contain configuration values,
tokens, upstream error strings, document content, or signed URLs.

For orchestration, use `/livez` for restart/liveness, `/readyz` for traffic
readiness, and `/health` only when the complete dependency picture is useful.
For example, Kubernetes can probe the HTTP listener with an explicit
`Host: 127.0.0.1:3000` header when the service binds `0.0.0.0:3000`.

## Access policy

`/livez`, `/readyz`, and `/health` use the same listener as MCP. No credential or session is required.
Host must name the configured IP or `localhost`, `127.0.0.1`, `::1`
(IPv6 uses authority brackets). Host ports may differ from the listening port
for container publication, as on the MCP route. If supplied, Origin
must exactly match `http://<configured authority>` or
`http://localhost:<configured port>`; callers without Origin are accepted.
Disallowed values return 403 without diagnostics. These route guards are
explicit because the MCP SDK gates apply only inside `/mcp`. Restrict network
access to operators and probes, and do not expose this endpoint through a
public proxy by default. Host/Origin checks are not authentication.

## One-shot diagnostics

```sh
notion-knowledge-server --diagnostics
```

This validates configuration and writes the same JSON with `transport` set to
`one-shot`, then exits 0. Invalid configuration exits 2 with a redacted error.
It reports the composition of this new process; it does not contact an existing
server, open HTTP, authenticate to Notion or inspect an index. Do not use it as
a running-container probe. Stdio stdout remains exclusively MCP protocol output
during normal operation; only the explicitly requested one-shot modes write
plain diagnostic output.

## Verification

```sh
cargo test -p notion-knowledge-core --locked
cargo test -p notion-knowledge-server --locked
```

Tests cover process liveness, local readiness, the healthy/degraded dependency
matrix, runtime probe changes, Host/Origin access policy, bootstrap unavailable
states, release identity and secret/signed-URL omission in one-shot output.
