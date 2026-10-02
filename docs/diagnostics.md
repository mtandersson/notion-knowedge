# Identity and health diagnostics

The root `Cargo.toml` workspace package version is the release source of truth.
The shared core identity supplies the MCP `serverInfo`, startup diagnostic,
`--version` and health JSON. `--version` works without runtime configuration.
Container builds must pass the same Cargo version as `VERSION`; the Dockerfile
rejects a missing or mismatched version instead of labelling a different binary.

## Running-process health

With `--http`, `GET /health` reports the serving process's current dependency
snapshots, without MCP initialization or a session:

```sh
curl -i http://127.0.0.1:3000/health
```

The response schema is:

```json
{
  "server": {"name": "notion-knowledge", "version": "0.1.0"},
  "transport": "http",
  "status": "degraded",
  "dependencies": {"notion": "unconfigured", "index": "unavailable"}
}
```

Dependency states are `healthy` (a current successful check), `unconfigured`,
`unavailable` (including an absent adapter), and `unknown` (unchecked or stale).
Both dependencies must be healthy for aggregate `healthy` and HTTP 200; all
other combinations produce `degraded` and HTTP 503 with the same JSON schema.
Responses use `Cache-Control: no-store`. They contain only fixed identity,
transport and state labels: no configuration values, tokens, upstream error
strings, document content or signed URLs.

The bootstrap has no Notion or index adapter. Notion is `unconfigured` in auth
mode `none` and `unavailable` in mode `integration`, even with a valid token.
The index is always `unavailable`. Thus the current binary deliberately returns
503; successful configuration validation or an MCP handshake does not establish
dependency health. Concrete adapters should implement core `HealthProbe` with a
bounded, current snapshot and be wired into server `Diagnostics`; the HTTP
handler observes each probe on every request, without performing upstream I/O.
Adapters must expire stale observations to `unknown`.

An orchestrator can use this JSON and the HTTP status as an overall dependency
health gate. For example, in an HTTP deployment bound to `0.0.0.0:3000`, a
Kubernetes HTTP probe can request `/health` on port 3000 with an explicit
`Host: 127.0.0.1:3000` header. Keep this gate disabled for the current bootstrap
if the empty MCP catalog is intentionally being deployed. This endpoint is
**not liveness**: an upstream outage must not cause restart loops. Separate
liveness, local-dependency readiness policy and a container HEALTHCHECK remain
#108; this ticket establishes the diagnostic contract they can reuse.

## Access policy

`/health` uses the same listener as MCP. No credential or session is required.
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

Tests cover the healthy/degraded state matrix, runtime probe changes, HTTP
status/access policy, bootstrap unavailable states, release identity and
secret/signed-URL omission in one-shot output.
