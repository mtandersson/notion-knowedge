# Local Docker Compose stack (#105)

Use the existing production-image build in a local-only Compose service. This
currently starts **one HTTP MCP server**, not an imaginary indexing worker or
Notion service. The current bootstrap does not have a configured retrieval
adapter; it will report an unavailable index until #145 integrates the worker
and adapters into the image.

## Requirements and start

Install Docker Engine/Desktop with Docker Compose V2. From the repository root:

```sh
./scripts/compose-dev.sh up -d --build
./scripts/compose-dev.sh ps
```

The script reads the canonical Cargo workspace version from `Cargo.toml` and
passes it as the required Docker `VERSION` build arg. Do not hard-code a version
in the Compose file: the Dockerfile intentionally fails if it does not match
Cargo. The first build downloads pinned Rust/base layers and dependencies.
Run `./scripts/compose-dev.sh up -d --build` after source changes.

MCP: `http://127.0.0.1:3000/mcp`
Liveness: `http://127.0.0.1:3000/livez`

The host port may be changed with `NK_COMPOSE_PORT` (for example,
`NK_COMPOSE_PORT=3100 ./scripts/compose-dev.sh up -d --build`). The
**container** listener remains port 3000. Publishing is deliberately limited to
host loopback. Do not expose it on public interfaces: the current server is not
a production-authenticated public MCP endpoint.

## Configuration and secrets

No credentials are needed for the default bootstrap. To use the optional
upstream integration auth, create an untracked `.env` file at the repository
root, with restrictive permissions (e.g. `chmod 600 .env`):

```dotenv
NK_NOTION_AUTH=integration
NOTION_TOKEN=replace-with-your-real-token
```

Only `NK_NOTION_AUTH` and `NOTION_TOKEN` are forwarded to the container from
Compose variables. `.env` is gitignored, and is never copied by the image's
allowlisted `.dockerignore`. Environment variables can also be supplied in
the calling shell; they take precedence over `.env`. **Do not** commit secrets,
put them in image build args, publish `docker compose config` output when
credentials are present, or use `--env-file` with a tracked secret file.

`NK_NOTION_AUTH=integration` enables the configured Notion backend credential;
it is **not** MCP client authentication. This local Compose stack does not
configure the staged OAuth callback, grant state or remote access. Set up those
features separately when the full auth flow has landed.

## Persistence and health

Three Docker **named volumes** survive an ordinary `down` command:

| Volume | Container path | Intended use |
| --- | --- | --- |
| `index` | `/var/lib/notion-knowledge/index` | Rebuildable LanceDB index |
| `state` | `/var/lib/notion-knowledge/state` | SQLite synchronization state |
| `models` | `/var/lib/notion-knowledge/models` | Model/cache data |

The container runs read-only as non-root UID/GID 65532, with only the three
volumes writable. Fresh named volumes inherit the pre-created image directory
ownership. There are no sidecar databases or mandatory external services in
the current executable.

Compose's healthcheck uses the image's `--healthcheck` liveness probe,
which checks `/livez` for the running HTTP process. It intentionally **does
not** require `/readyz` or `/health` to return 200: both can return 503
while the real indexing and upstream dependencies are not configured. Use
`/readyz` for traffic-readiness checks once adapters are integrated.

## Validate and shut down

```sh
./scripts/compose-dev.sh config --quiet
./scripts/compose-dev.sh logs -f notion-knowledge
python3 scripts/smoke-compose.py
./scripts/compose-dev.sh down
```

`smoke-compose.py` builds and boots an **isolated disposable Compose project**
on a local ephemeral port, verifies HTTP liveness and the documented readiness/diagnostic
semantics, and tears down **only its own**
project and volumes even if a check fails. It requires Docker/Compose and
network access to build; it never reads a real Notion token. The ordinary
Compose project's data is untouched.

`down` preserves local volumes. **`down --volumes` deletes the index,
state and model data for that project** and should only be used deliberately.
