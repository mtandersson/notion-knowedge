# Production server and indexing container

This image packages the real `notion-knowledge-server` composition root, MCP
transports and the `webhook-worker` feature: local Qwen, LanceDB and SQLite. The
authoritative indexing worker runs in process under HTTP supervision. Defaults
advertise the read-only tools
`knowledge_search`, `knowledge_get` and `knowledge_sync_status`. Retrieval
and authoritative scope are unconfigured: search/get report unavailable, and
status reports null local-state fields. Explicit configured roles below load
external assets and operate real persistent state. Building the feature does not automatically download weights, grant
content access or initialize an OAuth reader index. There is no separate
indexing worker executable.

## Build and update

Docker with BuildKit and Python 3.9+ are sufficient; the host needs no Rust
installation. Build from the repository root:

```sh
version=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' Cargo.toml)
docker build --build-arg VERSION="$version" \
  --build-arg REVISION="$(git rev-parse HEAD)" \
  -t notion-knowledge:local .
python3 scripts/smoke-container.py
```

The smoke script builds `notion-knowledge:smoke` (override with `--image TAG`),
checks OCI labels, actual executable linkage, and non-root writes to fresh
external volumes, validates startup/configuration errors,
and initializes/discovers tools over both stdio and HTTP in the final image. It also verifies the image healthcheck command plus liveness, readiness, and dependency-diagnostic semantics.
It requires access to the local Docker daemon and registry/Cargo downloads;
it uses no upstream credentials. A small permission probe is compiled in the
pinned builder and mounted read-only into a disposable test container; it never
enters the shipped image. CI runs the same script in the Container smoke
job. Its HTTP ports are dynamically allocated and published on loopback only.

The Dockerfile pins the Rust 1.98.1 Bookworm builder and Debian 12 distroless
C++ runtime by immutable multi-platform image index digests. The build uses
`cargo build --release --locked -p notion-knowledge-server --features webhook-worker` and the committed
Cargo.lock. This makes base layers, toolchain and dependency versions repeatable;
it does not promise byte-identical OCI archives across Docker versions or
platforms. Linux amd64 is verified in CI; other architectures require their
own final-image smoke before being supported. Protobuf compiler and well-known
schemas (`protobuf-compiler` and `libprotobuf-dev`, pinned Bookworm
`3.21.12-3+deb12u1`) are build-only dependencies; neither enters the runtime.

To update, select compatible builder/runtime releases, resolve their immutable
index digests with `docker buildx imagetools inspect IMAGE:TAG`, change both
Dockerfile references, and rerun the smoke script. Check the printed `ldd` output
from the builder and exercise the actual model/index pipeline in the final
distroless C++ runtime. If future adapters add dynamic
libraries, add them to the runtime deliberately and verify the final executable.
The Nix flake remains the development/ordinary Rust CI toolchain; the pinned
container builder defines the image toolchain independently.

OCI source is fixed to the repository URL. VERSION must mirror the root Cargo
workspace package version; missing or mismatched values fail the build. REVISION
defaults to `unknown`; release builds must supply the Git commit. The smoke
script derives VERSION directly from Cargo.toml. See [health diagnostics](diagnostics.md)
for the running HTTP dependency report; `--check` validates configuration only.
The smoke script derives these values from Cargo.toml and HEAD. Do not pass
credentials through build arguments. `.dockerignore` allows only Cargo manifests/lockfile, Rust and SQL source,
and required vendored source/license metadata into the context. Its real Docker
context probe rejects nested model assets, indexes, state, environment and key
files, including beneath an otherwise required crate/vendor directory. Environment
files, keys, model weights, derived indexes, Git metadata and build outputs are
excluded. When adding other required source assets, review the allowlist rather
than broadening it to local runtime data.

## Docker Rust dependency layers

The Dockerfile uses pinned `cargo-chef 0.1.78` atop the same immutable
Rust 1.98.1 toolchain. A planner sees the full allowlisted workspace, lockfile,
and local Candle patch, and computes `recipe.json`. The builder then cooks
the **locked release dependency graph for the exact server/webhook-worker
feature set** before receiving the real application sources. Normal Rust
source edits rerun the inexpensive recipe computation and final application
compile without forcing a complete third-party recompile when the cook layer
is available. A distinct `version-check` stage rejects a missing/mismatched
VERSION **before installing cargo-chef or cooking dependencies**, so the two
mandatory negative-version Docker builds in `smoke-container.py` do not spend
the remaining CI runtime compiling a fresh dependency graph. This corrects
the 45-minute timeout observed in [PR #488 run 39848108430](https://github.com/mtandersson/notion-knowedge/actions/runs/39848108430).
The final real release build retains the original VERSION check as defense in depth. The vendored security patch is copied **before** the cook and
again before the real build; changing that patch invalidates cached dependencies.

The final executable is still built from the actual sources with
`cargo build --release --locked -p notion-knowledge-server --features webhook-worker`.
`ldd`, VERSION checks, image identity, non-root runtime, final-image indexing,
transport/permission/health, and Compose smoke remain mandatory. The chef tool,
protobuf compiler, and Cargo build output never enter the final distroless image;
the disposable mount probe still uses the unchanged pinned Rust base.
Changes to manifests, Cargo.lock, vendor code, cook flags, or toolchain can
invalidate dependencies deliberately. A new Rust tool/toolchain stage requires
an initial cold build and fresh cache exports, and GitHub cache eviction or
cache-service errors can still cause another cold build.

**Performance evidence requirement:** Compare distinct hosted runs with a
real source edit and a second source edit that keeps manifests/lock/vendor
unchanged. Capture `cargo chef cook` `CACHED` versus actual `Compiling`
events, final binary build time, export time and size, and complete smoke/gate
outcomes. A recipe's stability alone is not proof of a cache hit or speedup.
See [#487](https://github.com/mtandersson/notion-knowedge/issues/487)
for the new benchmark and prior [#185 cache-root investigation](docker-cache-causal-evidence.md).

## CI layer cache

CI uses pinned Buildx/build-push actions to import GitHub Actions layers and
load both the final image and its pinned probe builder. The probe stage uses
the same Rust base as compilation but excludes Cargo downloads/build outputs.
The final-image smoke runs as:

```sh
python3 scripts/test-container-smoke.py
python3 scripts/smoke-container.py --prebuilt --builder-image notion-knowledge:smoke-builder
```

`--prebuilt` skips only the successful final-image build; missing/mismatched
VERSION builds still have to fail. All label, binary identity, linkage, volume,
configuration, diagnostics, MCP/session/health and shutdown checks remain.
`--builder-image` uses the loaded builder for the disposable permission probe.
The default command still builds both images for local development.

Successful main runs warm `nk-container-v1-Linux-X64-main` and its `-builder`
companion. Each same-repository PR has the equivalent numbered pair. For each
builder export, the read-only local action selects the available own builder
index, otherwise the trusted main builder index, otherwise no remote import.
The final-image solve selects own final, own builder, trusted main final,
trusted main builder, then no remote import. A builder index can retain the
compiler result even when its final-image index is evicted. It never configures
overlapping logical cache sources in one solve. Lookup errors and eviction can cause a
local rebuild; they never skip production checks. Forks can restore authorized
main caches but never export. GitHub isolates PR writes to their merge ref.

After all final-image smoke succeeds, CI exports the builder target to its own
`-builder` scope and the final target to its own ordinary scope, both `mode=max`
with ten-minute timeouts. Two roots preserve expensive compilation and runtime
COPY results through lazy warm solves. BuildKit's exact engine digest is pinned;
the index lookup protocol is coupled to that version and must be reviewed when
upgrading. Index presence is an availability snapshot, not proof that every blob
remains available or that a changed input is compatible. Versioned indices and
shared content-addressed blobs remain subject to repository quota/LRU eviction
and seven-day unused-cache eviction; there are two logical roots per active PR.

Docker's content-addressed build keys invalidate compilation for changed Rust,
Cargo manifests/lockfile, VERSION, or the pinned builder/Dockerfile. REVISION is
applied in the final stage, allowing unchanged compilation to survive a new
commit while labels are checked against the checkout. The `.dockerignore`
allowlist excludes credentials and runtime/model/index data from every exported
layer. No ordinary Nix CI binary is copied into the container.

Cache benefits require hosted cold/warm logs, not configuration alone. Record
restore/export size and duration, compilation cache hits, and final-image smoke
results in the [hosted delivery evidence](docker-cache-evidence.md) before closing #167.

## Run

Stdio is the default entrypoint. Preserve stdin with `-i` and omit `-t` so MCP
stdout remains newline-delimited JSON without terminal framing:

```sh
docker run --rm -i --read-only --network=none notion-knowledge:local
```

For an MCP client's stdio configuration, use `docker` as the command and
`["run", "--rm", "-i", "--read-only", "--network=none", "notion-knowledge:local"]`
as its arguments. The bootstrap requires no Notion token. `--network=none`
is appropriate for unconfigured diagnostics. Configured authoritative reads,
indexing and OAuth need controlled Notion connectivity; model loading/inference
itself remains offline.

One-shot configuration validation:

```sh
docker run --rm --read-only notion-knowledge:local --check
```

HTTP requires binding to the container interface, since the server's default
127.0.0.1 address is internal to the container:

```sh
docker run --rm --read-only --name notion-knowledge \
  -e NK_HTTP_HOST=0.0.0.0 -p 127.0.0.1:3000:3000 \
  notion-knowledge:local --http
```

Connect to `http://127.0.0.1:3000/mcp`. The Dockerfile declares SIGINT for
`docker stop`, matching the server's graceful HTTP shutdown handler.

The image also declares a Docker `HEALTHCHECK` that executes
`notion-knowledge-server --healthcheck`. For an HTTP PID 1 it probes the
process-only `/livez` endpoint over loopback, so an unavailable Notion API or
local index does not mark the container dead. A stdio PID 1 has no HTTP listener;
there the same command treats the running PID 1 as live and does not invent a
readiness requirement. Use `/readyz` separately in an orchestrator to gate HTTP
traffic on the required local index. `/health` remains the full dependency
diagnostic and is intentionally not the restart probe.

Configuration remains environment-based; see the README for variable validation
and redaction. Runtime environment variables (or a local `--env-file`) stay
outside image layers.

Host validation permits loopback names and the configured bind IP; arbitrary
public hostname routing is not configured. Browser Origin permits the configured
HTTP authority and `http://localhost:3000` at the listener port. With bind
`0.0.0.0`, browser clients should use `http://localhost:3000` as their Origin;
`http://127.0.0.1:3000` is not an allowed Origin in this configuration. Clients
without Origin are accepted. If mapping a different host port, allowed Origins
still use the *container listener port*. The stock Compose example is a loopback bootstrap. The integration indexer override and production
[OAuth and public-host HTTPS setup](oauth-production-setup.md) require separate
explicit roles below; the model assets remain externally provisioned. Keep the backend private behind the reviewed proxy.

## Actual external data locations

The runtime uses numeric UID/GID **65532:65532**. The image contains empty,
writable directories owned by that user:

| Container path | Consumed configuration |
| --- | --- |
| `/var/lib/notion-knowledge/index` | `NK_WEBHOOK_INDEX_DIR`; bound chunks, vectors, FTS and ANN |
| `/var/lib/notion-knowledge/state/state.sqlite` | `NK_WEBHOOK_STATE_FILE`; operational state/checkpoints and durable ledgers |
| `/var/lib/notion-knowledge/models` | `NK_WEBHOOK_QWEN_ASSETS`; exact public weights/config/tokenizer, mounted read-only |

Index/state directories are mode 0700 and newly created SQLite is 0600.
Model assets must be readable by UID 65532. No models
or indexes are baked into the image. A fresh Docker named volume mounted at one
of these paths inherits the directory ownership. For bind mounts, create the
host directories and grant UID/GID 65532 appropriate access before mounting;
Docker does not fix bind-mount ownership. For example, on a Linux host:

```sh
mkdir -p runtime/index runtime/state runtime/models
sudo chown 65532:65532 runtime/index runtime/state runtime/models
sudo chmod 700 runtime/index runtime/state
# Add these flags to the explicitly configured runtime invocation:
# --mount type=bind,src="$PWD/runtime/index",dst=/var/lib/notion-knowledge/index
# --mount type=bind,src="$PWD/runtime/state",dst=/var/lib/notion-knowledge/state
# --mount type=bind,src="$PWD/runtime/models",dst=/var/lib/notion-knowledge/models,readonly
```

`--read-only` protects the image filesystem; explicit writable mounts remain
writable. The current bootstrap runs without any mounts and writes no data.

## Production model and indexing acceptance

After the ordinary image smoke, run the real production pipeline smoke with
public assets provisioned outside the checkout:

```sh
python3 scripts/smoke-retrieval-container.py --prebuilt \
  --image notion-knowledge:smoke --assets /absolute/public-qwen-assets
```

`--download-assets` explicitly fetches the public pinned revision and verifies
all three checksums against the production provider source. CI uses it with
`--assets "$RUNNER_TEMP/nk-qwen-assets"`. Weights stay outside BuildKit and its
exported caches; no model, index data or credentials enter image layers.
The test also requires OpenSSL. Its pinned disposable Python image supplies only
the local HTTPS source fixture and read-only volume probes, never a fake worker.

The smoke resolves the unchanged `https://api.notion.com` endpoint only inside
an internal Docker network to a disposable TLS fixture with an ephemeral CA.
No ports are published, no real provider credentials are supplied and no
production endpoint override or fake embedding provider is installed. It runs
signed webhook admission, the actual supervised worker, real offline Qwen
inference and guarded Lance/SQLite commits. It then invokes the shipping
`--index ensure` command and source-filtered MCP semantic/lexical/hybrid search
and fresh source get. It checks real native loading, missing/tampered assets,
private storage ownership, read-only retrieval bytes and reader replacement.
Its destructive rebuild operates only on isolated, quiescent fixture volumes
with no accepted writes; the independent authoritative source stays intact.
It never touches an existing Compose project, OAuth grant or production state.

Lance requires writable scratch even when its durable index is external.
Configured runtime invocations therefore need a private, bounded disposable
`/tmp`, while the image root remains read-only. Compose and the fixture use:

```sh
--tmpfs /tmp:rw,nosuid,nodev,noexec,size=268435456,mode=0700,uid=65532,gid=65532
```

256 MiB is the example/fixture scratch cap, not a general corpus capacity or
production memory guarantee. A spill exceeding it fails instead of filling the
image filesystem. Size scratch and process/container resource limits for the
actual workload and acceptance policy. Scratch is neither authoritative data
nor persistent operation state.

## Supported roles and initial indexing

The Compose workflow below is private/local configuration, not a production
resource sizing policy. Apply the separately reviewed [production resource
envelope](production-resource-limits.md) and actual-host acceptance before public
deployment; a small fixture does not prove workload capacity.

The same feature-built executable supports distinct configurations:

1. **Private integration indexer:** `--http`, verified webhook configuration,
   trusted scope and offline assets. It supervises the worker in process. Its
   anonymous MCP endpoint discloses no private indexed content. Publish only the
   reviewed webhook route through the HTTPS boundary.
2. **Trusted local reader/operator:** stdio with `NK_RETRIEVAL=true`, matching
   integration scope/assets and compatible existing state. Local `--reconcile`
   and `--index` commands use the production authority as one-shot operators.
3. **Public OAuth reader:** `--http`, complete [OAuth setup](oauth-production-setup.md),
   matching scope and `NK_RETRIEVAL=true`. Content uses the approved request-bound
   grant. This OAuth process does not accept the indexer's integration credential
   or run an integration webhook/reconciliation writer. Keep grant key/state
   private, persistent and separate from disposable retrieval storage; never share
   the grant runtime across replicas. This topology does not claim installed
   OAuth mutation/file ingestion (#441/#446).

For an actual private indexer, put configuration in a local ignored `.env`,
never image build arguments. Supply `NOTION_TOKEN`, the exact
`NK_NOTION_SCOPE_WORKSPACE_ID` and comma-separated `NK_NOTION_SCOPE_ROOTS`,
verified `NK_WEBHOOK_INTEGRATION_ID`, `NK_WEBHOOK_SUBSCRIPTION_ID`,
`NK_WEBHOOK_VERIFICATION_TOKEN`, and an absolute `NK_MODEL_ASSETS` directory
provisioned from [local Qwen](local-qwen.md). Scope exclusions/generation must
match every reader/operator sharing the derived state. Then:

```sh
scripts/compose-dev.sh -f compose.indexer.yaml config --quiet
scripts/compose-dev.sh -f compose.indexer.yaml up -d --build
```

This override consumes the named index/state volumes and a read-only model bind;
it refuses to create a missing host asset directory. After startup creates an
empty compatible table, populate it through signed authoritative hints or the
existing [manual reconciliation](manual-reconciliation.md) command. Stop
competing writers before an initial operator population, then run:

```sh
scripts/compose-dev.sh -f compose.indexer.yaml stop
scripts/compose-dev.sh -f compose.indexer.yaml run --rm -T --no-deps \
  -e NK_WEBHOOK_WORKER=false -e NK_WEBHOOK_MODE=disabled \
  notion-knowledge --reconcile apply initial-population-1
scripts/compose-dev.sh -f compose.indexer.yaml run --rm -T --no-deps \
  -e NK_WEBHOOK_WORKER=false -e NK_WEBHOOK_MODE=disabled \
  notion-knowledge --index ensure
scripts/compose-dev.sh -f compose.indexer.yaml up -d --no-build
```

Use a new reconciliation run ID for a new inventory; resume an interrupted run
rather than discarding receipts. `--index ensure` trains/optimizes the production
Cosine IVF_FLAT index after real vectors exist. Configure the separate reader
over the same compatible paths; read activation never populates or repairs
state. Trusted stdio uses `docker run -i`, the external mounts/scope above,
`NK_RETRIEVAL=true`, private scratch and controlled Notion connectivity, with no
HTTP argument and no enabled webhook worker. Verify [source-filtered search/get](installed-retrieval.md).
Local static bearer mode on a non-loopback container listener cannot substitute
for production OAuth.

## Updates and safe recovery

Stop writers and back up operational SQLite, private OAuth grant/key state and
configuration before image replacement. Build with canonical VERSION and the new
commit REVISION, run both final-image smokes and inspect model/vector identity
compatibility. Recreate the container while retaining correctly owned mounts.
Existing compatible state is reopened; missing/changed assets or foreign bindings
fail rather than downloading, falling back or attaching a new identity to old
vectors. Existing unsafe permissions are not silently repaired; inspect their
provenance, stop users and explicitly provision the documented owner/modes.

For a compatible **ANN-only** rebuild, keep all canonical chunks and the entire
operational database, stop competing writers and run `--index rebuild`. This
replaces derived ANN state through the guarded coordinator/lease and preserves
receipts, checkpoints and durable operation ledgers. It does not re-embed chunks
under a different model. Do not delete operational SQLite to change model
identity; use a separately supported, audited re-embedding/recovery procedure
before that migration. A same-dimensional revision is still an identity change.

Production state can include accepted-write idempotency/audit records and pending
durable mutation-refresh work, and configured OAuth grants have their own private
persistent paths. These are not disposable retrieval caches. Preserve them and
resolve/drain pending work during recovery. `docker compose down -v` is not an
update or general rebuild procedure. The automated fixture discards only its
isolated, completed derived index/checkpoint volumes with no accepted writes;
its independent public source is indexed again from scratch.
