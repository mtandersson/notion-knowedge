# Current server container

This image packages the real `notion-knowledge-server` composition root and its
current MCP transports. The tool catalog is empty. Notion requests, retrieval,
model loading, and index processing are not implemented yet. Reserved data
locations below are not consumed by this bootstrap. Parent issue #104 remains
open until follow-on #145 integrates and verifies those adapters in the image.
There is no separate indexing worker executable today.

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
and initializes/discovers tools over both stdio and HTTP in the final image.
It requires access to the local Docker daemon and registry/Cargo downloads;
it uses no upstream credentials. A small permission probe is compiled in the
pinned builder and mounted read-only into a disposable test container; it never
enters the shipped image. CI runs the same script in the Container smoke
job. Its HTTP ports are dynamically allocated and published on loopback only.

The Dockerfile pins the Rust 1.98.1 Bookworm builder and Debian 12 distroless
C++ runtime by immutable multi-platform image index digests. The build uses
`cargo build --release --locked -p notion-knowledge-server` and the committed
Cargo.lock. This makes base layers, toolchain and dependency versions repeatable;
it does not promise byte-identical OCI archives across Docker versions or
platforms. Linux amd64 is verified in CI; other architectures require their
own final-image smoke before being supported.

To update, select compatible builder/runtime releases, resolve their immutable
index digests with `docker buildx imagetools inspect IMAGE:TAG`, change both
Dockerfile references, and rerun the smoke script. Check the printed `ldd` output
from the builder: the current binary needs the ELF loader, libc, libm and
libgcc_s supplied by the distroless C++ runtime. If future adapters add dynamic
libraries, add them to the runtime deliberately and verify the final executable.
The Nix flake remains the development/ordinary Rust CI toolchain; the pinned
container builder defines the image toolchain independently.

OCI source is fixed to the repository URL. VERSION must mirror the root Cargo
workspace package version; missing or mismatched values fail the build. REVISION
defaults to `unknown`; release builds must supply the Git commit. The smoke
script derives VERSION directly from Cargo.toml. See [health diagnostics](diagnostics.md)
for the running HTTP dependency report; `--check` validates configuration only.
The smoke script derives these values from Cargo.toml and HEAD. Do not pass
credentials through build arguments. `.dockerignore` allows only Cargo manifests,
Cargo.lock and Rust source files under crates into the context. Environment
files, keys, model weights, derived indexes, Git metadata and build outputs are
excluded. When adding other required source assets, review the allowlist rather
than broadening it to local runtime data.

## CI layer cache

CI uses pinned Buildx/build-push actions to import GitHub Actions layers and
load both the final image and its pinned builder. The final-image smoke runs as:

```sh
python3 scripts/test-container-smoke.py
python3 scripts/smoke-container.py --prebuilt --builder-image notion-knowledge:smoke-builder
```

`--prebuilt` skips only the successful final-image build; missing/mismatched
VERSION builds still have to fail. All label, binary identity, linkage, volume,
configuration, diagnostics, MCP/session/health and shutdown checks remain.
`--builder-image` uses the loaded builder for the disposable permission probe.
The default command still builds both images for local development.

The `nk-container-v1-Linux-X64-main` scope is warmed by successful main runs.
Each same-repository PR uses a single numbered scope and can restore main plus
its own scope. GitHub additionally restricts PR exports to that PR's merge ref;
main cannot restore them. Fork PRs only import and never export. Manual branch
runs use their branch scope. Export happens only after smoke succeeds, with
`mode=max` retaining intermediate compilation layers and a ten-minute timeout.
One mutable scope per active PR replaces its previous snapshot; GitHub's
repository cache quota/LRU eviction and seven-day unused-cache eviction bound
storage.

Docker's content-addressed build keys invalidate compilation for changed Rust,
Cargo manifests/lockfile, VERSION, or the pinned builder/Dockerfile. REVISION is
applied in the final stage, allowing unchanged compilation to survive a new
commit while labels are checked against the checkout. The `.dockerignore`
allowlist excludes credentials and runtime/model/index data from every exported
layer. No ordinary Nix CI binary is copied into the container.

Cache benefits require hosted cold/warm logs, not configuration alone. Record
restore/export size and duration, compilation cache hits, and final-image smoke
results in the delivery evidence before closing #167.

## Run

Stdio is the default entrypoint. Preserve stdin with `-i` and omit `-t` so MCP
stdout remains newline-delimited JSON without terminal framing:

```sh
docker run --rm -i --read-only --network=none notion-knowledge:local
```

For an MCP client's stdio configuration, use `docker` as the command and
`["run", "--rm", "-i", "--read-only", "--network=none", "notion-knowledge:local"]`
as its arguments. The bootstrap requires no Notion token. `--network=none`
is appropriate for this current runtime; future Notion adapters will need
outbound connectivity.

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
Configuration remains environment-based; see the README for variable validation
and redaction. Runtime environment variables (or a local `--env-file`) stay
outside image layers.

Host validation permits loopback names and the configured bind IP; arbitrary
public hostname routing is not configured. Browser Origin permits the configured
HTTP authority and `http://localhost:3000` at the listener port. With bind
`0.0.0.0`, browser clients should use `http://localhost:3000` as their Origin;
`http://127.0.0.1:3000` is not an allowed Origin in this configuration. Clients
without Origin are accepted. If mapping a different host port, allowed Origins
still use the *container listener port*. MCP authentication and public-host
HTTPS proxy configuration are not implemented. Keep publishing on loopback
unless a separate deployment provides the required access controls.

## Reserved external data locations

The runtime uses numeric UID/GID **65532:65532**. The image contains empty,
writable directories owned by that user:

| Container path | Reserved purpose |
| --- | --- |
| `/var/lib/notion-knowledge/index` | Rebuildable retrieval indexes |
| `/var/lib/notion-knowledge/state` | Derived synchronization state |
| `/var/lib/notion-knowledge/models` | Local model files/cache |

These are future integration locations, not active bootstrap settings. No models
or indexes are baked into the image. A fresh Docker named volume mounted at one
of these paths inherits the directory ownership. For bind mounts, create the
host directories and grant UID/GID 65532 appropriate access before mounting;
Docker does not fix bind-mount ownership. For example, on a Linux host:

```sh
mkdir -p runtime/index runtime/state runtime/models
sudo chown 65532:65532 runtime/index runtime/state runtime/models
# Add these flags to the HTTP invocation when testing future adapter integration:
# --mount type=bind,src="$PWD/runtime/index",dst=/var/lib/notion-knowledge/index
# --mount type=bind,src="$PWD/runtime/state",dst=/var/lib/notion-knowledge/state
# --mount type=bind,src="$PWD/runtime/models",dst=/var/lib/notion-knowledge/models
```

`--read-only` protects the image filesystem; explicit writable mounts remain
writable. The current bootstrap runs without any mounts and writes no data.
