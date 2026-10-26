# Pinned multi-platform image indexes; update both pins deliberately (docs/container.md).
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS toolchain

# Fail missing/mismatched OCI versions *before* installing cargo-chef or
# compiling dependencies. The real negative-version smoke builds use --target
# builder and must not trigger a cold 30-minute Rust build just to fail.
FROM toolchain AS version-check
ARG VERSION
WORKDIR /build
COPY Cargo.toml ./
RUN test "$VERSION" = "$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' Cargo.toml)"

# Keep the same pinned Rust toolchain in chef, planner, and builder. The
# build-only protobuf compiler is required for Lance; it never reaches runtime.
FROM version-check AS chef
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler=3.21.12-3+deb12u1 libprotobuf-dev=3.21.12-3+deb12u1 \
    && rm -rf /var/lib/apt/lists/*
# Pin the tool version and its resolved dependency graph, not a mutable chef image.
RUN cargo install cargo-chef --version 0.1.78 --locked
WORKDIR /build

# Source edits rerun prepare, but unchanged Cargo graph emits the same recipe.
# The vendor tree is a required local security-patched dependencies.
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG VERSION
COPY --from=planner /build/recipe.json recipe.json
# The path-patched Candle dependency must exist during the locked cook phase.
COPY vendor ./vendor
RUN cargo chef cook --release --locked --package notion-knowledge-server \
    --features webhook-worker --recipe-path recipe.json
# Only after dependency compilation do we copy the real application sources.
# This preserves its compiled third-party crates across normal Rust source edits.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Restore the complete real vendor sources if the chef skeleton touched them.
COPY vendor ./vendor
# OCI metadata may mirror Cargo's version, but may never override it.
RUN test "$VERSION" = "$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' Cargo.toml)" \
    && cargo build --release --locked -p notion-knowledge-server --features webhook-worker \
    && ldd target/release/notion-knowledge-server \
    && mkdir -p /runtime-data/index /runtime-data/state /runtime-data/models \
    && chmod 0700 /runtime-data/index /runtime-data/state \
    && chown -R 65532:65532 /runtime-data

# Disposable mount-probe compiler: same pinned toolchain, without Cargo outputs.
FROM toolchain AS probe-builder

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
ARG VERSION
ARG REVISION=unknown
LABEL org.opencontainers.image.title="notion-knowledge-server" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.source="https://github.com/mtandersson/notion-knowedge" \
      org.opencontainers.image.revision=$REVISION
COPY --from=builder /build/target/release/notion-knowledge-server /usr/local/bin/notion-knowledge-server
COPY --from=builder --chown=65532:65532 /runtime-data /var/lib/notion-knowledge
USER 65532:65532
WORKDIR /var/lib/notion-knowledge
EXPOSE 3000
STOPSIGNAL SIGINT
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/notion-knowledge-server", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/notion-knowledge-server"]
