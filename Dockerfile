# Pinned multi-platform image indexes; update both pins deliberately (docs/container.md).
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder
ARG VERSION
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# OCI metadata may mirror Cargo's version, but may never override it.
RUN test "$VERSION" = "$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' Cargo.toml)" \
    && cargo build --release --locked -p notion-knowledge-server \
    && ldd target/release/notion-knowledge-server \
    && mkdir -p /runtime-data/index /runtime-data/state /runtime-data/models \
    && chown -R 65532:65532 /runtime-data

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
ENTRYPOINT ["/usr/local/bin/notion-knowledge-server"]

# CI selection benchmark: mixed
