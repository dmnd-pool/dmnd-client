# Keep this Rust version equal to rust-toolchain.toml
FROM rust:1.88-slim-trixie AS builder

WORKDIR /app

# Install build deps: openssl-sys needs pkg-config and libssl-dev, jemalloc-sys needs make.
RUN apt-get update \
  && apt-get install -y --no-install-recommends pkg-config libssl-dev make ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY . .
# One set of caches for each architecture: a multi-platform build runs the platforms at the same
# time, and they must not write to the same cargo folders.
ARG TARGETARCH
RUN --mount=type=cache,id=cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
  --mount=type=cache,id=cargo-git-${TARGETARCH},target=/usr/local/cargo/git \
  --mount=type=cache,id=cargo-target-${TARGETARCH},target=/app/target \
  if [ "$TARGETARCH" = "arm64" ]; then export JEMALLOC_SYS_WITH_LG_PAGE=16; fi \
  && cargo build --locked --release --bin dmnd-client \
  && cp target/release/dmnd-client /usr/local/bin/dmnd-client

# Runtime stage
FROM debian:trixie-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends libssl3t64 ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --create-home --home-dir /home/dmnd --uid 1000 --user-group dmnd

COPY --from=builder /usr/local/bin/dmnd-client /usr/local/bin/dmnd-client

# HEADFUL: a container has no browser to open. Open the dashboard on the computer instead.
# AUTO_UPDATE: a self-replacing binary is lost when the container is recreated: update by pulling
# a new image tag instead.
ENV HEADFUL=false \
  AUTO_UPDATE=false

USER dmnd
# config.toml and logs/ are read and written relative to this directory.
WORKDIR /home/dmnd

# 32767: Downstream listening address.
# 3001: dashboard and API.
EXPOSE 32767 3001

ENTRYPOINT ["dmnd-client"]
