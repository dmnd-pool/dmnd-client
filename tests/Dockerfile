FROM rust:1.88.0-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    capnproto libcapnp-dev libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .

ENV CARGO_PROFILE_DEV_DEBUG=0
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target,sharing=locked \
    rm -f target/debug/deps/mining_e2e-* \
    && cargo test --locked --features mining-e2e --test mining_e2e --no-run -j 4 \
    && mkdir /out \
    && for binary in target/debug/deps/mining_e2e-*; do \
         if [ -x "$binary" ] && [ -f "$binary" ]; then cp "$binary" /out/mining-e2e; fi; \
       done \
    && test -x /out/mining-e2e \
    && cp target/debug/dmnd-client /out/dmnd-client \
    && strip /out/mining-e2e /out/dmnd-client

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libcurl4 libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY tests/bin/linux-amd64/ /opt/mining-tools/
RUN cd /opt/mining-tools \
    && sha256sum --check SHA256SUMS \
    && ./tp-simulator --help >/dev/null \
    && ./minerd --help >/dev/null

WORKDIR /workspace
COPY --from=builder /out/dmnd-client /workspace/target/debug/dmnd-client
COPY --from=builder /out/mining-e2e /usr/local/bin/mining-e2e
ENV TP_SIMULATOR_BIN=/opt/mining-tools/tp-simulator \
    CPUMINER_BIN=/opt/mining-tools/minerd \
    TP_SIMULATOR_VERSION=bundled-0.1.0 \
    MINING_E2E_ARTIFACTS=/artifacts
RUN mkdir /artifacts && chmod 777 /artifacts

ENTRYPOINT ["/usr/local/bin/mining-e2e", "--nocapture", "--test-threads=1"]
