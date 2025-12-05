FROM rust as builder

WORKDIR /app

# Install build deps
RUN apt-get update && apt-get install -y pkg-config libssl-dev ca-certificates

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release || true

# Copy source and build
COPY . .
RUN cargo build --release

# Runtime stage
FROM ubuntu:24.04
WORKDIR /usr/local/bin

RUN apt-get update && apt-get install -y libssl3 ca-certificates

COPY --from=builder /app/target/release/dmnd-client .

ENTRYPOINT ["dmnd-client"]
