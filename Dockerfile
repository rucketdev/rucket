# syntax=docker/dockerfile:1

# Build stage
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build release binary with musl for static linking
RUN cargo build --release --target x86_64-unknown-linux-musl --bin rucket

# Runtime stage - distroless for minimal attack surface
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.source="https://github.com/rucketdev/rucket"
LABEL org.opencontainers.image.description="S3-compatible object storage server"
LABEL org.opencontainers.image.licenses="AGPL-3.0-only"

# Copy the static binary
COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/rucket /rucket

# Default data directory
VOLUME ["/data"]

# S3 API port
EXPOSE 9000

ENTRYPOINT ["/rucket"]
CMD ["serve", "--bind", "0.0.0.0:9000", "--data-dir", "/data"]
