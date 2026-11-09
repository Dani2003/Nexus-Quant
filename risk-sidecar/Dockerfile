# Multi-stage build for Rust Risk Sidecar
FROM rust:1.80-slim as builder

WORKDIR /usr/src/risk-sidecar

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copy proto and sidecar sources
COPY proto /usr/src/proto
COPY risk-sidecar /usr/src/risk-sidecar

WORKDIR /usr/src/risk-sidecar
RUN cargo build --release

# Minimal runtime image
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /usr/src/risk-sidecar/target/release/risk-sidecar /app/risk-sidecar

EXPOSE 50051
ENTRYPOINT ["/app/risk-sidecar"]

